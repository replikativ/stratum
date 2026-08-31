(ns stratum.query.postprocess
  "Post-processing pipeline for query results: formatting, distinct, having, order, limit/offset."
  (:require [clojure.string :as str]
            [stratum.query.normalization :as norm]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Result Formatting
;; ============================================================================

(defn apply-linear-recipe
  "Reassemble the user-visible value when the planner rewrote the agg
   from `s·x + o` form into the base agg on `x`. The recipe lives in
   the agg's metadata under `:linear-recipe` (set by
   `plan/linear-agg-rewrite`). Returns nil when the raw value is nil
   (empty matching set or SQL NULL)."
  [raw-value cnt {:keys [scale offset reassemble]}]
  (when (some? raw-value)
    (case reassemble
      :sum     (+ (* (double scale) (double raw-value))
                  (* (double offset) (double cnt)))
      :avg     (+ (* (double scale) (double raw-value)) (double offset))
      :min-max (+ (* (double scale) (double raw-value)) (double offset)))))

(defn rewrite-row-with-recipes
  "Walk a result row and replace each agg-keyed value with its
   reassembled `s·x + o` form when the corresponding agg carries a
   `:linear-recipe` metadata. Used by paths whose decoder doesn't
   already route through `format-fused-result` (scalar agg, …).
   Idempotent on rows whose aggs have no recipe."
  [row aggs]
  (reduce (fn [r agg]
            (if-let [recipe (:linear-recipe (meta agg))]
              (let [k (keyword (or (:as agg) (:op agg)))
                    raw (get r k)
                    cnt (or (:_count r) 0)]
                (assoc r k (apply-linear-recipe raw cnt recipe)))
              r))
          row
          aggs))

(defn apply-recipes-to-results
  "Apply F21 linear recipes to a group-by result. Handles both shapes:
   - vec of row maps (default) → calls `rewrite-row-with-recipes` on each
   - columnar map `{:col arr … :n-rows N :_count arr}` → rewrites
     the agg's column array element-wise, threading per-row counts
     for the SUM offset term

   No-op when no agg carries a recipe (cheap fast path)."
  [results aggs]
  (if (some #(:linear-recipe (meta %)) aggs)
    (cond
      (sequential? results)
      (mapv #(rewrite-row-with-recipes % aggs) results)

      (and (map? results) (:n-rows results))
      (let [n (long (:n-rows results))
            ^longs cnts (let [c (:_count results)]
                          (cond
                            (nil? c) (let [a (long-array n)] (java.util.Arrays/fill a 1) a)
                            :else c))]
        (reduce
         (fn [r agg]
           (if-let [recipe (:linear-recipe (meta agg))]
             (let [k    (keyword (or (:as agg) (:op agg)))
                   src  (get r k)
                   out  (double-array n)
                   {:keys [scale offset reassemble]} recipe
                   s    (double scale)
                   o    (double offset)]
               (cond
                 (nil? src) r

                 (= :sum reassemble)
                 (do (dotimes [i n]
                       (let [raw (cond
                                   (instance? (Class/forName "[D") src) (aget ^doubles src i)
                                   (instance? (Class/forName "[J") src) (double (aget ^longs src i))
                                   :else                                 (double (nth src i)))
                             c   (aget cnts i)]
                         (aset out i (+ (* s raw) (* o (double c))))))
                     (assoc r k out))

                 :else  ; :avg / :min-max — additive offset, no count
                 (do (dotimes [i n]
                       (let [raw (cond
                                   (instance? (Class/forName "[D") src) (aget ^doubles src i)
                                   (instance? (Class/forName "[J") src) (double (aget ^longs src i))
                                   :else                                 (double (nth src i)))]
                         (aset out i (+ (* s raw) o))))
                     (assoc r k out))))
             r))
         results
         aggs))

      :else results)
    results))

(defn format-fused-result
  "Format fused SIMD result into standard output. When the agg carries
   a `:linear-recipe` (set by the F21 rewrite pass) the raw Java
   accumulator is post-processed back into the user's `s·x + o` value."
  [result agg]
  (let [alias  (or (:as agg) (:op agg))
        cnt    (:count result)
        recipe (:linear-recipe (meta agg))
        raw    (case (:op agg)
                 (:count :count-non-null) (long cnt)
                 (:min :max :sum :sum-product) (if (zero? cnt) nil (:result result))
                 :avg (if (zero? cnt) nil (:result result))
                 (:result result))
        value  (if recipe (apply-linear-recipe raw cnt recipe) raw)]
    [{(keyword alias) value
      :_count cnt}]))

;; ============================================================================
;; Aggregates over dictionary-encoded string columns
;; ============================================================================
;;
;; A dict-encoded string column is a `long[]` of codes to every kernel in
;; the engine; nothing below this layer knows the codes denote strings.
;; That splits the aggregate vocabulary three ways:
;;
;;   * COUNT / COUNT(col) / COUNT(DISTINCT col) are exact regardless of
;;     encoding — the dict is a bijection, so cardinalities carry over.
;;     These keep working untouched.
;;
;;   * MIN / MAX are order queries. They return the winning *code*, which
;;     denotes the right string only when the dict is sorted (see
;;     `stratum.column/dict-sorted?`). We map the code back to its string
;;     here; that mapping is the only place aggregate results are decoded,
;;     since group keys are decoded much deeper (`group_by/decode-accs-to-map`
;;     via `group-dicts`) while agg results come out of a bare `double[]`.
;;
;;   * SUM / AVG / VARIANCE / STDDEV / CORR / SUM(a*b) are arithmetic on
;;     codes. There is no string answer to decode to — the number is
;;     meaningless and always has been. They raise.
;;     MEDIAN / PERCENTILE raise for the same reason: both interpolate
;;     between neighbouring values, and the midpoint of two codes is not a
;;     code. (PostgreSQL agrees: `percentile_cont` rejects text; only the
;;     discrete `percentile_disc` accepts it.)
;;
;; Messages go in the exception MESSAGE, not ex-data — the pgwire server
;; drops ex-data when translating to an ErrorResponse.

(def ^:private string-order-agg-ops
  "Aggregates whose result is one of the input values, so it can be
   decoded back to a string."
  #{:min :max})

(def ^:private string-invalid-agg-ops
  "Aggregates that are arithmetic on dict codes — no string meaning."
  #{:sum :avg :stddev :stddev-pop :variance :variance-pop
    :corr :sum-product :median :percentile :approx-quantile})

(defn- dict-string-col?
  [col-info]
  (and col-info (= :string (:dict-type col-info)) (some? (:dict col-info))))

(defn- agg-source-cols
  "Column keywords an agg reads directly. Expression-sourced aggs
   (`:expr`) are excluded: the expression path builds its own result
   column and dict, so the source column's encoding no longer describes
   the values being aggregated."
  [agg]
  (when-not (:expr agg)
    (cond
      (:col agg)  [(:col agg)]
      (:cols agg) (:cols agg)
      :else       nil)))

(defn validate-string-aggs!
  "Reject aggregates that would silently compute arithmetic on dictionary
   codes.

   `aggs` are normalized+aliased agg maps, `columns` the normalized column
   map. MIN/MAX are always answerable — `query.string-order` ranks the
   dictionary at query time, so no dictionary-order precondition is
   imposed here and no column is ever refused for how it was encoded.

   Throws ex-info with a self-contained message; returns nil otherwise."
  [aggs columns]
  (doseq [agg aggs
          col (agg-source-cols agg)
          :let [col-info (get columns col)]
          :when (dict-string-col? col-info)]
    (let [op (:op agg)]
      (when (contains? string-invalid-agg-ops op)
        (throw (ex-info
                (str (str/upper-case (name op)) " is not defined for the string column "
                     (pr-str col)
                     " — it would compute arithmetic on dictionary codes, not on the "
                     "strings themselves. Use COUNT, COUNT(DISTINCT ...), MIN or MAX "
                     "on string columns, or CAST the column to a numeric type.")
                {:op op :col col :dict-type :string}))))))

(defn string-agg-decoders
  "Map of result alias → `String[]` dict, for every MIN/MAX over a
   dict-encoded string column. Empty map when there is nothing to decode,
   which is the overwhelmingly common case and costs one pass over the
   agg list.

   The dict must be the one the codes actually index — callers pass the
   RANKED columns from `query.string-order`, not the raw source columns."
  [aggs columns]
  (persistent!
   (reduce (fn [acc agg]
             (let [col (when (contains? string-order-agg-ops (:op agg))
                         (first (agg-source-cols agg)))
                   col-info (when col (get columns col))]
               (if (and col-info (dict-string-col? col-info))
                 (assoc! acc (keyword (or (:as agg) (:op agg))) (:dict col-info))
                 acc)))
           (transient {})
           aggs)))

(defn- decode-code
  "Map one aggregate result (a dict code, arriving as a double from the
   `double[]` accumulators or as a long from the group-by decoder) back
   to its string. nil (empty group) and out-of-range codes yield nil.

   Values that are not numbers are passed through untouched, which makes
   the decode idempotent: `compile-query`'s fallback branch routes
   through `q`, so a result can legitimately arrive already decoded."
  [v ^"[Ljava.lang.String;" dict]
  (if-not (number? v)
    v
    (let [i (long (Math/round (double v)))]
      (if (and (<= 0 i) (< i (alength dict)))
        (aget dict (int i))
        nil))))

(defn decode-string-aggs
  "Rewrite MIN/MAX results that are dict codes into the strings they
   denote. `results` is either the row-map vector or, for `:result
   :columns`, the columnar map; both shapes are handled. A no-op when
   `decoders` is empty."
  [results decoders]
  (if (empty? decoders)
    results
    (cond
      ;; Columnar output: {alias array-or-seq ... :n-rows N}
      (map? results)
      (reduce-kv (fn [acc alias dict]
                   (if-let [col (get acc alias)]
                     (assoc acc alias
                            (into-array String (map #(decode-code % dict) (seq col))))
                     acc))
                 results decoders)

      ;; Row-map output
      (sequential? results)
      (mapv (fn [row]
              (reduce-kv (fn [r alias dict]
                           (if (contains? r alias)
                             (assoc r alias (decode-code (get r alias) dict))
                             r))
                         row decoders))
            results)

      :else results)))

(defn- canonicalize-distinct-val
  "Normalize a value before hashing for DISTINCT comparison so SQL
   semantics hold:
     -0.0 ≡ +0.0   (Java Double bit-pattern equality would split them)
   NaN handling could be added similarly if needed; SQL leaves NaN
   semantics implementation-defined."
  [v]
  (if (and (double? v) (zero? (double v)))
    0.0
    v))

(defn apply-distinct
  "Apply SELECT DISTINCT: deduplicate result rows using a HashSet on
   value vectors. Excludes internal :_count key. Numeric +0.0 / -0.0
   are canonicalized so they compare equal."
  [results]
  (let [seen (java.util.HashSet.)]
    (filterv (fn [row]
               (let [pairs (sort-by key (dissoc row :_count))
                     vals  (mapv (fn [[_ v]] (canonicalize-distinct-val v)) pairs)]
                 (.add seen vals)))
             results)))

(defn- resolve-having-val
  "Resolve a HAVING argument: keywords are looked up in the result row
   (they reference other aggregate columns, e.g. HAVING MIN(a) < MAX(a));
   numbers pass through as-is."
  [row arg]
  (if (keyword? arg)
    (let [direct (get row arg)]
      (if (some? direct)
        direct
        (let [cn (name arg)
              idx (clojure.string/last-index-of cn "_")]
          (if idx
            (get row (keyword (subs cn 0 idx)))
            direct))))
    arg))

(defn- resolve-having-col
  "Resolve the column (LHS) of a HAVING predicate from a result row.
   Tries exact key first, then falls back to base key without _col suffix."
  [row col]
  (let [direct (get row col)]
    (if (some? direct)
      direct
      (let [cn (name col)
            idx (clojure.string/last-index-of cn "_")]
        (if idx
          (get row (keyword (subs cn 0 idx)))
          direct)))))

(defn apply-having
  "Apply :having predicates to grouped results.
   Having predicates reference result columns (aggregation aliases).
   Both the column (LHS) and comparison args (RHS) may be keyword
   references to aggregate columns (e.g. HAVING MIN(a) < MAX(a))."
  [results having-preds]
  (if (empty? having-preds)
    results
    (let [normalized (mapv norm/normalize-pred having-preds)]
      (filterv (fn [row]
                 (every? (fn [pred]
                           (let [col (first pred)
                                 op (second pred)
                                 args (subvec pred 2)
                                 v (resolve-having-col row col)]
                             (cond
                               ;; IS NULL / IS NOT NULL work on nil values
                               (= op :is-null) (or (nil? v) (and (number? v) (Double/isNaN (double v))))
                               (= op :is-not-null) (and (some? v) (not (and (number? v) (Double/isNaN (double v)))))
                               ;; NULL/NaN comparisons return false (SQL three-valued logic)
                               (nil? v) false
                               (and (number? v) (Double/isNaN (double v))) false
                               :else
                               (let [resolve #(resolve-having-val row %)]
                                 (case op
                                   :lt    (< (double v) (double (resolve (first args))))
                                   :gt    (> (double v) (double (resolve (first args))))
                                   :lte   (<= (double v) (double (resolve (first args))))
                                   :gte   (>= (double v) (double (resolve (first args))))
                                   :eq    (== (double v) (double (resolve (first args))))
                                   :neq   (not (== (double v) (double (resolve (first args)))))
                                   :range (let [lo (double (resolve (first args)))
                                                hi (double (resolve (second args)))]
                                            (and (>= (double v) lo) (< (double v) hi)))
                                   true)))))
                         normalized))
               results))))

(defn- eval-order-expr
  "Evaluate an ORDER BY expression on a result row map.
   Handles keywords (column refs) and expression vectors ([:* :a :b])."
  [expr row]
  (cond
    (keyword? expr) (get row expr)
    (number? expr) expr
    (sequential? expr)
    (let [[op & args] expr
          a (eval-order-expr (first args) row)
          b (when (second args) (eval-order-expr (second args) row))]
      (if (and (#{:+ :- :* :/ :add :sub :mul :div :mod} op) (or (nil? a) (nil? b)))
        nil ;; NULL propagation for arithmetic
        (case op
          (:+ :add) (+ (double a) (double b))
          (:- :sub) (- (double a) (double b))
          (:* :mul) (* (double a) (double b))
          (:/ :div) (/ (double a) (double b))
          (:mod) (mod (double a) (double b))
          (:lower) (when a (clojure.string/lower-case (str a)))
          (:upper) (when a (clojure.string/upper-case (str a)))
          nil)))
    :else expr))

(defn- make-row-comparator
  "Build a comparator function for result map rows from order specs."
  ^java.util.Comparator [order-specs]
  (let [comparators
        (mapv (fn [spec]
                (let [[col dir] (if (vector? spec) spec [spec :asc])
                      dir (or dir :asc)
                      get-val (if (keyword? col)
                                #(get % col)
                                #(eval-order-expr col %))]
                  (fn [a b]
                    (let [va (get-val a)
                          vb (get-val b)
                          ;; Build one ASC comparison and reverse it exactly once
                          ;; below for DESC. PostgreSQL's default is NULLS LAST
                          ;; for ASC and NULLS FIRST for DESC; making the nil
                          ;; branches direction-aware here and then reversing the
                          ;; result made the fallback sorter put NULL last in both
                          ;; directions.
                          cmp (cond
                                (and (nil? va) (nil? vb)) 0
                                (nil? va) 1
                                (nil? vb) -1
                                :else (compare va vb))]
                      (if (= dir :desc) (- cmp) cmp)))))
              order-specs)]
    (reify java.util.Comparator
      (compare [_ a b]
        (int (reduce (fn [_ cmp-fn]
                       (let [c (int (cmp-fn a b))]
                         (if (zero? c) 0 (reduced c))))
                     0 comparators))))))

(defn apply-order
  "Apply :order sorting to results. When limit is provided and small relative
   to data size, uses a bounded heap (Top-N) for O(N log k) instead of O(N log N)."
  ([results order-specs] (apply-order results order-specs nil nil))
  ([results order-specs limit offset]
   (if (empty? order-specs)
     results
     (let [n (count results)
           effective-k (when limit (+ (long limit) (long (or offset 0))))
           use-topn? (and effective-k (> n 0) (< effective-k (quot n 10)))
           cmp (make-row-comparator order-specs)]
       (if use-topn?
         ;; Top-N: maintain bounded heap of size k
         (let [k (int effective-k)
               ;; Use a max-heap (PriorityQueue with reversed comparator) of size k
               ;; We keep the k smallest elements by evicting the max whenever size > k
               ^java.util.PriorityQueue pq (java.util.PriorityQueue. (inc k) (.reversed cmp))]
           (doseq [row results]
             (.offer pq row)
             (when (> (.size pq) k)
               (.poll pq)))
           ;; PQ now has k smallest elements; sort them
           (sort cmp (vec (.toArray pq))))
         ;; Full sort
         (sort cmp results))))))

(defn apply-limit-offset
  "Apply :limit and :offset to results."
  [results limit offset]
  (cond->> results
    offset (drop offset)
    limit  (take limit)
    true   vec))
