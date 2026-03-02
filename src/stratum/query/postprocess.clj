(ns stratum.query.postprocess
  "Post-processing pipeline for query results: formatting, distinct, having, order, limit/offset."
  (:require [stratum.query.normalization :as norm]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Result Formatting
;; ============================================================================

(defn format-fused-result
  "Format fused SIMD result into standard output."
  [result agg]
  (let [alias (or (:as agg) (:op agg))
        cnt (:count result)
        value (case (:op agg)
                (:count :count-non-null) (long cnt)
                (:min :max :sum :sum-product) (if (zero? cnt) nil (:result result))
                :avg (if (zero? cnt) nil (:result result))
                (:result result))]
    [{(keyword alias) value
      :_count cnt}]))

(defn apply-distinct
  "Apply SELECT DISTINCT: deduplicate result rows using a HashSet on value vectors.
   Excludes internal :_count key from deduplication comparison."
  [results]
  (let [seen (java.util.HashSet.)]
    (filterv (fn [row]
               (let [vals (vec (sort-by key (dissoc row :_count)))]
                 (.add seen vals)))
             results)))

(defn apply-having
  "Apply :having predicates to grouped results.
   Having predicates reference result columns (aggregation aliases)."
  [results having-preds]
  (if (empty? having-preds)
    results
    (let [normalized (mapv norm/normalize-pred having-preds)]
      (filterv (fn [row]
                 (every? (fn [pred]
                           (let [col (first pred)
                                 op (second pred)
                                 args (subvec pred 2)
                                 ;; Try exact key first, then fall back to base key without _col suffix
                                 ;; (handles auto-alias dedup where :sum_price becomes :sum when single agg).
                                 ;; Use last-index-of so :sum_price_usd → :sum_price (correct)
                                 ;; instead of first-index-of which would give :sum (wrong).
                                 v (let [direct (get row col)]
                                     (if (some? direct)
                                       direct
                                       (let [cn (name col)
                                             idx (clojure.string/last-index-of cn "_")]
                                         (if idx
                                           (get row (keyword (subs cn 0 idx)))
                                           direct))))]
                             (cond
                               ;; IS NULL / IS NOT NULL work on nil values
                               (= op :is-null) (or (nil? v) (and (number? v) (Double/isNaN (double v))))
                               (= op :is-not-null) (and (some? v) (not (and (number? v) (Double/isNaN (double v)))))
                               ;; NULL/NaN comparisons return false (SQL three-valued logic)
                               (nil? v) false
                               (and (number? v) (Double/isNaN (double v))) false
                               :else
                               (case op
                                 :lt    (< (double v) (double (first args)))
                                 :gt    (> (double v) (double (first args)))
                                 :lte   (<= (double v) (double (first args)))
                                 :gte   (>= (double v) (double (first args)))
                                 :eq    (== (double v) (double (first args)))
                                 :neq   (not (== (double v) (double (first args))))
                                 :range (let [lo (double (first args))
                                              hi (double (second args))]
                                          (and (>= (double v) lo) (< (double v) hi)))
                                 true))))
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
                          ;; SQL NULL ordering: NULLs last for ASC, first for DESC
                          cmp (cond
                                (and (nil? va) (nil? vb)) 0
                                (nil? va) (if (= dir :desc) -1 1)   ;; NULL sorts last (ASC) or first (DESC)
                                (nil? vb) (if (= dir :desc) 1 -1)
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
