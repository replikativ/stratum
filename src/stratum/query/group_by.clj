(ns stratum.query.group-by
  "Group-by execution for Stratum queries: dense, hash, chunked paths;
   scalar eval helpers; chunk streaming utilities; zone map helpers."
  (:require [stratum.query.simd-primitive :as qc]
            [stratum.query.normalization :as norm]
            [stratum.query.expression :as expr]
            [stratum.index :as index]
            [stratum.chunk :as chunk]
            [stratum.stats :as stats]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsChunked ColumnOpsAnalytics ColumnOpsVar]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:dynamic *dense-group-limit*
  "Maximum key space for dense group-by arrays.
   Dense path pre-allocates double[maxKey][2*nAggs] per thread.
   Memory cost: maxKey * nAggs * 2 * 8 bytes per thread.
   At 200K with 2 acc slots: 3.2MB per thread — fits L2 cache.
   Conditional morsel splitting kicks in for large allocations (>64KB).
   Set to nil for automatic sizing based on memory budget."
  200000)

;; NOTE: Global caches (WeakHashMap for double[] and max/min) were removed.
;; They retained O(columns × rows × 8) bytes as long as the source arrays lived,
;; which prevents scaling to 1B rows. Per-query local caches (HashMap in each
;; execute call) provide the same dedup benefit within a single query without
;; unbounded retention.

;; ============================================================================
;; Column Null Detection
;; ============================================================================

(defn column-has-nulls?
  "Check if a single column contains NULL values.
   Uses PSS tree-aggregated null-count for index columns (O(1)).
   For array columns, uses Java array scan (short-circuits on first NULL)."
  [col-data]
  (cond
    ;; Index: O(1) via PSS measure (tree-aggregated stats at root)
    (and (map? col-data) (= :index (:source col-data)))
    (let [idx (:index col-data)
          ^stratum.stats.ChunkStats stats (stratum.index/idx-stats idx)]
      (pos? (long (or (:null-count stats) 0))))
    ;; Raw double[]
    (instance? (Class/forName "[D") col-data)
    (ColumnOps/arrayHasNaN ^doubles col-data (alength ^doubles col-data))
    ;; Raw long[]
    (instance? (Class/forName "[J") col-data)
    (ColumnOps/arrayHasLongNull ^longs col-data (alength ^longs col-data))
    ;; Column map {:type _ :data array} — check the data array
    (and (map? col-data) (:data col-data))
    (let [d (:data col-data)]
      (cond
        (instance? (Class/forName "[J") d)
        (ColumnOps/arrayHasLongNull ^longs d (alength ^longs d))
        (instance? (Class/forName "[D") d)
        (ColumnOps/arrayHasNaN ^doubles d (alength ^doubles d))
        :else false))
    ;; Unknown — conservatively assume nulls
    :else true))

(defn columns-have-nulls?
  "Check if any of the named columns contain NULL values."
  [columns col-names]
  (boolean
   (some (fn [col-name]
           (column-has-nulls? (get columns col-name)))
         col-names)))

;; ============================================================================
;; Predicate Bound Preparation
;; ============================================================================

(defn prepare-pred-bounds
  "Split predicates by column type and prepare bound arrays for Java fused filter.
   Returns {:long-preds :dbl-preds :n-long :n-dbl
            :long-pred-types :long-lo :long-hi
            :dbl-pred-types :dbl-lo :dbl-hi}."
  [preds columns]
  (let [long-preds  (filterv #(= :int64 (:type (get columns (first %)))) preds)
        dbl-preds   (filterv #(= :float64 (:type (get columns (first %)))) preds)
        n-long      (count long-preds)
        n-dbl       (count dbl-preds)
        long-pred-types (int-array (mapv #(get qc/op->pred-type (second %)) long-preds))
        long-lo (long-array (mapv (fn [[_col op & args]]
                                    (case op
                                      (:range :not-range :gt :eq :gte :neq) (long (first args))
                                      0))
                                  long-preds))
        long-hi (long-array (mapv (fn [[_col op & args]]
                                    (case op
                                      (:range :not-range) (long (second args))
                                      (:lt :lte) (long (first args))
                                      0))
                                  long-preds))
        dbl-pred-types (int-array (mapv #(get qc/op->pred-type (second %)) dbl-preds))
        dbl-lo (double-array (mapv (fn [[_col op & args]]
                                     (case op
                                       (:range :not-range :gt :eq :gte :neq) (double (first args))
                                       0.0))
                                   dbl-preds))
        dbl-hi (double-array (mapv (fn [[_col op & args]]
                                     (case op
                                       (:range :not-range) (double (second args))
                                       (:lt :lte) (double (first args))
                                       0.0))
                                   dbl-preds))]
    {:long-preds long-preds :dbl-preds dbl-preds
     :n-long n-long :n-dbl n-dbl
     :long-pred-types long-pred-types :long-lo long-lo :long-hi long-hi
     :dbl-pred-types dbl-pred-types :dbl-lo dbl-lo :dbl-hi dbl-hi}))

(defn prepare-pred-arrays
  "Prepare predicate arrays including column data for Java fused filter on materialized arrays.
   Extends prepare-pred-bounds with column data arrays from :data fields."
  [preds columns]
  (let [{:keys [long-preds dbl-preds n-long n-dbl] :as bounds} (prepare-pred-bounds preds columns)
        long-cols (if (pos? n-long)
                    (into-array expr/long-array-class
                                (mapv #(:data (get columns (first %))) long-preds))
                    (make-array Long/TYPE 0 0))
        dbl-cols (if (pos? n-dbl)
                   (into-array expr/double-array-class
                               (mapv #(:data (get columns (first %))) dbl-preds))
                   (make-array Double/TYPE 0 0))]
    (assoc bounds :long-cols long-cols :dbl-cols dbl-cols)))

;; ============================================================================
;; Chunk-Streaming Fused Execution (zero-copy from indices)
;; ============================================================================

(defn collect-chunk-entries
  "Collect aligned chunk entries from an index.
   Returns vec of ChunkEntry records."
  [idx]
  (let [tree (index/idx-tree idx)]
    (vec (pss/slice tree nil nil))))

(defn chunk-array
  "Get the raw array (long[] or double[]) from a chunk entry."
  [^ChunkEntry entry]
  (chunk/chunk-data (.chunk entry)))

(defn build-zone-filters
  "Build zone map predicates for chunk-level pruning.
   Returns a vector of {:col col-name :may-contain fn :fully-inside fn-or-nil}
   for each predicate that can use zone maps."
  [preds]
  (into []
        (keep (fn [pred]
                (let [col-name (first pred)
                      op (second pred)
                      args (subvec pred 2)
                      value (case op
                              (:gt :gte :lt :lte :eq :neq) (first args)
                              (:range :not-range) args
                              nil)]
                  (when value
                    {:col col-name
                     :may-contain (stats/zone-may-contain-predicate op value)
                     :fully-inside (stats/zone-fully-inside-predicate op value)}))))
        preds))

(defn classify-chunk
  "Classify a chunk as :skip, :stats-only, or :simd based on zone predicates.
   zone-filters is from build-zone-filters.
   col-entries maps col-name -> vec of ChunkEntry.
   c is the chunk ordinal index."
  [zone-filters col-entries ^long c]
  (if (empty? zone-filters)
    :simd  ;; No zone filters, must process all
    (loop [filters zone-filters
           all-fully-inside? true]
      (if (empty? filters)
        (if all-fully-inside? :stats-only :simd)
        (let [{:keys [col may-contain fully-inside]} (first filters)
              entries (get col-entries col)
              ^ChunkEntry entry (nth entries c)
              chunk-stats (.stats entry)]
          (if-not (may-contain chunk-stats)
            :skip  ;; This pred proves no values match
            (recur (rest filters)
                   (and all-fully-inside?
                        (some? fully-inside)
                        (fully-inside chunk-stats)))))))))

;; Forward declaration (aget-col defined below, used in eval-agg-expr)
(declare aget-col)

;; ============================================================================
;; Expression Evaluation
;; ============================================================================

(defn eval-agg-expr
  "Evaluate an arithmetic expression tree for a single row.
   expr can be:
     - keyword       → column reference
     - number         → literal
     - {:op :mul/:add/:sub/:div :args [...]} → recursive"
  ^double [expr col-arrays ^long i]
  (cond
    (keyword? expr)
    (let [col (get col-arrays expr)]
      (if (expr/long-array? col)
        (double (aget ^longs col i))
        (aget ^doubles col i)))

    (number? expr)
    (double expr)

    (map? expr)
    (let [args (:args expr)]
      (case (:op expr)
        :mul (let [a (eval-agg-expr (nth args 0) col-arrays i)
                   b (eval-agg-expr (nth args 1) col-arrays i)]
               (if (> (count args) 2)
                 (reduce (fn [^double acc arg] (* acc (double (eval-agg-expr arg col-arrays i))))
                         (* a b) (subvec args 2))
                 (* a b)))
        :add (let [a (eval-agg-expr (nth args 0) col-arrays i)
                   b (eval-agg-expr (nth args 1) col-arrays i)]
               (if (> (count args) 2)
                 (reduce (fn [^double acc arg] (+ acc (double (eval-agg-expr arg col-arrays i))))
                         (+ a b) (subvec args 2))
                 (+ a b)))
        :sub (- (double (eval-agg-expr (nth args 0) col-arrays i))
                (double (eval-agg-expr (nth args 1) col-arrays i)))
        :div (/ (double (eval-agg-expr (nth args 0) col-arrays i))
                (double (eval-agg-expr (nth args 1) col-arrays i)))
        ;; Math functions
        :abs   (Math/abs (eval-agg-expr (nth args 0) col-arrays i))
        :sqrt  (Math/sqrt (eval-agg-expr (nth args 0) col-arrays i))
        :log   (Math/log (eval-agg-expr (nth args 0) col-arrays i))
        :log10 (Math/log10 (eval-agg-expr (nth args 0) col-arrays i))
        :exp   (Math/exp (eval-agg-expr (nth args 0) col-arrays i))
        :round (double (Math/round (eval-agg-expr (nth args 0) col-arrays i)))
        :floor (Math/floor (eval-agg-expr (nth args 0) col-arrays i))
        :ceil  (Math/ceil (eval-agg-expr (nth args 0) col-arrays i))
        :sign  (Math/signum (eval-agg-expr (nth args 0) col-arrays i))
        :mod   (let [a (eval-agg-expr (nth args 0) col-arrays i)
                     b (eval-agg-expr (nth args 1) col-arrays i)]
                 (rem a b))
        :pow   (Math/pow (eval-agg-expr (nth args 0) col-arrays i)
                         (eval-agg-expr (nth args 1) col-arrays i))
        ;; Date extraction — requires reading the long value from the column
        (:year :month :day :hour :minute :second :day-of-week :week-of-year)
        (let [col-key (nth args 0)
              col-data (get col-arrays col-key)
              v (if (expr/long-array? col-data)
                  (aget ^longs col-data i)
                  (long (aget ^doubles col-data i)))]
          (case (:op expr)
            :year   (let [z (+ v 719468)
                          era (quot (if (>= z 0) z (- z 146096)) 146097)
                          doe (- z (* era 146097))
                          yoe (quot (- doe (quot doe 1460) (- (quot doe 36524)) (quot doe 146096)) 365)
                          y (+ yoe (* era 400))
                          doy (- doe (+ (* 365 yoe) (quot yoe 4) (- (quot yoe 100))))
                          mp (quot (+ (* 5 doy) 2) 153)
                          m (+ mp (if (< mp 10) 3 -9))]
                      (double (+ y (if (<= m 2) 1 0))))
            :month  (let [z (+ v 719468)
                          era (quot (if (>= z 0) z (- z 146096)) 146097)
                          doe (- z (* era 146097))
                          yoe (quot (- doe (quot doe 1460) (- (quot doe 36524)) (quot doe 146096)) 365)
                          doy (- doe (+ (* 365 yoe) (quot yoe 4) (- (quot yoe 100))))
                          mp (quot (+ (* 5 doy) 2) 153)]
                      (double (+ mp (if (< mp 10) 3 -9))))
            :day    (let [z (+ v 719468)
                          era (quot (if (>= z 0) z (- z 146096)) 146097)
                          doe (- z (* era 146097))
                          yoe (quot (- doe (quot doe 1460) (- (quot doe 36524)) (quot doe 146096)) 365)
                          doy (- doe (+ (* 365 yoe) (quot yoe 4) (- (quot yoe 100))))
                          mp (quot (+ (* 5 doy) 2) 153)]
                      (double (+ (- doy (quot (+ (* 153 mp) 2) 5)) 1)))
            :hour   (double (quot (mod (+ (mod v 86400) 86400) 86400) 3600))
            :minute (double (quot (mod (mod (+ (mod v 86400) 86400) 86400) 3600) 60))
            :second (double (mod (mod (+ (mod v 86400) 86400) 86400) 60))
            :day-of-week (double (mod (+ (mod v 7) 10) 7))
            :week-of-year (double 1))) ;; simplified for scalar path
        ;; Date/time arithmetic (scalar path)
        :date-trunc
        (let [unit (nth args 0)
              col-key (nth args 1)
              col-data (get col-arrays col-key)
              v (long (if (expr/long-array? col-data)
                        (aget ^longs col-data i)
                        (long (aget ^doubles col-data i))))]
          (double (case unit
                    :day    (long (* (Math/floorDiv v 86400) 86400))
                    :hour   (long (* (Math/floorDiv v 3600) 3600))
                    :minute (long (* (Math/floorDiv v 60) 60))
                    ;; year/month need Hinnant via vectorized path
                    :year   (let [^longs r (ColumnOps/arrayDateTruncYear (long-array [v]) 1)]
                              (aget r 0))
                    :month  (let [^longs r (ColumnOps/arrayDateTruncMonth (long-array [v]) 1)]
                              (aget r 0)))))

        :date-add
        (let [unit (nth args 0)
              n (nth args 1)
              col-key (nth args 2)
              col-data (get col-arrays col-key)
              v (long (if (expr/long-array? col-data)
                        (aget ^longs col-data i)
                        (long (aget ^doubles col-data i))))]
          (double (case unit
                    :days    (+ v (* (long n) 86400))
                    :hours   (+ v (* (long n) 3600))
                    :minutes (+ v (* (long n) 60))
                    :seconds (+ v (long n))
                    :months  (let [^longs r (ColumnOps/arrayDateAddMonths (long-array [v]) (int n) 1)]
                               (aget r 0))
                    :years   (let [^longs r (ColumnOps/arrayDateAddMonths (long-array [v]) (int (* n 12)) 1)]
                               (aget r 0)))))

        :date-diff
        (let [unit (nth args 0)
              col-data1 (get col-arrays (nth args 1))
              col-data2 (get col-arrays (nth args 2))
              v1 (long (if (expr/long-array? col-data1)
                         (aget ^longs col-data1 i) (long (aget ^doubles col-data1 i))))
              v2 (long (if (expr/long-array? col-data2)
                         (aget ^longs col-data2 i) (long (aget ^doubles col-data2 i))))]
          (case unit
            :days    (/ (double (- v1 v2)) 86400.0)
            :seconds (double (- v1 v2))))

        :epoch-days
        (let [col-data (get col-arrays (nth args 0))
              v (long (if (expr/long-array? col-data)
                        (aget ^longs col-data i) (long (aget ^doubles col-data i))))]
          (double (quot v 86400)))

        :epoch-seconds
        (let [col-data (get col-arrays (nth args 0))
              v (long (if (expr/long-array? col-data)
                        (aget ^longs col-data i) (long (aget ^doubles col-data i))))]
          (double (* v 86400)))

        ;; NULL handling (scalar path)
        :coalesce
        (let [a (eval-agg-expr (nth args 0) col-arrays i)]
          (if (or (Double/isNaN a) (== a (double Long/MIN_VALUE)))
            (eval-agg-expr (nth args 1) col-arrays i)
            a))

        :nullif
        (let [a (eval-agg-expr (nth args 0) col-arrays i)
              val (double (nth args 1))]
          (if (== a val) Double/NaN a))

        :greatest
        (reduce (fn [^double acc arg]
                  (Math/max acc (double (eval-agg-expr arg col-arrays i))))
                (double (eval-agg-expr (nth args 0) col-arrays i))
                (rest args))

        :least
        (reduce (fn [^double acc arg]
                  (Math/min acc (double (eval-agg-expr arg col-arrays i))))
                (double (eval-agg-expr (nth args 0) col-arrays i))
                (rest args))

        ;; String functions (use expr/*columns-meta* for dict access)
        :length
        (let [col-key (nth args 0)
              col-data (get col-arrays col-key)
              col-meta (when expr/*columns-meta* (get expr/*columns-meta* col-key))]
          (if (and (expr/long-array? col-data) col-meta (:dict col-meta))
            (let [^"[Ljava.lang.String;" dict (:dict col-meta)
                  code (aget ^longs col-data i)]
              (double (.length ^String (aget dict (int code)))))
            ;; fallback: if it's a raw string column somehow, shouldn't happen
            (double (aget ^longs col-data i))))
        ;; CAST (numeric targets only — string target is pre-materialized)
        :cast
        (let [source-key (nth args 0)
              target-type (nth args 1)
              col-data (get col-arrays source-key)
              col-meta (when expr/*columns-meta* (get expr/*columns-meta* source-key))]
          (case target-type
            :double
            (cond
              (and (expr/long-array? col-data) col-meta (:dict col-meta))
              (let [^String s (aget ^"[Ljava.lang.String;" (:dict col-meta) (int (aget ^longs col-data i)))]
                (try (Double/parseDouble (.trim s)) (catch NumberFormatException _ Double/NaN)))
              (expr/long-array? col-data)
              (double (aget ^longs col-data i))
              :else (aget ^doubles col-data i))
            :long
            (cond
              (and (expr/long-array? col-data) col-meta (:dict col-meta))
              (let [^String s (aget ^"[Ljava.lang.String;" (:dict col-meta) (int (aget ^longs col-data i)))]
                (try (double (Long/parseLong (.trim s))) (catch NumberFormatException _ Double/NaN)))
              (expr/long-array? col-data)
              (double (aget ^longs col-data i))
              :else (double (long (aget ^doubles col-data i))))
            :string 0.0)) ;; string CAST should be pre-materialized, return dummy
        ;; CASE/WHEN
        :case
        (let [branches (:branches expr)
              has-else? (some #(= :else (:op %)) branches)]
          (double
           (or (some (fn [branch]
                       (if (= :else (:op branch))
                         (eval-agg-expr (:val branch) col-arrays i)
                         (let [pred (norm/normalize-pred (:pred branch))
                               col-ref (first pred)
                               op (second pred)
                               pred-args (subvec pred 2)
                               pv (if (keyword? col-ref)
                                    (aget-col (get col-arrays col-ref) i)
                                    (eval-agg-expr col-ref col-arrays i))
                               match? (case op
                                        :lt  (< pv (double (first pred-args)))
                                        :gt  (> pv (double (first pred-args)))
                                        :lte (<= pv (double (first pred-args)))
                                        :gte (>= pv (double (first pred-args)))
                                        :eq  (== pv (double (first pred-args)))
                                        :neq (not (== pv (double (first pred-args))))
                                        false)]
                           (when match?
                             (eval-agg-expr (:val branch) col-arrays i)))))
                     branches)
               (if has-else? 0.0 Double/NaN))))))

    :else
    (throw (ex-info (str "Unknown expression type: " (type expr)) {:expr expr}))))

(defn aget-col
  "Read element i from a column array (long[] or double[]), return as double."
  ^double [col ^long i]
  (if (expr/long-array? col)
    (double (aget ^longs col i))
    (aget ^doubles col i)))
;; ============================================================================
;; Scalar Fallback Execution
;; ============================================================================

(defn eval-pred-scalar
  "Evaluate a single predicate for a row.
   Handles standard comparisons, IN, NOT-IN, NOT-RANGE, and OR."
  [col-arrays ^long i pred]
  (let [col-ref (first pred)
        op (second pred)]
    (if (= :or op)
      ;; OR combinator: any sub-predicate must match
      (let [sub-preds (subvec pred 2)]
        (some (fn [sub-pred]
                (let [sub-col (first sub-pred)]
                  (eval-pred-scalar col-arrays i sub-pred)))
              sub-preds))
      ;; Standard predicate
      (let [col-data (if (keyword? col-ref) (get col-arrays col-ref) col-ref)
            v (if (expr/long-array? col-data)
                (aget ^longs col-data i)
                (aget ^doubles col-data i))
            args (subvec pred 2)]
        (case op
          :lt    (< (double v) (double (first args)))
          :gt    (> (double v) (double (first args)))
          :lte   (<= (double v) (double (first args)))
          :gte   (>= (double v) (double (first args)))
          :eq    (== (double v) (double (first args)))
          :neq   (not (== (double v) (double (first args))))
          :range (let [lo (double (first args))
                       hi (double (second args))]
                   (and (>= (double v) lo) (<= (double v) hi)))
          :not-range (let [lo (double (first args))
                           hi (double (second args))]
                       (or (< (double v) lo) (> (double v) hi)))
          :in    (let [s (first args)]
                   (if (every? number? s)
                     (contains? s (double v))
                     (contains? s v)))
          :not-in (let [s (first args)]
                    (if (every? number? s)
                      (not (contains? s (double v)))
                      (not (contains? s v))))
          :is-null (if (expr/long-array? col-data)
                     (== (long v) Long/MIN_VALUE)
                     (Double/isNaN (double v)))
          :is-not-null (if (expr/long-array? col-data)
                         (not= (long v) Long/MIN_VALUE)
                         (not (Double/isNaN (double v)))))))))

(defn execute-scalar-aggs
  "Execute aggregations over matching rows using scalar loop."
  [preds aggs columns length]
  (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) columns)
        ;; Find matching indices
        matching (transient [])
        _ (dotimes [i length]
            (when (every? (fn [pred]
                            (eval-pred-scalar col-arrays i pred))
                          preds)
              (conj! matching i)))
        match-indices (persistent! matching)
        match-count (count match-indices)]
    (if (empty? aggs)
      {:_count match-count}
      (into {:_count match-count}
            (map (fn [agg]
                   (let [alias (or (:as agg) (:op agg))
                         result
                         (case (:op agg)
                           :count (long match-count)
                           :count-non-null
                           (if-let [expr (:expr agg)]
                             (long (reduce (fn [^long acc i]
                                             (let [v (eval-agg-expr expr col-arrays (int i))]
                                               (if (Double/isNaN v)
                                                 acc (unchecked-inc acc))))
                                           0 match-indices))
                             (let [col-data (get col-arrays (:col agg))
                                   is-long (expr/long-array? col-data)]
                               (long (reduce (fn [^long acc i]
                                               (let [v (aget-col col-data (int i))]
                                                 (if (if is-long
                                                       (= (long v) Long/MIN_VALUE)
                                                       (Double/isNaN v))
                                                   acc (unchecked-inc acc))))
                                             0 match-indices))))
                           :sum (if (zero? match-count)
                                  nil
                                  (if-let [expr (:expr agg)]
                                    (reduce (fn [^double acc i]
                                              (let [v (eval-agg-expr expr col-arrays (int i))]
                                                (if (Double/isNaN v) acc (+ acc v))))
                                            0.0 match-indices)
                                    (let [col-data (get col-arrays (:col agg))
                                          is-long (expr/long-array? col-data)]
                                      (reduce (fn [^double acc i]
                                                (let [v (aget-col col-data (int i))]
                                                  (if (if is-long
                                                        (= (long v) Long/MIN_VALUE)
                                                        (Double/isNaN v))
                                                    acc (+ acc v))))
                                              0.0 match-indices))))
                           :min (if (zero? match-count)
                                  nil
                                  (let [col-data (get col-arrays (:col agg))
                                        is-long (expr/long-array? col-data)]
                                    (reduce (fn [^double acc i]
                                              (let [v (aget-col col-data (int i))]
                                                (if (if is-long
                                                      (= (long v) Long/MIN_VALUE)
                                                      (Double/isNaN v))
                                                  acc (Math/min acc v))))
                                            Double/POSITIVE_INFINITY match-indices)))
                           :max (if (zero? match-count)
                                  nil
                                  (let [col-data (get col-arrays (:col agg))
                                        is-long (expr/long-array? col-data)]
                                    (reduce (fn [^double acc i]
                                              (let [v (aget-col col-data (int i))]
                                                (if (if is-long
                                                      (= (long v) Long/MIN_VALUE)
                                                      (Double/isNaN v))
                                                  acc (Math/max acc v))))
                                            Double/NEGATIVE_INFINITY match-indices)))
                           :avg (if (zero? match-count)
                                  nil
                                  (if-let [expr (:expr agg)]
                                    (let [[sum cnt]
                                          (reduce (fn [[^double s ^long c] i]
                                                    (let [v (eval-agg-expr expr col-arrays (int i))]
                                                      (if (Double/isNaN v) [s c] [(+ s v) (inc c)])))
                                                  [0.0 0] match-indices)]
                                      (if (zero? cnt) nil (/ sum (double cnt))))
                                    (let [col-data (get col-arrays (:col agg))
                                          is-long (expr/long-array? col-data)
                                          [sum cnt]
                                          (reduce (fn [[^double s ^long c] i]
                                                    (let [v (aget-col col-data (int i))]
                                                      (if (if is-long
                                                            (= (long v) Long/MIN_VALUE)
                                                            (Double/isNaN v))
                                                        [s c] [(+ s v) (inc c)])))
                                                  [0.0 0] match-indices)]
                                      (if (zero? cnt) nil (/ sum (double cnt))))))
                           :sum-product
                           (let [[c1 c2] (:cols agg)
                                 a1 (get col-arrays c1)
                                 a2 (get col-arrays c2)]
                             (reduce (fn [^double acc i]
                                       (+ acc (* (aget-col a1 (int i)) (aget-col a2 (int i)))))
                                     0.0 match-indices))
                           (:variance :variance-pop)
                           (let [col-data (get col-arrays (:col agg))
                                 is-long (expr/long-array? col-data)
                                 [sum cnt]
                                 (reduce (fn [[^double s ^long c] i]
                                           (let [v (aget-col col-data (int i))]
                                             (if (if is-long (= (long v) Long/MIN_VALUE) (Double/isNaN v))
                                               [s c] [(+ s v) (inc c)])))
                                         [0.0 0] match-indices)]
                             (if (< cnt (if (= (:op agg) :variance-pop) 1 2))
                               Double/NaN
                               (let [mean (/ sum (double cnt))]
                                 (/ (reduce (fn [^double acc i]
                                              (let [v (aget-col col-data (int i))]
                                                (if (if is-long (= (long v) Long/MIN_VALUE) (Double/isNaN v))
                                                  acc
                                                  (let [d (- v mean)] (+ acc (* d d))))))
                                            0.0 match-indices)
                                    (double (if (= (:op agg) :variance-pop) cnt (dec cnt)))))))
                           (:stddev :stddev-pop)
                           (let [var-op (if (= (:op agg) :stddev-pop) :variance-pop :variance)
                                 v (execute-scalar-aggs preds [{:op var-op :col (:col agg) :as nil}] columns length)]
                             (Math/sqrt (double (get v var-op))))
                           :median
                           (let [col-data (get col-arrays (:col agg))
                                 work (double-array match-count)]
                             (dotimes [j match-count]
                               (aset work j (double (aget-col col-data (int (nth match-indices j))))))
                             (if (zero? match-count) Double/NaN
                                 (ColumnOps/percentile work (int match-count) 0.5)))
                           :percentile
                           (let [col-data (get col-arrays (:col agg))
                                 p (double (:param agg))
                                 work (double-array match-count)]
                             (dotimes [j match-count]
                               (aset work j (double (aget-col col-data (int (nth match-indices j))))))
                             (if (zero? match-count) Double/NaN
                                 (ColumnOps/percentile work (int match-count) p)))
                           :approx-quantile
                           (let [col-data (get col-arrays (:col agg))
                                 p (double (:param agg))
                                 work (double-array match-count)]
                             (dotimes [j match-count]
                               (aset work j (double (aget-col col-data (int (nth match-indices j))))))
                             (if (zero? match-count) Double/NaN
                                 (ColumnOpsAnalytics/tdigestApproxQuantile work (int match-count) p 200.0)))
                           :count-distinct
                           (let [col-data (get col-arrays (:col agg))]
                             (if (and (expr/long-array? col-data)
                                      (= match-count length))
                               ;; Fast path: no filter, long[] column → parallel Java hash / BitSet
                               (ColumnOpsExt/countDistinctLongParallel ^longs col-data (int length))
                               (if (expr/long-array? col-data)
                                 ;; Filtered long[] → build mask, use Java masked parallel path
                                 (let [mask (long-array length)]
                                   (doseq [i match-indices]
                                     (aset mask (int i) 1))
                                   (ColumnOpsExt/countDistinctLongMaskedParallel ^longs col-data mask (int length)))
                                 ;; Fallback for double[] columns
                                 (let [s (java.util.HashSet.)]
                                   (doseq [i match-indices]
                                     (.add s (aget-col col-data (int i))))
                                   (.size s))))))]
                     [alias result]))
                 aggs)))))

;; ============================================================================
;; Group-By Execution (Java-accelerated)
;; ============================================================================

(defn java-group-by-eligible?
  "Check if group-by can use the Java fusedFilterGroupAggregate path.
   Requires: all group columns are long[], supported agg ops.
   Compound aggs (variance/stddev/corr) are eligible because they decompose into SUM/COUNT."
  [group-cols aggs columns]
  (let [supported-agg-ops #{:sum :count :min :max :avg :sum-product :variance :variance-pop :stddev :stddev-pop :corr :count-distinct}
        group-cols-ok? (every? #(= :int64 (:type (get columns %))) group-cols)
        aggs-ok? (every? #(supported-agg-ops (:op %)) aggs)]
    (and group-cols-ok? aggs-ok?)))

(defn prepare-agg-source-col
  "Prepare a double[] source column for a single aggregate.
   Pre-computes expressions using vectorized array ops, converts long[] to double[].
   Uses cache to avoid duplicate longToDouble conversions across aggs.
   For :sum-product, returns col1 only (Java multiplies col1 * col2 inline)."
  ^doubles [agg col-arrays ^long length ^java.util.HashMap cache]
  (case (:op agg)
    :count
    (double-array 0) ;; dummy, never accessed by Java COUNT path

    :sum-product
    (let [[c1 _c2] (:cols agg)]
      (expr/col-as-doubles-cached (get col-arrays c1) length cache))

    (:sum :avg)
    (if-let [expr (:expr agg)]
      (expr/eval-expr-vectorized expr col-arrays length cache)
      (expr/col-as-doubles-cached (get col-arrays (:col agg)) length cache))

    (:min :max)
    (if-let [expr (:expr agg)]
      (expr/eval-expr-vectorized expr col-arrays length cache)
      (expr/col-as-doubles-cached (get col-arrays (:col agg)) length cache))

    :count-distinct
    (double-array 0) ;; dummy — COUNT DISTINCT uses separate Java path with long[] column

    ;; fallback
    (double-array 0)))

(defn prepare-agg-source-col2
  "Prepare the second source column for SUM_PRODUCT aggregates.
   Returns col2 for :sum-product, dummy for all other agg types."
  ^doubles [agg col-arrays ^long length ^java.util.HashMap cache]
  (if (= :sum-product (:op agg))
    (let [[_c1 c2] (:cols agg)]
      (expr/col-as-doubles-cached (get col-arrays c2) length cache))
    (double-array 0)))

(defn prepare-agg-source-col-premul
  "Like prepare-agg-source-col but pre-multiplies SUM_PRODUCT columns.
   Used by the fused join path which doesn't have inline SUM_PRODUCT support."
  ^doubles [agg col-arrays ^long length ^java.util.HashMap cache]
  (if (= :sum-product (:op agg))
    (let [[c1 c2] (:cols agg)
          a1 (expr/col-as-doubles-cached (get col-arrays c1) length cache)
          a2 (expr/col-as-doubles-cached (get col-arrays c2) length cache)]
      (ColumnOps/arrayMul a1 a2 (int length)))
    (prepare-agg-source-col agg col-arrays length cache)))

(defn prepare-agg-raw-col
  "Return raw column data (long[] or double[]) without longToDouble conversion.
   For SUM_PRODUCT, pre-multiplies (returns double[]). For COUNT, returns nil.
   For expressions, returns double[] (eval-expr always produces double[])."
  [agg col-arrays ^long length ^java.util.HashMap cache]
  (case (:op agg)
    :count nil
    :sum-product (let [[c1 c2] (:cols agg)
                       a1 (expr/col-as-doubles-cached (get col-arrays c1) length cache)
                       a2 (expr/col-as-doubles-cached (get col-arrays c2) length cache)]
                   (ColumnOps/arrayMul a1 a2 (int length)))
    (:sum :avg :min :max)
    (if-let [expr (:expr agg)]
      (expr/eval-expr-vectorized expr col-arrays length cache)
      (get col-arrays (:col agg)))  ;; raw: may be long[] or double[]
    nil))

(defn decode-key
  "Decode a composite group key back to individual column values."
  [^long key ^longs group-muls ^long n-group]
  (if (= 1 n-group)
    [key]
    (loop [remaining key
           idx 0
           vals (transient [])]
      (if (>= idx n-group)
        (persistent! vals)
        (let [mul (aget group-muls idx)
              v (quot remaining mul)
              remaining (rem remaining mul)]
          (recur remaining (inc idx) (conj! vals v)))))))

(defn decode-accs-to-map
  "Decode a key + accumulator array into a Clojure result map.
   group-dicts is a vector of dict arrays (or nil) per group column,
   used to reverse dictionary-encoded string columns."
  [key group-cols ^longs group-muls n-group aggs ^doubles accs group-dicts]
  (let [group-vals (decode-key (long key) group-muls (long n-group))
        ;; Reverse dict-encode: map integer keys back to strings
        group-vals (if group-dicts
                     (mapv (fn [v dict]
                             (if dict
                               (aget ^"[Ljava.lang.String;" dict (int (long v)))
                               v))
                           group-vals group-dicts)
                     group-vals)
        base (into {} (map vector group-cols group-vals))
        agg-results
        (into {}
              (map-indexed
               (fn [idx agg]
                 (let [alias (or (:as agg) (:op agg))
                       agg-base (* idx 2)
                       cnt (long (aget accs (inc agg-base)))
                       result (case (:op agg)
                                :count cnt
                                (:sum :sum-product) (if (zero? cnt) nil (aget accs agg-base))
                                (:min :max) (if (zero? cnt) nil (aget accs agg-base))
                                :avg (if (zero? cnt) nil
                                         (/ (aget accs agg-base) (double cnt)))
                                (aget accs agg-base))]
                   [alias result]))
               aggs))]
    (merge base {:_count (long (aget accs 1))} agg-results)))

(defn decode-group-results
  "Decode Java Object[] = {long[] keys, double[] flatAccs} into Clojure result maps.
   flatAccs has stride = numAggs*2 (sum+count per agg)."
  [^objects result-pair group-cols ^longs group-muls aggs group-dicts]
  (let [n-group (count group-cols)
        ^longs keys (aget result-pair 0)
        ^doubles flat-accs (aget result-pair 1)
        n (alength keys)
        n-aggs (count aggs)
        stride (int (* 2 n-aggs))
        ;; Pre-compute agg aliases and ops
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        agg-ops (mapv :op aggs)]
    (loop [i 0 results (transient [])]
      (if (< i n)
        (let [key (aget keys i)
              base-offset (* i stride)
              ;; Decode group key
              group-vals (decode-key key group-muls n-group)
              group-vals (if group-dicts
                           (mapv (fn [v dict]
                                   (if dict
                                     (aget ^"[Ljava.lang.String;" dict (int (long v)))
                                     v))
                                 group-vals group-dicts)
                           group-vals)
              ;; Build result map in one shot
              m (loop [j 0 m (transient (-> (into {} (map vector group-cols group-vals))
                                            (assoc :_count (long (aget flat-accs (inc base-offset))))))]
                  (if (< j n-aggs)
                    (let [ab (+ base-offset (* j 2))
                          cnt (long (aget flat-accs (inc ab)))
                          op (nth agg-ops j)
                          val (case op
                                :count cnt
                                (:sum :sum-product) (if (zero? cnt) nil (aget flat-accs ab))
                                (:min :max) (if (zero? cnt) nil (aget flat-accs ab))
                                :avg (if (zero? cnt) nil
                                         (/ (aget flat-accs ab) (double cnt)))
                                (aget flat-accs ab))]
                      (recur (inc j) (assoc! m (nth agg-aliases j) val)))
                    (persistent! m)))]
          (recur (inc i) (conj! results m)))
        (persistent! results)))))

(defn compact-dense-flat-to-pair
  "Compact a flat dense accumulator array into [long[] keys, double[] accs] result pair.
   Filters out groups where accs[key*acc-size + count-offset] <= 0."
  [^doubles flat-array ^long max-key ^long acc-size ^long count-offset]
  (let [key-list (java.util.ArrayList.)]
    (dotimes [k max-key]
      (when (> (aget flat-array (+ (* (long k) acc-size) count-offset)) 0.0)
        (.add key-list (long k))))
    (let [n (.size key-list)
          keys (long-array n)
          compact-accs (double-array (* n acc-size))]
      (dotimes [i n]
        (let [k (long (.get key-list i))]
          (aset keys i k)
          (System/arraycopy flat-array (int (* k acc-size)) compact-accs (* i acc-size) acc-size)))
      (object-array [keys compact-accs]))))

(defn compact-dense-2d-to-pair
  "Compact a 2D dense accumulator array into [long[] keys, double[] accs] result pair.
   Filters out null or empty groups (accs[count-offset] <= 0)."
  [^"[[D" result-array ^long acc-size ^long count-offset]
  (let [max-key (alength result-array)
        key-list (java.util.ArrayList.)]
    (dotimes [k max-key]
      (let [^doubles accs (aget result-array k)]
        (when (and accs (> (aget accs count-offset) 0.0))
          (.add key-list (long k)))))
    (let [n (.size key-list)
          keys (long-array n)
          compact-accs (double-array (* n acc-size))]
      (dotimes [i n]
        (let [k (long (.get key-list i))
              ^doubles accs (aget result-array (int k))]
          (aset keys i k)
          (System/arraycopy accs 0 compact-accs (* i acc-size) acc-size)))
      (object-array [keys compact-accs]))))

(defn decode-dense-group-results
  "Decode Java double[] (flat dense array) into Clojure result maps.
   Compacts non-empty groups and delegates to decode-group-results."
  [^doubles flat-array max-key group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (int (* 2 (count aggs)))
        pair (compact-dense-flat-to-pair flat-array (long max-key) acc-size 1)]
    (decode-group-results pair group-cols group-muls aggs group-dicts)))

;; ============================================================================
;; Columnar Result Format
;; ============================================================================

(defn decode-group-key-columns
  "Decode composite keys into per-column arrays.
   Returns a vector of arrays (one per group column)."
  [^longs keys ^longs group-muls ^long n-group group-dicts]
  (let [n (alength keys)
        ;; Create output arrays: Object[] for dict-encoded, long[] otherwise
        col-arrays (mapv (fn [idx]
                           (if (and group-dicts (nth group-dicts idx))
                             (make-array String n)
                             (long-array n)))
                         (range n-group))]
    (dotimes [i n]
      (let [key (aget keys i)]
        ;; Decode composite key into individual column values
        (loop [remaining key idx 0]
          (when (< idx n-group)
            (let [mul (aget group-muls idx)
                  v (quot remaining mul)
                  remaining (rem remaining mul)]
              (if (and group-dicts (nth group-dicts idx))
                (aset ^"[Ljava.lang.String;" (nth col-arrays idx) i
                      (aget ^"[Ljava.lang.String;" (nth group-dicts idx) (int v)))
                (aset ^longs (nth col-arrays idx) i v))
              (recur remaining (inc idx)))))))
    col-arrays))

(defn decode-agg-columns
  "Extract per-agg result columns from flat accumulator array.
   Returns a vector of arrays (one per agg)."
  [^doubles flat-accs ^long n ^long stride aggs]
  (let [n-aggs (count aggs)]
    (mapv (fn [idx]
            (let [op (:op (nth aggs idx))
                  result (if (= :count op) (long-array n) (double-array n))]
              (dotimes [i n]
                (let [base (+ (* i stride) (* idx 2))
                      cnt (long (aget flat-accs (inc base)))]
                  (if (= :count op)
                    (aset ^longs result i cnt)
                    (aset ^doubles result i
                          (if (zero? cnt)
                            Double/NaN ;; NULL sentinel for SUM/MIN/MAX/AVG with no matching rows
                            (case op
                              :avg (/ (aget flat-accs base) (double cnt))
                              (aget flat-accs base)))))))
              result))
          (range n-aggs))))

(defn decode-group-results-columnar
  "Decode Java Object[] = {long[] keys, double[] flatAccs} into columnar format.
   Returns {:col1 array1 :col2 array2 ... :n-rows N}."
  [^objects result-pair group-cols ^longs group-muls aggs group-dicts]
  (let [^longs keys (aget result-pair 0)
        ^doubles flat-accs (aget result-pair 1)
        n (alength keys)
        n-group (count group-cols)
        n-aggs (count aggs)
        stride (int (* 2 n-aggs))
        ;; Decode group keys into per-column arrays
        group-arrays (decode-group-key-columns keys group-muls n-group group-dicts)
        ;; Decode agg results into per-column arrays
        agg-arrays (decode-agg-columns flat-accs n stride aggs)
        ;; Build result map: {col-keyword array ...}
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)]
    (-> (into {} (map vector group-cols group-arrays))
        (into (map vector agg-aliases agg-arrays))
        (assoc :n-rows n))))

(defn build-multi-key-lookup
  "Build a HashMap<Long, long[]> from compact multi-key hash lookup arrays.
   lookup-hashes: long[] of distinct hash values
   lookup-vals: long[] flat array of group column values (nDistinct * n-group-orig)
   Returns java.util.HashMap mapping hash → long[] of group column values."
  ^java.util.HashMap [^longs lookup-hashes ^longs lookup-vals ^long n-group-orig]
  (let [m (java.util.HashMap. (int (alength lookup-hashes)))]
    (dotimes [i (alength lookup-hashes)]
      (let [base (* i n-group-orig)
            vals (long-array n-group-orig)]
        (System/arraycopy lookup-vals (int base) vals 0 (int n-group-orig))
        (.put m (Long/valueOf (aget lookup-hashes i)) vals)))
    m))

(defn decode-group-results-multi-key
  "Decode hash-based group-by results using multi-key reverse lookup.
   Like decode-group-results but resolves group columns via lookup map instead of quot/rem."
  [^objects result-pair group-cols ^java.util.HashMap key-lookup aggs group-dicts]
  (let [n-group (count group-cols)
        ^longs keys (aget result-pair 0)
        ^doubles flat-accs (aget result-pair 1)
        n (alength keys)
        n-aggs (count aggs)
        stride (int (* 2 n-aggs))
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        agg-ops (mapv :op aggs)]
    (loop [i 0 results (transient [])]
      (if (< i n)
        (let [key (aget keys i)
              ^longs gvals (.get key-lookup (Long/valueOf key))
              base-offset (* i stride)
              ;; Build group column map
              group-map (loop [g 0 m (transient {})]
                          (if (< g n-group)
                            (let [v (aget gvals g)
                                  col (nth group-cols g)
                                  decoded-v (if (and group-dicts (nth group-dicts g))
                                              (aget ^"[Ljava.lang.String;" (nth group-dicts g) (int v))
                                              v)]
                              (recur (inc g) (assoc! m col decoded-v)))
                            m))
              m (loop [j 0 m (assoc! group-map :_count (long (aget flat-accs (inc base-offset))))]
                  (if (< j n-aggs)
                    (let [ab (+ base-offset (* j 2))
                          cnt (long (aget flat-accs (inc ab)))
                          op (nth agg-ops j)
                          val (case op
                                :count cnt
                                (:sum :sum-product) (if (zero? cnt) nil (aget flat-accs ab))
                                (:min :max) (if (zero? cnt) nil (aget flat-accs ab))
                                :avg (if (zero? cnt) nil
                                         (/ (aget flat-accs ab) (double cnt)))
                                (aget flat-accs ab))]
                      (recur (inc j) (assoc! m (nth agg-aliases j) val)))
                    (persistent! m)))]
          (recur (inc i) (conj! results m)))
        (persistent! results)))))

(defn decode-group-key-columns-multi-key
  "Decode hash keys into per-column arrays using multi-key reverse lookup.
   Like decode-group-key-columns but uses lookup map instead of quot/rem."
  [^longs keys ^java.util.HashMap key-lookup ^long n-group group-dicts]
  (let [n (alength keys)
        col-arrays (mapv (fn [idx]
                           (if (and group-dicts (nth group-dicts idx))
                             (make-array String n)
                             (long-array n)))
                         (range n-group))]
    (dotimes [i n]
      (let [^longs gvals (.get key-lookup (Long/valueOf (aget keys i)))]
        (dotimes [g n-group]
          (let [v (aget gvals g)]
            (if (and group-dicts (nth group-dicts g))
              (aset ^"[Ljava.lang.String;" (nth col-arrays g) i
                    (aget ^"[Ljava.lang.String;" (nth group-dicts g) (int v)))
              (aset ^longs (nth col-arrays g) i v))))))
    col-arrays))

(defn decode-group-results-multi-key-columnar
  "Decode hash-based group-by results into columnar format using multi-key reverse lookup."
  [^objects result-pair group-cols ^java.util.HashMap key-lookup aggs group-dicts]
  (let [^longs keys (aget result-pair 0)
        ^doubles flat-accs (aget result-pair 1)
        n (alength keys)
        n-group (count group-cols)
        n-aggs (count aggs)
        stride (int (* 2 n-aggs))
        group-arrays (decode-group-key-columns-multi-key keys key-lookup n-group group-dicts)
        agg-arrays (decode-agg-columns flat-accs n stride aggs)
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)]
    (-> (into {} (map vector group-cols group-arrays))
        (into (map vector agg-aliases agg-arrays))
        (assoc :n-rows n))))

(defn decode-dense-group-results-columnar
  "Decode Java double[] (flat dense array) into columnar format.
   Compacts non-empty groups and delegates to decode-group-results-columnar."
  [^doubles flat-array max-key group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (int (* 2 (count aggs)))
        pair (compact-dense-flat-to-pair flat-array (long max-key) acc-size 1)]
    (decode-group-results-columnar pair group-cols group-muls aggs group-dicts)))

(defn decode-dense-group-results-2d
  "Decode Java double[][] (per-key accumulator arrays) into Clojure result maps.
   Compacts non-empty groups and delegates to decode-group-results."
  [^"[[D" result-array group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (int (* 2 (count aggs)))
        pair (compact-dense-2d-to-pair result-array acc-size 1)]
    (decode-group-results pair group-cols group-muls aggs group-dicts)))

(defn decode-dense-group-results-columnar-2d
  "Decode Java double[][] (per-key accumulator arrays) into columnar format.
   Compacts non-empty groups and delegates to decode-group-results-columnar."
  [^"[[D" result-array group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (int (* 2 (count aggs)))
        pair (compact-dense-2d-to-pair result-array acc-size 1)]
    (decode-group-results-columnar pair group-cols group-muls aggs group-dicts)))

(defn compute-variance
  "Compute sample variance from sum, sum-of-squares, count."
  ^double [^double sum ^double sumsq ^long n]
  (if (< n 2)
    Double/NaN
    (/ (- (* (double n) sumsq) (* sum sum))
       (* (double n) (double (dec n))))))

(defn compute-variance-pop
  "Compute population variance from sum, sum-of-squares, count."
  ^double [^double sum ^double sumsq ^long n]
  (if (< n 1)
    Double/NaN
    (let [nd (double n)]
      (- (/ sumsq nd) (let [mean (/ sum nd)] (* mean mean))))))

(defn compute-correlation
  "Compute Pearson correlation from sum-x, sum-y, sum-xy, sum-x², sum-y², count."
  [sx sy sxy sxx syy n]
  (let [sx (double sx) sy (double sy) sxy (double sxy)
        sxx (double sxx) syy (double syy) nd (double n)
        num (- (* nd sxy) (* sx sy))
        denom (Math/sqrt (* (- (* nd sxx) (* sx sx))
                            (- (* nd syy) (* sy sy))))]
    (if (zero? denom) Double/NaN (/ num denom))))

(defn var-acc-slots
  "Number of accumulator slots for an agg type in the variable-width format."
  ^long [op]
  (case op
    :variance 3   ;; [sum, sum_sq, count]
    :variance-pop 3
    :stddev   3   ;; same as variance
    :stddev-pop 3
    :corr     6   ;; [sumx, sumy, sumxy, sumxx, sumyy, count]
    2))            ;; SUM/COUNT/MIN/MAX/AVG

(defn compute-var-agg-offsets
  "Compute accumulator offsets for variable-width agg layout."
  [aggs]
  (loop [i 0 off 0 offsets []]
    (if (>= i (count aggs))
      offsets
      (recur (inc i) (+ off (var-acc-slots (:op (nth aggs i)))) (conj offsets off)))))

(defn decode-var-acc-value
  "Decode a single aggregate value from variable-width accumulators.
   accs: double[] array, off: absolute offset into accs for this agg."
  [op ^doubles accs ^long off]
  (case op
    :variance (let [cnt (long (aget accs (+ off 2)))]
                (if (zero? cnt) nil
                    (compute-variance (aget accs off) (aget accs (+ off 1)) cnt)))
    :variance-pop (let [cnt (long (aget accs (+ off 2)))]
                    (if (zero? cnt) nil
                        (compute-variance-pop (aget accs off) (aget accs (+ off 1)) cnt)))
    :stddev (let [cnt (long (aget accs (+ off 2)))]
              (if (zero? cnt) nil
                  (Math/sqrt (compute-variance (aget accs off) (aget accs (+ off 1)) cnt))))
    :stddev-pop (let [cnt (long (aget accs (+ off 2)))]
                  (if (zero? cnt) nil
                      (Math/sqrt (compute-variance-pop (aget accs off) (aget accs (+ off 1)) cnt))))
    :corr (let [cnt (long (aget accs (+ off 5)))]
            (if (zero? cnt) nil
                (compute-correlation (aget accs off) (aget accs (+ off 1)) (aget accs (+ off 2))
                                     (aget accs (+ off 3)) (aget accs (+ off 4)) cnt)))
    :count (long (aget accs (+ off (dec (var-acc-slots op)))))
    :avg (let [sum (aget accs off)
               cnt (long (aget accs (+ off 1)))]
           (if (zero? cnt) nil (/ sum (double cnt))))
    ;; SUM/MIN/MAX — check count slot (last slot before next agg)
    (let [slots (var-acc-slots op)
          cnt (long (aget accs (+ off (dec slots))))]
      (if (zero? cnt) nil (aget accs off)))))

(defn dense-var-2d-to-flat
  "Convert 2D variable-width accumulators to flat format for delegation."
  [^"[[D" result-array ^long acc-size]
  (let [max-key (alength result-array)
        flat (double-array (* max-key acc-size))]
    (dotimes [k max-key]
      (when-let [^doubles accs (aget result-array k)]
        (System/arraycopy accs 0 flat (* (long k) acc-size) acc-size)))
    flat))

(defn decode-dense-var-group-results-flat
  "Decode flat double[] with variable-width accumulator layout into row maps.
   Flat layout: groups[key * accSize + aggOffset] with variable slots per agg.
   Used by chunked var group-by path."
  [^doubles flat-array max-key group-cols ^longs group-muls aggs group-dicts]
  (let [n-group (count group-cols)
        agg-offsets (compute-var-agg-offsets aggs)
        acc-size (long (+ (long (last agg-offsets)) (var-acc-slots (:op (last aggs)))))
        first-count-slot (dec (var-acc-slots (:op (first aggs))))]
    (persistent!
     (reduce
      (fn [out k]
        (let [base (long (* (long k) acc-size))]
          (if (<= (aget flat-array (+ base first-count-slot)) 0.0)
            out
            (let [group-vals (decode-key (long k) group-muls (long n-group))
                  group-vals (if group-dicts
                               (mapv (fn [v dict]
                                       (if dict
                                         (aget ^"[Ljava.lang.String;" dict (int (long v)))
                                         v))
                                     group-vals group-dicts)
                               group-vals)
                  row (transient (into {} (map vector group-cols group-vals)))]
              (doseq [a-idx (range (count aggs))]
                (let [agg (nth aggs a-idx)
                      alias (or (:as agg) (:op agg))
                      off (long (+ base (long (nth agg-offsets a-idx))))]
                  (assoc! row alias (decode-var-acc-value (:op agg) flat-array off))))
              (conj! out (persistent! row))))))
      (transient [])
      (range max-key)))))

(defn decode-dense-var-group-results-flat-columnar
  "Decode flat double[] with variable-width accumulator layout into columnar format.
   Used by chunked var group-by path."
  [^doubles flat-array max-key group-cols ^longs group-muls aggs group-dicts]
  (let [n-group (count group-cols)
        agg-offsets (compute-var-agg-offsets aggs)
        acc-size (long (+ (long (last agg-offsets)) (var-acc-slots (:op (last aggs)))))
        first-count-slot (dec (var-acc-slots (:op (first aggs))))
        key-list (java.util.ArrayList.)]
    (dotimes [k max-key]
      (when (> (aget flat-array (+ (* (long k) acc-size) first-count-slot)) 0.0)
        (.add key-list (long k))))
    (let [n (.size key-list)
          keys-arr (long-array n)]
      (dotimes [i n] (aset keys-arr i (long (.get key-list i))))
      (let [group-arrays (decode-group-key-columns keys-arr group-muls n-group group-dicts)
            agg-aliases (mapv #(or (:as %) (:op %)) aggs)
            agg-arrays
            (mapv
             (fn [a-idx]
               (let [agg (nth aggs a-idx)
                     agg-off (long (nth agg-offsets a-idx))
                     op (:op agg)]
                 (if (= :count op)
                   (let [result (long-array n)
                         cnt-off (+ agg-off (dec (var-acc-slots op)))]
                     (dotimes [i n]
                       (let [base (* (long (.get key-list i)) acc-size)]
                         (aset result i (long (aget flat-array (+ base cnt-off))))))
                     result)
                   (let [result (double-array n)]
                     (dotimes [i n]
                       (let [off (+ (* (long (.get key-list i)) acc-size) agg-off)]
                         (aset result i (double (decode-var-acc-value op flat-array off)))))
                     result))))
             (range (count aggs)))
            cnt-slot0 (+ (long (first agg-offsets)) (dec (var-acc-slots (:op (first aggs)))))
            count-arr (long-array n)]
        (dotimes [i n]
          (let [base (* (long (.get key-list i)) acc-size)]
            (aset count-arr i (long (aget flat-array (+ base cnt-slot0))))))
        (-> (into {} (map vector group-cols group-arrays))
            (into (map vector agg-aliases agg-arrays))
            (assoc :_count count-arr :n-rows n))))))

(defn decode-dense-var-group-results
  "Decode dense variable-width accumulator results from Java into row maps.
   Converts 2D to flat format and delegates."
  [^"[[D" result-array group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (reduce + (mapv #(var-acc-slots (:op %)) aggs))
        flat (dense-var-2d-to-flat result-array acc-size)]
    (decode-dense-var-group-results-flat flat (alength result-array) group-cols group-muls aggs group-dicts)))

(defn decode-dense-var-group-results-columnar
  "Decode dense variable-width accumulator results into columnar format.
   Converts 2D to flat format and delegates."
  [^"[[D" result-array group-cols ^longs group-muls aggs group-dicts]
  (let [acc-size (reduce + (mapv #(var-acc-slots (:op %)) aggs))
        flat (dense-var-2d-to-flat result-array acc-size)]
    (decode-dense-var-group-results-flat-columnar flat (alength result-array) group-cols group-muls aggs group-dicts)))

(defn apply-null-sentinels
  "Convert NULL sentinel group key values back to nil.
   null-sentinels is a vector of sentinel values (one per group col), nil entries mean no NULLs."
  [decoded group-cols null-sentinels columnar?]
  (if-not (some identity null-sentinels)
    decoded
    (if columnar?
      ;; Columnar: check each group column's array for sentinel values
      ;; For columns with null sentinels, we need to mark those positions
      ;; Since long[] can't hold nil, we convert to Object[] for affected columns
      (reduce (fn [result [idx col]]
                (let [sentinel (nth null-sentinels idx)]
                  (if-not sentinel
                    result
                    (let [sentinel (long sentinel)
                          arr (get result col)]
                      (if (expr/long-array? arr)
                        ;; Check if any values match sentinel — convert to Object[] with nil
                        (let [^longs la arr
                              n (alength la)
                              has-sentinel (loop [i 0]
                                             (if (>= i n) false
                                                 (if (= (aget la i) sentinel) true
                                                     (recur (inc i)))))]
                          (if has-sentinel
                            ;; Convert to Object array for nil support
                            (let [oa (object-array n)]
                              (dotimes [i n]
                                (let [v (aget la i)]
                                  (aset oa i (if (= v sentinel) nil v))))
                              (assoc result col oa))
                            result))
                        result)))))
              decoded
              (map-indexed vector group-cols))
      ;; Row-oriented: replace sentinel values with nil in each row
      (mapv (fn [row]
              (reduce (fn [m [idx col]]
                        (let [sentinel (nth null-sentinels idx)]
                          (if-not sentinel m
                                  (let [v (get m col)]
                                    (if (and (number? v) (== (long v) (long sentinel)))
                                      (assoc m col nil)
                                      m)))))
                      row (map-indexed vector group-cols)))
            decoded))))

(defn apply-group-offsets
  "Reverse normalization on decoded group column values.
   group-offsets is a vector of [offset gcd] pairs (one per group col), or nil if no normalization.
   Decoded value = raw * gcd + offset.
   null-sentinels is a vector of sentinel values per group col (or nil), used to restore nil for NULL groups."
  [decoded group-cols group-offsets null-sentinels columnar?]
  (let [decoded (if-not group-offsets
                  decoded
                  (if columnar?
                    ;; Columnar: reverse normalization on each group column's long array
                    (reduce (fn [result [idx col]]
                              (let [[off g] (nth group-offsets idx)
                                    off (long off) g (long g)]
                                (if (and (zero? off) (== g 1))
                                  result
                                  (let [^longs arr (get result col)
                                        n (alength arr)
                                        la (long-array n)]
                                    (if (== g 1)
                                      (dotimes [i n] (aset la i (+ (aget arr i) off)))
                                      (dotimes [i n] (aset la i (+ (* (aget arr i) g) off))))
                                    (assoc result col la)))))
                            decoded
                            (map-indexed vector group-cols))
                    ;; Row-oriented: reverse normalization on each row's group column values
                    (mapv (fn [row]
                            (reduce (fn [m [idx col]]
                                      (let [[off g] (nth group-offsets idx)
                                            off (long off) g (long g)]
                                        (if (and (zero? off) (== g 1)) m
                                            (if (== g 1)
                                              (update m col #(+ (long %) off))
                                              (update m col #(+ (* (long %) g) off))))))
                                    row (map-indexed vector group-cols)))
                          decoded)))]
    ;; After offset reversal, convert null sentinels to nil
    (apply-null-sentinels decoded group-cols null-sentinels columnar?)))

(defn match-sum-mul-const-sub
  "Match SUM(a * (c - b)) or SUM((c - b) * a) pattern.
   Returns {:col-a kw :col-b kw :const num} if matched, nil otherwise."
  [agg]
  (when (and (= :sum (:op agg)) (:expr agg))
    (let [expr (:expr agg)]
      (when (= :mul (:op expr))
        (let [[arg1 arg2] (:args expr)]
          (or
            ;; Pattern: a * (c - b)
           (when (and (keyword? arg1) (map? arg2)
                      (= :sub (:op arg2))
                      (= 2 (count (:args arg2)))
                      (number? (first (:args arg2)))
                      (keyword? (second (:args arg2))))
             {:col-a arg1 :col-b (second (:args arg2)) :const (first (:args arg2))})
            ;; Pattern: (c - b) * a
           (when (and (keyword? arg2) (map? arg1)
                      (= :sub (:op arg1))
                      (= 2 (count (:args arg1)))
                      (number? (first (:args arg1)))
                      (keyword? (second (:args arg1))))
             {:col-a arg2 :col-b (second (:args arg1)) :const (first (:args arg1))})))))))

(defn find-existing-sum-idx
  "Find index of an existing SUM(col) in the agg list. Returns index or nil."
  [out-aggs col]
  (some (fn [idx]
          (let [a (nth out-aggs idx)]
            (when (and (= :sum (:op a)) (= col (:col a)) (nil? (:expr a)))
              idx)))
        (range (count out-aggs))))

(defn decompose-compound-aggs
  "Decompose compound aggs (variance/stddev/corr/sum-mul-const-sub) into basic SUM/COUNT aggs.
   Returns {:aggs expanded-aggs :mapping {original-idx {:type :variance :indices {...}}}}
   The basic aggs are invisible to the user — they have hidden keyword aliases."
  [aggs col-arrays ^long length]
  (loop [i 0
         out-aggs []
         mapping {}
         remaining aggs]
    (if (empty? remaining)
      {:aggs (vec out-aggs) :mapping mapping}
      (let [agg (first remaining)
            op (:op agg)
            smcs (when (= :sum op) (match-sum-mul-const-sub agg))]
        (cond
          smcs
          ;; SUM(a * (c - b)) → c * SUM(a) - SUM_PRODUCT(a, b)
          (let [{:keys [col-a col-b const]} smcs
                ;; Find or add SUM(col-a) in the agg list
                existing-sum-idx (find-existing-sum-idx out-aggs col-a)
                sum-idx (or existing-sum-idx (count out-aggs))
                sum-alias (if existing-sum-idx
                            (:as (nth out-aggs existing-sum-idx))
                            (keyword (str "__sum_" i)))
                out-aggs (if existing-sum-idx
                           out-aggs
                           (conj out-aggs {:op :sum :col col-a :as sum-alias}))
                ;; Add SUM_PRODUCT(col-a, col-b)
                sp-idx (count out-aggs)
                sp-alias (keyword (str "__sp_" i))]
            (recur (inc i)
                   (conj out-aggs
                         {:op :sum-product :cols [col-a col-b] :as sp-alias})
                   (assoc mapping i {:type :sum-mul-const-sub
                                     :alias (or (:as agg) :sum)
                                     :const (double const)
                                     :sum-idx sum-idx
                                     :sumprod-idx sp-idx
                                     :sum-alias sum-alias
                                     :sumprod-alias sp-alias})
                   (rest remaining)))

          (#{:variance :variance-pop} op)
          (let [col (:col agg)
                idx-base (count out-aggs)
                sq-alias (keyword (str "__sq_" i))
                sq-col (expr/eval-expr-vectorized {:op :mul :args [col col]} col-arrays length nil)]
            (recur (inc i)
                   (conj out-aggs
                         {:op :sum :col col :as (keyword (str "__sum_" i))}
                         {:op :sum :col sq-alias :as (keyword (str "__sumsq_" i))}
                         {:op :count :col nil :as (keyword (str "__cnt_" i))})
                   (assoc mapping i {:type op
                                     :alias (or (:as agg) op)
                                     :sum-idx idx-base
                                     :sumsq-idx (inc idx-base)
                                     :count-idx (+ idx-base 2)
                                     :sq-col sq-alias
                                     :sq-data sq-col})
                   (rest remaining)))

          (#{:stddev :stddev-pop} op)
          (let [col (:col agg)
                idx-base (count out-aggs)
                sq-alias (keyword (str "__sq_" i))
                sq-col (expr/eval-expr-vectorized {:op :mul :args [col col]} col-arrays length nil)]
            (recur (inc i)
                   (conj out-aggs
                         {:op :sum :col col :as (keyword (str "__sum_" i))}
                         {:op :sum :col sq-alias :as (keyword (str "__sumsq_" i))}
                         {:op :count :col nil :as (keyword (str "__cnt_" i))})
                   (assoc mapping i {:type op
                                     :alias (or (:as agg) op)
                                     :sum-idx idx-base
                                     :sumsq-idx (inc idx-base)
                                     :count-idx (+ idx-base 2)
                                     :sq-col sq-alias
                                     :sq-data sq-col})
                   (rest remaining)))

          (= :corr op)
          (let [[cx cy] (:cols agg)
                idx-base (count out-aggs)
                xy-alias (keyword (str "__xy_" i))
                xx-alias (keyword (str "__xx_" i))
                yy-alias (keyword (str "__yy_" i))
                xy-col (expr/eval-expr-vectorized {:op :mul :args [cx cy]} col-arrays length nil)
                xx-col (expr/eval-expr-vectorized {:op :mul :args [cx cx]} col-arrays length nil)
                yy-col (expr/eval-expr-vectorized {:op :mul :args [cy cy]} col-arrays length nil)]
            (recur (inc i)
                   (conj out-aggs
                         {:op :sum :col cx :as (keyword (str "__sumx_" i))}
                         {:op :sum :col cy :as (keyword (str "__sumy_" i))}
                         {:op :sum :col xy-alias :as (keyword (str "__sumxy_" i))}
                         {:op :sum :col xx-alias :as (keyword (str "__sumxx_" i))}
                         {:op :sum :col yy-alias :as (keyword (str "__sumyy_" i))}
                         {:op :count :col nil :as (keyword (str "__cnt_" i))})
                   (assoc mapping i {:type :corr
                                     :alias (or (:as agg) :corr)
                                     :sumx-idx idx-base
                                     :sumy-idx (inc idx-base)
                                     :sumxy-idx (+ idx-base 2)
                                     :sumxx-idx (+ idx-base 3)
                                     :sumyy-idx (+ idx-base 4)
                                     :count-idx (+ idx-base 5)
                                     :xy-col xy-alias :xy-data xy-col
                                     :xx-col xx-alias :xx-data xx-col
                                     :yy-col yy-alias :yy-data yy-col})
                   (rest remaining)))

          :else
          ;; Non-compound — pass through
          (recur (inc i)
                 (conj out-aggs agg)
                 (assoc mapping i {:type :passthrough :alias (or (:as agg) (:op agg)) :idx (count out-aggs)})
                 (rest remaining)))))))

;; compute-variance moved earlier (before decode-dense-var-group-results)

(defn recompose-row-results
  "Recompose a single row-based result map from decomposed aggs."
  [row orig-aggs mapping]
  (reduce-kv
   (fn [acc orig-idx m]
     (case (:type m)
       :passthrough
       acc  ;; already has the right key

       (:variance :variance-pop)
       (let [alias (:alias m)
             sum (double (get row (keyword (str "__sum_" orig-idx))))
             sumsq (double (get row (keyword (str "__sumsq_" orig-idx))))
             cnt (long (get row (keyword (str "__cnt_" orig-idx))))
             var-fn (if (= (:type m) :variance-pop) compute-variance-pop compute-variance)]
         (-> acc
             (assoc alias (var-fn sum sumsq cnt))
             (dissoc (keyword (str "__sum_" orig-idx))
                     (keyword (str "__sumsq_" orig-idx))
                     (keyword (str "__cnt_" orig-idx)))))

       (:stddev :stddev-pop)
       (let [alias (:alias m)
             sum (double (get row (keyword (str "__sum_" orig-idx))))
             sumsq (double (get row (keyword (str "__sumsq_" orig-idx))))
             cnt (long (get row (keyword (str "__cnt_" orig-idx))))
             var-fn (if (= (:type m) :stddev-pop) compute-variance-pop compute-variance)]
         (-> acc
             (assoc alias (Math/sqrt (var-fn sum sumsq cnt)))
             (dissoc (keyword (str "__sum_" orig-idx))
                     (keyword (str "__sumsq_" orig-idx))
                     (keyword (str "__cnt_" orig-idx)))))

       :corr
       (let [alias (:alias m)
             sx (double (get row (keyword (str "__sumx_" orig-idx))))
             sy (double (get row (keyword (str "__sumy_" orig-idx))))
             sxy (double (get row (keyword (str "__sumxy_" orig-idx))))
             sxx (double (get row (keyword (str "__sumxx_" orig-idx))))
             syy (double (get row (keyword (str "__sumyy_" orig-idx))))
             n (long (get row (keyword (str "__cnt_" orig-idx))))]
         (-> acc
             (assoc alias (compute-correlation sx sy sxy sxx syy n))
             (dissoc (keyword (str "__sumx_" orig-idx))
                     (keyword (str "__sumy_" orig-idx))
                     (keyword (str "__sumxy_" orig-idx))
                     (keyword (str "__sumxx_" orig-idx))
                     (keyword (str "__sumyy_" orig-idx))
                     (keyword (str "__cnt_" orig-idx)))))

       :sum-mul-const-sub
       (let [alias (:alias m)
             sum-alias (:sum-alias m)
             sum-val (double (get row sum-alias))
             sp-val (double (get row (:sumprod-alias m)))
             acc (-> acc
                     (assoc alias (- (* (:const m) sum-val) sp-val))
                     (dissoc (:sumprod-alias m)))]
          ;; Only remove synthetic SUM (not user-visible ones)
         (if (.startsWith (name sum-alias) "__")
           (dissoc acc sum-alias)
           acc))))
   row mapping))

(defn recompose-columnar-results
  "Recompose columnar results from decomposed aggs."
  [result orig-aggs mapping]
  (let [n-rows (:n-rows result)]
    (reduce-kv
     (fn [acc orig-idx m]
       (case (:type m)
         :passthrough acc

         :variance
         (let [alias (:alias m)
               ^doubles sums (get acc (keyword (str "__sum_" orig-idx)))
               ^doubles sumsqs (get acc (keyword (str "__sumsq_" orig-idx)))
               ^longs cnts (get acc (keyword (str "__cnt_" orig-idx)))
               out (double-array n-rows)]
           (dotimes [i n-rows]
             (aset out i (compute-variance (aget sums i) (aget sumsqs i) (aget cnts i))))
           (-> acc
               (assoc alias out)
               (dissoc (keyword (str "__sum_" orig-idx))
                       (keyword (str "__sumsq_" orig-idx))
                       (keyword (str "__cnt_" orig-idx)))))

         :stddev
         (let [alias (:alias m)
               ^doubles sums (get acc (keyword (str "__sum_" orig-idx)))
               ^doubles sumsqs (get acc (keyword (str "__sumsq_" orig-idx)))
               ^longs cnts (get acc (keyword (str "__cnt_" orig-idx)))
               out (double-array n-rows)]
           (dotimes [i n-rows]
             (aset out i (Math/sqrt (compute-variance (aget sums i) (aget sumsqs i) (aget cnts i)))))
           (-> acc
               (assoc alias out)
               (dissoc (keyword (str "__sum_" orig-idx))
                       (keyword (str "__sumsq_" orig-idx))
                       (keyword (str "__cnt_" orig-idx)))))

         :corr
         (let [alias (:alias m)
               ^doubles sxs (get acc (keyword (str "__sumx_" orig-idx)))
               ^doubles sys (get acc (keyword (str "__sumy_" orig-idx)))
               ^doubles sxys (get acc (keyword (str "__sumxy_" orig-idx)))
               ^doubles sxxs (get acc (keyword (str "__sumxx_" orig-idx)))
               ^doubles syys (get acc (keyword (str "__sumyy_" orig-idx)))
               ^longs cnts (get acc (keyword (str "__cnt_" orig-idx)))
               out (double-array n-rows)]
           (dotimes [i n-rows]
             (aset out i (double (compute-correlation
                                  (aget sxs i) (aget sys i) (aget sxys i)
                                  (aget sxxs i) (aget syys i) (long (aget cnts i))))))
           (-> acc
               (assoc alias out)
               (dissoc (keyword (str "__sumx_" orig-idx))
                       (keyword (str "__sumy_" orig-idx))
                       (keyword (str "__sumxy_" orig-idx))
                       (keyword (str "__sumxx_" orig-idx))
                       (keyword (str "__sumyy_" orig-idx))
                       (keyword (str "__cnt_" orig-idx)))))

         :sum-mul-const-sub
         (let [alias (:alias m)
               sum-alias (:sum-alias m)
               ^doubles sum-arr (get acc sum-alias)
               ^doubles sp-arr (get acc (:sumprod-alias m))
               c (double (:const m))
               out (double-array n-rows)]
           (dotimes [i n-rows]
             (aset out i (- (* c (aget sum-arr i)) (aget sp-arr i))))
           (let [acc (-> acc
                         (assoc alias out)
                         (dissoc (:sumprod-alias m)))]
             (if (.startsWith (name sum-alias) "__")
               (dissoc acc sum-alias)
               acc)))))
     result mapping)))

(defn has-compound-aggs?
  "Check if any aggs are compound (variance/stddev/corr) or decomposable expressions."
  [aggs]
  (some #(or (#{:variance :variance-pop :stddev :stddev-pop :corr} (:op %))
             (match-sum-mul-const-sub %))
        aggs))

(defn has-native-compound-aggs?
  "Check if any aggs need the native ColumnOpsVar VARIANCE/CORR path."
  [aggs]
  (some #(#{:variance :variance-pop :stddev :stddev-pop :corr} (:op %)) aggs))

(defn decode-dense-count-distinct-results
  "Decode merged numeric double[][] + count-distinct int[] results into row maps.
   cd-results is a map of {agg-idx int[]}, numeric-aggs/cd-aggs partition the original aggs."
  [numeric-result cd-results aggs group-cols group-muls group-dicts max-key-inc]
  (let [^"[[D" numeric-result numeric-result
        ^longs group-muls group-muls
        max-key-inc (int max-key-inc)
        n-group (count group-cols)
        ;; Build index: which agg indices are numeric vs count-distinct
        numeric-indices (vec (keep-indexed (fn [i a] (when (not= :count-distinct (:op a)) i)) aggs))
        cd-indices (vec (keep-indexed (fn [i a] (when (= :count-distinct (:op a)) i)) aggs))
        results (transient [])]
    (dotimes [key max-key-inc]
      ;; A group is active if numeric result has data OR any cd result has data
      (let [^doubles accs (when numeric-result (aget numeric-result key))
            has-numeric? (and accs (> (aget accs 1) 0.0))
            has-cd? (some (fn [cd-idx]
                            (let [^ints cd-arr (get cd-results cd-idx)]
                              (> (aget cd-arr key) 0)))
                          cd-indices)
            active? (or has-numeric? has-cd?)]
        (when active?
          (let [group-vals (decode-key (long key) group-muls (long n-group))
                group-vals (if group-dicts
                             (mapv (fn [v dict]
                                     (if dict
                                       (aget ^"[Ljava.lang.String;" dict (int (long v)))
                                       v))
                                   group-vals group-dicts)
                             group-vals)
                base-map (into {} (map vector group-cols group-vals))
                ;; Add _count from numeric result if available
                cnt (if has-numeric? (long (aget accs 1)) 0)
                m (assoc base-map :_count cnt)
                ;; Add numeric agg results
                m (reduce (fn [m ni]
                            (let [agg (nth aggs ni)
                                  alias (or (:as agg) (:op agg))
                                  ;; Find position in the numeric-only agg array
                                  pos (.indexOf ^java.util.List numeric-indices ni)
                                  agg-base (* pos 2)
                                  c (if accs (long (aget accs (inc agg-base))) 0)
                                  val (case (:op agg)
                                        :count c
                                        (:sum :sum-product :min :max) (if accs (aget accs agg-base) 0.0)
                                        :avg (if (or (nil? accs) (zero? c)) Double/NaN
                                                 (/ (aget accs agg-base) (double c)))
                                        (if accs (aget accs agg-base) 0.0))]
                              (assoc m alias val)))
                          m numeric-indices)
                ;; Add count-distinct results
                m (reduce (fn [m ci]
                            (let [agg (nth aggs ci)
                                  alias (or (:as agg) (:op agg))
                                  ^ints cd-arr (get cd-results ci)]
                              (assoc m alias (long (aget cd-arr key)))))
                          m cd-indices)]
            (conj! results m)))))
    (persistent! results)))

(defn decode-dense-count-distinct-results-columnar
  "Decode merged numeric double[][] + count-distinct int[] results into columnar format."
  [numeric-result cd-results aggs group-cols group-muls group-dicts max-key-inc]
  (let [^"[[D" numeric-result numeric-result
        ^longs group-muls group-muls
        max-key-inc (int max-key-inc)
        n-group (count group-cols)
        numeric-indices (vec (keep-indexed (fn [i a] (when (not= :count-distinct (:op a)) i)) aggs))
        cd-indices (vec (keep-indexed (fn [i a] (when (= :count-distinct (:op a)) i)) aggs))
        ;; Collect active keys
        key-list (java.util.ArrayList.)]
    (dotimes [k max-key-inc]
      (let [^doubles accs (when numeric-result (aget numeric-result k))
            has-numeric? (and accs (> (aget accs 1) 0.0))
            has-cd? (some (fn [cd-idx]
                            (let [^ints cd-arr (get cd-results cd-idx)]
                              (> (aget cd-arr k) 0)))
                          cd-indices)]
        (when (or has-numeric? has-cd?)
          (.add key-list (long k)))))
    (let [n (.size key-list)
          ;; Build group key arrays
          keys-arr (long-array n)]
      (dotimes [i n] (aset keys-arr i (long (.get key-list i))))
      (let [group-arrays (decode-group-key-columns keys-arr group-muls n-group group-dicts)
            ;; Build agg column arrays
            agg-aliases (mapv #(or (:as %) (:op %)) aggs)
            agg-arrays (mapv (fn [agg-idx]
                               (let [agg (nth aggs agg-idx)]
                                 (if (= :count-distinct (:op agg))
                                   ;; Count-distinct → long[]
                                   (let [^ints cd-arr (get cd-results agg-idx)
                                         out (long-array n)]
                                     (dotimes [i n]
                                       (aset out i (long (aget cd-arr (int (aget keys-arr i))))))
                                     out)
                                   ;; Numeric agg → double[] or long[]
                                   (let [pos (.indexOf ^java.util.List numeric-indices (int agg-idx))
                                         agg-base (* pos 2)
                                         is-count? (= :count (:op agg))
                                         out (if is-count? (long-array n) (double-array n))]
                                     (dotimes [i n]
                                       (let [k (int (aget keys-arr i))
                                             ^doubles accs (when numeric-result (aget numeric-result k))
                                             cnt (if accs (long (aget accs (inc agg-base))) 0)]
                                         (if is-count?
                                           (aset ^longs out i cnt)
                                           (aset ^doubles out i
                                                 (case (:op agg)
                                                   :avg (if (or (nil? accs) (zero? cnt)) Double/NaN
                                                            (/ (aget accs agg-base) (double cnt)))
                                                   (if accs (aget accs agg-base) 0.0))))))
                                     out))))
                             (range (count aggs)))]
        (-> (into {} (map vector group-cols group-arrays))
            (into (map vector agg-aliases agg-arrays))
            (assoc :n-rows n))))))

(defn decode-hash-cd-results
  "Decode hash-based COUNT DISTINCT results merged with optional numeric results.
   numeric-result: Object[] {long[] keys, double[] flatAccs} or nil
   cd-results: {agg-idx Object[]{long[] keys, int[] counts}}
   Returns vector of row maps."
  [numeric-result cd-results aggs numeric-indices group-cols ^longs group-muls group-dicts]
  (let [n-group (count group-cols)
        n-aggs (count aggs)
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        ;; Collect all unique group keys from CD results
        all-keys (java.util.LinkedHashSet.)
        _ (doseq [[_idx ^objects cd-res] cd-results]
            (let [^longs ks (aget cd-res 0)]
              (dotimes [i (alength ks)] (.add all-keys (aget ks i)))))
        ;; Also add keys from numeric result
        _ (when numeric-result
            (let [^longs nk (aget ^objects numeric-result 0)]
              (dotimes [i (alength nk)] (.add all-keys (aget nk i)))))
        ;; Build lookup maps for each CD result: key → count
        cd-lookups (into {} (map (fn [[idx ^objects cd-res]]
                                   (let [^longs ks (aget cd-res 0)
                                         ^ints cs (aget cd-res 1)
                                         m (java.util.HashMap.)]
                                     (dotimes [i (alength ks)]
                                       (.put m (aget ks i) (aget cs i)))
                                     [idx m]))
                                 cd-results))
        ;; Build lookup for numeric result: key → index
        numeric-lookup (when numeric-result
                         (let [^longs nk (aget ^objects numeric-result 0)
                               m (java.util.HashMap.)]
                           (dotimes [i (alength nk)]
                             (.put m (aget nk i) (int i)))
                           m))
        ^doubles numeric-accs (when numeric-result (aget ^objects numeric-result 1))
        n-numeric (count numeric-indices)
        stride (int (* 2 n-numeric))]
    (reduce (fn [results key-long]
              (let [key (long key-long)
                    group-vals (decode-key key group-muls (long n-group))
                    group-vals (if group-dicts
                                 (mapv (fn [v dict]
                                         (if dict (aget ^"[Ljava.lang.String;" dict (int (long v))) v))
                                       group-vals group-dicts)
                                 group-vals)
                    m (into {} (map vector group-cols group-vals))
                    m (reduce (fn [m i]
                                (let [agg (nth aggs i)
                                      alias (nth agg-aliases i)]
                                  (if (= :count-distinct (:op agg))
                                    (let [^java.util.HashMap lm (get cd-lookups i)
                                          cd-count (if lm (or (.get lm key) 0) 0)]
                                      (assoc m alias (long cd-count)))
                                    ;; Numeric agg
                                    (let [pos (.indexOf ^java.util.List numeric-indices (int i))
                                          ni (when numeric-lookup (.get ^java.util.HashMap numeric-lookup key))
                                          agg-base (when ni (+ (* (int ni) stride) (* pos 2)))
                                          cnt (if agg-base (long (aget numeric-accs (inc (int agg-base)))) 0)
                                          val (case (:op agg)
                                                :count cnt
                                                (:sum :sum-product :min :max) (if agg-base (aget numeric-accs (int agg-base)) 0.0)
                                                :avg (if (or (nil? agg-base) (zero? cnt)) Double/NaN
                                                         (/ (aget numeric-accs (int agg-base)) (double cnt)))
                                                (if agg-base (aget numeric-accs (int agg-base)) 0.0))]
                                      (assoc m alias val)))))
                              m (range n-aggs))
                    m (assoc m :_count (if numeric-lookup
                                         (let [ni (.get ^java.util.HashMap numeric-lookup key)]
                                           (if ni (long (aget numeric-accs (inc (* (int ni) stride)))) 0))
                                         0))]
                (conj results m)))
            [] all-keys)))

(defn decode-hash-cd-results-columnar
  "Decode hash-based COUNT DISTINCT results into columnar format.
   Returns {:col array ... :n-rows N}."
  [numeric-result cd-results aggs numeric-indices group-cols ^longs group-muls group-dicts]
  (let [n-group (count group-cols)
        n-aggs (count aggs)
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        ;; Collect all unique group keys
        all-keys (java.util.LinkedHashSet.)
        _ (doseq [[_idx ^objects cd-res] cd-results]
            (let [^longs ks (aget cd-res 0)]
              (dotimes [i (alength ks)] (.add all-keys (aget ks i)))))
        _ (when numeric-result
            (let [^longs nk (aget ^objects numeric-result 0)]
              (dotimes [i (alength nk)] (.add all-keys (aget nk i)))))
        n (.size all-keys)
        keys-arr (long-array n)
        _ (let [iter (.iterator all-keys)]
            (dotimes [i n] (aset keys-arr i (long (.next iter)))))
        ;; Build group key columns
        group-arrays (decode-group-key-columns keys-arr group-muls n-group group-dicts)
        ;; Build CD lookups
        cd-lookups (into {} (map (fn [[idx ^objects cd-res]]
                                   (let [^longs ks (aget cd-res 0)
                                         ^ints cs (aget cd-res 1)
                                         m (java.util.HashMap.)]
                                     (dotimes [i (alength ks)]
                                       (.put m (aget ks i) (aget cs i)))
                                     [idx m]))
                                 cd-results))
        numeric-lookup (when numeric-result
                         (let [^longs nk (aget ^objects numeric-result 0)
                               m (java.util.HashMap.)]
                           (dotimes [i (alength nk)] (.put m (aget nk i) (int i)))
                           m))
        ^doubles numeric-accs (when numeric-result (aget ^objects numeric-result 1))
        n-numeric (count numeric-indices)
        stride (int (* 2 n-numeric))
        ;; Build agg columns
        agg-arrays (mapv (fn [agg-idx]
                           (let [agg (nth aggs agg-idx)]
                             (if (= :count-distinct (:op agg))
                               (let [^java.util.HashMap lm (get cd-lookups agg-idx)
                                     out (long-array n)]
                                 (dotimes [i n]
                                   (aset out i (long (or (when lm (.get lm (aget keys-arr i))) 0))))
                                 out)
                               (let [pos (.indexOf ^java.util.List numeric-indices (int agg-idx))
                                     is-count? (= :count (:op agg))
                                     out (if is-count? (long-array n) (double-array n))]
                                 (dotimes [i n]
                                   (let [k (aget keys-arr i)
                                         ni (when numeric-lookup (.get ^java.util.HashMap numeric-lookup k))
                                         agg-base (when ni (+ (* (int ni) stride) (* pos 2)))
                                         cnt (if agg-base (long (aget numeric-accs (inc (int agg-base)))) 0)]
                                     (if is-count?
                                       (aset ^longs out i cnt)
                                       (aset ^doubles out i
                                             (case (:op agg)
                                               :avg (if (or (nil? agg-base) (zero? cnt)) Double/NaN
                                                        (/ (aget numeric-accs (int agg-base)) (double cnt)))
                                               (if agg-base (aget numeric-accs (int agg-base)) 0.0))))))
                                 out))))
                         (range n-aggs))]
    (-> (into {} (map vector group-cols group-arrays))
        (into (map vector agg-aliases agg-arrays))
        (assoc :n-rows n))))

(defn decode-hash-cd-results-multi-key
  "Decode hash-based COUNT DISTINCT results using multi-key reverse lookup.
   Like decode-hash-cd-results but uses key-lookup HashMap for group column decoding."
  [numeric-result cd-results aggs numeric-indices group-cols ^java.util.HashMap key-lookup group-dicts]
  (let [n-group (count group-cols)
        n-aggs (count aggs)
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        all-keys (java.util.LinkedHashSet.)
        _ (doseq [[_idx ^objects cd-res] cd-results]
            (let [^longs ks (aget cd-res 0)]
              (dotimes [i (alength ks)] (.add all-keys (aget ks i)))))
        _ (when numeric-result
            (let [^longs nk (aget ^objects numeric-result 0)]
              (dotimes [i (alength nk)] (.add all-keys (aget nk i)))))
        cd-lookups (into {} (map (fn [[idx ^objects cd-res]]
                                   (let [^longs ks (aget cd-res 0)
                                         ^ints cs (aget cd-res 1)
                                         m (java.util.HashMap.)]
                                     (dotimes [i (alength ks)]
                                       (.put m (aget ks i) (aget cs i)))
                                     [idx m]))
                                 cd-results))
        numeric-lookup (when numeric-result
                         (let [^longs nk (aget ^objects numeric-result 0)
                               m (java.util.HashMap.)]
                           (dotimes [i (alength nk)]
                             (.put m (aget nk i) (int i)))
                           m))
        ^doubles numeric-accs (when numeric-result (aget ^objects numeric-result 1))
        n-numeric (count numeric-indices)
        stride (int (* 2 n-numeric))]
    (reduce (fn [results key-long]
              (let [key (long key-long)
                    ^longs gvals (.get key-lookup (Long/valueOf key))
                    group-vals (mapv (fn [g]
                                       (let [v (aget gvals (int g))]
                                         (if (and group-dicts (nth group-dicts g))
                                           (aget ^"[Ljava.lang.String;" (nth group-dicts g) (int v))
                                           v)))
                                     (range n-group))
                    m (into {} (map vector group-cols group-vals))
                    m (reduce (fn [m i]
                                (let [agg (nth aggs i)
                                      alias (nth agg-aliases i)]
                                  (if (= :count-distinct (:op agg))
                                    (let [^java.util.HashMap lm (get cd-lookups i)
                                          cd-count (if lm (or (.get lm key) 0) 0)]
                                      (assoc m alias (long cd-count)))
                                    (let [pos (.indexOf ^java.util.List numeric-indices (int i))
                                          ni (when numeric-lookup (.get ^java.util.HashMap numeric-lookup key))
                                          agg-base (when ni (+ (* (int ni) stride) (* pos 2)))
                                          cnt (if agg-base (long (aget numeric-accs (inc (int agg-base)))) 0)
                                          val (case (:op agg)
                                                :count cnt
                                                (:sum :sum-product :min :max) (if agg-base (aget numeric-accs (int agg-base)) 0.0)
                                                :avg (if (or (nil? agg-base) (zero? cnt)) Double/NaN
                                                         (/ (aget numeric-accs (int agg-base)) (double cnt)))
                                                (if agg-base (aget numeric-accs (int agg-base)) 0.0))]
                                      (assoc m alias val)))))
                              m (range n-aggs))
                    m (assoc m :_count (if numeric-lookup
                                         (let [ni (.get ^java.util.HashMap numeric-lookup key)]
                                           (if ni (long (aget numeric-accs (inc (* (int ni) stride)))) 0))
                                         0))]
                (conj results m)))
            [] all-keys)))

(defn decode-hash-cd-results-multi-key-columnar
  "Decode hash-based COUNT DISTINCT results into columnar format using multi-key lookup."
  [numeric-result cd-results aggs numeric-indices group-cols ^java.util.HashMap key-lookup group-dicts]
  (let [n-group (count group-cols)
        n-aggs (count aggs)
        agg-aliases (mapv #(or (:as %) (:op %)) aggs)
        all-keys (java.util.LinkedHashSet.)
        _ (doseq [[_idx ^objects cd-res] cd-results]
            (let [^longs ks (aget cd-res 0)]
              (dotimes [i (alength ks)] (.add all-keys (aget ks i)))))
        _ (when numeric-result
            (let [^longs nk (aget ^objects numeric-result 0)]
              (dotimes [i (alength nk)] (.add all-keys (aget nk i)))))
        n (.size all-keys)
        keys-arr (long-array n)
        _ (let [iter (.iterator all-keys)]
            (dotimes [i n] (aset keys-arr i (long (.next iter)))))
        ;; Build group key columns using multi-key lookup
        group-arrays (decode-group-key-columns-multi-key keys-arr key-lookup n-group group-dicts)
        cd-lookups (into {} (map (fn [[idx ^objects cd-res]]
                                   (let [^longs ks (aget cd-res 0)
                                         ^ints cs (aget cd-res 1)
                                         m (java.util.HashMap.)]
                                     (dotimes [i (alength ks)]
                                       (.put m (aget ks i) (aget cs i)))
                                     [idx m]))
                                 cd-results))
        numeric-lookup (when numeric-result
                         (let [^longs nk (aget ^objects numeric-result 0)
                               m (java.util.HashMap.)]
                           (dotimes [i (alength nk)] (.put m (aget nk i) (int i)))
                           m))
        ^doubles numeric-accs (when numeric-result (aget ^objects numeric-result 1))
        n-numeric (count numeric-indices)
        stride (int (* 2 n-numeric))
        agg-arrays (mapv (fn [agg-idx]
                           (let [agg (nth aggs agg-idx)]
                             (if (= :count-distinct (:op agg))
                               (let [^java.util.HashMap lm (get cd-lookups agg-idx)
                                     out (long-array n)]
                                 (dotimes [i n]
                                   (aset out i (long (or (when lm (.get lm (aget keys-arr i))) 0))))
                                 out)
                               (let [pos (.indexOf ^java.util.List numeric-indices (int agg-idx))
                                     is-count? (= :count (:op agg))
                                     out (if is-count? (long-array n) (double-array n))]
                                 (dotimes [i n]
                                   (let [k (aget keys-arr i)
                                         ni (when numeric-lookup (.get ^java.util.HashMap numeric-lookup k))
                                         agg-base (when ni (+ (* (int ni) stride) (* pos 2)))
                                         cnt (if agg-base (long (aget numeric-accs (inc (int agg-base)))) 0)]
                                     (if is-count?
                                       (aset ^longs out i cnt)
                                       (aset ^doubles out i
                                             (case (:op agg)
                                               :avg (if (or (nil? agg-base) (zero? cnt)) Double/NaN
                                                        (/ (aget numeric-accs (int agg-base)) (double cnt)))
                                               (if agg-base (aget numeric-accs (int agg-base)) 0.0))))))
                                 out))))
                         (range n-aggs))]
    (-> (into {} (map vector group-cols group-arrays))
        (into (map vector agg-aliases agg-arrays))
        (assoc :n-rows n))))

(defn agg-op->java-type
  "Map agg op keyword to Java AGG constant."
  [op]
  (case op
    :sum           (int ColumnOps/AGG_SUM)
    :sum-product   (int ColumnOps/AGG_SUM_PRODUCT)
    :count         (int ColumnOps/AGG_COUNT)
    :min           (int ColumnOps/AGG_MIN)
    :max           (int ColumnOps/AGG_MAX)
    :avg           (int ColumnOps/AGG_SUM)
    :count-distinct (int ColumnOps/AGG_COUNT)
    (int ColumnOps/AGG_SUM)))

(defn replace-null-sentinel
  "Replace Long.MIN_VALUE (NULL) with the given replacement value in a long[] array.
   Returns a new array."
  ^longs [^longs col ^long replacement ^long length]
  (let [out (long-array length)]
    (dotimes [i length]
      (let [v (aget col i)]
        (aset out i (if (= v Long/MIN_VALUE) replacement v))))
    out))

(defn prepare-group-key-arrays
  "Prepare group key columns, compute normalization and strides.
   Returns {:group-arrays :group-muls :group-maxes :group-dicts :group-offsets :max-key :use-dense? :n-group :null-sentinels}."
  [group-cols columns ^long length]
  (let [n-group (count group-cols)
        group-col-data (mapv #(:data (get columns %)) group-cols)
        group-dicts (let [dicts (mapv #(:dict (get columns %)) group-cols)]
                      (when (some identity dicts) dicts))
        ;; arrayMaxMinLong now returns [max, min, hasNull] — skips Long.MIN_VALUE
        group-max-mins (mapv (fn [^longs col col-key]
                               (let [col-info (get columns col-key)]
                                 (if-let [dict (:dict col-info)]
                                   ;; Dict-encoded: max = dict_size - 1, min = 0
                                   ;; Check for NULL (Long.MIN_VALUE) in encoded data
                                   (let [has-null (loop [i 0]
                                                    (if (>= i (alength col)) 0
                                                        (if (= (aget col i) Long/MIN_VALUE) 1
                                                            (recur (inc i)))))]
                                     (long-array [(dec (alength ^"[Ljava.lang.String;" dict)) 0 has-null]))
                                   (ColumnOpsExt/arrayMaxMinLong col (alength col)))))
                             group-col-data group-cols)
        ;; Track which columns have NULLs
        has-nulls (mapv (fn [^longs mm] (== 1 (aget mm 2))) group-max-mins)
        any-nulls? (some true? has-nulls)
        group-maxes-raw (mapv (fn [^longs mm] (aget mm 0)) group-max-mins)
        group-mins-raw (mapv (fn [^longs mm] (aget mm 1)) group-max-mins)
        dense-limit (long (or *dense-group-limit* 100000))
        raw-max-key-est (try (long (reduce * (mapv (fn [mx has-null]
                                                     (+ (inc (long mx)) (if has-null 1 0)))
                                                   group-maxes-raw has-nulls)))
                             (catch ArithmeticException _ Long/MAX_VALUE))
        offset-norm-est (when (> raw-max-key-est dense-limit)
                          (try (long (reduce * (mapv (fn [mx mn has-null]
                                                       (+ (inc (Math/subtractExact (long mx) (long mn)))
                                                          (if has-null 1 0)))
                                                     group-maxes-raw group-mins-raw has-nulls)))
                               (catch ArithmeticException _ Long/MAX_VALUE)))
        need-offset-only? (and offset-norm-est (<= offset-norm-est dense-limit))
        [group-col-data group-maxes group-offsets null-sentinels]
        (if need-offset-only?
          (let [len (int length)
                ;; Subtract min from each column, then replace Long.MIN_VALUE with (max-min+1)
                new-data-and-sentinels
                (mapv (fn [^longs col mn mx has-null]
                        (let [mn (long mn) mx (long mx)
                              norm-max (- mx mn)
                              null-slot (inc norm-max)] ;; NULL maps to one beyond range
                          (if (zero? mn)
                            ;; No offset needed, just replace NULLs
                            (if has-null
                              [(replace-null-sentinel col null-slot len) null-slot]
                              [col nil])
                            ;; Subtract min, then replace NULLs
                            (let [subtracted (ColumnOps/arraySubLongScalar col mn len)]
                              (if has-null
                                [(replace-null-sentinel subtracted null-slot len) null-slot]
                                [subtracted nil])))))
                      group-col-data group-mins-raw group-maxes-raw has-nulls)
                new-data (mapv first new-data-and-sentinels)
                sentinels (mapv second new-data-and-sentinels)
                new-maxes (mapv (fn [mx mn has-null]
                                  (let [norm-max (- (long mx) (long mn))]
                                    (if has-null (inc norm-max) norm-max)))
                                group-maxes-raw group-mins-raw has-nulls)]
            [new-data new-maxes (mapv (fn [mn] [(long mn) 1]) group-mins-raw) sentinels])
          ;; No offset normalization — still handle NULLs if any
          (if any-nulls?
            (let [len (int length)
                  new-data-and-sentinels
                  (mapv (fn [^longs col mx has-null]
                          (let [mx (long mx)
                                null-slot (inc mx)]
                            (if has-null
                              [(replace-null-sentinel col null-slot len) null-slot]
                              [col nil])))
                        group-col-data group-maxes-raw has-nulls)
                  new-data (mapv first new-data-and-sentinels)
                  sentinels (mapv second new-data-and-sentinels)
                  new-maxes (mapv (fn [mx has-null]
                                    (if has-null (inc (long mx)) (long mx)))
                                  group-maxes-raw has-nulls)]
              [new-data new-maxes nil sentinels])
            [group-col-data group-maxes-raw nil nil]))
        group-arrays (into-array expr/long-array-class group-col-data)
        ;; Compute strides (muls) and max-key separately to detect overflow precisely.
        ;; Muls may fit in long even when max-key overflows (e.g., 6 cols × 100M rows).
        ;; When max-key overflows, key-overflow? signals that composite positional encoding
        ;; would produce wrong results — the hash path must use multi-key hashing instead.
        {:keys [group-muls max-key use-dense? key-overflow?]}
        (let [muls (long-array n-group)
              _ (aset muls (dec n-group) 1)
              muls-ok? (try
                         (loop [i (- n-group 2)]
                           (when (>= i 0)
                             (aset muls i (Math/multiplyExact
                                           (inc (long (nth group-maxes (inc i))))
                                           (aget muls (inc i))))
                             (recur (dec i))))
                         true
                         (catch ArithmeticException _ false))]
          (if muls-ok?
            ;; Muls computed OK — check if max-key fits
            (let [group-muls (long-array (vec muls))]
              (try
                (let [max-key (long (reduce (fn [^long acc i]
                                              (Math/addExact acc
                                                             (Math/multiplyExact (long (nth group-maxes i))
                                                                                 (aget group-muls i))))
                                            (long 0) (range n-group)))]
                  {:group-muls group-muls :max-key max-key
                   :use-dense? (<= (inc max-key) dense-limit)
                   :key-overflow? false})
                (catch ArithmeticException _
                  ;; Muls are correct but max-key overflows — composite keys will wrap
                  {:group-muls group-muls :max-key Long/MAX_VALUE
                   :use-dense? false :key-overflow? true})))
            ;; Even strides overflow — key space is enormous
            {:group-muls (long-array n-group) :max-key Long/MAX_VALUE
             :use-dense? false :key-overflow? true}))]
    {:group-arrays group-arrays :group-muls group-muls :group-maxes group-maxes
     :group-dicts group-dicts :group-offsets group-offsets :max-key max-key
     :use-dense? use-dense? :n-group n-group :key-overflow? (boolean key-overflow?)
     :null-sentinels null-sentinels}))

(defn prepare-agg-type-arrays
  "Prepare agg type/source column arrays for Java.
   Returns {:agg-types :agg-cols :agg-col2s :numeric-* variants :has-cd? :n-aggs}."
  [aggs col-arrays columns ^long length]
  (let [has-cd? (some #(= :count-distinct (:op %)) aggs)
        numeric-agg-indices (vec (keep-indexed (fn [i a] (when (not= :count-distinct (:op a)) i)) aggs))
        numeric-aggs (mapv #(nth aggs %) numeric-agg-indices)
        n-numeric (count numeric-aggs)
        n-aggs (count aggs)
        agg-types (int-array (mapv #(agg-op->java-type (:op %)) aggs))
        numeric-agg-types (int-array (mapv #(agg-op->java-type (:op %)) numeric-aggs))
        conv-cache (java.util.HashMap.)
        agg-cols (into-array expr/double-array-class
                             (mapv #(prepare-agg-source-col % col-arrays length conv-cache) aggs))
        agg-col2s (into-array expr/double-array-class
                              (mapv #(prepare-agg-source-col2 % col-arrays length conv-cache) aggs))
        numeric-agg-cols (into-array expr/double-array-class
                                     (mapv #(prepare-agg-source-col (nth aggs %) col-arrays length conv-cache) numeric-agg-indices))
        numeric-agg-col2s (into-array expr/double-array-class
                                      (mapv #(prepare-agg-source-col2 (nth aggs %) col-arrays length conv-cache) numeric-agg-indices))]
    {:has-cd? has-cd? :numeric-agg-indices numeric-agg-indices :numeric-aggs numeric-aggs
     :n-numeric n-numeric :n-aggs n-aggs :agg-types agg-types :numeric-agg-types numeric-agg-types
     :conv-cache conv-cache :agg-cols agg-cols :agg-col2s agg-col2s
     :numeric-agg-cols numeric-agg-cols :numeric-agg-col2s numeric-agg-col2s}))

(defn execute-group-by-java
  "Execute grouped aggregation via Java fusedFilterGroupAggregate.
   Single-pass in Java: filter predicates + group key + accumulate, no Clojure per-row overhead."
  [preds aggs group-cols columns length columnar?]
  (let [;; Decompose compound aggs (variance/stddev/corr) if present
        col-arrays-raw (into {} (map (fn [[k v]] [k (:data v)])) columns)
        compound? (has-compound-aggs? aggs)
        orig-aggs aggs
        {:keys [aggs mapping]}
        (if compound?
          (decompose-compound-aggs aggs col-arrays-raw length)
          {:aggs aggs :mapping nil})
        ;; Add any temp columns from decomposition
        columns (if compound?
                  (reduce-kv
                   (fn [cols _idx m]
                     (case (:type m)
                       (:variance :variance-pop :stddev :stddev-pop)
                       (assoc cols (:sq-col m) {:type :float64 :data (:sq-data m)})
                       :corr
                       (-> cols
                           (assoc (:xy-col m) {:type :float64 :data (:xy-data m)})
                           (assoc (:xx-col m) {:type :float64 :data (:xx-data m)})
                           (assoc (:yy-col m) {:type :float64 :data (:yy-data m)}))
                       cols))
                   columns mapping)
                  columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) columns)

        ;; Prepare predicates, group keys, and agg arrays
        {:keys [n-long long-pred-types long-cols long-lo long-hi
                n-dbl dbl-pred-types dbl-cols dbl-lo dbl-hi]} (prepare-pred-arrays preds columns)
        {:keys [group-arrays group-muls group-maxes group-dicts group-offsets
                max-key use-dense? n-group key-overflow? null-sentinels]} (prepare-group-key-arrays group-cols columns length)
        {:keys [has-cd? numeric-agg-indices numeric-aggs n-numeric n-aggs
                agg-types numeric-agg-types conv-cache agg-cols agg-col2s
                numeric-agg-cols numeric-agg-col2s]} (prepare-agg-type-arrays aggs col-arrays columns length)
        ;; Check if any agg source columns have NULLs — skip NaN masking when not needed
        nan-safe (boolean
                  (columns-have-nulls?
                   columns
                   (distinct (mapcat (fn [a]
                                       (case (:op a)
                                         :count []
                                         :count-distinct [(:col a)]
                                         (:sum-product :corr) (:cols a)
                                         [(:col a)]))
                                     aggs))))]

    ;; Call Java - dispatch between dense and HashMap paths
    ;; Dense path uses parallel execution for large datasets
    (if use-dense?
      ;; Native compound agg path: VARIANCE/CORR accumulators in Java (no decomposition overhead)
      (if (and (has-native-compound-aggs? orig-aggs) use-dense? (not has-cd?))
        (let [max-key-inc (int (inc max-key))
              ;; Use orig-aggs (before decomposition) for the native path
              native-aggs orig-aggs
              n-native (count native-aggs)
              ;; Map agg ops to Java constants (including VARIANCE/CORR)
              native-agg-types (int-array (mapv (fn [agg]
                                                  (case (:op agg)
                                                    (:sum :sum-product) (int ColumnOps/AGG_SUM)
                                                    :count             (int ColumnOps/AGG_COUNT)
                                                    :min               (int ColumnOps/AGG_MIN)
                                                    :max               (int ColumnOps/AGG_MAX)
                                                    :avg               (int ColumnOps/AGG_SUM)
                                                    (:variance :variance-pop :stddev :stddev-pop) (int ColumnOpsVar/AGG_VARIANCE)
                                                    :corr              (int ColumnOpsVar/AGG_CORR)
                                                    (int ColumnOps/AGG_SUM)))
                                                native-aggs))
              ;; Prepare aggCols (first source column per agg)
              native-agg-cols (into-array expr/double-array-class
                                          (mapv (fn [agg]
                                                  (case (:op agg)
                                                    :count (double-array 0)
                                                    :count-distinct (double-array 0)
                                                    :corr (expr/col-as-doubles-cached
                                                           (get col-arrays-raw (first (:cols agg))) length conv-cache)
                                                    :sum-product
                                                    (let [[c1 c2] (:cols agg)]
                                                      (ColumnOps/arrayMul
                                                       (expr/col-as-doubles-cached (get col-arrays-raw c1) length conv-cache)
                                                       (expr/col-as-doubles-cached (get col-arrays-raw c2) length conv-cache)
                                                       (int length)))
                                          ;; :sum :avg :variance :stddev :min :max
                                                    (if-let [expr (:expr agg)]
                                                      (expr/eval-expr-vectorized expr col-arrays-raw length conv-cache)
                                                      (expr/col-as-doubles-cached (get col-arrays-raw (:col agg)) length conv-cache))))
                                                native-aggs))
              ;; Prepare aggCols2 (second source column for CORR, null for others)
              native-agg-cols2 (into-array expr/double-array-class
                                           (mapv (fn [agg]
                                                   (if (= :corr (:op agg))
                                                     (expr/col-as-doubles-cached
                                                      (get col-arrays-raw (second (:cols agg))) length conv-cache)
                                                     (double-array 0)))
                                                 native-aggs))
              ^"[[D" result-array
              (ColumnOpsVar/fusedFilterGroupAggregateDenseVarParallel
               (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
               (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
               (int n-group) group-arrays group-muls
               (int n-native) native-agg-types ^"[[D" native-agg-cols ^"[[D" native-agg-cols2
               (int length) max-key-inc nan-safe)]
          (-> (if columnar?
                (decode-dense-var-group-results-columnar result-array group-cols group-muls native-aggs group-dicts)
                (decode-dense-var-group-results result-array group-cols group-muls native-aggs group-dicts))
              (apply-group-offsets group-cols group-offsets null-sentinels columnar?)))
        (let [all-count? (and (not has-cd?) (every? #(= :count (:op %)) aggs))
              max-key-inc (int (inc max-key))]
          (if has-cd?
          ;; COUNT DISTINCT path: split into numeric + CD aggs
            (let [;; Run numeric aggs (if any) through standard path
                ;; COUNT-only returns double[][], general returns flat double[] → convert to double[][] for CD decode
                  ^"[[D" numeric-result
                  (when (pos? n-numeric)
                    (let [all-num-count? (every? #(= :count (:op %)) numeric-aggs)]
                      (if all-num-count?
                        (ColumnOps/fusedFilterGroupCountDenseParallel
                         (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                         (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                         (int n-group) group-arrays group-muls
                         (int n-numeric) (int length) max-key-inc)
                      ;; General path returns flat double[] — reshape to double[][] for CD decode
                        (let [^doubles flat (ColumnOps/fusedFilterGroupAggregateDenseParallel
                                             (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                                             (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                                             (int n-group) group-arrays group-muls
                                             (int n-numeric) numeric-agg-types ^"[[D" numeric-agg-cols ^"[[D" numeric-agg-col2s
                                             (int length) max-key-inc nan-safe)
                              acc-size (int (* 2 n-numeric))
                              result-2d ^"[[D" (make-array Double/TYPE (int max-key-inc) (int acc-size))]
                          (dotimes [k max-key-inc]
                            (System/arraycopy flat (* k acc-size) (aget result-2d k) 0 acc-size))
                          result-2d))))
                ;; Run each COUNT DISTINCT agg through dedicated Java path
                  cd-results
                  (into {}
                        (keep-indexed
                         (fn [i agg]
                           (when (= :count-distinct (:op agg))
                             (let [col-data (:data (get columns (:col agg)))
                                   ^longs distinct-col (if (expr/long-array? col-data)
                                                         col-data
                                                    ;; double[] → convert to long[] for hashing
                                                         (let [^doubles d col-data
                                                               l (long-array length)]
                                                           (dotimes [j length]
                                                             (aset l j (Double/doubleToLongBits (aget d j))))
                                                           l))]
                               [i (ColumnOps/fusedFilterGroupCountDistinctDenseParallel
                                   (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                                   (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                                   (int n-group) group-arrays group-muls
                                   distinct-col (int length) max-key-inc)])))
                         aggs))
                  decoded (-> (if columnar?
                                (decode-dense-count-distinct-results-columnar
                                 numeric-result cd-results aggs group-cols group-muls group-dicts max-key-inc)
                                (decode-dense-count-distinct-results
                                 numeric-result cd-results aggs group-cols group-muls group-dicts max-key-inc))
                              (apply-group-offsets group-cols group-offsets null-sentinels columnar?))]
              (if compound?
                (if columnar?
                  (recompose-columnar-results decoded orig-aggs mapping)
                  (mapv #(recompose-row-results % orig-aggs mapping) decoded))
                decoded))
          ;; No COUNT DISTINCT — existing paths
            (if all-count?
            ;; JIT-isolated COUNT-only path: returns double[][] (has its own flat long[] internally)
              (let [^"[[D" result-array
                    (ColumnOps/fusedFilterGroupCountDenseParallel
                     (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                     (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                     (int n-group) group-arrays group-muls
                     (int n-aggs) (int length) max-key-inc)
                    decoded (-> (if columnar?
                                  (decode-dense-group-results-columnar-2d result-array group-cols group-muls aggs group-dicts)
                                  (decode-dense-group-results-2d result-array group-cols group-muls aggs group-dicts))
                                (apply-group-offsets group-cols group-offsets null-sentinels columnar?))]
                decoded)
            ;; General path: flat double[] layout
              (let [^doubles result-flat
                    (ColumnOps/fusedFilterGroupAggregateDenseParallel
                     (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                     (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                     (int n-group) group-arrays group-muls
                     (int n-aggs) agg-types ^"[[D" agg-cols ^"[[D" agg-col2s
                     (int length) max-key-inc nan-safe)
                    decoded (-> (if columnar?
                                  (decode-dense-group-results-columnar result-flat max-key-inc group-cols group-muls aggs group-dicts)
                                  (decode-dense-group-results result-flat max-key-inc group-cols group-muls aggs group-dicts))
                                (apply-group-offsets group-cols group-offsets null-sentinels columnar?))]
                (if compound?
                  (if columnar?
                    (recompose-columnar-results decoded orig-aggs mapping)
                    (mapv #(recompose-row-results % orig-aggs mapping) decoded))
                  decoded))))))
      ;; Multi-key hash fallback: when composite key space overflows long,
      ;; hash all group columns into a single key, pass to existing partitioned path,
      ;; and decode using reverse lookup table.
      (let [;; Compute multi-key hash if overflow, else use original group-arrays/muls
            [eff-group-arrays eff-group-muls eff-n-group multi-key-lookup]
            (if key-overflow?
              (let [^objects mk-result
                    (ColumnOpsExt/computeMultiKeyHashWithLookup
                     group-arrays (int n-group) (int length)
                     -7046029254386353131)
                    ^longs hash-col (aget mk-result 0)
                    collision? (== 1 (int (aget mk-result 3)))]
                (if collision?
                  ;; Retry with different seed (collision prob ~10^-6, double collision ~10^-12)
                  (let [^objects mk2 (ColumnOpsExt/computeMultiKeyHashWithLookup
                                      group-arrays (int n-group) (int length)
                                      -2401053088876215618)
                        collision2? (== 1 (int (aget mk2 3)))]
                    (when collision2?
                      (throw (ex-info "Multi-key hash collision persists after retry (astronomically unlikely)"
                                      {:group-cols group-cols :n-group n-group})))
                    [(into-array expr/long-array-class [(aget mk2 0)])
                     (long-array [1])
                     (int 1)
                     (build-multi-key-lookup (aget mk2 1) (aget mk2 2) n-group)])
                  [(into-array expr/long-array-class [hash-col])
                   (long-array [1])
                   (int 1)
                   (build-multi-key-lookup (aget mk-result 1) (aget mk-result 2) n-group)]))
              [group-arrays group-muls n-group nil])
            eff-n-group (int eff-n-group)]
        (if has-cd?
          ;; COUNT DISTINCT in hash path: call Java per CD agg, merge with numeric if needed
          (let [;; Run numeric aggs through partitioned if there are any
                ^objects numeric-result
                (when (pos? n-numeric)
                  (ColumnOps/fusedFilterGroupAggregatePartitioned
                   (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                   (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                   eff-n-group eff-group-arrays eff-group-muls
                   (int n-numeric) numeric-agg-types ^"[[D" numeric-agg-cols ^"[[D" numeric-agg-col2s
                   (int length)))
                ;; Run each CD agg through hash CD
                cd-results
                (into {}
                      (keep-indexed
                       (fn [i agg]
                         (when (= :count-distinct (:op agg))
                           (let [col-data (:data (get columns (:col agg)))
                                 ^longs distinct-col (if (expr/long-array? col-data)
                                                       col-data
                                                       (let [^doubles d col-data
                                                             l (long-array length)]
                                                         (dotimes [j length]
                                                           (aset l j (Double/doubleToLongBits (aget d j))))
                                                         l))]
                             [i (ColumnOpsExt/fusedFilterGroupCountDistinctHash
                                 (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                                 (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                                 eff-n-group eff-group-arrays eff-group-muls
                                 distinct-col (int length))])))
                       aggs))
                decoded (if multi-key-lookup
                          ;; Multi-key: decode CD + numeric using reverse hash lookup
                          (if columnar?
                            (decode-hash-cd-results-multi-key-columnar
                             numeric-result cd-results aggs
                             numeric-agg-indices group-cols multi-key-lookup group-dicts)
                            (decode-hash-cd-results-multi-key
                             numeric-result cd-results aggs
                             numeric-agg-indices group-cols multi-key-lookup group-dicts))
                          (if columnar?
                            (decode-hash-cd-results-columnar numeric-result cd-results aggs
                                                             numeric-agg-indices group-cols group-muls group-dicts)
                            (decode-hash-cd-results numeric-result cd-results aggs
                                                    numeric-agg-indices group-cols group-muls group-dicts)))]
            (if compound?
              (if columnar?
                (recompose-columnar-results decoded orig-aggs mapping)
                (mapv #(recompose-row-results % orig-aggs mapping) decoded))
              decoded))
          (let [^objects result-pair
                (ColumnOps/fusedFilterGroupAggregatePartitioned
                 (int n-long) long-pred-types ^"[[J" long-cols long-lo long-hi
                 (int n-dbl) dbl-pred-types ^"[[D" dbl-cols dbl-lo dbl-hi
                 eff-n-group eff-group-arrays eff-group-muls
                 (int n-aggs) agg-types ^"[[D" agg-cols ^"[[D" agg-col2s
                 (int length))
                decoded (if multi-key-lookup
                          ;; Multi-key hash decode
                          (let [d (if columnar?
                                    (decode-group-results-multi-key-columnar
                                     result-pair group-cols multi-key-lookup aggs group-dicts)
                                    (decode-group-results-multi-key
                                     result-pair group-cols multi-key-lookup aggs group-dicts))]
                            (if compound?
                              (if columnar?
                                (recompose-columnar-results d orig-aggs mapping)
                                (mapv #(recompose-row-results % orig-aggs mapping) d))
                              d))
                          ;; Normal composite key decode
                          (if columnar?
                            (let [cd (decode-group-results-columnar result-pair group-cols group-muls aggs group-dicts)]
                              (if compound?
                                (recompose-columnar-results cd orig-aggs mapping)
                                cd))
                            (let [rows (decode-group-results result-pair group-cols group-muls aggs group-dicts)]
                              (if compound?
                                (mapv #(recompose-row-results % orig-aggs mapping) rows)
                                rows))))]
            decoded))))))

(defn chunked-group-by-eligible?
  "Check if group-by can use chunk-streaming on indices.
   Requires: all referenced columns are index-backed, all group columns are int64,
   supported agg ops (no COUNT-DISTINCT), no compiled mask predicates."
  [group-cols aggs preds columns length]
  (let [supported-ops #{:sum :count :min :max :avg :sum-product :variance :variance-pop :stddev :stddev-pop :corr}
        length (long length)
        ;; All referenced columns (group + agg + pred sources)
        all-refs (distinct (concat group-cols
                                   (keep :col aggs)
                                   (mapcat (fn [a] (when (#{:sum-product :corr} (:op a)) (:cols a))) aggs)
                                   (map first preds)))]
    (and
      ;; Enough rows to justify overhead
     (>= length 1000)
      ;; All referenced columns are index-sourced
     (every? #(= :index (:source (get columns %))) all-refs)
      ;; All group columns are int64
     (every? #(= :int64 (:type (get columns %))) group-cols)
      ;; Supported agg ops only
     (every? #(supported-ops (:op %)) aggs)
      ;; No expression aggs (already pre-computed into temp array columns)
     (not (some :expr aggs))
      ;; No compiled mask predicates
     (not (some #(= :__mask (first %)) preds)))))

(defn decompose-smcs-aggs
  "Decompose SUM(a * (c - b)) aggs into SUM(a) + SUM_PRODUCT(a,b).
   Purely algebraic — no data arrays needed. Returns {:aggs decomposed :mapping mapping}
   if any were decomposed, nil otherwise."
  [aggs]
  (when (some match-sum-mul-const-sub aggs)
    (loop [i 0
           out-aggs []
           mapping {}
           remaining aggs]
      (if (empty? remaining)
        {:aggs (vec out-aggs) :mapping mapping}
        (let [agg (first remaining)
              smcs (match-sum-mul-const-sub agg)]
          (if smcs
            (let [{:keys [col-a col-b const]} smcs
                  existing-sum-idx (find-existing-sum-idx out-aggs col-a)
                  sum-idx (or existing-sum-idx (count out-aggs))
                  sum-alias (if existing-sum-idx
                              (:as (nth out-aggs existing-sum-idx))
                              (keyword (str "__sum_" i)))
                  out-aggs (if existing-sum-idx
                             out-aggs
                             (conj out-aggs {:op :sum :col col-a :as sum-alias}))
                  sp-idx (count out-aggs)
                  sp-alias (keyword (str "__sp_" i))]
              (recur (inc i)
                     (conj out-aggs {:op :sum-product :cols [col-a col-b] :as sp-alias})
                     (assoc mapping i {:type :sum-mul-const-sub
                                       :alias (or (:as agg) :sum)
                                       :const (double const)
                                       :sum-idx sum-idx
                                       :sumprod-idx sp-idx
                                       :sum-alias sum-alias
                                       :sumprod-alias sp-alias})
                     (rest remaining)))
            (recur (inc i)
                   (conj out-aggs agg)
                   (assoc mapping i {:type :passthrough :alias (or (:as agg) (:op agg)) :idx (count out-aggs)})
                   (rest remaining))))))))

(defn execute-chunked-group-by
  "Execute group-by by streaming over aligned index chunks.
   Avoids full materialization by processing one chunk (~8K rows, 64KB) at a time.
   Returns nil if not eligible (caller falls through to materialization).
   Algebraically decomposes SUM(a*(c-b)) expressions before eligibility check."
  [preds aggs group-cols columns length columnar?]
  (let [smcs-decomp (decompose-smcs-aggs aggs)
        orig-aggs aggs
        aggs (if smcs-decomp (:aggs smcs-decomp) aggs)]
    (when (chunked-group-by-eligible? group-cols aggs preds columns length)
    ;; Collect chunk entries per column
      (let [all-refs (distinct (concat group-cols
                                       (keep :col aggs)
                                       (mapcat (fn [a] (when (#{:sum-product :corr} (:op a)) (:cols a))) aggs)
                                       (map first preds)))
            col-entries (into {}
                              (map (fn [col-name]
                                     [col-name (collect-chunk-entries (:index (get columns col-name)))]))
                              all-refs)
            first-col-entries (get col-entries (first all-refs))
            n-chunks (count first-col-entries)
            n-group (count group-cols)

          ;; Compute max/min per group column from ChunkStats (O(chunks) not O(N)!)
          ;; Also detect NULLs (Long.MIN_VALUE sentinel) — fall back to array path if found
            group-max-mins
            (mapv (fn [gc]
                    (let [entries (get col-entries gc)]
                      (loop [i 0 mx Long/MIN_VALUE mn Long/MAX_VALUE has-null false]
                        (if (>= i n-chunks)
                          [mx mn has-null]
                          (let [^ChunkEntry entry (nth entries i)
                                ^stratum.stats.ChunkStats cs (.stats entry)
                                cs-mx (double (:max-val cs))
                                cs-mn (double (:min-val cs))]
                            ;; All-NULL chunk or chunk with NULLs (min=Long.MIN_VALUE)
                            (if (> cs-mn cs-mx)
                              (recur (inc i) mx mn true) ;; all-NULL chunk
                              (let [cmx (long cs-mx)
                                    cmn (long cs-mn)
                                    chunk-null (= cmn Long/MIN_VALUE)]
                                (recur (inc i)
                                       (max mx cmx)
                                       (if chunk-null mn (min mn cmn)) ;; skip corrupted min
                                       (or has-null chunk-null)))))))))
                  group-cols)
          ;; If any group column has NULLs, fall back to array path
          ;; (array path handles NULLs via prepare-group-key-arrays)
            any-group-nulls? (boolean (some #(nth % 2) group-max-mins))]
        (when-not any-group-nulls?
          (let [group-maxes-raw (mapv first group-max-mins)
                group-mins-raw (mapv second group-max-mins)

          ;; Key normalization (same logic as execute-group-by-java)
                dense-limit (long (or *dense-group-limit* 100000))
                raw-max-key-est (try (long (reduce * (mapv #(inc (long %)) group-maxes-raw)))
                                     (catch ArithmeticException _ Long/MAX_VALUE))
                offset-norm-est (when (> raw-max-key-est dense-limit)
                                  (try (long (reduce * (mapv (fn [mx mn] (inc (Math/subtractExact (long mx) (long mn))))
                                                             group-maxes-raw group-mins-raw)))
                                       (catch ArithmeticException _ Long/MAX_VALUE)))
                need-offset-only? (and offset-norm-est (<= offset-norm-est dense-limit))
                [group-maxes group-offsets-raw]
                (if need-offset-only?
                  [(mapv (fn [mx mn] (- (long mx) (long mn))) group-maxes-raw group-mins-raw)
                   group-mins-raw]
                  [group-maxes-raw (mapv (constantly 0) group-cols)])
          ;; Check if dense path is feasible (overflow → nil → hash fallback)
                {:keys [group-muls-vec max-key]}
                (try
                  (let [muls (long-array n-group)]
                    (aset muls (dec n-group) 1)
                    (loop [i (- n-group 2)]
                      (when (>= i 0)
                        (aset muls i (* (inc (long (nth group-maxes (inc i))))
                                        (aget muls (inc i))))
                        (recur (dec i))))
                    {:group-muls-vec (vec muls)
                     :max-key (long (reduce (fn [^long acc i]
                                              (+ acc (* (long (nth group-maxes i))
                                                        (long (nth muls i)))))
                                            0 (range n-group)))})
                  (catch ArithmeticException _ nil))]
      ;; Only proceed if dense path fits
            (when (and max-key (<= (inc (long max-key)) dense-limit))
              (let [max-key-inc (int (inc max-key))

              ;; Zone map pruning
                    zone-filters (build-zone-filters preds)
                    classifications (when (seq zone-filters)
                                      (mapv #(classify-chunk zone-filters col-entries %) (range n-chunks)))
                    simd-chunks (if classifications
                                  (into [] (keep-indexed
                                            (fn [i cls] (when (not= :skip cls) i)))
                                        classifications)
                                  (vec (range n-chunks)))
                    n-simd (count simd-chunks)]

                (when (pos? n-simd)
                  (let [{:keys [long-preds dbl-preds n-long long-pred-types long-lo long-hi
                                n-dbl dbl-pred-types dbl-lo dbl-hi]} (prepare-pred-bounds preds columns)

                  ;; Detect if we need the variable-width accumulator path
                        has-var-aggs? (boolean (some #(#{:variance :variance-pop :stddev :stddev-pop :corr} (:op %)) aggs))

                  ;; Check if any agg source columns have NULLs (O(1) via PSS stats)
                        nan-safe (boolean
                                  (columns-have-nulls?
                                   columns
                                   (distinct (mapcat (fn [a]
                                                       (case (:op a)
                                                         :count []
                                                         (:sum-product :corr) (:cols a)
                                                         [(:col a)]))
                                                     aggs))))

                  ;; Prepare agg types
                        n-aggs (count aggs)
                        agg-types (int-array (mapv (fn [agg]
                                                     (case (:op agg)
                                                       :sum   (int ColumnOps/AGG_SUM)
                                                       :sum-product (int ColumnOps/AGG_SUM_PRODUCT)
                                                       :count (int ColumnOps/AGG_COUNT)
                                                       :min   (int ColumnOps/AGG_MIN)
                                                       :max   (int ColumnOps/AGG_MAX)
                                                       :avg   (int ColumnOps/AGG_SUM)
                                                       (:variance :variance-pop :stddev :stddev-pop) (int ColumnOpsVar/AGG_VARIANCE)
                                                       :corr  (int ColumnOpsVar/AGG_CORR)
                                                       (int ColumnOps/AGG_SUM)))
                                                   aggs))

                  ;; Extract native addresses for surviving chunks
                        chunk-lengths (int-array n-simd)
                        _ (dotimes [s n-simd]
                            (let [c (int (nth simd-chunks s))
                                  ^ChunkEntry entry (nth first-col-entries c)]
                              (aset chunk-lengths s (int (chunk/chunk-length (.chunk entry))))))
                        max-chunk-len (int (reduce max 0 (map #(aget chunk-lengths %) (range n-simd))))

                  ;; Group column arrays: Object[numGroupCols][nSimd]
                        group-col-arrs (into-array expr/object-array-class
                                                   (mapv (fn [gc]
                                                           (let [entries (get col-entries gc)
                                                                 arrs (object-array n-simd)]
                                                             (dotimes [s n-simd]
                                                               (let [c (int (nth simd-chunks s))
                                                                     ^ChunkEntry entry (nth entries c)]
                                                                 (aset arrs s (chunk-array entry))))
                                                             arrs))
                                                         group-cols))
                        group-muls-arr (long-array group-muls-vec)
                        group-offsets-arr (long-array (mapv long group-offsets-raw))

                  ;; Pred column arrays
                        long-pred-arrs (into-array expr/object-array-class
                                                   (mapv (fn [[col-name]]
                                                           (let [entries (get col-entries col-name)
                                                                 arrs (object-array n-simd)]
                                                             (dotimes [s n-simd]
                                                               (let [c (int (nth simd-chunks s))
                                                                     ^ChunkEntry entry (nth entries c)]
                                                                 (aset arrs s (chunk-array entry))))
                                                             arrs))
                                                         long-preds))
                        dbl-pred-arrs (into-array expr/object-array-class
                                                  (mapv (fn [[col-name]]
                                                          (let [entries (get col-entries col-name)
                                                                arrs (object-array n-simd)]
                                                            (dotimes [s n-simd]
                                                              (let [c (int (nth simd-chunks s))
                                                                    ^ChunkEntry entry (nth entries c)]
                                                                (aset arrs s (chunk-array entry))))
                                                            arrs))
                                                        dbl-preds))

                  ;; Agg column arrays + type flags
                        agg-col-arrs (into-array expr/object-array-class
                                                 (mapv (fn [agg]
                                                         (if (= :count (:op agg))
                                                           (object-array n-simd) ;; dummy, never used
                                                           (let [col-name (case (:op agg)
                                                                            (:sum-product :corr) (first (:cols agg))
                                                                            (:col agg))
                                                                 entries (get col-entries col-name)
                                                                 arrs (object-array n-simd)]
                                                             (dotimes [s n-simd]
                                                               (let [c (int (nth simd-chunks s))
                                                                     ^ChunkEntry entry (nth entries c)]
                                                                 (aset arrs s (chunk-array entry))))
                                                             arrs)))
                                                       aggs))
                        agg-col2-arrs (into-array expr/object-array-class
                                                  (mapv (fn [agg]
                                                          (if (#{:sum-product :corr} (:op agg))
                                                            (let [col-name (second (:cols agg))
                                                                  entries (get col-entries col-name)
                                                                  arrs (object-array n-simd)]
                                                              (dotimes [s n-simd]
                                                                (let [c (int (nth simd-chunks s))
                                                                      ^ChunkEntry entry (nth entries c)]
                                                                  (aset arrs s (chunk-array entry))))
                                                              arrs)
                                                            (object-array n-simd)))
                                                        aggs))
                        agg-col-is-long (boolean-array (mapv (fn [agg]
                                                               (if (= :count (:op agg))
                                                                 false
                                                                 (let [cn (case (:op agg)
                                                                            (:sum-product :corr) (first (:cols agg))
                                                                            (:col agg))]
                                                                   (= :int64 (:type (get columns cn))))))
                                                             aggs))
                        agg-col2-is-long (boolean-array (mapv (fn [agg]
                                                                (if (#{:sum-product :corr} (:op agg))
                                                                  (= :int64 (:type (get columns (second (:cols agg)))))
                                                                  false))
                                                              aggs))

                  ;; Call Java — dispatch between standard and var-width chunked path
                        ^doubles result-flat
                        (if has-var-aggs?
                          (ColumnOpsChunked/fusedGroupAggregateDenseVarChunkedParallel
                           (int n-long) ^ints long-pred-types
                           ^"[[Ljava.lang.Object;" long-pred-arrs ^longs long-lo ^longs long-hi
                           (int n-dbl) ^ints dbl-pred-types
                           ^"[[Ljava.lang.Object;" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
                           (int n-group) ^"[[Ljava.lang.Object;" group-col-arrs ^longs group-muls-arr ^longs group-offsets-arr
                           (int n-aggs) ^ints agg-types ^"[[Ljava.lang.Object;" agg-col-arrs ^"[[Ljava.lang.Object;" agg-col2-arrs
                           ^booleans agg-col-is-long ^booleans agg-col2-is-long
                           ^ints chunk-lengths (int n-simd) (int max-chunk-len) (int max-key-inc) nan-safe nil)
                          (ColumnOpsChunked/fusedGroupAggregateDenseChunkedParallel
                           (int n-long) ^ints long-pred-types
                           ^"[[Ljava.lang.Object;" long-pred-arrs ^longs long-lo ^longs long-hi
                           (int n-dbl) ^ints dbl-pred-types
                           ^"[[Ljava.lang.Object;" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
                           (int n-group) ^"[[Ljava.lang.Object;" group-col-arrs ^longs group-muls-arr ^longs group-offsets-arr
                           (int n-aggs) ^ints agg-types ^"[[Ljava.lang.Object;" agg-col-arrs ^"[[Ljava.lang.Object;" agg-col2-arrs
                           ^booleans agg-col-is-long ^booleans agg-col2-is-long
                           ^ints chunk-lengths (int n-simd) (int max-chunk-len) (int max-key-inc) nan-safe nil))

                  ;; Dictionary info for group columns
                        group-dicts (let [dicts (mapv #(:dict (get columns %)) group-cols)]
                                      (when (some identity dicts) dicts))

                  ;; Group offsets for decode (re-add subtracted mins)
                        group-offsets-decode (when need-offset-only?
                                               (mapv (fn [mn] [(long mn) 1]) group-mins-raw))]

                    (let [decoded (-> (if has-var-aggs?
                                        (if columnar?
                                          (decode-dense-var-group-results-flat-columnar result-flat max-key-inc group-cols group-muls-arr aggs group-dicts)
                                          (decode-dense-var-group-results-flat result-flat max-key-inc group-cols group-muls-arr aggs group-dicts))
                                        (if columnar?
                                          (decode-dense-group-results-columnar result-flat max-key-inc group-cols group-muls-arr aggs group-dicts)
                                          (decode-dense-group-results result-flat max-key-inc group-cols group-muls-arr aggs group-dicts)))
                                      (apply-group-offsets group-cols group-offsets-decode nil columnar?))]
                      (if smcs-decomp
                        (if columnar?
                          (recompose-columnar-results decoded orig-aggs (:mapping smcs-decomp))
                          (mapv #(recompose-row-results % orig-aggs (:mapping smcs-decomp)) decoded))
                        decoded))))))))))))

(defn execute-group-by
  "Execute grouped aggregation. Tries Java path first, falls back to Clojure scalar."
  [preds aggs group-cols columns length columnar?]
  (if-let [java-result (and (java-group-by-eligible? group-cols aggs columns)
                            (execute-group-by-java preds aggs group-cols columns length columnar?))]
    java-result
    ;; Fast path: grouped median/percentile via Java two-pass scatter + quickSelect
    ;; Eligible: single long[] group column, all aggs are median/percentile, dense key range
    (if-let [fast-result
             (when (and (= 1 (count group-cols))
                        (= :int64 (:type (get columns (first group-cols))))
                        (every? #(#{:median :percentile} (:op %)) aggs))
               (let [gc (first group-cols)
                     ^longs group-data (:data (get columns gc))
                     max-key (inc (ColumnOps/arrayMaxLong group-data (int length)))
                     dense-limit (long (or *dense-group-limit* 100000))]
                 (when (<= max-key dense-limit)
                   (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) columns)
                         ;; Build pred mask if needed
                         ^longs mask (when (seq preds)
                                       (let [m (long-array length)]
                                         (dotimes [i length]
                                           (when (every? #(eval-pred-scalar col-arrays i %) preds)
                                             (aset m i 1)))
                                         m))
                         ;; Run per-agg Java grouped percentile
                         agg-results (mapv (fn [agg]
                                             (let [col-data (:data (get columns (:col agg)))
                                                   pct (double (case (:op agg)
                                                                 :median 0.5
                                                                 :percentile (:param agg)))
                                                   is-long? (expr/long-array? col-data)]
                                               (if is-long?
                                                 (ColumnOps/groupPercentileDenseLong group-data ^longs col-data mask (int length) (int max-key) pct)
                                                 (ColumnOps/groupPercentileDense group-data ^doubles col-data mask (int length) (int max-key) pct))))
                                           aggs)
                         ;; Decode results: emit one row per non-empty group
                         group-dict (:dict (get columns gc))]
                     (let [results (java.util.ArrayList.)]
                       (dotimes [g (int max-key)]
                         (let [^doubles first-arr (first agg-results)
                               cnt-check (aget first-arr g)]
                           (when-not (Double/isNaN cnt-check)
                             (let [group-val (if group-dict
                                               (aget ^"[Ljava.lang.String;" group-dict g)
                                               (long g))
                                   row (into {gc group-val}
                                             (map-indexed
                                              (fn [idx agg]
                                                [(or (:as agg) (:op agg))
                                                 (aget ^doubles (nth agg-results idx) g)])
                                              aggs))]
                               (.add results row)))))
                       (vec results))))))]
      fast-result
    ;; Clojure scalar fallback
      (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) columns)
            ^java.util.HashMap groups (java.util.HashMap.)
          ;; Detect if we need collection-based accumulators
            collection-agg-indices (into {}
                                         (keep-indexed
                                          (fn [idx agg]
                                            (when (#{:count-distinct :median :percentile :approx-quantile} (:op agg))
                                              [idx (:op agg)])))
                                         aggs)
          ;; HashMap<GroupKey, Object[]> for collection aggs (HashSet or ArrayList per agg)
            ^java.util.HashMap coll-groups (when (seq collection-agg-indices) (java.util.HashMap.))]
        (dotimes [i length]
          (when (every? #(eval-pred-scalar col-arrays i %) preds)
            (let [key (let [group-cols-data (mapv #(get col-arrays %) group-cols)]
                        (if (= 1 (count group-cols))
                          (let [cd (first group-cols-data)]
                            (if (expr/long-array? cd)
                              (aget ^longs cd (int i))
                              (aget ^doubles cd (int i))))
                          (mapv (fn [cd]
                                  (if (expr/long-array? cd)
                                    (aget ^longs cd (int i))
                                    (aget ^doubles cd (int i))))
                                group-cols-data)))
                  ^doubles accs (or (.get groups key)
                                    (let [n-aggs (count aggs)
                                          a (double-array (* 2 n-aggs))]
                                      (dotimes [idx n-aggs]
                                        (let [agg (nth aggs idx)
                                              base (* idx 2)]
                                          (case (:op agg)
                                            :min (aset a base Double/POSITIVE_INFINITY)
                                            :max (aset a base Double/NEGATIVE_INFINITY)
                                            (aset a base 0.0))
                                          (aset a (inc base) 0.0)))
                                      (.put groups key a)
                                      a))
                ;; Init collection accumulators if needed
                  ^objects coll-accs (when coll-groups
                                       (or (.get coll-groups key)
                                           (let [n-aggs (count aggs)
                                                 ca (object-array n-aggs)]
                                             (doseq [[idx op] collection-agg-indices]
                                               (aset ca idx (case op
                                                              :count-distinct (java.util.HashSet.)
                                                              (:median :percentile :approx-quantile) (java.util.ArrayList.))))
                                             (.put coll-groups key ca)
                                             ca)))]
            ;; Accumulate
              (loop [agg-idx 0, ag aggs]
                (when (seq ag)
                  (let [agg (first ag)
                        base (* agg-idx 2)]
                    (case (:op agg)
                      :count
                      (aset accs (inc base) (+ (aget accs (inc base)) 1.0))
                      :count-non-null
                      (let [col-data (when (:col agg) (get col-arrays (:col agg)))
                            v (if-let [expr (:expr agg)]
                                (eval-agg-expr expr col-arrays i)
                                (aget-col col-data i))
                            is-null (or (Double/isNaN v)
                                        (and (expr/long-array? col-data)
                                             (= (aget ^longs col-data (int i)) Long/MIN_VALUE)))]
                        (when-not is-null
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :sum
                      (let [v (if-let [expr (:expr agg)]
                                (eval-agg-expr expr col-arrays i)
                                (aget-col (get col-arrays (:col agg)) i))]
                        (when (== v v) ;; skip NaN (SQL NULL)
                          (aset accs base (+ (aget accs base) v))
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :min
                      (let [v (aget-col (get col-arrays (:col agg)) i)]
                        (when (== v v)
                          (aset accs base (Math/min (aget accs base) v))
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :max
                      (let [v (aget-col (get col-arrays (:col agg)) i)]
                        (when (== v v)
                          (aset accs base (Math/max (aget accs base) v))
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :avg
                      (let [v (if-let [expr (:expr agg)]
                                (eval-agg-expr expr col-arrays i)
                                (aget-col (get col-arrays (:col agg)) i))]
                        (when (== v v)
                          (aset accs base (+ (aget accs base) v))
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :sum-product
                      (let [[c1 c2] (:cols agg)
                            v1 (aget-col (get col-arrays c1) i)
                            v2 (aget-col (get col-arrays c2) i)]
                        (when (and (== v1 v1) (== v2 v2))
                          (aset accs base (+ (aget accs base) (* v1 v2)))
                          (aset accs (inc base) (+ (aget accs (inc base)) 1.0))))
                      :count-distinct
                      (let [v (aget-col (get col-arrays (:col agg)) i)]
                        (.add ^java.util.HashSet (aget coll-accs agg-idx) (Double/valueOf v))
                        (aset accs (inc base) (+ (aget accs (inc base)) 1.0)))
                      (:median :percentile :approx-quantile)
                      (let [v (aget-col (get col-arrays (:col agg)) i)]
                        (.add ^java.util.ArrayList (aget coll-accs agg-idx) (Double/valueOf v))
                        (aset accs (inc base) (+ (aget accs (inc base)) 1.0)))
                    ;; default
                      (aset accs (inc base) (+ (aget accs (inc base)) 1.0)))
                    (recur (inc agg-idx) (next ag))))))))
      ;; Convert to result maps
        (let [entries (vec (.entrySet groups))
              group-dicts (let [dicts (mapv #(:dict (get columns %)) group-cols)]
                            (when (some identity dicts) dicts))]
          (into [] (keep (fn [^java.util.Map$Entry entry]
                           (let [key (.getKey entry)
                                 ^doubles accs (.getValue entry)
                                 ^objects coll-accs (when coll-groups (.get coll-groups key))
                                 base (if (= 1 (count group-cols))
                                        (let [k (if (and group-dicts (first group-dicts))
                                                  (aget ^"[Ljava.lang.String;" (first group-dicts) (int (long key)))
                                                  key)]
                                          {(first group-cols) k})
                                        (let [keys (if (vector? key) key [key])]
                                          (into {} (map (fn [gc k dict]
                                                          [gc (if dict
                                                                (aget ^"[Ljava.lang.String;" dict (int (long k)))
                                                                k)])
                                                        group-cols keys (or group-dicts (repeat nil))))))
                                 agg-results
                                 (into {}
                                       (map-indexed
                                        (fn [idx agg]
                                          (let [alias (or (:as agg) (:op agg))
                                                agg-base (* idx 2)
                                                cnt (long (aget accs (inc agg-base)))
                                                result (case (:op agg)
                                                         (:count :count-non-null) cnt
                                                         :sum (aget accs agg-base)
                                                         :min (aget accs agg-base)
                                                         :max (aget accs agg-base)
                                                         :avg (if (zero? cnt) Double/NaN
                                                                  (/ (aget accs agg-base) (double cnt)))
                                                         :sum-product (aget accs agg-base)
                                                         :count-distinct
                                                         (long (.size ^java.util.HashSet (aget coll-accs idx)))
                                                         (:median :percentile)
                                                         (let [^java.util.ArrayList vals (aget coll-accs idx)
                                                               n (.size vals)]
                                                           (if (zero? n)
                                                             Double/NaN
                                                             (let [work (double-array n)]
                                                               (dotimes [j n]
                                                                 (aset work j (double (.get vals j))))
                                                               (ColumnOps/percentile work n
                                                                                     (double (or (:param agg) 0.5))))))
                                                         :approx-quantile
                                                         (let [^java.util.ArrayList vals (aget coll-accs idx)
                                                               n (.size vals)]
                                                           (if (zero? n)
                                                             Double/NaN
                                                             (let [work (double-array n)]
                                                               (dotimes [j n]
                                                                 (aset work j (double (.get vals j))))
                                                               (ColumnOpsAnalytics/tdigestApproxQuantile work n
                                                                                                         (double (:param agg)) 200.0))))
                                                         (aget accs agg-base))]
                                            [alias result]))
                                        aggs))]
                    ;; Filter out groups with zero valid agg rows (all NaN)
                             (let [max-cnt (loop [idx 0 mx 0.0]
                                             (if (>= idx (count aggs))
                                               mx
                                               (recur (inc idx)
                                                      (Math/max mx (aget accs (inc (* idx 2)))))))]
                               (when (pos? max-cnt)
                                 (merge base {:_count (long (aget accs 1))} agg-results))))))
                entries))))))

