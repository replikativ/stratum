(ns stratum.query
  "Query DSL for Stratum columnar indices.

   Provides a declarative query interface that routes to the optimal execution
   strategy: fused SIMD for simple filter+aggregate, multi-index for flexible
   queries, and scalar fallback for small datasets.

   Predicate syntax matches Datahike for zero-cost integration:
     [:< :col 5]  or  '(< :col 5)     ;; both accepted
     [:between :col lo hi]             ;; range predicate

   Aggregation syntax:
     [:sum :col]  or  '(sum :col)      ;; both accepted
     [:count]                          ;; count all matching

   == Query Format ==

   (q/q
     {:from   {:col-name array-or-index ...}
      :where  [pred1 pred2 ...]          ;; filter predicates (AND)
      :agg    [agg1 agg2 ...]            ;; aggregations
      :group  [:col1 :col2]              ;; group-by keys
      :having [pred ...]                 ;; post-agg filter
      :order  [[:col :asc] ...]          ;; sorting
      :limit  N                          ;; row limit
      :offset N})                        ;; skip rows

   == TPC-H Q06 Example ==

   (q/execute
     {:from  {:shipdate sd-arr :discount dc-arr :quantity qt-arr :price px-arr}
      :where [[:between :shipdate 8766 9131]
              [:between :discount 0.05 0.07]
              [:< :quantity 24]]
      :agg   [[:as [:sum [:* :price :discount]] :revenue]]})"
  (:require [stratum.query.simd-primitive :as qc]
            [stratum.query.normalization :as norm]
            [stratum.query.predicate :as pred]
            [stratum.query.expression :as expr]
            [stratum.query.columns :as cols]
            [stratum.query.group-by :as gb]
            [stratum.query.join :as jn]
            [stratum.query.postprocess :as post]
            [stratum.query.window :as win]
            [stratum.index :as index]
            [stratum.chunk :as chunk]
            [stratum.specification :as spec]
            [stratum.column :as column]
            [stratum.dataset :as dataset])
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsChunked ColumnOpsChunkedSimd ColumnOpsAnalytics]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:dynamic *dense-group-limit*
  "Maximum key space for dense group-by arrays. See stratum.query.group-by/*dense-group-limit*."
  200000)

;; ============================================================================
;; Column Type Detection
;; ============================================================================

;; Re-export encode-column from stratum.column for backward compatibility
(def encode-column
  "Detect column type and normalize to canonical format.
   See stratum.column/encode-column for details."
  column/encode-column)

(defn- prepare-columns
  "Normalize all columns in :from map. Preserves index sources.

   For Dataset support, datasets are already normalized (no preparation needed)."
  [from-map]
  (cols/prepare-columns from-map))

(defn- get-column-length
  "Get the length of a column (array or index-backed)."
  ^long [col-info]
  (cols/get-column-length col-info))

;; ============================================================================
;; Zone Map Optimization (Phase 3B)
;; ============================================================================
;;
;; Zone map optimization is implemented via index-level functions:
;; - idx-filter-zonemap: Filter with chunk skipping
;; - idx-count-zonemap: Count with 3-way classification (skip/stats/simd)
;; - idx-filter-gte/gt/lte/lt/eq/range: Optimized predicates
;;
;; These functions provide 2-10x speedup by:
;; 1. Skipping chunks where min/max prove no matches exist
;; 2. Using stats-only for chunks where all values match
;; 3. SIMD evaluation only for chunks with partial matches
;;
;; Zone map routing is integrated into q via compute-surviving-chunks
;; and execute-chunked-fused (lines 1008-1027, 376-519).

;; ============================================================================
;; Projection Pushdown (Phase 3C)
;; ============================================================================

(defn- materialize-column
  "Ensure a column has array :data, materializing from index if needed."
  [col-info]
  (cols/materialize-column col-info))

(defn- materialize-columns
  "Materialize all index-sourced columns to arrays (with memory budget check)."
  [columns]
  (cols/materialize-columns columns))

(defn- all-indices?
  "Check if all columns are sourced from Stratum indices."
  [columns]
  (every? #(= :index (:source (val %))) columns))

(defn- any-index?
  "Check if any column is sourced from a Stratum index."
  [columns]
  (some #(= :index (:source (val %))) columns))

(defn- execute-group-by
  "Bridge q/*dense-group-limit* → gb/*dense-group-limit* then delegate."
  [preds aggs group-cols columns length columnar?]
  (binding [gb/*dense-group-limit* *dense-group-limit*]
    (gb/execute-group-by preds aggs group-cols columns length columnar?)))

;; ============================================================================
;; Predicate Pre-processing (computed expressions, OR/IN/NOT)
;; ============================================================================

(defn- materialize-computed-preds
  "Pre-compute expression predicates into temporary columns.
   For predicates like [{:op :mul :args [:a :b]} :gt 1000], computes the
   expression into a temporary double[] and replaces with [:_expr_0 :gt 1000].
   Returns [updated-preds updated-columns]."
  [preds columns ^long length]
  (let [mat-cols (materialize-columns columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)]
    (loop [i 0
           out-preds []
           out-cols mat-cols
           remaining preds]
      (if (empty? remaining)
        [out-preds out-cols]
        (let [pred (first remaining)
              col-ref (first pred)]
          (if (map? col-ref)
            ;; Expression predicate — pre-compute to temp column
            (let [expr-name (keyword (str "_expr_" i))
                  computed (expr/eval-expr-vectorized col-ref col-arrays length)]
              (recur (inc i)
                     (conj out-preds (assoc pred 0 expr-name))
                     (assoc out-cols expr-name {:type :float64 :data computed})
                     (rest remaining)))
            ;; Normal predicate — pass through
            (recur i (conj out-preds pred) out-cols (rest remaining))))))))

;; ============================================================================
;; Fused SIMD Execution
;; ============================================================================

(defn- agg->compiler-spec
  "Convert normalized aggregate to query_compiler format."
  [agg]
  (case (:op agg)
    :sum         [:sum (:col agg)]
    :sum-product [:sum-product (first (:cols agg)) (second (:cols agg))]
    :count       [:count]
    :count-non-null [:count]
    :min         [:min (:col agg)]
    :max         [:max (:col agg)]
    ;; avg decomposes to sum / count
    :avg         [:sum (:col agg)]
    (throw (ex-info (str "Unsupported SIMD agg: " (:op agg))
                    {:agg agg}))))

(defn- execute-fused
  "Execute via fused SIMD path (fastest for simple filter+aggregate)."
  [preds agg columns length]
  (let [compiler-agg (if agg (agg->compiler-spec agg) [:count])
        ;; COUNT(col) → inject IS_NOT_NULL predicate so SIMD COUNT skips NULLs
        preds (if (and agg (= :count-non-null (:op agg)))
                (conj (vec preds) [:is-not-null (:col agg)])
                preds)
        spec {:columns columns
              :predicates preds
              :aggregate compiler-agg
              :length length}
        result (qc/execute-query spec)]
    (if (and agg (= :avg (:op agg)))
      ;; avg = sum / count
      (let [cnt (:count result)]
        (if (zero? cnt)
          {:result Double/NaN :count 0}
          {:result (/ (:result result) (double cnt)) :count cnt}))
      result)))

(defn- ensure-doubles
  "Ensure column data is double[], converting long[] if needed."
  ^doubles [col-info ^long length]
  (let [data (:data col-info)]
    (if (expr/double-array? data)
      data
      (ColumnOps/longToDouble ^longs data (int length)))))

(defn- execute-fused-multi-sum
  "Execute multiple SUM-like aggs in a single pass via Java fusedSimdMultiSumParallel.
   All aggs must be :sum, :sum-product, :count, or :avg (no :min/:max).
   Evaluates predicates once, accumulates all SUM values simultaneously."
  [preds aggs columns length]
  (let [{:keys [n-long long-pred-types long-cols long-lo long-hi
                n-dbl dbl-pred-types dbl-cols dbl-lo dbl-hi]} (gb/prepare-pred-arrays preds columns)

        ;; Collect SUM columns (skip COUNT-only aggs)
        sum-aggs (filterv #(not= :count (:op %)) aggs)
        n-sum    (count sum-aggs)
        ;; Check if ALL sum columns are long[] and no double preds and no SUM_PRODUCT
        ;; → use all-long path (LongVector accumulators, no longToDouble allocation)
        all-long? (and (zero? n-dbl)
                       (every? (fn [a]
                                 (and (not= :sum-product (:op a))
                                      (let [col-info (case (:op a)
                                                       (:sum :avg) (get columns (:col a)))]
                                        (expr/long-array? (:data col-info)))))
                               sum-aggs))
        ;; Call Java single-pass
        ^doubles result (if all-long?
                          ;; All-long path: LongVector accumulators, no conversion
                          (let [sum-long-cols (into-array expr/long-array-class
                                                          (mapv (fn [a]
                                                                  (:data (get columns (:col a))))
                                                                sum-aggs))]
                            (ColumnOpsExt/fusedSimdMultiSumAllLongParallel
                             (int n-long) long-pred-types
                             ^"[[J" long-cols ^longs long-lo ^longs long-hi
                             (int n-sum) ^"[[J" sum-long-cols
                             (int length)))
                          ;; Double path: ensure-doubles conversion
                          (let [sum-cols1 (into-array expr/double-array-class
                                                      (mapv (fn [a]
                                                              (case (:op a)
                                                                (:sum :avg) (ensure-doubles (get columns (:col a)) length)
                                                                :sum-product (ensure-doubles (get columns (first (:cols a))) length)))
                                                            sum-aggs))
                                sum-cols2 (into-array expr/double-array-class
                                                      (mapv (fn [a]
                                                              (case (:op a)
                                                                :sum-product (ensure-doubles (get columns (second (:cols a))) length)
                                                                nil))
                                                            sum-aggs))]
                            (let [nan-safe (boolean
                                            (or (some #(ColumnOps/arrayHasNaN ^doubles % (alength ^doubles %))
                                                      sum-cols1)
                                                (some #(when % (ColumnOps/arrayHasNaN ^doubles % (alength ^doubles %)))
                                                      sum-cols2)))]
                              (ColumnOpsExt/fusedSimdMultiSumParallel
                               (int n-long) long-pred-types
                               ^"[[J" long-cols ^longs long-lo ^longs long-hi
                               (int n-dbl) dbl-pred-types
                               ^"[[D" dbl-cols ^doubles dbl-lo ^doubles dbl-hi
                               (int n-sum) ^"[[D" sum-cols1 ^"[[D" sum-cols2
                               (int length) nan-safe))))
        cnt (long (aget result n-sum))
        ;; Map sum-agg index for each agg
        sum-idx (volatile! 0)]
    [(into {:_count cnt}
           (map (fn [a]
                  [(or (:as a) (:op a))
                   (case (:op a)
                     :count (long cnt)
                     :avg (let [si @sum-idx] (vswap! sum-idx inc)
                               (if (zero? cnt) nil (/ (aget result si) (double cnt))))
                     (:sum :sum-product) (let [si @sum-idx] (vswap! sum-idx inc)
                                              (if (zero? cnt) nil (aget result si))))])
                aggs))]))

;; ============================================================================
;; Chunk-Streaming Fused Execution (zero-copy from indices)
;; ============================================================================

(defn- accumulate-stats-chunk
  "Accumulate a stats-only chunk's statistics into running accumulators.
   accum is [sum count min-val max-val].
   Returns updated [sum count min-val max-val]."
  [accum agg-type-kw agg-col1-name col-entries c]
  (let [[sum cnt mn mx] accum
        sum (double sum) cnt (long cnt) mn (double mn) mx (double mx) c (long c)
        ;; Get stats for the agg column (or any column for COUNT)
        ^ChunkEntry entry (if agg-col1-name
                            (nth (get col-entries agg-col1-name) c)
                            (nth (val (first col-entries)) c))
        ^stratum.stats.ChunkStats chunk-stats (.stats entry)
        chunk-count (:count chunk-stats)]
    (case agg-type-kw
      :count [sum (+ cnt chunk-count) mn mx]
      :sum [(+ sum (double (:sum chunk-stats))) (+ cnt chunk-count) mn mx]
      :min [sum (+ cnt chunk-count)
            (Math/min mn (double (:min-val chunk-stats))) mx]
      :max [sum (+ cnt chunk-count)
            mn (Math/max mx (double (:max-val chunk-stats)))]
      ;; sum-product cannot use stats (no Σxy stored) — shouldn't reach here
      [sum cnt mn mx])))

(defn- compute-surviving-chunks
  "Determine which chunk indices survive zone map pruning.
   Returns vector of surviving chunk ordinal indices, or nil if no pruning benefit.
   Only works when all columns are index-sourced with aligned chunks."
  [preds columns]
  (when (and (all-indices? columns) (seq preds))
    (let [zone-filters (gb/build-zone-filters preds)]
      (when (seq zone-filters)
        (let [;; Get chunk entries for each predicate column
              pred-col-names (into #{} (map :col) zone-filters)
              col-entries (into {}
                                (keep (fn [[col-name col-info]]
                                        (when (contains? pred-col-names col-name)
                                          [col-name (gb/collect-chunk-entries (:index col-info))])))
                                columns)
              first-entries (val (first col-entries))
              n-chunks (count first-entries)
              survivors (into []
                              (filter (fn [c]
                                    ;; A chunk survives if all may-contain predicates pass
                                        (every? (fn [{:keys [col may-contain]}]
                                                  (let [entries (get col-entries col)
                                                        ^ChunkEntry entry (nth entries c)
                                                        chunk-stats (.stats entry)]
                                                    (may-contain chunk-stats)))
                                                zone-filters))
                                      (range n-chunks)))]
          (when (< (count survivors) n-chunks)
            survivors))))))

(defn- materialize-column-pruned
  "Materialize only surviving chunks of a column into a shorter array.
   For non-index columns, returns data as-is (can't prune)."
  [col-info surviving-indices]
  (if (:data col-info)
    col-info  ;; Already an array, can't prune
    (assoc col-info :data
           (index/idx-materialize-to-array-pruned
            (:index col-info) surviving-indices))))

(defn- materialize-columns-pruned
  "Materialize all columns with chunk pruning. Only surviving chunks are copied."
  [columns surviving-indices]
  (into {} (map (fn [[k v]] [k (materialize-column-pruned v surviving-indices)])) columns))

(defn- execute-chunked-fused
  "Execute fused filter+aggregate by streaming over aligned chunks.

   Pre-extracts all native addresses into flat arrays and calls a single
   Java method (fusedSimdChunkedParallel) that:
   1. Broadcasts SIMD constants once (amortized across all chunks)
   2. Partitions chunks across ForkJoinPool threads
   3. Each thread processes its chunk batch with SIMD inner loops
   4. Merges partial results across threads

   Zone map pruning:
   - Chunks where predicates prove no matches are skipped entirely
   - Chunks where all values satisfy all predicates use stats-only aggregation
   - Only remaining chunks go through SIMD processing"
  [preds agg columns _length]
  (let [;; Prepare predicate arrays (shared across all chunks)
        compiler-agg (if agg (agg->compiler-spec agg) [:count])
        agg-type-kw (first compiler-agg)
        {:keys [long-preds dbl-preds n-long n-dbl
                long-pred-types long-lo long-hi
                dbl-pred-types dbl-lo dbl-hi]} (gb/prepare-pred-bounds preds columns)

        agg-type (int (case agg-type-kw
                        :sum-product ColumnOps/AGG_SUM_PRODUCT
                        :sum         ColumnOps/AGG_SUM
                        :count       ColumnOps/AGG_COUNT
                        :min         ColumnOps/AGG_MIN
                        :max         ColumnOps/AGG_MAX))

        ;; Column name orderings
        long-col-names (mapv first long-preds)
        dbl-col-names (mapv first dbl-preds)
        agg-col1-name (case agg-type-kw
                        :sum-product (second compiler-agg)
                        (:sum :min :max) (second compiler-agg)
                        nil)
        agg-col2-name (when (= :sum-product agg-type-kw)
                        (nth compiler-agg 2))

        ;; Collect chunk entries per column (vec of ChunkEntry per col)
        col-entries (into {}
                          (map (fn [[col-name col-info]]
                                 [col-name (gb/collect-chunk-entries (:index col-info))]))
                          columns)
        first-col-entries (val (first col-entries))
        n-chunks (count first-col-entries)

        ;; Zone map pruning: always classify chunks for skip pruning.
        ;; Stats-only aggregation only for SUM/COUNT/MIN/MAX (not SUM_PRODUCT).
        zone-filters (gb/build-zone-filters preds)
        has-zone-filters? (seq zone-filters)
        stats-eligible? (and has-zone-filters?
                             (not= :sum-product agg-type-kw))
        classifications (when has-zone-filters?
                          (mapv #(gb/classify-chunk zone-filters col-entries %) (range n-chunks)))
        ;; Get indices of chunks that need SIMD processing
        ;; For SUM_PRODUCT: :skip chunks are pruned, :stats-only treated as :simd
        simd-chunks (if classifications
                      (into [] (keep-indexed
                                (fn [i cls]
                                  (when (if stats-eligible?
                                          (= :simd cls)
                                          (not= :skip cls))
                                    i)))
                            classifications)
                      (vec (range n-chunks)))
        n-simd (count simd-chunks)

        ;; Pre-extract chunk arrays only for SIMD chunks (typed 3D arrays for JIT)
        chunk-lengths    (int-array n-simd)
        long-pred-arrs   (make-array expr/long-array-class n-long n-simd)
        dbl-pred-arrs    (make-array expr/double-array-class n-dbl n-simd)
        agg-arr1s        (make-array Double/TYPE n-simd 0)
        agg-arr2s        (make-array Double/TYPE n-simd 0)]

    ;; Fill arrays only for SIMD chunks
    (dotimes [s n-simd]
      (let [c (int (nth simd-chunks s))
            ^ChunkEntry ref-entry (nth first-col-entries c)]
        (aset chunk-lengths s (int (chunk/chunk-length (.chunk ref-entry)))))
      (dotimes [p n-long]
        (let [col-name (nth long-col-names p)
              ^ChunkEntry entry (nth (get col-entries col-name) (nth simd-chunks s))]
          (aset ^objects (aget ^"[[[J" long-pred-arrs p) s (chunk/chunk-as-longs (.chunk entry)))))
      (dotimes [p n-dbl]
        (let [col-name (nth dbl-col-names p)
              ^ChunkEntry entry (nth (get col-entries col-name) (nth simd-chunks s))]
          (aset ^objects (aget ^"[[[D" dbl-pred-arrs p) s (chunk/chunk-as-doubles (.chunk entry)))))
      (when agg-col1-name
        (let [^ChunkEntry entry (nth (get col-entries agg-col1-name) (nth simd-chunks s))]
          (aset ^objects agg-arr1s s (chunk/chunk-as-doubles (.chunk entry)))))
      (when agg-col2-name
        (let [^ChunkEntry entry (nth (get col-entries agg-col2-name) (nth simd-chunks s))]
          (aset ^objects agg-arr2s s (chunk/chunk-as-doubles (.chunk entry))))))

    ;; Accumulate stats-only chunks
    (let [stats-init (if classifications
                       (reduce (fn [accum c]
                                 (if (= :stats-only (nth classifications c))
                                   (accumulate-stats-chunk accum
                                                           agg-type-kw agg-col1-name col-entries c)
                                   accum))
                               [0.0 0 Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY]
                               (range n-chunks))
                       [0.0 0 Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY])
          [stats-sum stats-count stats-min stats-max] stats-init]

      ;; SIMD processing for remaining chunks
      (if (zero? n-simd)
        ;; All chunks were handled by stats or skipped
        (let [result-val (case agg-type-kw
                           :count (double stats-count)
                           :sum stats-sum
                           :min (if (zero? (long stats-count)) 0.0 stats-min)
                           :max (if (zero? (long stats-count)) 0.0 stats-max)
                           0.0)]
          {:result result-val :count (long stats-count)})

        ;; Process SIMD chunks via Java
        (let [^doubles result
              (ColumnOpsChunkedSimd/fusedSimdChunkedParallel
               (int n-long) ^ints long-pred-types
               ^"[[[J" long-pred-arrs ^longs long-lo ^longs long-hi
               (int n-dbl) ^ints dbl-pred-types
               ^"[[[D" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
               (int agg-type) ^"[[D" agg-arr1s ^"[[D" agg-arr2s
               ^ints chunk-lengths (int n-simd))
              simd-result (aget result 0)
              simd-count (long (aget result 1))
              total-count (+ simd-count (long stats-count))]
          ;; Merge SIMD result with stats accumulation
          (if (zero? (long stats-count))
            {:result simd-result :count total-count}
            {:result (case agg-type-kw
                       :count (double total-count)
                       :sum (+ simd-result (double stats-sum))
                       :min (if (zero? simd-count)
                              (double stats-min)
                              (Math/min simd-result (double stats-min)))
                       :max (if (zero? simd-count)
                              (double stats-max)
                              (Math/max simd-result (double stats-max)))
                       simd-result)
             :count total-count}))))))

(defn- execute-chunked-fused-count
  "JIT-isolated chunked COUNT for index inputs.
   Calls ColumnOpsExt/fusedSimdChunkedCountParallel which has no aggType switch,
   avoiding JIT aggType interference from fusedSimdChunkBatch (B5 idx: 27ms→2ms).
   No agg columns needed — only predicate columns are streamed."
  [preds columns _length]
  (let [{:keys [long-preds dbl-preds n-long n-dbl
                long-pred-types long-lo long-hi
                dbl-pred-types dbl-lo dbl-hi]} (gb/prepare-pred-bounds preds columns)

        long-col-names (mapv first long-preds)
        dbl-col-names (mapv first dbl-preds)

        col-entries (into {}
                          (map (fn [[col-name col-info]]
                                 [col-name (gb/collect-chunk-entries (:index col-info))]))
                          columns)
        first-col-entries (val (first col-entries))
        n-chunks (count first-col-entries)

        ;; Zone map pruning
        zone-filters (gb/build-zone-filters preds)
        stats-eligible? (seq zone-filters)
        classifications (when stats-eligible?
                          (mapv #(gb/classify-chunk zone-filters col-entries %) (range n-chunks)))
        simd-chunks (if classifications
                      (into [] (keep-indexed
                                (fn [i cls] (when (= :simd cls) i)))
                            classifications)
                      (vec (range n-chunks)))
        n-simd (count simd-chunks)

        ;; Pre-extract chunk arrays (pred columns only, no agg columns)
        chunk-lengths    (int-array n-simd)
        long-pred-arrs   (make-array expr/long-array-class n-long n-simd)
        dbl-pred-arrs    (make-array expr/double-array-class n-dbl n-simd)]

    ;; Fill arrays for SIMD chunks
    (dotimes [s n-simd]
      (let [c (int (nth simd-chunks s))
            ^ChunkEntry ref-entry (nth first-col-entries c)]
        (aset chunk-lengths s (int (chunk/chunk-length (.chunk ref-entry)))))
      (dotimes [p n-long]
        (let [col-name (nth long-col-names p)
              ^ChunkEntry entry (nth (get col-entries col-name) (nth simd-chunks s))]
          (aset ^objects (aget ^"[[[J" long-pred-arrs p) s (chunk/chunk-as-longs (.chunk entry)))))
      (dotimes [p n-dbl]
        (let [col-name (nth dbl-col-names p)
              ^ChunkEntry entry (nth (get col-entries col-name) (nth simd-chunks s))]
          (aset ^objects (aget ^"[[[D" dbl-pred-arrs p) s (chunk/chunk-as-doubles (.chunk entry))))))

    ;; Stats-only chunks: accumulate count
    (let [stats-count (if classifications
                        (reduce (fn [^long acc c]
                                  (if (= :stats-only (nth classifications c))
                                    (let [^ChunkEntry entry (nth first-col-entries c)]
                                      (+ acc (long (chunk/chunk-length (.chunk entry)))))
                                    acc))
                                0 (range n-chunks))
                        0)]

      (if (zero? n-simd)
        {:result (double stats-count) :count (long stats-count)}

        (let [^doubles result
              (ColumnOpsChunkedSimd/fusedSimdChunkedCountParallel
               (int n-long) ^ints long-pred-types
               ^"[[[J" long-pred-arrs ^longs long-lo ^longs long-hi
               (int n-dbl) ^ints dbl-pred-types
               ^"[[[D" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
               ^ints chunk-lengths (int n-simd))
              simd-count (long (aget result 1))
              total-count (+ simd-count (long stats-count))]
          {:result (double total-count) :count total-count})))))

;; ============================================================================
;; Projection (SELECT) Support
;; ============================================================================

(defn- normalize-select-item
  "Normalize a select item to {:name key :ref col-keyword} or {:name key :expr expr-map}.

   Accepts:
     :col                       → {:name :col :ref :col}
     [:as :col :alias]          → {:name :alias :ref :col}
     [:as [:* :a :b] :product]  → {:name :product :expr {:op :mul ...}}
     [:* :a :b]                 → {:name :_expr_0 :expr {:op :mul ...}}"
  [item idx]
  (cond
    (keyword? item)
    (let [k (norm/strip-ns item)]
      {:name k :ref k})

    (and (sequential? item) (= :as (first item)))
    (let [items (vec item)
          inner (nth items 1)
          alias (nth items 2)]
      (cond
        (keyword? inner) {:name alias :ref (norm/strip-ns inner)}
        (or (number? inner) (string? inner)) {:name alias :literal inner}
        :else {:name alias :expr (norm/normalize-expr inner)}))

    (sequential? item)
    {:name (keyword (str "_expr_" idx)) :expr (norm/normalize-expr item)}

    (number? item)
    {:name (keyword (str "_literal_" idx)) :literal item}

    (string? item)
    {:name (keyword (str "_literal_" idx)) :literal item}

    :else
    (throw (ex-info (str "Invalid select item: " item) {:item item}))))

(defn- aget-col-decoded
  "Read element i from a column, decoding dict-encoded strings back to String.
   For non-dict columns, returns typed value (long or double).
   For NULL sentinels (Long.MIN_VALUE or NaN), returns nil."
  [col-data col-info ^long i]
  (if (expr/long-array? col-data)
    (let [v (aget ^longs col-data i)]
      (cond
        (= v Long/MIN_VALUE) nil
        (:dict col-info) (aget ^"[Ljava.lang.String;" (:dict col-info) (int v))
        :else v))
    (let [v (aget ^doubles col-data i)]
      (if (Double/isNaN v) nil v))))

(defn- execute-projection
  "Execute a projection (SELECT) query.
   When columnar? is true, returns {col-keyword array ... :n-rows N}.
   Otherwise returns a vector of row maps."
  [preds select-items columns length columnar?]
  (let [mat-cols (materialize-columns columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
        ;; Filter rows by predicates
        match-indices (if (empty? preds)
                        nil ;; no filtering needed — all rows match
                        (let [matching (transient [])]
                          (dotimes [i length]
                            (when (every? #(gb/eval-pred-scalar col-arrays i %) preds)
                              (conj! matching i)))
                          (persistent! matching)))
        n-matched (if match-indices (count match-indices) length)]
    (if columnar?
      ;; Columnar output: return arrays directly (avoids N map allocations)
      (let [result (transient {:n-rows n-matched})]
        (doseq [sel select-items]
          (let [k (:name sel)]
            (if (contains? sel :literal)
              ;; Literal value — fill constant array
              (let [lit (:literal sel)]
                (if (integer? lit)
                  (let [out (long-array n-matched)]
                    (java.util.Arrays/fill out (long lit))
                    (assoc! result k out))
                  (if (number? lit)
                    (let [out (double-array n-matched)]
                      (java.util.Arrays/fill out (double lit))
                      (assoc! result k out))
                    (let [out (make-array String n-matched)]
                      (java.util.Arrays/fill ^"[Ljava.lang.String;" out (str lit))
                      (assoc! result k out)))))
              (if-let [ref (:ref sel)]
                (let [src-col (get columns ref)
                      src-data (if (map? src-col) (:data src-col) src-col)]
                  (if match-indices
                    ;; Scatter-gather filtered rows
                    (let [n n-matched]
                      (cond
                        (expr/long-array? src-data)
                        (let [out (long-array n)]
                          (dotimes [i n] (aset out i (aget ^longs src-data (int (nth match-indices i)))))
                          (assoc! result k out))

                        (expr/double-array? src-data)
                        (let [out (double-array n)]
                          (dotimes [i n] (aset out i (aget ^doubles src-data (int (nth match-indices i)))))
                          (assoc! result k out))

                        :else
                        (let [out (make-array String n)]
                          (dotimes [i n] (aset ^"[Ljava.lang.String;" out i
                                               (aget ^"[Ljava.lang.String;" src-data (int (nth match-indices i)))))
                          (assoc! result k out))))
                    ;; No filtering — return source array directly
                    (assoc! result k src-data)))
                ;; Expression column — evaluate for all rows
                (let [out (double-array n-matched)]
                  (dotimes [i n-matched]
                    (let [ii (int (if match-indices (nth match-indices i) i))]
                      (aset out i (double (gb/eval-agg-expr (:expr sel) col-arrays ii)))))
                  (assoc! result k out))))))
        (persistent! result))
      ;; Row-oriented output (original path)
      (let [indices (or match-indices (range length))]
        (mapv (fn [i]
                (let [ii (long i)]
                  (into {}
                        (map (fn [sel]
                               [(:name sel)
                                (if (contains? sel :literal)
                                  (:literal sel)
                                  (if-let [ref (:ref sel)]
                                    (aget-col-decoded (get col-arrays ref) (get mat-cols ref) ii)
                                    (gb/eval-agg-expr (:expr sel) col-arrays ii)))]))
                        select-items)))
              indices)))))

;; ============================================================================
;; Results → Column Arrays (for CTEs / subqueries)
;; ============================================================================

(defn results->columns
  "Convert a vector of result maps into a column map {:col-name array, ...}.
   Used by CTE execution and subquery materialization."
  [results]
  (if (and (map? results) (:n-rows results))
    ;; Already columnar
    results
    (when (seq results)
      (let [ks (vec (keys (first results)))
            n (count results)]
        (reduce (fn [m k]
                  (let [first-val (get (first results) k)
                        arr (cond
                              (integer? first-val)
                              (let [la (long-array n)]
                                (dotimes [i n]
                                  (aset la i (long (get (nth results i) k 0))))
                                la)

                              (float? first-val)
                              (let [da (double-array n)]
                                (dotimes [i n]
                                  (aset da i (double (get (nth results i) k 0.0))))
                                da)

                              :else
                              (let [sa (make-array String n)]
                                (dotimes [i n]
                                  (aset ^"[Ljava.lang.String;" sa i
                                        (str (get (nth results i) k))))
                                sa))]
                    (assoc m k arr)))
                {}
                ks)))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn- validate-query
  "Validate query inputs. Throws ex-info with descriptive message on error.

   from: StratumDataset or column map {col-name data}"
  [from where agg group select]
  (when (nil? from)
    (throw (ex-info "Query :from must be a StratumDataset or non-empty map"
                    {:from from})))

  ;; Extract column names from dataset or map
  (let [col-names (if (satisfies? dataset/IDataset from)
                    (set (dataset/column-names from))
                    (do
                      (when (empty? from)
                        (throw (ex-info "Query :from map cannot be empty"
                                        {:from from})))
                      (set (keys from))))]
    ;; Validate WHERE column references
    (doseq [pred (or where [])]
      (let [items (vec pred)
            op-raw (first items)]
        ;; Skip OR/NOT combinators — their sub-preds will be checked recursively
        (when-not (or (= :or op-raw) (= 'or op-raw)
                      (= :not op-raw) (= 'not op-raw)
                      (= :in op-raw) (= 'in op-raw))
          (let [col-ref (norm/strip-ns (second items))]
            (when (and (keyword? col-ref)
                       (not (contains? col-names col-ref)))
              (throw (ex-info (str "Unknown column " col-ref " in :where predicate. Available: " (sort col-names))
                              {:column col-ref :available col-names :pred pred})))))))
    ;; Validate GROUP column references (skip expressions — vectors like [:minute :et])
    (doseq [g (or group [])]
      (let [g (norm/strip-ns g)]
        (when (and (keyword? g) (not (contains? col-names g)))
          (throw (ex-info (str "Unknown column " g " in :group. Available: " (sort col-names))
                          {:column g :available col-names})))))
    ;; Validate aggregation column references
    (doseq [a (or agg [])]
      (let [items (vec a)
            ;; Unwrap [:as inner alias]
            inner (if (= :as (first items)) (vec (second items)) items)
            op-raw (first inner)]
        (when-not (or (= :count op-raw) (= 'count op-raw))
          (let [col-refs (subvec inner 1)]
            (doseq [ref col-refs]
              (when (and (keyword? ref) (not (contains? col-names (norm/strip-ns ref))))
                (throw (ex-info (str "Unknown column " ref " in :agg. Available: " (sort col-names))
                                {:column ref :available col-names :agg a}))))))))
    ;; Validate SELECT column references
    (doseq [s (or select [])]
      (cond
        (keyword? s)
        (when-not (contains? col-names (norm/strip-ns s))
          (throw (ex-info (str "Unknown column " s " in :select. Available: " (sort col-names))
                          {:column s :available col-names})))

        (and (sequential? s) (= :as (first s)) (keyword? (second s)))
        (when-not (contains? col-names (norm/strip-ns (second s)))
          (throw (ex-info (str "Unknown column " (second s) " in :select. Available: " (sort col-names))
                          {:column (second s) :available col-names})))))))

(defn q
  "Query columnar data using Stratum's analytical engine.

   Takes a query map describing the data source, filters, aggregations,
   window functions, and result format. Routes automatically to the optimal
   execution strategy (SIMD fused, Java group-by, index scan, or scalar).

   == Query Map Keys ==

   :from     Data source. Either a StratumDataset (preferred) or a plain map
             of {col-keyword data} where data is long[], double[], String[],
             {:type :int64/:float64, :data array}, or PersistentColumnIndex.

   :where    Vector of predicates, implicitly ANDed. Accepts both keyword
             operators [:< :col 5] and Datahike-style symbols '(< :col 5).
             Supported ops: < > <= >= = != not= between in not-in
             like not-like contains starts-with ends-with is-null is-not-null.
             Combinators: [:or p1 p2], [:not p]. Expressions: [:> [:* :a :b] 10].

   :select   Vector of columns or expressions to project. Each element is
             a keyword (:col), an expression ([:upper :name]), or an alias
             ([:as :col :alias]). Produces row-level output (no aggregation).

   :agg      Vector of aggregation specs. Keyword form: [:sum :col],
             [:count], [:avg :col], [:as [:sum :col] :alias].
             Symbol form: '(sum :col), '(count).
             Expression form: [:sum [:* :price :qty]].
             Ops: sum count avg min max stddev variance corr count-distinct
             median percentile approx-quantile sum-product.

   :group    Vector of column keywords or expressions to group by.
             Required when :agg is present and you want per-group results.

   :having   Vector of predicates on aggregated results (post-GROUP filter).

   :window   Vector of window function specs, each a map:
             {:op :row-number/:rank/:dense-rank/:ntile/:percent-rank/:cume-dist
                  :sum/:count/:avg/:min/:max/:lag/:lead
              :as :result-col-name (required)
              :col :source-col (for agg/lag/lead)
              :partition-by [:col ...] (optional)
              :order-by [[:col :asc/:desc] ...] (optional)
              :frame {:type :rows/:range, :start ..., :end ...} (optional)
              :offset N (for lag/lead)
              :default val (for lag/lead)}

   :order    Vector of [col-kw :asc/:desc] pairs for sorting.

   :limit    Maximum number of result rows (long).

   :offset   Number of rows to skip (long).

   :result   Set to :columns for columnar output format.

   :distinct If true, deduplicate result rows.

   :join     Vector of join specs: {:with {col data}, :on [:= :l :r],
             :type :inner/:left/:right/:full}.

   == Return Values ==

   Default:       Vector of maps [{:col1 v1 :col2 v2} ...]
   :result :columns  Map of {:col1 array :col2 array :n-rows N}
   Without :agg:  [{:_count N}] (match count only)

   == Examples ==

   ;; Filter + aggregate
   (q {:from data
       :where [[:> :price 10]]
       :agg [[:sum :price] [:count]]
       :group [:category]})

   ;; Window function
   (q {:from data
       :window [{:op :rank :as :rnk
                 :partition-by [:dept]
                 :order-by [[:salary :desc]]}]})

   ;; Columnar output (15x faster for high-cardinality group-by)
   (q {:from data
       :agg [[:sum :revenue]]
       :group [:region]
       :result :columns})"
  [{:keys [from join where select agg group having order limit offset result distinct window _union _set-op]
    :as query}]
  ;; Handle set operations (UNION/INTERSECT/EXCEPT)
  (if-let [set-op (or _set-op (when _union {:op :union :queries (:queries _union) :all? (:all? _union)}))]
    (let [{:keys [op queries all?]} set-op
          sub-results (mapv q queries)]
      (case op
        :union
        (let [combined (vec (apply concat sub-results))]
          (if all? combined (vec (clojure.core/distinct combined))))
        :intersect
        (let [first-result (set (first sub-results))]
          (vec (reduce (fn [acc r] (clojure.set/intersection acc (set r)))
                       first-result (rest sub-results))))
        :except
        (let [first-result (first sub-results)
              to-remove (reduce (fn [acc r] (into acc r)) #{} (rest sub-results))]
          (vec (remove to-remove first-result)))))
    (do
  ;; Structural validation via malli specs
      (spec/validate! spec/SQuery query {:op :execute})
  ;; Semantic validation: column existence, type checks
      (when-not (seq join)
        (validate-query from where agg group select))

  ;; Extract normalized columns from dataset or prepare from map
  ;; Datasets already have normalized columns (via encode-column in make-dataset)
  ;; Maps need normalization via prepare-columns
      (let [columns (if (satisfies? dataset/IDataset from)
                      (dataset/columns from)  ;; Already normalized
                      (prepare-columns from))
        ;; Strip SQL table-qualifier namespaces from group/order/window keywords
            group (when group (mapv #(if (keyword? %) (norm/strip-ns %) %) group))
            order (when order (mapv (fn [[c dir]] [(norm/strip-ns c) dir]) order))
            window (when window
                     (mapv (fn [ws]
                             (cond-> ws
                               (:col ws) (update :col norm/strip-ns)
                               (:partition-by ws) (update :partition-by #(mapv norm/strip-ns %))
                               (:order-by ws) (update :order-by #(mapv (fn [[c d]] [(norm/strip-ns c) d]) %))))
                           window))
        ;; FUSED JOIN+GROUP+AGG: probe+gather+group-by in single Java pass
        ;; Eligible when: single INNER join, GROUP BY dim cols, agg fact cols, no WHERE
            fused-result
            (when (and (seq join) (seq group) (seq agg) (empty? (or where [])) (nil? select))
              (let [norm-aggs (norm/auto-alias-aggs (mapv norm/normalize-agg agg))]
                (when (jn/fused-join-group-agg-eligible? join group norm-aggs columns)
                  (let [fact-length (get-column-length (val (first columns)))
                        columnar? (= :columns result)]
                    (jn/execute-fused-join-group-agg
                     columns fact-length (first join) group norm-aggs columnar?)))))
        ;; FUSED JOIN+GLOBAL-AGG: single-pass probe+accumulate, no gather
        ;; For join queries without GROUP BY (H2O J1/J2/J3 pattern)
            fused-global-join-result
            (when (and (not fused-result)
                       (seq join) (not (seq group)) (seq agg)
                       (empty? (or where [])) (nil? select))
              (let [norm-aggs (norm/auto-alias-aggs (mapv norm/normalize-agg agg))]
                (when (jn/fused-join-global-agg-eligible? join norm-aggs)
                  (let [fact-length (get-column-length (val (first columns)))]
                    (jn/execute-fused-join-global-agg
                     columns fact-length (first join) norm-aggs)))))
            fused-any (or fused-result fused-global-join-result)]
        (if fused-any
      ;; Post-processing for fused results
          (if (= :columns result)
            fused-any
            (let [results (if distinct (post/apply-distinct fused-any) fused-any)
                  results (if (seq having) (post/apply-having results having) results)
                  results (if (seq order) (post/apply-order results order limit offset) results)
                  results (if (or limit offset) (post/apply-limit-offset results limit offset) results)]
              results))
      ;; Normal path
          (let [;; JOIN PHASE: execute joins before any predicate processing
                [columns length]
                (if (seq join)
                  (let [initial-length (get-column-length (val (first columns)))
                        result (jn/execute-joins columns initial-length join)]
                    [(:columns result) (long (:length result))])
                  [columns nil])]
  ;; Bind expr/*columns-meta* so expr/eval-expr-vectorized and gb/eval-agg-expr can access dict metadata
            (binding [expr/*columns-meta* (into {} (keep (fn [[k v]] (when (:dict v) [k v]))) columns)]
              (let [preds (mapv norm/normalize-pred (or where []))
                    aggs (norm/auto-alias-aggs (mapv norm/normalize-agg (or agg [])))
        ;; Determine data length from first column (use join length if available)
                    length (long (or length
                                     (get-column-length (val (first columns)))))

        ;; Zone map pruning: compute surviving chunks for index-sourced columns.
        ;; This must happen before any materialization to avoid double work.
        ;; Only applies when ALL columns are from indices AND we have simple predicates.
                    surviving-chunks (when (and (any-index? columns)
                                                (seq preds)
                                                (not (some #(map? (first %)) preds)))
                                       (try (compute-surviving-chunks preds columns)
                                            (catch Exception _ nil)))

        ;; Apply pruned materialization: replace index columns with shorter arrays
        ;; containing only the chunks that survive zone map filtering.
                    [columns length] (if surviving-chunks
                                       (let [pruned (materialize-columns-pruned columns surviving-chunks)
                                             first-data (:data (val (first pruned)))
                                             new-len (cond
                                                       (expr/long-array? first-data) (alength ^longs first-data)
                                                       (expr/double-array? first-data) (alength ^doubles first-data)
                                                       :else length)]
                                         [pruned (long new-len)])
                                       [columns length])

        ;; Pre-compute expression predicates (e.g., [:> [:* :a :b] 1000])
                    [preds columns] (if (some #(map? (first %)) preds)
                                      (materialize-computed-preds preds columns length)
                                      [preds columns])

        ;; Materialize string predicates (LIKE, CONTAINS, etc.) into mask columns
                    [preds columns] (if (some #(pred/string-pred-ops (second %)) preds)
                                      (let [mat-cols (materialize-columns columns)]
                                        (pred/materialize-string-preds preds mat-cols length))
                                      [preds columns])

        ;; Resolve string equality/inequality on dict-encoded columns
        ;; Converts [:col :eq "Alice"] → [:col :eq 0] using dict codes
                    preds (if (some #(and (pred/dict-resolvable-ops (second %))
                                          (>= (count %) 3)
                                          (string? (nth % 2))) preds)
                            (pred/resolve-dict-equality-preds preds columns)
                            preds)

        ;; Pre-materialize string-producing expressions (UPPER, LOWER, SUBSTR, etc.)
        ;; into dict-encoded temp columns. This must happen before group-by pre-computation
        ;; because string exprs produce dict-encoded columns, not numeric arrays.
                    [group aggs select columns]
                    (let [has-string-expr? (or (some #(and (sequential? %)
                                                           (expr/string-producing-expr? (norm/normalize-expr (vec %))))
                                                     group)
                                               (some #(and (:expr %)
                                                           (expr/string-producing-expr? (:expr %)))
                                                     aggs))]
                      (if has-string-expr?
                        (let [result (expr/materialize-string-exprs group aggs select columns length)]
              ;; set! updates the thread-local binding of *columns-meta* so that
              ;; subsequent expression eval within this `binding` scope sees the
              ;; newly materialized dict-encoded temp columns.
                          (set! expr/*columns-meta* (into {} (keep (fn [[k v]] (when (:dict v) [k v]))) (nth result 3)))
                          result)
                        [group aggs select columns]))

        ;; Compile non-SIMD predicates (OR, IN, NOT-IN) into a mask column.
        ;; The mask is materialized as a long[] and added as [:__mask :eq 1].
        ;; This lets the SIMD/Java paths handle any predicate shape with zero changes.
        ;; Only materialize columns referenced by non-SIMD preds (not all columns),
        ;; so that unreferenced index columns stay as indices for the chunked path.
                    [preds columns] (let [[simd-preds non-simd-preds] (pred/split-preds preds columns)]
                                      (if (seq non-simd-preds)
                                        (let [;; Extract column keys referenced by non-SIMD preds
                                              pred-col-keys (let [ks (atom #{})]
                                                              (letfn [(walk [p]
                                                                        (let [op (second p)]
                                                                          (case op
                                                                            :or (doseq [sub (subvec p 2)] (walk sub))
                                                                            (:in :not-in) (swap! ks conj (first p))
                                                                            (when (keyword? (first p)) (swap! ks conj (first p))))))]
                                                                (doseq [p non-simd-preds] (walk p)))
                                                              @ks)
                                              ;; Only materialize those columns, keep others as indices
                                              partial-mat (reduce (fn [cols k]
                                                                    (if-let [c (get cols k)]
                                                                      (assoc cols k (materialize-column c))
                                                                      cols))
                                                                  columns pred-col-keys)
                                              mask-fn (pred/compile-pred-mask non-simd-preds partial-mat)
                                              mask-arr (mask-fn length)]
                                          [(conj simd-preds [:__mask :eq 1])
                                           (assoc partial-mat :__mask {:type :int64 :data mask-arr})])
                                        [simd-preds columns]))

        ;; Detect fused extract+count eligibility BEFORE expression pre-computation
        ;; Eligible: single extract expr group, all-COUNT aggs, no predicates, long[] source
                    orig-group group
                    fused-extract? (and (= 1 (count group))
                                        (not (keyword? (first group)))
                                        (seq aggs)
                                        (every? #(= :count (:op %)) aggs)
                                        (empty? preds)
                                        (let [expr (norm/normalize-expr (vec (first group)))]
                                          (and (map? expr)
                                               (#{:minute :hour :second :day-of-week} (:op expr))
                                               (= 1 (count (:args expr)))
                                               (keyword? (first (:args expr)))
                                               (let [col-info (get columns (first (:args expr)))]
                                                 (or (expr/long-array? (:data col-info))
                                                     (and (:index col-info)
                                                          (= :int64 (:type col-info))))))))

        ;; Pre-compute expression group-by columns (e.g., [:minute :et] → __grp_0)
        ;; Skip when fused-extract will handle extraction inline
                    [group columns] (if (and (seq group)
                                             (not fused-extract?)
                                             (some #(not (keyword? %)) group))
                                      (let [mat-cols (materialize-columns columns)
                                            col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                                            cache (java.util.HashMap.)]
                                        (loop [idx 0, new-group [], new-cols columns]
                                          (if (>= idx (count group))
                                            [new-group new-cols]
                                            (let [g (nth group idx)]
                                              (if (keyword? g)
                                                (recur (inc idx) (conj new-group g) new-cols)
                                                (let [expr (norm/normalize-expr (vec g))
                                                      la (expr/eval-expr-to-long expr col-arrays length cache)
                                                      col-name (keyword (str "__grp_" idx))]
                                                  (recur (inc idx)
                                                         (conj new-group col-name)
                                                         (assoc new-cols col-name {:type :int64 :data la}))))))))
                                      [group columns])

        ;; Pre-materialize expression aggs into temp columns so SIMD routing matches.
        ;; E.g. [:avg [:length :url]] → compute LENGTH into __agg_expr_0, then [:avg :__agg_expr_0]
        ;; Only for global aggs (no group-by) — group-by does this after materialization
        ;; to preserve chunked index streaming eligibility.
                    [aggs columns] (if (and (seq aggs) (not (seq group))
                                            (some :expr aggs))
                                     (let [mat-cols (materialize-columns columns)
                                           col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                                           cache (java.util.HashMap.)]
                                       (loop [idx 0, new-aggs [], new-cols columns]
                                         (if (>= idx (count aggs))
                                           [new-aggs new-cols]
                                           (let [agg (nth aggs idx)]
                                             (if-let [expr (:expr agg)]
                                               (let [result-arr (expr/eval-expr-vectorized expr col-arrays length cache)
                                                     col-name (keyword (str "__agg_expr_" idx))]
                                                 (recur (inc idx)
                                                        (conj new-aggs (-> agg
                                                                           (dissoc :expr)
                                                                           (assoc :col col-name)))
                                                        (assoc new-cols col-name {:type :float64 :data result-arr})))
                                               (recur (inc idx) (conj new-aggs agg) new-cols))))))
                                     [aggs columns])

        ;; Normalize select items if present
                    select-items (when (seq select)
                                   (vec (map-indexed #(normalize-select-item %2 %1) select)))

        ;; Pre-materialize string-producing expressions in SELECT into temp columns
                    [select-items columns]
                    (if (and (seq select-items)
                             (some #(and (:expr %) (expr/string-producing-expr? (:expr %))) select-items))
                      (let [counter (atom 0)]
                        (reduce (fn [[items cols] sel]
                                  (if (and (:expr sel) (expr/string-producing-expr? (:expr sel)))
                                    (let [n (swap! counter inc)
                                          col-name (keyword (str "__sel_str_" n))
                                          col-entry (expr/eval-string-expr (:expr sel) cols length)]
                                      [(conj items (-> sel (dissoc :expr) (assoc :ref col-name)))
                                       (assoc cols col-name col-entry)])
                                    [(conj items sel) cols]))
                                [[] columns]
                                select-items))
                      [select-items columns])

        ;; Window functions: pre-routing only when no GROUP BY
        ;; When GROUP BY is present, windows run post-GROUP-BY on aggregated results
                    columns (if (and (seq window) (not (seq group)))
                              (win/execute-window-functions columns length window)
                              columns)

        ;; Window having pushdown: evaluate having on raw arrays before projection.
        ;; Avoids materializing ~6M row maps when only ~120K survive (e.g. ROW_NUMBER <= 2).
                    [columns length having]
                    (if (and (seq window) (not (seq group)) (seq having))
                      (let [normalized-havings (mapv norm/normalize-pred having)
                            win-cols (set (map :as window))
                            ;; Only push down if all having cols come from window results
                            all-window-cols? (every? #(contains? win-cols (first %)) normalized-havings)]
                        (if all-window-cols?
                          (let [matching (java.util.ArrayList.)]
                            (dotimes [i (int length)]
                              (when (every?
                                     (fn [pred]
                                       (let [col-key (first pred)
                                             col-entry (get columns col-key)
                                             col-data (if (map? col-entry) (:data col-entry) col-entry)
                                             v (if (expr/double-array? col-data)
                                                 (aget ^doubles col-data i)
                                                 (double (aget ^longs col-data i)))
                                             op (second pred)
                                             arg (double (nth pred 2))]
                                         (case op
                                           :lt (< v arg)
                                           :gt (> v arg)
                                           :lte (<= v arg)
                                           :gte (>= v arg)
                                           :eq (== v arg)
                                           :neq (not (== v arg))
                                           true)))
                                     normalized-havings)
                                (.add matching (int i))))
                            (let [n (.size matching)]
                              (if (< n (long length))
                                (let [indices (int-array n)
                                      _ (dotimes [i n] (aset indices i (int (.get matching i))))
                                      new-cols (reduce-kv
                                                (fn [m k v]
                                                  (let [data (if (map? v) (:data v) v)]
                                                    (assoc m k
                                                           (cond
                                                             (expr/long-array? data)
                                                             (let [g (ColumnOps/gatherLong ^longs data indices (int n))]
                                                               (if (map? v) (assoc v :data g) g))
                                                             (expr/double-array? data)
                                                             (let [g (ColumnOps/gatherDouble ^doubles data indices (int n))]
                                                               (if (map? v) (assoc v :data g) g))
                                                             (expr/string-array? data)
                                                             (let [out (make-array String n)]
                                                               (dotimes [i n]
                                                                 (aset ^"[Ljava.lang.String;" out i
                                                                       (aget ^"[Ljava.lang.String;" data (aget indices i))))
                                                               (if (map? v) (assoc v :data out) out))
                                                             :else v))))
                                                {}
                                                columns)]
                                  [new-cols n nil]) ;; nil clears having — already applied
                                [columns length having])))
                          [columns length having]))
                      [columns length having])

        ;; If we have window functions, ensure they appear in the select projection
                    select-items (if (and (seq window) (not (seq group)) (seq select-items))
                       ;; Add window result columns to select items if not already present
                                   (let [win-names (set (map :as window))
                                         existing-names (set (map :name select-items))]
                                     (into select-items
                                           (keep (fn [ws]
                                                   (when-not (contains? existing-names (:as ws))
                                                     {:name (:as ws) :ref (:as ws)})))
                                           window))
                                   select-items)

        ;; If we have window functions but no explicit select, build a projection
        ;; that includes all original columns + window columns
                    select-items (if (and (seq window) (not (seq select-items)) (not (seq aggs)) (not (seq group)))
                                   (let [base-cols (vec (remove #{:__mask} (keys columns)))
                                         items (mapv (fn [k] {:name k :ref k}) base-cols)]
                                     items)
                                   select-items)

        ;; Route to execution strategy
                    results
                    (cond
          ;; Projection (SELECT without aggregation)
                      (and (seq select-items) (empty? aggs) (not (seq group)))
                      (execute-projection preds select-items columns length (= :columns result))

          ;; Fused extract + COUNT group-by: extract inline, no intermediate array
                      fused-extract?
                      (let [expr (norm/normalize-expr (vec (first orig-group)))
                            col-key (first (:args expr))
                            col-info (get columns col-key)
                            ^longs raw-col (or (:data col-info)
                                               (index/idx-materialize-to-array (:index col-info)))
                            expr-type (case (:op expr)
                                        :minute      (int ColumnOpsExt/EXTRACT_MINUTE)
                                        :hour        (int ColumnOpsExt/EXTRACT_HOUR)
                                        :second      (int ColumnOpsExt/EXTRACT_SECOND)
                                        :day-of-week (int ColumnOpsExt/EXTRACT_DAY_OF_WEEK))
                            n-aggs (int (count aggs))
                            ^"[[D" result-array (ColumnOpsExt/fusedExtractCountDenseParallel
                                                 raw-col expr-type n-aggs (int length))
                            max-key (int (ColumnOpsExt/extractMaxKey expr-type))
                ;; Build group-col name from original expression
                            group-col-name (keyword (str (name (:op expr))))
                            agg-aliases (mapv #(or (:as %) (:op %)) aggs)
                            columnar? (= :columns result)]
                        (if columnar?
              ;; Columnar output
                          (let [key-list (java.util.ArrayList.)
                                _ (dotimes [k max-key]
                                    (let [^doubles accs (aget result-array k)]
                                      (when (and accs (> (aget accs 1) 0.0))
                                        (.add key-list (long k)))))
                                n (.size key-list)
                                keys-arr (long-array n)
                                _ (dotimes [i n] (aset keys-arr i (long (.get key-list i))))
                                agg-arrs (mapv (fn [a-idx]
                                                 (let [la (long-array n)]
                                                   (dotimes [i n]
                                                     (let [k (long (.get key-list i))
                                                           ^doubles accs (aget result-array (int k))]
                                                       (aset la i (long (aget accs (inc (* a-idx 2)))))))
                                                   la))
                                               (range n-aggs))]
                            (-> {group-col-name keys-arr :n-rows n}
                                (into (map vector agg-aliases agg-arrs))))
              ;; Row-oriented output
                          (let [results (transient [])]
                            (dotimes [k max-key]
                              (let [^doubles accs (aget result-array k)]
                                (when (and accs (> (aget accs 1) 0.0))
                                  (let [cnt (long (aget accs 1))
                                        row (-> {group-col-name (long k)}
                                                (into (map (fn [alias] [alias cnt]) agg-aliases))
                                                (assoc :_count cnt))]
                                    (conj! results row)))))
                            (persistent! results))))

          ;; Group-by query — try chunk-streaming first, fallback to materialization
                      (seq group)
                      (let [columnar? (= :columns result)]
                        (or (gb/execute-chunked-group-by preds aggs group columns length columnar?)
                            (let [mat-cols (materialize-columns columns)
                                  ;; Pre-compute expression aggs after materialization so
                                  ;; chunked streaming is tried first with raw indices.
                                  [aggs mat-cols]
                                  (if (some :expr aggs)
                                    (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                                          cache (java.util.HashMap.)]
                                      (loop [idx 0, new-aggs [], new-cols mat-cols]
                                        (if (>= idx (count aggs))
                                          [new-aggs new-cols]
                                          (let [agg (nth aggs idx)]
                                            (if-let [expr (:expr agg)]
                                              (let [result-arr (expr/eval-expr-vectorized expr col-arrays length cache)
                                                    col-name (keyword (str "__agg_expr_" idx))]
                                                (recur (inc idx)
                                                       (conj new-aggs (-> agg
                                                                          (dissoc :expr)
                                                                          (assoc :col col-name)))
                                                       (assoc new-cols col-name {:type :float64 :data result-arr})))
                                              (recur (inc idx) (conj new-aggs agg) new-cols))))))
                                    [aggs mat-cols])]
                              (execute-group-by preds aggs group mat-cols length columnar?))))

          ;; Unfiltered COUNT — short-circuit to avoid JIT poisoning.
          ;; fusedSimdCountRange compiled with numPreds=0 marks predicate branches
          ;; as UnreachedCode. When a filtered COUNT later hits those branches,
          ;; the JIT deoptimizes (2ms → 30ms). Returning length directly avoids
          ;; ever calling fusedSimdCountRange with zero predicates.
                      (and (= 1 (count aggs))
                           (= :count (:op (first aggs)))
                           (empty? preds))
                      (post/format-fused-result {:result 0.0 :count length} (first aggs))

          ;; Unfiltered SUM/MIN/MAX/AVG/COUNT on index inputs — stats-only O(chunks).
          ;; Walks chunk-level statistics instead of materializing + SIMD.
                      (and (seq aggs)
                           (empty? preds)
                           (not (seq group))
                           (all-indices? columns)
                           (every? (fn [a] (and (#{:sum :min :max :avg :count} (:op a))
                                                (nil? (:expr a))))
                                   aggs))
                      (let [result-row
                            (reduce
                             (fn [row agg]
                               (let [alias (keyword (or (:as agg) (:op agg)))]
                                 (if (= :count (:op agg))
                                   (assoc row alias (long length) :_count length)
                                   (let [entries (gb/collect-chunk-entries (:index (get columns (:col agg))))
                                         n-chunks (count entries)
                                         [sum cnt mn mx]
                                         (loop [i 0 sum 0.0 cnt (long 0) mn Double/MAX_VALUE mx (- Double/MAX_VALUE)]
                                           (if (>= i n-chunks)
                                             [sum cnt mn mx]
                                             (let [^ChunkEntry entry (nth entries i)
                                                   ^stratum.stats.ChunkStats cs (.stats entry)]
                                               (recur (inc i)
                                                      (+ sum (double (:sum cs)))
                                                      (+ cnt (long (:count cs)))
                                                      (Math/min mn (double (:min-val cs)))
                                                      (Math/max mx (double (:max-val cs)))))))]
                                     (assoc row alias
                                            (case (:op agg)
                                              :sum (if (zero? cnt) nil sum)
                                              :min (if (zero? cnt) nil mn)
                                              :max (if (zero? cnt) nil mx)
                                              :avg (if (zero? cnt) nil (/ sum (double cnt))))
                                            :_count cnt)))))
                             {}
                             aggs)]
                        [result-row])

          ;; Chunk-streaming SIMD: all inputs are indices + simd-eligible
          ;; COUNT uses JIT-isolated fusedSimdChunkedCountParallel in ColumnOpsExt
          ;; to avoid fusedSimdChunkBatch aggType interference (B5 idx: 2ms→27ms).
                      (and (all-indices? columns)
                           (= 1 (count aggs))
                           (pred/simd-eligible? preds aggs columns length)
                           (nil? (:expr (first aggs))))
                      (let [agg (first aggs)]
                        (if (= :count (:op agg))
              ;; COUNT: JIT-isolated chunked count (no agg columns, no aggType switch)
                          (let [result (execute-chunked-fused-count preds columns length)]
                            (post/format-fused-result result agg))
                          (if (= :avg (:op agg))
                ;; avg = sum / count via chunked path
                            (let [sum-result (execute-chunked-fused
                                              preds {:op :sum :col (:col agg) :as nil}
                                              columns length)
                                  cnt (:count sum-result)]
                              (post/format-fused-result
                               (if (zero? cnt) {:result Double/NaN :count 0}
                                   {:result (/ (:result sum-result) (double cnt)) :count cnt})
                               agg))
                            (post/format-fused-result
                             (execute-chunked-fused preds agg columns length)
                             agg))))

          ;; Block-skip COUNT on arrays: skip blocks via min/max statistics
                      (and (= 1 (count aggs))
                           (= :count (:op (first aggs)))
                           (seq preds)
                           (not (all-indices? columns))
                           (pred/simd-eligible? preds aggs columns length)
                           (nil? (:expr (first aggs))))
                      (let [mat-cols (materialize-columns columns)
                            pp (gb/prepare-pred-arrays preds mat-cols)
                            ^doubles r (ColumnOpsExt/fusedSimdCountBlockSkipParallel
                                        (int (:n-long pp))
                                        ^ints (:long-pred-types pp)
                                        ^"[[J" (:long-cols pp)
                                        ^longs (:long-lo pp)
                                        ^longs (:long-hi pp)
                                        (int (:n-dbl pp))
                                        ^ints (:dbl-pred-types pp)
                                        ^"[[D" (:dbl-cols pp)
                                        ^doubles (:dbl-lo pp)
                                        ^doubles (:dbl-hi pp)
                                        (int length))]
                        (post/format-fused-result {:result 0.0 :count (long (aget r 1))} (first aggs)))

          ;; Single fused SIMD aggregate on arrays (fastest path for arrays)
                      (and (= 1 (count aggs))
                           (pred/simd-eligible? preds aggs columns length)
                           (nil? (:expr (first aggs))))
                      (let [mat-cols (materialize-columns columns)]
                        (post/format-fused-result
                         (execute-fused preds (first aggs) mat-cols length)
                         (first aggs)))

          ;; Multiple SUM-like aggs — single-pass via fusedSimdMultiSum (1 pass vs N)
                      (and (seq aggs)
                           (not (seq group))
                           (pred/multi-agg-simd-eligible? preds aggs columns length)
                           (every? #(#{:sum :sum-product :count :avg} (:op %)) aggs)
                           (<= (count (filterv #(not= :count (:op %)) aggs)) 4))
                      (let [mat-cols (materialize-columns columns)]
                        (execute-fused-multi-sum preds aggs mat-cols length))

          ;; Multiple simple aggs with MIN/MAX — fall back to N separate SIMD passes
                      (and (seq aggs)
                           (not (seq group))
                           (pred/multi-agg-simd-eligible? preds aggs columns length))
                      (let [mat-cols (materialize-columns columns)
                            agg-results (mapv (fn [a] (execute-fused preds a mat-cols length)) aggs)
                            cnt (:count (first agg-results))]
                        [(into {:_count cnt}
                               (map (fn [a r]
                                      [(or (:as a) (:op a))
                                       (case (:op a)
                                         (:count :count-non-null) (long (:count r))
                                         (:min :max :sum :sum-product) (if (zero? (:count r)) nil (:result r))
                                         :avg (if (zero? (:count r)) nil (:result r))
                                         (:result r))])
                                    aggs agg-results))])

          ;; Fast path for ungrouped percentile/median/approx-quantile — bypass Clojure scalar loop
                      (and (seq aggs)
                           (not (seq group))
                           (every? #(#{:median :percentile :approx-quantile} (:op %)) aggs))
                      (let [mat-cols (materialize-columns columns)
                            col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                ;; Build predicate mask (long[]) if predicates exist
                            mask (when (seq preds)
                                   (let [m (long-array length)]
                                     (dotimes [i length]
                                       (when (every? #(gb/eval-pred-scalar col-arrays i %) preds)
                                         (aset m i 1)))
                                     m))
                            cnt (if mask
                                  (areduce ^longs mask i s (long 0) (+ s (aget ^longs mask i)))
                                  (long length))]
                        [(into {:_count cnt}
                               (map (fn [agg]
                                      (let [alias (or (:as agg) (:op agg))
                                            col-data (get col-arrays (:col agg))
                                            is-long? (expr/long-array? col-data)
                                            pct (double (case (:op agg)
                                                          :median 0.5
                                                          (:percentile :approx-quantile) (:param agg)))
                                            result (case (:op agg)
                                                     (:median :percentile)
                                                     (if mask
                                                       (if is-long?
                                                         (ColumnOps/percentileFilteredLong ^longs col-data ^longs mask (int length) pct)
                                                         (ColumnOps/percentileFiltered ^doubles col-data ^longs mask (int length) pct))
                                                       (ColumnOps/percentile (if is-long?
                                                                               (let [la ^longs col-data
                                                                                     da (double-array (alength la))]
                                                                                 (dotimes [i (alength la)] (aset da i (double (aget la i))))
                                                                                 da)
                                                                               ^doubles col-data)
                                                                             (int length) pct))
                                                     :approx-quantile
                                                     (if mask
                                                       (let [work (double-array cnt)
                                                             pos (int-array 1)]
                                                         (dotimes [i length]
                                                           (when (== 1 (aget ^longs mask i))
                                                             (aset work (aget pos 0)
                                                                   (if is-long?
                                                                     (double (aget ^longs col-data i))
                                                                     (aget ^doubles col-data i)))
                                                             (aset pos 0 (inc (aget pos 0)))))
                                                         (ColumnOpsAnalytics/tdigestApproxQuantileParallel work (int cnt) pct 200.0))
                                                       (let [da (if is-long?
                                                                  (let [la ^longs col-data
                                                                        d (double-array (alength la))]
                                                                    (dotimes [i (alength la)] (aset d i (double (aget la i))))
                                                                    d)
                                                                  ^doubles col-data)]
                                                         (ColumnOpsAnalytics/tdigestApproxQuantileParallel da (int length) pct 200.0))))]
                                        [alias result]))
                                    aggs))])

          ;; Multiple or complex aggregations - scalar path
                      (seq aggs)
                      (let [mat-cols (materialize-columns columns)]
                        [(gb/execute-scalar-aggs preds aggs mat-cols length)])

          ;; Filter only (no aggregation) - just count
                      :else
                      (let [mat-cols (materialize-columns columns)
                            col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)]
                        [{:_count (loop [i 0 cnt 0]
                                    (if (>= i length)
                                      cnt
                                      (if (every? #(gb/eval-pred-scalar col-arrays i %) preds)
                                        (recur (inc i) (inc cnt))
                                        (recur (inc i) cnt))))}]))]

    ;; Post-processing pipeline (skip for columnar results — raw arrays)
                (if (= :columns result)
                  results
                  (let [;; Post-GROUP-BY window execution: run windows on aggregated results
                        results (if (and (seq window) (seq group) (seq results))
                                  (let [grouped-cols (results->columns results)
                                        n-grouped (count results)
                                        with-windows (win/execute-window-functions grouped-cols n-grouped window)
                                        all-ks (keys with-windows)]
                                    (mapv (fn [i]
                                            (reduce (fn [m k]
                                                      (let [v (get with-windows k)]
                                                        (assoc m k
                                                               (cond
                                                                 (expr/long-array? v) (aget ^longs v i)
                                                                 (expr/double-array? v) (aget ^doubles v i)
                                                                 (expr/string-array? v)
                                                                 (aget ^"[Ljava.lang.String;" v i)
                                                     ;; Wrapped in {:type :data} map
                                                                 (map? v)
                                                                 (let [d (:data v)]
                                                                   (cond
                                                                     (expr/long-array? d) (aget ^longs d i)
                                                                     (expr/double-array? d) (aget ^doubles d i)
                                                                     :else (aget ^"[Ljava.lang.String;" d i)))
                                                                 :else (get m k)))))
                                                    {} all-ks))
                                          (range n-grouped)))
                                  results)
                        results (if distinct (post/apply-distinct results) results)
                        results (if (seq having) (post/apply-having results having) results)
                        results (if (seq order) (post/apply-order results order limit offset) results)
                        results (if (or limit offset) (post/apply-limit-offset results limit offset) results)]
                    results))))))))))

(defn compile-query
  "Compile a query for repeated execution. Returns a zero-arg function.
   Use when the same query will be executed multiple times on the same data."
  [{:keys [from where agg] :as query}]
  (let [columns (prepare-columns from)
        preds (mapv norm/normalize-pred (or where []))
        aggs (mapv norm/normalize-agg (or agg []))
        length (get-column-length (val (first columns)))]
    (cond
      ;; Chunk-streaming path: all indices + simd-eligible
      ;; COUNT uses JIT-isolated fusedSimdChunkedCountParallel
      (and (all-indices? columns)
           (= 1 (count aggs))
           (pred/simd-eligible? preds aggs columns length)
           (nil? (:expr (first aggs)))
           (nil? (:group query)))
      (let [agg (first aggs)]
        (fn []
          (if (= :count (:op agg))
            (post/format-fused-result (execute-chunked-fused-count preds columns length) agg)
            (let [result (execute-chunked-fused preds agg columns length)]
              (if (= :avg (:op agg))
                (let [cnt (:count result)]
                  [{(keyword (or (:as agg) :avg))
                    (if (zero? cnt) Double/NaN (/ (:result result) (double cnt)))
                    :_count cnt}])
                (post/format-fused-result result agg))))))

      ;; Array-based SIMD path
      (and (= 1 (count aggs))
           (pred/simd-eligible? preds aggs columns length)
           (nil? (:expr (first aggs)))
           (nil? (:group query)))
      (let [mat-cols (materialize-columns columns)
            agg (first aggs)
            compiler-agg (agg->compiler-spec agg)
            compiled (qc/compile-query
                      {:columns mat-cols
                       :predicates preds
                       :aggregate compiler-agg
                       :length length})]
        (fn []
          (let [result (compiled)]
            (if (= :avg (:op agg))
              (let [cnt (:count result)]
                [{(keyword (or (:as agg) :avg))
                  (if (zero? cnt) Double/NaN (/ (:result result) (double cnt)))
                  :_count cnt}])
              (post/format-fused-result result agg)))))
      ;; Fall back to interpreted execution
      :else
      (fn [] (q query)))))

(defn explain
  "Show execution plan without running the query.

   Returns a map describing which strategy would be used:
     :strategy    — keyword naming the execution path
     :predicates  — number and types of predicates
     :aggregates  — normalized aggregate specs
     :group-by    — group columns if present
     :data-source — :index or :array
     :n-rows      — estimated row count
     :query       — the normalized query map"
  [{:keys [from join where select agg group] :as query}]
  (let [columns (prepare-columns from)
        preds (mapv norm/normalize-pred (or where []))
        aggs (norm/auto-alias-aggs (mapv norm/normalize-agg (or agg [])))
        length (get-column-length (val (first columns)))
        data-source (if (all-indices? columns) :index :array)
        has-join? (seq join)
        has-group? (seq group)
        has-select? (seq select)
        pred-info {:count (count preds)
                   :long-preds (count (filter #(= :int64 (:type (get columns (second %)))) preds))
                   :double-preds (count (filter #(= :float64 (:type (get columns (second %)))) preds))}
        strategy (cond
                   (and has-join? has-group? (seq aggs) (empty? (or where [])) (nil? select)
                        (jn/fused-join-group-agg-eligible? join group aggs columns))
                   :fused-join-group-by

                   (and has-select? (empty? aggs) (not has-group?))
                   :projection

                   (and has-group?
                        (gb/chunked-group-by-eligible? group aggs preds columns length))
                   :chunked-group-by

                   has-group?
                   (let [all-count? (every? #(= :count (:op %)) aggs)]
                     (if all-count?
                       :dense-count-group-by
                       :dense-group-by))

                   (and (all-indices? columns)
                        (= 1 (count aggs))
                        (pred/simd-eligible? preds aggs columns length)
                        (nil? (:expr (first aggs))))
                   :chunked-simd

                   (and (= 1 (count aggs))
                        (pred/simd-eligible? preds aggs columns length)
                        (nil? (:expr (first aggs))))
                   :fused-simd

                   (and (seq aggs)
                        (not has-group?)
                        (pred/multi-agg-simd-eligible? preds aggs columns length)
                        (every? #(#{:sum :sum-product :count :avg} (:op %)) aggs)
                        (<= (count (filterv #(not= :count (:op %)) aggs)) 4))
                   :fused-multi-sum

                   (and (seq aggs) (not has-group?)
                        (pred/multi-agg-simd-eligible? preds aggs columns length))
                   :multi-pass-simd

                   (seq aggs)
                   :scalar-agg

                   :else
                   :filter-count)]
    {:strategy strategy
     :predicates pred-info
     :aggregates (mapv #(select-keys % [:op :col :as]) aggs)
     :group-by (vec (or group []))
     :data-source data-source
     :n-rows length
     :columns (count columns)
     :join (when has-join? {:count (count join)})
     :query (dissoc query :from)}))

;; ============================================================================
;; Materialization Utilities (for Datahike integration)
;; ============================================================================

(defn tuples->columns
  "Convert positional tuples into a column map suitable for Stratum queries.

   Takes a collection of tuples (vectors or arrays, positional) and a
   column-names vector specifying the name for each position.

   Returns a map of {col-name typed-array ...} where the array type is
   inferred from the first non-nil value in each column:
   - integers → long[]
   - floats   → double[]
   - else     → String[]

   Example:
     (tuples->columns [[\"Alice\" 30] [\"Bob\" 25]] [:name :age])
     => {:name (String[] [\"Alice\" \"Bob\"]), :age (long[] [30 25])}"
  [tuples column-names]
  (let [n (count tuples)
        tuples (vec tuples)]
    (when (pos? n)
      (reduce
       (fn [m [idx col-name]]
         (let [;; Find first non-nil value for type inference
               sample (loop [i 0]
                        (when (< i n)
                          (let [v (nth (nth tuples i) idx)]
                            (if (some? v) v (recur (inc i))))))
               arr (cond
                     (or (nil? sample) (integer? sample))
                     (let [la (long-array n)]
                       (dotimes [i n]
                         (let [v (nth (nth tuples i) idx)]
                           (aset la i (if (some? v) (long v) 0))))
                       la)

                     (float? sample)
                     (let [da (double-array n)]
                       (dotimes [i n]
                         (let [v (nth (nth tuples i) idx)]
                           (aset da i (if (some? v) (double v) 0.0))))
                       da)

                     :else
                     (let [sa (make-array String n)]
                       (dotimes [i n]
                         (let [v (nth (nth tuples i) idx)]
                           (aset ^"[Ljava.lang.String;" sa i
                                 (if (some? v) (str v) nil))))
                       sa))]
           (assoc m col-name arr)))
       {}
       (map-indexed vector column-names)))))

(defn columns->tuples
  "Convert a column map into a vector of positional tuples.

   Takes a column map {col-name array ...} and a column-names vector
   specifying which columns to include and in what order.

   Returns a vector of vectors, one per row.

   Example:
     (columns->tuples {:name (String[] ...) :age (long[] ...)} [:name :age])
     => [[\"Alice\" 30] [\"Bob\" 25]]"
  [col-map column-names]
  (let [;; Determine row count from first column
        first-col (get col-map (first column-names))
        n (cond
            (expr/long-array? first-col) (alength ^longs first-col)
            (expr/double-array? first-col) (alength ^doubles first-col)
            (expr/string-array? first-col) (alength ^"[Ljava.lang.String;" first-col)
            ;; Handle normalized column maps {:data arr}
            (and (map? first-col) (:data first-col))
            (let [d (:data first-col)]
              (cond
                (expr/long-array? d) (alength ^longs d)
                (expr/double-array? d) (alength ^doubles d)
                :else (alength ^"[Ljava.lang.String;" d)))
            :else 0)
        ;; Resolve each column to its raw array
        arrays (mapv (fn [col-name]
                       (let [c (get col-map col-name)]
                         (if (and (map? c) (:data c)) (:data c) c)))
                     column-names)]
    (vec (for [i (range n)]
           (mapv (fn [^Object arr]
                   (cond
                     (expr/long-array? arr) (aget ^longs arr i)
                     (expr/double-array? arr) (aget ^doubles arr i)
                     (expr/string-array? arr) (aget ^"[Ljava.lang.String;" arr i)
                     :else nil))
                 arrays)))))

;; ============================================================================
;; JIT Warmup
;; ============================================================================

(defn jit-warmup!
  "Exercise all major Java hot paths with tiny data so the JIT compiler sees
   every code shape before real queries arrive. This prevents deoptimization
   cliffs when a long-running server encounters a new query type for the first
   time after the JIT has already specialized on other shapes.

   Takes ~1-2 seconds. Call once during server startup, after class loading
   but before serving client queries."
  []
  (let [n      1000
        rng    (java.util.Random. 42)
        ;; Tiny arrays covering all types
        longs  (long-array (repeatedly n #(.nextLong rng)))
        dbls   (double-array (repeatedly n #(.nextDouble rng)))
        longs2 (long-array (repeatedly n #(.nextLong rng)))
        dbls2  (double-array (repeatedly n #(.nextDouble rng)))
        ;; Small group keys (10 groups for dense, 500 for hash)
        gk-lo  (long-array (map #(mod % 10) (range n)))
        gk-hi  (long-array (map #(mod % 500) (range n)))
        ;; String dictionary data
        strs   (into-array String (map #(str "val_" (mod % 50)) (range n)))
        enc    (encode-column strs)
        ;; Index inputs (exercises chunked paths)
        idx-l  (index/index-from-array longs)
        idx-d  (index/index-from-array dbls)
        from-a {:a longs :b dbls :c longs2 :d dbls2 :g gk-lo :gh gk-hi
                :s enc}
        from-i {:a idx-l :b idx-d :g {:type :int64 :data gk-lo}
                :s enc}
        iters  5]
    ;; 1. Fused SIMD filter+aggregate (SUM, COUNT, SUM_PRODUCT, MIN, MAX)
    (dotimes [_ iters]
      (q {:from from-a :where [[:> :a 0]] :agg [[:sum :b]]})
      (q {:from from-a :where [[:> :a 0]] :agg [[:count]]})
      (q {:from from-a :where [[:> :a 0] [:< :b 0.5]]
          :agg [[:sum :b] [:count] [:min :b] [:max :b]]})
      ;; Multi-sum path
      (q {:from from-a :where [[:> :a 0]]
          :agg [[:sum :b] [:sum :c] [:sum :d]]}))

    ;; 2. Dense group-by (all agg types: SUM, COUNT, AVG, MIN, MAX)
    (dotimes [_ iters]
      (q {:from from-a :group [:g]
          :agg [[:sum :b] [:count] [:avg :b] [:min :b] [:max :b]]})
      ;; COUNT-only group-by (separate JIT path)
      (q {:from from-a :group [:g] :agg [[:count]]}))

    ;; 3. Hash group-by (high-cardinality)
    (dotimes [_ iters]
      (q {:from from-a :group [:gh] :agg [[:sum :b] [:count]]}))

    ;; 4. String group-by with dictionary encoding
    (dotimes [_ iters]
      (q {:from from-a :group [:s] :agg [[:sum :b]]}))

    ;; 5. VARIANCE/CORR paths
    (dotimes [_ iters]
      (q {:from from-a :group [:g] :agg [[:variance :b]]})
      (q {:from from-a :group [:g] :agg [[:corr :b :d]]}))

    ;; 6. LIKE fast-path
    (dotimes [_ iters]
      (q {:from from-a :where [[:like :s "%val_1%"]] :agg [[:count]]}))

    ;; 7. Chunked index paths (SIMD + group-by)
    (dotimes [_ iters]
      (q {:from from-i :where [[:> :a 0]] :agg [[:sum :b]]})
      (q {:from from-i :where [[:> :a 0]] :agg [[:count]]})
      (q {:from from-i :group [:g] :agg [[:sum :b] [:count]]})
      (q {:from from-i :group [:g] :agg [[:count]]})
      (q {:from from-i :group [:g] :agg [[:variance :b]]}))

    ;; 8. Expression aggs (LENGTH, arithmetic)
    (dotimes [_ iters]
      (q {:from from-a :agg [[:avg [:length :s]]]})
      (q {:from from-a :group [:g] :agg [[:avg [:length :s]]]}))

    ;; 9. Join path
    (let [dim-k (long-array (range 10))
          dim-v (double-array (repeatedly 10 #(.nextDouble rng)))]
      (dotimes [_ iters]
        (q {:from from-a
            :join [{:with {:dk dim-k :dv dim-v} :on [:= :g :dk] :type :inner}]
            :agg [[:sum :dv] [:count]]})))

    ;; 10. Window functions
    (dotimes [_ iters]
      (q {:from from-a
          :window [{:op :row-number :partition-by [:g] :order-by [[:a :asc]] :as :rn}]})
      (q {:from from-a
          :window [{:op :sum :col :b :partition-by [:g] :order-by [[:a :asc]] :as :rs}]}))

    ;; 11. COUNT DISTINCT
    (dotimes [_ iters]
      (q {:from from-a :agg [[:count-distinct :a]]})
      (q {:from from-a :group [:g] :agg [[:count-distinct :a]]}))

    ;; Force GC to clean up warmup garbage before real queries
    (System/gc)))
