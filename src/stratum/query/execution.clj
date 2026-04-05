(ns stratum.query.execution
  "Engine execution primitives: SIMD fused aggregation, chunk-streaming,
   group-by delegation, projection, zone-map pruning, and column utilities.

   Shared by both the original cond-routing in stratum.query and the
   IR-based plan executor in stratum.query.executor."
  (:require [stratum.query.simd-primitive :as qc]
            [stratum.query.normalization :as norm]
            [stratum.query.expression :as expr]
            [stratum.query.columns :as cols]
            [stratum.query.group-by :as gb]
            [stratum.query.postprocess :as post]
            [stratum.index :as index]
            [stratum.chunk :as chunk])
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsLong ColumnOpsChunked ColumnOpsChunkedSimd ColumnOpsChunkedLong ColumnOpsAnalytics]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Column Utilities
;; ============================================================================

(defn prepare-columns
  "Normalize all columns in :from map. Preserves index sources."
  [from-map]
  (cols/prepare-columns from-map))

(defn get-column-length
  "Get the length of a column (array or index-backed)."
  ^long [col-info]
  (cols/get-column-length col-info))

(defn materialize-column
  "Ensure a column has array :data, materializing from index if needed."
  [col-info]
  (cols/materialize-column col-info))

(defn materialize-columns
  "Materialize all index-sourced columns to arrays (with memory budget check)."
  [columns]
  (cols/materialize-columns columns))

(defn all-indices?
  "Check if all columns are sourced from Stratum indices."
  [columns]
  (every? #(= :index (:source (val %))) columns))

(defn any-index?
  "Check if any column is sourced from a Stratum index."
  [columns]
  (some #(= :index (:source (val %))) columns))

;; ============================================================================
;; Group-By Delegation
;; ============================================================================

(defn execute-group-by
  "Delegate to gb/execute-group-by. Callers should bind gb/*dense-group-limit*
   if a non-default limit is needed."
  [preds aggs group-cols columns length columnar?]
  (gb/execute-group-by preds aggs group-cols columns length columnar?))

;; ============================================================================
;; Predicate Pre-processing (computed expressions, OR/IN/NOT)
;; ============================================================================

(defn materialize-computed-preds
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

(defn agg->compiler-spec
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

(defn execute-fused
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

(defn ensure-doubles
  "Ensure column data is double[], converting long[] if needed."
  ^doubles [col-info ^long length]
  (let [data (:data col-info)]
    (if (expr/double-array? data)
      data
      (ColumnOps/longToDoubleNullSafe ^longs data (int length)))))

(defn execute-fused-multi-sum
  "Execute multiple SUM-like aggs in a single pass via Java fusedSimdMultiSumParallel.
   All aggs must be :sum, :sum-product, :count, or :avg (no :min/:max).
   Evaluates predicates once, accumulates all SUM values simultaneously."
  [preds aggs columns length]
  (let [{:keys [n-long long-pred-types long-cols long-lo long-hi
                n-dbl dbl-pred-types dbl-cols dbl-lo dbl-hi]} (gb/prepare-pred-arrays preds columns)

        ;; Collect SUM columns (skip COUNT-only aggs)
        sum-aggs (filterv #(not= :count (:op %)) aggs)
        n-sum    (count sum-aggs)
        ;; Check if ALL sum columns are long[] (including SUM_PRODUCT with both cols long[])
        ;; → use all-long path (LongVector accumulators, no longToDouble allocation)
        ;; Double predicates are OK — they're orthogonal to agg column type
        all-long? (every? (fn [a]
                            (case (:op a)
                              (:sum :avg)
                              (expr/long-array? (:data (get columns (:col a))))
                              :sum-product
                              (and (expr/long-array? (:data (get columns (first (:cols a)))))
                                   (expr/long-array? (:data (get columns (second (:cols a))))))
                              false))
                          sum-aggs)
        ;; For SUM_PRODUCT in long path: pre-multiply with overflow check
        ;; Returns nil on overflow → falls back to double path
        ;; Call Java single-pass — try long path first, fall back to double on overflow
        ^doubles result (or
                         (when all-long?
                            ;; All-long path: LongVector accumulators, no conversion
                           (let [overflow? (volatile! false)
                                 sum-long-cols
                                 (into-array expr/long-array-class
                                             (mapv (fn [a]
                                                     (case (:op a)
                                                       (:sum :avg) (:data (get columns (:col a)))
                                                       :sum-product
                                                       (let [c1 (:data (get columns (first (:cols a))))
                                                             c2 (:data (get columns (second (:cols a))))
                                                             r (ColumnOpsLong/arrayMulLongChecked ^longs c1 ^longs c2 (int length))]
                                                         (when-not r (vreset! overflow? true))
                                                         (or r (long-array 0)))))
                                                   sum-aggs))]
                             (when-not @overflow?
                               (if (zero? n-dbl)
                                 (ColumnOpsExt/fusedSimdMultiSumAllLongParallel
                                  (int n-long) long-pred-types
                                  ^"[[J" long-cols ^longs long-lo ^longs long-hi
                                  (int n-sum) ^"[[J" sum-long-cols
                                  (int length))
                                 (ColumnOpsLong/fusedSimdMultiSumAllLongMixedPredsParallel
                                  (int n-long) long-pred-types
                                  ^"[[J" long-cols ^longs long-lo ^longs long-hi
                                  (int n-dbl) dbl-pred-types
                                  ^"[[D" dbl-cols ^doubles dbl-lo ^doubles dbl-hi
                                  (int n-sum) ^"[[J" sum-long-cols
                                  (int length))))))
                          ;; Double path: ensure-doubles conversion (also fallback on overflow)
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

(defn accumulate-stats-chunk
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

(defn compute-surviving-chunks
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

(defn materialize-column-pruned
  "Materialize only surviving chunks of a column into a shorter array.
   For non-index columns, returns data as-is (can't prune)."
  [col-info surviving-indices]
  (if (:data col-info)
    col-info  ;; Already an array, can't prune
    (assoc col-info :data
           (index/idx-materialize-to-array-pruned
            (:index col-info) surviving-indices))))

(defn materialize-columns-pruned
  "Materialize all columns with chunk pruning. Only surviving chunks are copied."
  [columns surviving-indices]
  (into {} (map (fn [[k v]] [k (materialize-column-pruned v surviving-indices)])) columns))

(defn execute-chunked-multi-agg
  "Execute multiple aggregates in one pass over index chunks, no materialization.
   Uses ColumnOpsChunkedLong/fusedSimdChunkedMultiLongParallel for up to 4 aggs.
   Avg is decomposed to sum/count and divided after."
  [preds aggs columns length]
  (let [;; Decompose avg into sum (Java accumulates sum, we divide after)
        decomposed (mapv (fn [a] (if (= :avg (:op a)) (assoc a :op :sum :_was-avg true) a)) aggs)
        {:keys [n-long long-pred-types long-lo long-hi
                n-dbl dbl-pred-types dbl-lo dbl-hi
                long-preds dbl-preds]} (gb/prepare-pred-bounds preds columns)
        long-col-names (mapv first long-preds)
        dbl-col-names (mapv first dbl-preds)
        n-aggs (count decomposed)
        agg-types (int-array (mapv (fn [a] (case (:op a)
                                             :sum ColumnOps/AGG_SUM :count ColumnOps/AGG_COUNT
                                             :min ColumnOps/AGG_MIN :max ColumnOps/AGG_MAX))
                                   decomposed))
        ;; Collect chunk entries
        col-entries (into {} (map (fn [[k v]] [k (gb/collect-chunk-entries (:index v))])) columns)
        first-col-entries (val (first col-entries))
        n-chunks (count first-col-entries)
        ;; Pre-extract arrays
        chunk-lengths (int-array n-chunks)
        long-pred-arrs (make-array expr/long-array-class n-long n-chunks)
        dbl-pred-arrs (make-array expr/double-array-class n-dbl n-chunks)
        ;; aggChunkArrays: [numAggs][nChunks] → long[]
        agg-chunk-arrs (make-array expr/long-array-class n-aggs n-chunks)]
    ;; Fill arrays
    (dotimes [c n-chunks]
      (let [^ChunkEntry ref (nth first-col-entries c)]
        (aset chunk-lengths c (int (chunk/chunk-length (.chunk ref)))))
      (dotimes [p n-long]
        (let [^ChunkEntry e (nth (get col-entries (nth long-col-names p)) c)]
          (aset ^objects (aget ^"[[[J" long-pred-arrs p) c (chunk/chunk-as-longs (.chunk e)))))
      (dotimes [p n-dbl]
        (let [^ChunkEntry e (nth (get col-entries (nth dbl-col-names p)) c)]
          (aset ^objects (aget ^"[[[D" dbl-pred-arrs p) c (chunk/chunk-as-doubles (.chunk e)))))
      (dotimes [a n-aggs]
        (when-let [col-name (:col (nth decomposed a))]
          (let [^ChunkEntry e (nth (get col-entries col-name) c)]
            (aset ^objects (aget ^"[[[J" agg-chunk-arrs a) c (chunk/chunk-as-longs (.chunk e)))))))
    ;; Call Java multi-agg
    (let [^longs result
          (ColumnOpsChunkedLong/fusedSimdChunkedMultiLongParallel
           (int n-long) ^ints long-pred-types
           ^"[[[J" long-pred-arrs ^longs long-lo ^longs long-hi
           (int n-dbl) ^ints dbl-pred-types
           ^"[[[D" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
           (int n-aggs) ^ints agg-types ^"[[[J" agg-chunk-arrs
           ^ints chunk-lengths (int n-chunks))
          cnt (aget result 1)]
      [(into {:_count cnt}
             (map-indexed
              (fn [i a]
                (let [val (aget result (* i 2))
                      alias (or (:as a) (:op (nth aggs i)))]
                  [alias
                   (cond
                     (zero? cnt) nil
                     (:_was-avg a) (/ (double val) (double cnt))
                     (= :count (:op a)) cnt
                     :else val)]))
              decomposed))])))

(defn execute-chunked-fused
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

        ;; Check if aggregate column is int64 — use native long SIMD path if so
        agg-col-long? (and agg-col1-name
                           (not= :sum-product agg-type-kw)
                           (= :int64 (:type (get columns agg-col1-name))))

        ;; Pre-extract chunk arrays only for SIMD chunks (typed 3D arrays for JIT)
        chunk-lengths    (int-array n-simd)
        long-pred-arrs   (make-array expr/long-array-class n-long n-simd)
        dbl-pred-arrs    (make-array expr/double-array-class n-dbl n-simd)
        ;; Long path: long[][] for aggregate arrays; Double path: double[][]
        agg-arr1s-long   (when agg-col-long? (make-array Long/TYPE n-simd 0))
        agg-arr1s        (when-not agg-col-long? (make-array Double/TYPE n-simd 0))
        agg-arr2s        (when-not agg-col-long? (make-array Double/TYPE n-simd 0))]

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
          (if agg-col-long?
            ;; Long path: extract native long[] — zero copy
            (aset ^objects agg-arr1s-long s (chunk/chunk-as-longs (.chunk entry)))
            ;; Double path: convert if needed
            (let [arr (chunk/chunk-as-doubles (.chunk entry))]
              (aset ^objects agg-arr1s s
                    (if (expr/long-array? arr)
                      (ColumnOps/longToDouble ^longs arr (alength ^longs arr))
                      arr))))))
      (when (and agg-col2-name (not agg-col-long?))
        (let [^ChunkEntry entry (nth (get col-entries agg-col2-name) (nth simd-chunks s))
              arr (chunk/chunk-as-doubles (.chunk entry))]
          (aset ^objects agg-arr2s s
                (if (expr/long-array? arr)
                  (ColumnOps/longToDouble ^longs arr (alength ^longs arr))
                  arr)))))

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

        ;; Process SIMD chunks via Java — long or double path
        (if agg-col-long?
          ;; Native long SIMD path — zero-copy from chunks, long arithmetic
          (let [^longs result
                (ColumnOpsChunkedLong/fusedSimdChunkedLongParallel
                 (int n-long) ^ints long-pred-types
                 ^"[[[J" long-pred-arrs ^longs long-lo ^longs long-hi
                 (int n-dbl) ^ints dbl-pred-types
                 ^"[[[D" dbl-pred-arrs ^doubles dbl-lo ^doubles dbl-hi
                 (int agg-type) ^"[[J" agg-arr1s-long
                 ^ints chunk-lengths (int n-simd))
                simd-result (aget result 0)
                simd-count (aget result 1)
                total-count (+ simd-count (long stats-count))]
            (if (zero? (long stats-count))
              {:result simd-result :count total-count}
              {:result (case agg-type-kw
                         :count total-count
                         :sum (+ simd-result (long stats-sum))
                         :min (if (zero? simd-count) (long stats-min) (Math/min simd-result (long stats-min)))
                         :max (if (zero? simd-count) (long stats-max) (Math/max simd-result (long stats-max)))
                         simd-result)
               :count total-count}))
          ;; Double SIMD path (original)
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
               :count total-count})))))))

(defn execute-chunked-fused-count
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

(defn normalize-select-item
  "Normalize a select item to {:name key :ref col-keyword} or {:name key :expr expr-map}."
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

(defn aget-col-decoded
  "Read element i from a column, decoding dict-encoded strings back to String."
  [col-data col-info ^long i]
  (if (expr/long-array? col-data)
    (let [v (aget ^longs col-data i)]
      (cond
        (= v Long/MIN_VALUE) nil
        (:dict col-info) (aget ^"[Ljava.lang.String;" (:dict col-info) (int v))
        :else v))
    (let [v (aget ^doubles col-data i)]
      (if (Double/isNaN v) nil v))))

(defn execute-projection
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
