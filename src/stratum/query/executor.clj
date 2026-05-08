(ns stratum.query.executor
  "Physical plan executor.

   Walks the physical IR tree bottom-up, executing each node:
   - Scan/filter/join nodes produce a column context {:columns {...} :length N}
   - Agg/group-by/project nodes produce result rows [{...} ...]
   - Post-processing nodes transform result rows

   Entry point: (execute-physical plan)"
  (:require [stratum.query.ir :as ir]
            [stratum.query.execution :as x]
            [stratum.query.plan :as plan]
            [stratum.query.prepare :as prep]
            [stratum.query.normalization :as norm]
            [stratum.query.predicate :as pred]
            [stratum.query.estimate :as est]
            [stratum.query.expression :as expr]
            [stratum.query.columns :as cols]
            [stratum.query.group-by :as gb]
            [stratum.query.join :as jn]
            [stratum.query.postprocess :as post]
            [stratum.query.top-n :as top-n]
            [stratum.query.window :as win]
            [stratum.dataset :as dataset]
            [stratum.index :as index]
            [stratum.chunk :as chunk])
  (:import [stratum.query.ir
            PScan PChunkedScan PSIMDFilter PMaskFilter
            PStatsOnlyAgg PFusedSIMDAgg PFusedSIMDCount
            PChunkedSIMDAgg PChunkedSIMDCount PBlockSkipCount
            PFusedMultiSum PPercentileAgg PScalarAgg
            PChunkedDenseGroupBy PDenseGroupBy PHashGroupBy
            PFusedExtractCount PBitmapSemiJoin PHashJoin
            PPerfectHashJoin PFusedJoinGroupAgg PFusedJoinGlobalAgg
            PProject PWindow PHaving PSort PDistinct PLimit
            PMaterializeExpr LTopN]
           [stratum.internal ColumnOps ColumnOpsExt ColumnOpsAnalytics]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Column context helpers
;; ============================================================================

(defn- ctx-columns [ctx] (:columns ctx))
(defn- ctx-length ^long [ctx] (long (:length ctx)))

(defn- materialize-ctx
  "Ensure all index-backed columns in a context are materialized to arrays."
  [ctx]
  (update ctx :columns cols/materialize-columns))

;; ============================================================================
;; Predicate preparation (shared by filter and agg nodes)
;; ============================================================================

(defn- prepare-preds
  "Run the predicate pre-processing pipeline on a column context:
   expression materialization, string preds, dict resolution, SIMD/mask split.
   Returns {:columns cols :length N :preds preds}."
  [preds columns ^long length]
  (let [;; 1. Computed expression predicates ([:> {:op :mul ...} 1000])
        [preds columns] (if (some #(map? (first %)) preds)
                          (x/materialize-computed-preds preds columns length)
                          [preds columns])
        ;; 2. String predicates → mask columns
        [preds columns] (if (some #(pred/string-pred-ops (second %)) preds)
                          (let [mat-cols (cols/materialize-columns columns)]
                            (pred/materialize-string-preds preds mat-cols length))
                          [preds columns])
        ;; 3. Dict resolution (string equality on dict-encoded)
        preds (let [has-dict? (some #(and (pred/dict-resolvable-ops (second %))
                                          (>= (count %) 3)
                                          (let [v (nth % 2)]
                                            (or (string? v) (keyword? v)))) preds)]
                (if has-dict?
                  (pred/resolve-dict-equality-preds
                   (mapv (fn [p]
                           (if (and (>= (count p) 3) (keyword? (nth p 2)))
                             (assoc p 2 (str (nth p 2)))
                             p))
                         preds)
                   columns)
                  preds))
        ;; 4. Non-SIMD → mask compilation
        [simd-preds non-simd-preds] (pred/split-preds preds columns)
        [preds columns] (if (seq non-simd-preds)
                          (let [pred-col-keys (into #{} (keep (fn [p]
                                                                (let [c (first p)]
                                                                  (when (keyword? c) c))))
                                                    non-simd-preds)
                                partial-mat (reduce (fn [cs k]
                                                      (if-let [c (get cs k)]
                                                        (assoc cs k (cols/materialize-column c))
                                                        cs))
                                                    columns pred-col-keys)
                                mask-fn (pred/compile-pred-mask non-simd-preds partial-mat)
                                mask-arr (mask-fn length)]
                            [(conj simd-preds [:__mask :eq 1])
                             (assoc partial-mat :__mask {:type :int64 :data mask-arr})])
                          [simd-preds columns])]
    {:preds preds :columns columns :length length}))

;; ============================================================================
;; Execute dispatch
;; ============================================================================

(declare execute-node)

(defn- execute-scan [node]
  {:columns (:columns node) :length (:length node)})

(defn- execute-chunked-scan [node]
  ;; Zone-map pruning: compute surviving chunks and materialize only those
  (let [columns (:columns node)
        length  (:length node)
        surviving (:surviving-chunks node)]
    (if surviving
      (let [pruned (x/materialize-columns-pruned columns surviving)
            first-data (:data (val (first pruned)))
            new-len (cond
                      (expr/long-array? first-data) (alength ^longs first-data)
                      (expr/double-array? first-data) (alength ^doubles first-data)
                      :else length)]
        {:columns pruned :length (long new-len)})
      {:columns columns :length length})))

(defn- count-mask-hits
  "Count how many rows pass a compiled mask (if present in preds).
   Returns nil if no mask available."
  ^Long [preds columns ^long length]
  (when-let [mask-col (get columns :__mask)]
    (let [^longs mask (:data mask-col)]
      (when mask
        (loop [i 0 cnt 0]
          (if (>= i length)
            cnt
            (recur (inc i) (if (== 1 (aget mask i)) (inc cnt) cnt))))))))

(defn- execute-filter [node]
  ;; Filters modify the column context (potentially adding mask columns)
  ;; but don't reduce rows — the actual filtering happens in the agg/project node.
  ;; This matches how stratum currently works: preds are passed to the Java methods.
  (let [ctx (execute-node (:input node))
        {:keys [preds columns length]} (prepare-preds (:predicates node)
                                                      (ctx-columns ctx)
                                                      (ctx-length ctx))
        ;; Adaptive: compute actual mask count for downstream use
        actual-count (count-mask-hits preds columns length)]
    (cond-> {:columns columns :length length :preds preds}
      actual-count (assoc :actual-surviving-rows actual-count))))

(defn- get-preds-and-ctx
  "Extract predicates and column context from a child node.
   If child is a filter node, uses its prepared preds.
   Otherwise returns empty preds."
  [child-result]
  (if (:preds child-result)
    [(:preds child-result) (:columns child-result) (:length child-result)]
    [[] (:columns child-result) (long (:length child-result))]))

(defn- prepare-node-preds
  "For a physical node that carries its own predicates (absorbed from filter),
   run the predicate preparation pipeline and return [preds columns length].
   Also merges any context-level :preds (e.g. from a bitmap semi-join child)."
  [node-preds ctx]
  (let [ctx-preds (or (:preds ctx) [])
        all-preds (into (vec ctx-preds) node-preds)]
    (if (seq all-preds)
      (let [{:keys [preds columns length]}
            (prepare-preds all-preds (ctx-columns ctx) (ctx-length ctx))]
        [preds columns length])
      [[] (ctx-columns ctx) (ctx-length ctx)])))

;; --- Global aggregation strategies ------------------------------------------

(defn- execute-stats-only [node columnar?]
  (let [ctx (execute-node (:input node))
        columns (ctx-columns ctx)
        length (ctx-length ctx)
        aggs (:aggs node)
        result-row
        (reduce
         (fn [row agg]
           (let [alias (keyword (or (:as agg) (:op agg)))]
             (if (= :count (:op agg))
               (assoc row alias (long length) :_count length)
               (let [entries (gb/collect-chunk-entries (:index (get columns (:col agg))))
                     n-chunks (count entries)
                     col-long? (= :int64 (:type (get columns (:col agg))))
                     [sum cnt mn mx]
                     (loop [i 0 sum 0.0 cnt (long 0) mn Double/MAX_VALUE mx (- Double/MAX_VALUE)]
                       (if (>= i n-chunks)
                         [sum cnt mn mx]
                         (let [^stratum.index.ChunkEntry entry (nth entries i)
                               ^stratum.stats.ChunkStats cs (.stats entry)]
                           (recur (inc i)
                                  (+ sum (double (:sum cs)))
                                  (+ cnt (long (:count cs)))
                                  (Math/min mn (double (:min-val cs)))
                                  (Math/max mx (double (:max-val cs)))))))]
                 (assoc row alias
                        (case (:op agg)
                          :sum (if (zero? cnt) nil (if col-long? (long sum) sum))
                          :min (if (zero? cnt) nil (if col-long? (long mn) mn))
                          :max (if (zero? cnt) nil (if col-long? (long mx) mx))
                          :avg (if (zero? cnt) nil (/ sum (double cnt))))
                        :_count cnt)))))
         {}
         aggs)]
    [result-row]))

(defn- execute-fused-simd-agg [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        agg (:agg node)]
    (post/format-fused-result
     (x/execute-fused preds agg mat-cols length)
     agg)))

(defn- execute-fused-simd-count [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        agg (or (:agg node) {:op :count :as nil})]
    (if (empty? preds)
      ;; Unfiltered count — short-circuit
      (post/format-fused-result {:result 0.0 :count length} agg)
      (let [mat-cols (cols/materialize-columns columns)]
        (post/format-fused-result
         (x/execute-fused preds nil mat-cols length)
         agg)))))

(defn- execute-chunked-simd-agg [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        agg (:agg node)]
    (if (= :avg (:op agg))
      (let [sum-result (x/execute-chunked-fused preds {:op :sum :col (:col agg) :as nil}
                                                columns length)
            cnt (:count sum-result)]
        (post/format-fused-result
         (if (zero? cnt) {:result Double/NaN :count 0}
             {:result (/ (:result sum-result) (double cnt)) :count cnt})
         agg))
      (post/format-fused-result
       (x/execute-chunked-fused preds agg columns length)
       agg))))

(defn- execute-chunked-simd-count [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        agg (or (:agg node) {:op :count :as nil})]
    (post/format-fused-result
     (x/execute-chunked-fused-count preds columns length)
     agg)))

(defn- execute-block-skip-count [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
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
                    (int length))
        agg (or (:agg node) {:op :count :as nil})]
    (post/format-fused-result {:result 0.0 :count (long (aget r 1))} agg)))

(defn- execute-fused-multi-sum [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)]
    (x/execute-fused-multi-sum preds (:aggs node) mat-cols length)))

(defn- execute-percentile-agg [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
        aggs (:aggs node)
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
                                   (ColumnOps/percentile
                                    (if is-long?
                                      (let [la ^longs col-data da (double-array (alength la))]
                                        (dotimes [i (alength la)] (aset da i (double (aget la i)))) da)
                                      ^doubles col-data)
                                    (int length) pct))
                                 :approx-quantile
                                 (if mask
                                   (let [work (double-array cnt)
                                         pos (int-array 1)]
                                     (dotimes [i length]
                                       (when (== 1 (aget ^longs mask i))
                                         (aset work (aget pos 0)
                                               (if is-long? (double (aget ^longs col-data i))
                                                   (aget ^doubles col-data i)))
                                         (aset pos 0 (inc (aget pos 0)))))
                                     (ColumnOpsAnalytics/tdigestApproxQuantileParallel work (int cnt) pct 200.0))
                                   (let [da (if is-long?
                                              (let [la ^longs col-data d (double-array (alength la))]
                                                (dotimes [i (alength la)] (aset d i (double (aget la i)))) d)
                                              ^doubles col-data)]
                                     (ColumnOpsAnalytics/tdigestApproxQuantileParallel da (int length) pct 200.0))))]
                    [alias result]))
                aggs))]))

(defn- execute-scalar-agg [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)]
    [(gb/execute-scalar-aggs preds (:aggs node) mat-cols length)]))

;; --- Group-by strategies ----------------------------------------------------

(defn- execute-chunked-dense-group-by [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        group-keys (:group-keys node)
        aggs (:aggs node)]
    ;; Try chunked first (streaming, zero-copy), fall back to materialized
    (or (gb/execute-chunked-group-by preds aggs group-keys columns length columnar?)
        (let [mat-cols (cols/materialize-columns columns)
              ;; Pre-compute expression aggs after materialization
              [aggs mat-cols]
              (if (some :expr aggs)
                (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                      cache (java.util.HashMap.)]
                  (loop [idx 0, new-aggs [], new-cols mat-cols]
                    (if (>= idx (count aggs))
                      [new-aggs new-cols]
                      (let [agg (nth aggs idx)]
                        (if-let [e (:expr agg)]
                          (let [arr (expr/eval-expr-vectorized e col-arrays length cache)
                                cn (keyword (str "__agg_expr_" idx))]
                            (recur (inc idx)
                                   (conj new-aggs (-> agg (dissoc :expr) (assoc :col cn)))
                                   (assoc new-cols cn {:type :float64 :data arr})))
                          (recur (inc idx) (conj new-aggs agg) new-cols))))))
                [aggs mat-cols])]
          (x/execute-group-by preds aggs group-keys mat-cols length columnar?)))))

(defn- execute-dense-group-by [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        aggs (:aggs node)
        group-keys (:group-keys node)
        [aggs mat-cols]
        (if (some :expr aggs)
          (let [col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
                cache (java.util.HashMap.)]
            (loop [idx 0, new-aggs [], new-cols mat-cols]
              (if (>= idx (count aggs))
                [new-aggs new-cols]
                (let [agg (nth aggs idx)]
                  (if-let [e (:expr agg)]
                    (let [arr (expr/eval-expr-vectorized e col-arrays length cache)
                          cn (keyword (str "__agg_expr_" idx))]
                      (recur (inc idx)
                             (conj new-aggs (-> agg (dissoc :expr) (assoc :col cn)))
                             (assoc new-cols cn {:type :float64 :data arr})))
                    (recur (inc idx) (conj new-aggs agg) new-cols))))))
          [aggs mat-cols])
        ;; Adaptive: if filter context reports actual surviving rows, use it
        ;; to decide dense-group-limit. Very few surviving rows → tighter limit
        ;; avoids over-allocating the dense accumulator array.
        actual-rows (:actual-surviving-rows ctx)
        est-rows    (::plan/estimated-rows (meta node))]
    (if (and actual-rows (< actual-rows 1000))
      ;; Very few rows survive — reduce dense limit to save memory
      (binding [gb/*dense-group-limit* (max 10000 (* 10 (long actual-rows)))]
        (x/execute-group-by preds aggs group-keys mat-cols length columnar?))
      (x/execute-group-by preds aggs group-keys mat-cols length columnar?))))

(defn- execute-hash-group-by [node columnar?]
  ;; Same as dense — execute-group-by internally decides dense vs hash
  (execute-dense-group-by node columnar?))

;; --- Mask realization -------------------------------------------------------

(defn- realize-mask
  "If a child context has :preds from pushed-down predicates, compile them
   into a long[] mask. Returns nil if no preds."
  [ctx]
  (when-let [preds (seq (:preds ctx))]
    (let [columns (:columns ctx)
          length (long (:length ctx))
          ;; Materialize columns referenced by preds
          pred-col-keys (into #{} (keep (fn [p]
                                          (let [c (first p)]
                                            (when (keyword? c) c))))
                              preds)
          mat-cols (reduce (fn [cs k]
                             (if-let [c (get cs k)]
                               (assoc cs k (cols/materialize-column c))
                               cs))
                           columns pred-col-keys)
          ;; Check for already-compiled __mask column (from prepare-preds)
          mask-pred (first (filter #(= :__mask (first %)) preds))]
      (if mask-pred
        ;; Already have a compiled mask column
        ^longs (:data (get mat-cols :__mask))
        ;; Compile predicates to mask via pred/compile-pred-mask
        (let [mask-fn (pred/compile-pred-mask (vec preds) mat-cols)]
          (mask-fn length))))))

;; --- Join strategies --------------------------------------------------------

(defn- execute-hash-join [node]
  (let [build-ctx (execute-node (:build-side node))
        probe-ctx (execute-node (:probe-side node))
        ;; Realize masks from pushed-down predicates
        probe-mask (realize-mask probe-ctx)
        build-mask (realize-mask build-ctx)
        build-cols (ctx-columns build-ctx)
        probe-cols (ctx-columns probe-ctx)
        probe-length (ctx-length probe-ctx)
        join-spec {:with build-cols
                   :on (mapv (fn [[l r]] [:= l r]) (:on-pairs node))
                   :type (:join-type node)}
        spec (jn/normalize-join-spec join-spec)
        result (jn/execute-join (cols/materialize-columns probe-cols)
                                probe-length spec
                                :probe-mask probe-mask
                                :build-mask build-mask)]
    {:columns (:columns result) :length (long (:length result))}))

(defn- execute-bitmap-semi-join-node [node]
  (let [;; Execute probe (fact) side
        probe-ctx (execute-node (:input node))
        probe-cols (cols/materialize-columns (ctx-columns probe-ctx))
        probe-length (ctx-length probe-ctx)
        ;; Execute build (dim) side
        join-spec (:join-spec node)
        build-ctx (execute-node (:build-side join-spec))
        build-cols (cols/materialize-columns (ctx-columns build-ctx))
        build-mask (realize-mask build-ctx)
        ;; Extract join key columns
        [left-key right-key] (first (:on-pairs join-spec))
        ^longs dim-keys (:data (get build-cols right-key))
        dim-length (ctx-length build-ctx)
        ;; Build BitSet from (filtered) dim rows
        max-key (long (ColumnOps/arrayMaxLong dim-keys (int dim-length)))
        ^java.util.BitSet bs (java.util.BitSet. (int (inc max-key)))]
    (if build-mask
      ;; Only add dim rows that pass the mask
      (let [^longs bm build-mask]
        (dotimes [i dim-length]
          (let [k (aget dim-keys i)]
            (when (and (not (zero? (aget bm i)))
                       (>= k 0) (<= k max-key))
              (.set bs (int k))))))
      ;; No mask — add all dim rows
      (dotimes [i dim-length]
        (let [k (aget dim-keys i)]
          (when (and (>= k 0) (<= k max-key))
            (.set bs (int k))))))
    ;; Probe fact rows → build mask column
    (let [^longs fact-keys (:data (get probe-cols left-key))
          ^longs mask (long-array probe-length)]
      (dotimes [i probe-length]
        (let [k (aget fact-keys i)]
          (aset mask i (if (and (>= k 0) (<= k max-key) (.get bs (int k))) 1 0))))
      ;; Return probe columns + mask, plus probe-side preds forwarded
      (cond-> {:columns (assoc probe-cols :__semi_mask {:type :int64 :data mask})
               :length probe-length
               :preds (vec (concat (or (:preds probe-ctx) [])
                                   [[:__semi_mask :eq 1.0]]))}))))

(defn- execute-fused-join-group-agg [node columnar?]
  (let [left-ctx  (execute-node (:left node))
        right-ctx (execute-node (:right node))
        ;; Realize masks from pushed-down predicates
        probe-mask (realize-mask left-ctx)
        build-mask (realize-mask right-ctx)
        fact-cols  (ctx-columns left-ctx)
        fact-length (ctx-length left-ctx)
        dim-cols   (ctx-columns right-ctx)
        join-spec (:join-spec node)
        group-keys (:group-keys node)
        aggs (:aggs node)
        jn-spec {:with dim-cols
                 :on (mapv (fn [[l r]] [:= l r]) (:on-pairs join-spec))
                 :type (:type join-spec)}]
    (jn/execute-fused-join-group-agg
     fact-cols fact-length jn-spec group-keys aggs columnar?
     :probe-mask probe-mask :build-mask build-mask)))

(defn- execute-fused-join-global-agg [node columnar?]
  (let [left-ctx  (execute-node (:left node))
        right-ctx (execute-node (:right node))
        ;; Realize masks from pushed-down predicates
        probe-mask (realize-mask left-ctx)
        build-mask (realize-mask right-ctx)
        fact-cols  (ctx-columns left-ctx)
        fact-length (ctx-length left-ctx)
        dim-cols   (ctx-columns right-ctx)
        join-spec (:join-spec node)
        aggs (:aggs node)
        jn-spec {:with dim-cols
                 :on (mapv (fn [[l r]] [:= l r]) (:on-pairs join-spec))
                 :type (:type join-spec)}]
    (jn/execute-fused-join-global-agg
     fact-cols fact-length jn-spec aggs
     :probe-mask probe-mask :build-mask build-mask)))

;; --- Projection -------------------------------------------------------------

(defn- execute-project [node columnar?]
  (let [ctx (execute-node (:input node))
        ;; Projection may have a filter child with prepared preds
        [preds columns length] (if (:preds ctx)
                                 [(:preds ctx) (:columns ctx) (:length ctx)]
                                 [[] (ctx-columns ctx) (ctx-length ctx)])]
    (x/execute-projection preds (:items node) columns length columnar?)))

;; --- Window -----------------------------------------------------------------

(defn- execute-window [node columnar? results]
  (let [results (or results (execute-node (:input node)))
        specs (:specs node)]
    (cond
      ;; Path 1: input is already a vector of result rows (window over
      ;; an aggregated/projected result, e.g. window-on-group-by).
      (sequential? results)
      (let [grouped-cols (x/results->columns results)
            n-grouped (count results)
            with-windows (win/execute-window-functions grouped-cols n-grouped specs)
            all-ks (keys with-windows)]
        (mapv (fn [i]
                (reduce (fn [m k]
                          (let [v (get with-windows k)]
                            (assoc m k
                                   (cond
                                     (expr/long-array? v) (aget ^longs v i)
                                     (expr/double-array? v) (aget ^doubles v i)
                                     (expr/string-array? v) (aget ^"[Ljava.lang.String;" v i)
                                     (map? v)
                                     (let [d (:data v)]
                                       (cond
                                         (expr/long-array? d) (aget ^longs d i)
                                         (expr/double-array? d) (aget ^doubles d i)
                                         :else (aget ^"[Ljava.lang.String;" d i)))
                                     :else (get m k)))))
                        {} all-ks))
              (range n-grouped)))

      ;; Path 2: input is a column context (no upstream aggregate).
      ;; Mirrors the legacy `q` window block (q.clj:744+): materialize
      ;; columns, evaluate window specs row-major, return an augmented
      ;; column ctx that downstream PProject / PHaving / PSort can
      ;; consume in either columnar or row form.
      (and (map? results) (:columns results) (:length results))
      (let [length (long (:length results))
            mat (cols/materialize-columns (:columns results))
            with-windows (win/execute-window-functions mat length specs)]
        (assoc results :columns with-windows :length length))

      :else results)))

;; --- Expression materialization ----------------------------------------------

(defn- execute-materialize-expr [node]
  (let [ctx (execute-node (:input node))
        columns (ctx-columns ctx)
        length  (ctx-length ctx)
        mat-cols (cols/materialize-columns columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
        cache (java.util.HashMap.)
        arr (expr/eval-expr-vectorized (:expr node) col-arrays length cache)]
    (assoc ctx :columns (assoc mat-cols (:col-name node)
                               {:type (or (:target node) :float64) :data arr}))))

;; --- Top-N pushdown ---------------------------------------------------------

(defn- execute-top-n-node
  "Run the streaming top-N pushdown by delegating to
   `stratum.query.top-n/execute-top-n`. The IR's `LTopN` node carries
   the (single) order-spec, the LIMIT, and the (LProject) items it
   absorbed (or nil for SELECT *). We assemble the synthetic query
   map that `top-n/execute-top-n` expects from those fields plus the
   input column context."
  [node]
  (let [ctx (execute-node (:input node))
        columns (ctx-columns ctx)
        items   (:select node)
        ;; `top-n/execute-top-n` accepts a vector of plain column
        ;; keywords (or nil for SELECT *). Convert LProject items to
        ;; that shape; literal/computed projections fall back to nil
        ;; (which means SELECT all from the scan), and the planner's
        ;; eligibility gate ensures the project items are
        ;; keyword-only when LProject was peeled.
        select  (when (seq items)
                  (let [refs (mapv :ref items)]
                    (when (every? keyword? refs) refs)))
        order-spec (:order-spec node)
        order-spec (if (vector? order-spec) order-spec [order-spec :asc])]
    (top-n/execute-top-n
     {:order [order-spec]
      :limit (:limit node)
      :select select}
     columns)))

;; --- Post-processing --------------------------------------------------------

(defn- col-array
  "Return the underlying primitive array for a column entry. Accepts
   either `{:type :data}` maps or raw arrays."
  [v]
  (if (map? v) (:data v) v))

(defn- having-pred-matches?
  "Scalar evaluation of one normalized having predicate at row `i`,
   using the materialized `columns` map. Mirrors the operator set the
   legacy `q` window-having pushdown supports."
  [columns ^long i pred]
  (let [col-key (first pred)
        op      (second pred)
        col     (get columns col-key)
        data    (col-array col)
        v (cond
            (expr/double-array? data) (aget ^doubles data i)
            (expr/long-array? data)   (double (aget ^longs data i))
            :else nil)]
    (cond
      (= op :is-null)     (or (nil? v) (Double/isNaN v))
      (= op :is-not-null) (and (some? v) (not (Double/isNaN v)))
      (nil? v) false
      (Double/isNaN v) false
      :else
      (let [arg (when (> (count pred) 2) (double (nth pred 2)))]
        (case op
          :lt  (< v arg)
          :gt  (> v arg)
          :lte (<= v arg)
          :gte (>= v arg)
          :eq  (== v arg)
          :neq (not (== v arg))
          :range (let [lo arg
                       hi (double (nth pred 3))]
                   (and (>= v lo) (< v hi)))
          true)))))

(defn- having-fast-path-on-ctx
  "Fast path for `PHaving` whose input is a column context (e.g. a
   `PWindow` immediately below). Evaluates each predicate on the raw
   column arrays, gathers only the surviving rows, and returns a new
   column context the parent `PProject` can finish materializing.
   Mirrors the legacy `q` window-having pushdown."
  [ctx preds]
  (let [length  (long (:length ctx))
        columns (:columns ctx)
        normalized (mapv norm/normalize-pred preds)
        matches (java.util.ArrayList.)]
    (dotimes [i (int length)]
      (when (every? #(having-pred-matches? columns i %) normalized)
        (.add matches (int i))))
    (let [n (.size matches)]
      (if (= n length)
        ctx
        (let [indices (int-array n)
              _ (dotimes [k n] (aset indices k (int (.get matches k))))
              new-cols
              (reduce-kv
               (fn [m k v]
                 (let [data (col-array v)]
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
                              (dotimes [k n]
                                (aset ^"[Ljava.lang.String;" out k
                                      (aget ^"[Ljava.lang.String;" data (aget indices k))))
                              (if (map? v) (assoc v :data out) out))
                            :else v))))
               {} columns)]
          (-> ctx (assoc :columns new-cols :length n)))))))

(defn- execute-having [node results]
  (let [results (or results (execute-node (:input node)))]
    ;; Fast path: column-context input (e.g. PWindow → PHaving after the
    ;; window-having pushdown rewrite). Filter on raw column arrays and
    ;; return a column ctx so the next PProject only materializes
    ;; surviving rows.
    (if (and (map? results) (:columns results) (:length results))
      (having-fast-path-on-ctx results (:predicates node))
      (post/apply-having results (:predicates node)))))

(defn- execute-sort [node results]
  (let [results (or results (execute-node (:input node)))]
    (post/apply-order results (:order-specs node) (:limit node) (:offset node))))

(defn- execute-distinct [node results]
  (let [results (or results (execute-node (:input node)))]
    (post/apply-distinct results)))

(defn- execute-limit [node results]
  (let [results (or results (execute-node (:input node)))]
    (post/apply-limit-offset results (:limit node) (:offset node))))

;; ============================================================================
;; Main dispatch
;; ============================================================================

(defn execute-node
  "Execute a physical plan node, returning either a column context or result rows."
  ([node] (execute-node node false))
  ([node columnar?]
   (cond
     ;; Scan
     (instance? PScan node)         (execute-scan node)
     (instance? PChunkedScan node)  (execute-chunked-scan node)

     ;; Filter
     (instance? PSIMDFilter node)   (execute-filter node)
     (instance? PMaskFilter node)   (execute-filter node)

     ;; Global aggregation
     (instance? PStatsOnlyAgg node)    (execute-stats-only node columnar?)
     (instance? PFusedSIMDAgg node)    (execute-fused-simd-agg node columnar?)
     (instance? PFusedSIMDCount node)  (execute-fused-simd-count node columnar?)
     (instance? PChunkedSIMDAgg node)  (execute-chunked-simd-agg node columnar?)
     (instance? PChunkedSIMDCount node) (execute-chunked-simd-count node columnar?)
     (instance? PBlockSkipCount node)  (execute-block-skip-count node columnar?)
     (instance? PFusedMultiSum node)   (execute-fused-multi-sum node columnar?)
     (instance? PPercentileAgg node)   (execute-percentile-agg node columnar?)
     (instance? PScalarAgg node)       (execute-scalar-agg node columnar?)

     ;; Group-by
     (instance? PChunkedDenseGroupBy node) (execute-chunked-dense-group-by node columnar?)
     (instance? PDenseGroupBy node)        (execute-dense-group-by node columnar?)
     (instance? PHashGroupBy node)         (execute-hash-group-by node columnar?)
     (instance? PFusedExtractCount node)   (throw (ex-info "TODO: PFusedExtractCount" {}))

     ;; Join
     (instance? PHashJoin node)             (execute-hash-join node)
     (instance? PPerfectHashJoin node)      (execute-hash-join node) ;; same API, perfect hash is internal
     (instance? PBitmapSemiJoin node)       (execute-bitmap-semi-join-node node)
     (instance? PFusedJoinGroupAgg node)    (execute-fused-join-group-agg node columnar?)
     (instance? PFusedJoinGlobalAgg node)   (execute-fused-join-global-agg node columnar?)

     ;; Expression materialization
     (instance? PMaterializeExpr node) (execute-materialize-expr node)

     ;; Top-N pushdown — LTopN is recognized directly (no separate
     ;; physical record) and dispatched to the streaming primitive.
     (instance? LTopN node) (execute-top-n-node node)

     ;; Projection
     (instance? PProject node)  (execute-project node columnar?)

     ;; Post-processing
     (instance? PWindow node)   (execute-window node columnar? nil)
     (instance? PHaving node)   (execute-having node nil)
     (instance? PSort node)     (execute-sort node nil)
     (instance? PDistinct node) (execute-distinct node nil)
     (instance? PLimit node)    (execute-limit node nil)

     :else (throw (ex-info (str "Unknown physical node: " (type node)) {:node node})))))

;; ============================================================================
;; Entry point
;; ============================================================================

(defn execute-physical
  "Execute a physical plan tree, returning result rows.
   columnar? controls output format (arrays vs row maps).

   `expr/*columns-meta*` is bound by `run-query` from `prepare-query`'s
   output and must NOT be re-bound here — overriding it with `{}`
   would lose the dict info that string-expression evaluation depends
   on (LENGTH, etc.). When `execute-physical` is called directly
   without a prior binding, the var's root value (`{}`) applies."
  ([plan] (execute-physical plan false))
  ([plan columnar?]
   (let [result (execute-node plan columnar?)]
       ;; If the result is a column context (no agg/project), count matching rows
     (if (and (map? result) (:columns result) (:length result) (not (:n-rows result)))
       (let [preds (or (:preds result) [])
             columns (:columns result)
             length (long (:length result))]
         (if (empty? preds)
           [{:_count length}]
           (let [mat-cols (cols/materialize-columns columns)
                 col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)]
             [{:_count (loop [i 0 cnt 0]
                         (if (>= i length)
                           cnt
                           (if (every? #(gb/eval-pred-scalar col-arrays i %) preds)
                             (recur (inc i) (inc cnt))
                             (recur (inc i) cnt))))}])))
       result))))

;; ============================================================================
;; High-level entry points (plan → optimize → execute)
;; ============================================================================

(defn- prepare-and-build
  "Run the shared lowering passes (`prepare-query`) and then build a
   logical plan against the cleaned query. Returns
   `{:plan logical :columns-meta meta}`. The legacy `q` body has
   always run these passes inline; the planner needs them too so
   raw expression vectors don't reach the executor."
  [query]
  (let [from   (:from query)
        columns (cond
                  (nil? from) {}
                  (satisfies? dataset/IDataset from) (dataset/columns from)
                  :else (cols/prepare-columns from))
        length (if (seq columns)
                 (cols/get-column-length (val (first columns)))
                 0)
        ;; Anomaly resolution must run before any other lowering —
        ;; `[:anomaly-score …]` would otherwise reach
        ;; `normalize-expr` and throw "Unknown expression operator".
        ;; The legacy `q` runs this immediately after join evaluation;
        ;; we run it here against the un-joined source columns when
        ;; `:_anomaly-models` is set on the query map.
        models (:_anomaly-models query)
        [query columns] (if (seq models)
                          (prep/resolve-anomaly-columns query columns length models)
                          [query columns])
        {:keys [preds aggs group select columns columns-meta]}
        (prep/prepare-query query columns length)
        ;; Rebuild a query map for build-logical-plan with the
        ;; lowered slots in place. `:from` becomes the cleaned
        ;; columns map (containing temp materialized columns).
        cleaned (-> query
                    (assoc :from columns
                           :where preds
                           :agg   aggs
                           :group group
                           :select select
                           ;; Tell build-logical-plan that preds/aggs
                           ;; are already normalized — re-running
                           ;; norm/normalize-pred / normalize-agg on
                           ;; their output throws.
                           ::plan/pre-normalized? true))]
    {:plan         (plan/build-logical-plan cleaned)
     :columns-meta columns-meta}))

(defn- strip-injected-keys
  "Drop injected keys (`_having-only-keys`, `_order-only-keys`) from
   result rows. The SQL → IR translation injects extra aggregates
   into the projection so HAVING / ORDER BY can reference them, but
   they shouldn't appear in the user-visible output."
  [results having-only-keys order-only-keys]
  (let [drop-ks (concat having-only-keys order-only-keys)]
    (if (and (sequential? results) (seq drop-ks))
      (mapv #(if (map? %) (apply dissoc % drop-ks) %) results)
      results)))

(defn run-query
  "Lower the query, build logical plan, optimize to physical, execute.
   Result rows have the injected order-only / having-only aggregate
   columns stripped, matching the legacy `q` body."
  [query columnar?]
  (let [{:keys [plan columns-meta]} (prepare-and-build query)
        physical (plan/optimize plan)
        m (meta plan)
        having-only (::plan/having-only-keys m)
        order-only  (::plan/order-only-keys  m)]
    (binding [expr/*columns-meta* columns-meta]
      (-> (execute-physical physical columnar?)
          (strip-injected-keys having-only order-only)))))

(defn compile-physical
  "Build and optimize a physical plan for a query. The plan captures all
   data references and can be executed repeatedly via execute-physical."
  [query]
  (let [logical  (plan/build-logical-plan query)]
    (plan/optimize logical)))

(defn- scan-summary
  "Walk the plan to find the leaf scan and pull (length, n-cols) for
   the explain output. Mirrors the legacy explain shape so consumers
   that probe `:n-rows` / `:columns` keep working."
  [node]
  (loop [n node]
    (cond
      (or (instance? stratum.query.ir.PScan n)
          (instance? stratum.query.ir.PChunkedScan n)
          (instance? stratum.query.ir.LScan n))
      [(:length n) (count (:columns n))]
      (:input n)         (recur (:input n))
      (:left n)          (recur (:left n))
      (:probe-side n)    (recur (:probe-side n))
      :else              [nil nil])))

(defn explain-query
  "Build and explain a physical plan. Returns a map with both the
   planner-native keys (`:plan-tree`, `:strategy`) and legacy-shape
   keys (`:n-rows`, `:columns`) so existing callers don't break."
  [query]
  (let [logical  (plan/build-logical-plan query)
        physical (plan/optimize logical)
        [n-rows n-cols] (scan-summary physical)]
    {:plan-tree (plan/explain physical)
     :strategy  (keyword (.getSimpleName (class physical)))
     :n-rows    n-rows
     :columns   n-cols
     :query     (dissoc query :from)}))
