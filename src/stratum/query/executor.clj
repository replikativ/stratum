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
            [stratum.query.normalization :as norm]
            [stratum.query.predicate :as pred]
            [stratum.query.expression :as expr]
            [stratum.query.columns :as cols]
            [stratum.query.group-by :as gb]
            [stratum.query.join :as jn]
            [stratum.query.postprocess :as post]
            [stratum.query.window :as win]
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
            PMaterializeExpr]
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

(defn- execute-filter [node]
  ;; Filters modify the column context (potentially adding mask columns)
  ;; but don't reduce rows — the actual filtering happens in the agg/project node.
  ;; This matches how stratum currently works: preds are passed to the Java methods.
  (let [ctx (execute-node (:input node))
        {:keys [preds columns length]} (prepare-preds (:predicates node)
                                                       (ctx-columns ctx)
                                                       (ctx-length ctx))]
    {:columns columns :length length :preds preds}))

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
   run the predicate preparation pipeline and return [preds columns length]."
  [node-preds ctx]
  (if (seq node-preds)
    (let [{:keys [preds columns length]}
          (prepare-preds node-preds (ctx-columns ctx) (ctx-length ctx))]
      [preds columns length])
    [[] (ctx-columns ctx) (ctx-length ctx)]))

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
        [preds columns length] (prepare-node-preds (:predicates node) ctx)]
    (if (empty? preds)
      ;; Unfiltered count — short-circuit
      (post/format-fused-result {:result 0.0 :count length} {:op :count :as nil})
      (let [mat-cols (cols/materialize-columns columns)]
        (post/format-fused-result
         (x/execute-fused preds nil mat-cols length)
         {:op :count :as nil})))))

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
        [preds columns length] (prepare-node-preds (:predicates node) ctx)]
    (post/format-fused-result
     (x/execute-chunked-fused-count preds columns length)
     {:op :count :as nil})))

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
                    (int length))]
    (post/format-fused-result {:result 0.0 :count (long (aget r 1))} {:op :count :as nil})))

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
          [aggs mat-cols])]
    (x/execute-group-by preds aggs group-keys mat-cols length columnar?)))

(defn- execute-hash-group-by [node columnar?]
  ;; Same as dense — execute-group-by internally decides dense vs hash
  (execute-dense-group-by node columnar?))

;; --- Join strategies --------------------------------------------------------

(defn- execute-hash-join [node]
  (let [build-ctx (execute-node (:build-side node))
        probe-ctx (execute-node (:probe-side node))
        ;; Reconstruct join spec for jn/execute-joins
        build-cols (ctx-columns build-ctx)
        probe-cols (ctx-columns probe-ctx)
        probe-length (ctx-length probe-ctx)
        join-spec {:with build-cols
                   :on (mapv (fn [[l r]] [:= l r]) (:on-pairs node))
                   :type (:join-type node)}
        result (jn/execute-joins probe-cols probe-length [join-spec])]
    {:columns (:columns result) :length (long (:length result))}))

(defn- execute-fused-join-group-agg [node columnar?]
  (let [left-ctx  (execute-node (:left node))
        right-ctx (execute-node (:right node))
        fact-cols  (ctx-columns left-ctx)
        fact-length (ctx-length left-ctx)
        dim-cols   (ctx-columns right-ctx)
        join-spec (:join-spec node)
        group-keys (:group-keys node)
        aggs (:aggs node)
        ;; Build the join spec expected by jn/execute-fused-join-group-agg
        jn-spec {:with dim-cols
                 :on (mapv (fn [[l r]] [:= l r]) (:on-pairs join-spec))
                 :type (:type join-spec)}]
    (jn/execute-fused-join-group-agg
     fact-cols fact-length jn-spec group-keys aggs columnar?)))

(defn- execute-fused-join-global-agg [node columnar?]
  (let [left-ctx  (execute-node (:left node))
        right-ctx (execute-node (:right node))
        fact-cols  (ctx-columns left-ctx)
        fact-length (ctx-length left-ctx)
        dim-cols   (ctx-columns right-ctx)
        join-spec (:join-spec node)
        aggs (:aggs node)
        jn-spec {:with dim-cols
                 :on (mapv (fn [[l r]] [:= l r]) (:on-pairs join-spec))
                 :type (:type join-spec)}]
    (jn/execute-fused-join-global-agg
     fact-cols fact-length jn-spec aggs)))

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
    ;; Window on grouped results (result rows already computed)
    (if (sequential? results)
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
      results)))

;; --- Post-processing --------------------------------------------------------

(defn- execute-having [node results]
  (let [results (or results (execute-node (:input node)))]
    (post/apply-having results (:predicates node))))

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
     (instance? PChunkedSIMDCount node)(execute-chunked-simd-count node columnar?)
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
     (instance? PFusedJoinGroupAgg node)    (execute-fused-join-group-agg node columnar?)
     (instance? PFusedJoinGlobalAgg node)   (execute-fused-join-global-agg node columnar?)

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
   columnar? controls output format (arrays vs row maps)."
  ([plan] (execute-physical plan false))
  ([plan columnar?]
   (binding [expr/*columns-meta* {}]
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
         result)))))

;; ============================================================================
;; High-level entry points (plan → optimize → execute)
;; ============================================================================

(defn run-query
  "Build logical plan, optimize to physical, execute."
  [query columnar?]
  (let [logical  (plan/build-logical-plan query)
        physical (plan/optimize logical)]
    (execute-physical physical columnar?)))

(defn explain-query
  "Build and explain a physical plan."
  [query]
  (let [logical  (plan/build-logical-plan query)
        physical (plan/optimize logical)]
    {:plan-tree (plan/explain physical)
     :strategy  (keyword (.getSimpleName (class physical)))
     :query     (dissoc query :from)}))
