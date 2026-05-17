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
            [stratum.query.asof-join :as asof]
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
            PFusedMultiSum PPercentileAgg PScalarAgg PSplitAgg
            PChunkedDenseGroupBy PDenseGroupBy PHashGroupBy
            PFusedExtractCount PBitmapSemiJoin PHashJoin
            PPerfectHashJoin PAsofJoin PFusedJoinGroupAgg PFusedJoinGlobalAgg
            PProject PWindow PHaving PSort PDistinct PLimit
            PMaterializeExpr PRecompose LTopN LHead LSetOp LAnomaly LStringMaterialize]
           [stratum.internal ColumnOps ColumnOpsExt ColumnOpsAnalytics ColumnOpsNullable]))

(set! *warn-on-reflection* true)

;; Forward-declared because `execute-split-agg` (defined far above the
;; dispatch table) propagates the ANALYZE collector across futures.
(declare ^:dynamic *explain-collector*)

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
                          (let [;; Collect both LHS (`first p`) and any
                                ;; keyword RHS args (col-vs-col). Same
                                ;; shape fix as plan/pred-columns
                                ;; (copilot review #3) — round-3 agent
                                ;; found this duplicate walker still
                                ;; LHS-only.
                                pred-col-keys
                                (into #{}
                                      (mapcat (fn [p]
                                                (let [lhs (first p)]
                                                  (concat (when (keyword? lhs) [lhs])
                                                          (filter keyword? (subvec p 2))))))
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
;; Dynamic filter pushdown (F19) — runtime build→probe range push
;; ----------------------------------------------------------------------------
;; Mirror of DuckDB's `DynamicTableFilterSet`. After a join's BUILD side
;; materializes, we know the [min,max] of its join-key column. Probe-side
;; rows whose join key falls outside that range cannot match, so we
;; attach a `[:gte ... :lte ...]` predicate pair to the probe-side scan
;; before executing the probe — letting zone-map pruning skip whole
;; chunks. The scan node carries a `:dynamic-filters` `volatile!` (set
;; up in `strategy-selection`) that the scan executor reads at run time.
;; ============================================================================

(defn- dynamic-target-scan
  "Walk down the probe subtree to find the unique scan that owns column
   `k`. Recurses through pass-through wrappers (project / sort / limit /
   distinct / having / window / materialize-expr); halts at multi-input
   nodes (joins, set ops) and at filter wrappers — those would either
   ambiguate the source or short-circuit the lookup. Returns nil when
   no unique target is reachable."
  [node k]
  (cond
    (nil? node) nil

    (or (instance? PScan node) (instance? PChunkedScan node))
    (when (contains? (:columns node) k) node)

    (or (instance? PProject node)
        (instance? PSort node)
        (instance? PLimit node)
        (instance? PDistinct node)
        (instance? PHaving node)
        (instance? PWindow node)
        (instance? PMaterializeExpr node)
        (instance? PSIMDFilter node)
        (instance? PMaskFilter node))
    (recur (:input node) k)

    :else nil))

(defn- key-bounds
  "Return `[lo hi]` as Clojure numbers for the build-side join key
   column, or nil if it isn't a genuinely numeric heap array.

   Dict-encoded string columns advertise `:type :int64` (the storage
   type of the dict-code `long[]`) but their codes are local to the
   build-side dictionary — probe-side strings hash into a different
   set of codes, so a numeric range filter on those codes would be
   incorrect. We detect dict-encoded strings via `:dict-type` and
   skip them."
  [build-cols build-key ^long build-length]
  (let [info (get build-cols build-key)
        ctype (:type info)
        dict? (some? (:dict-type info))
        data  (when info (:data info))]
    (cond
      (zero? build-length) nil
      (nil? data) nil
      dict? nil

      (and (= :int64 ctype) (expr/long-array? data))
      (let [^longs arr data]
        [(ColumnOps/arrayMinLong arr (int build-length))
         (ColumnOps/arrayMaxLong arr (int build-length))])

      (and (= :float64 ctype) (expr/double-array? data))
      (let [^doubles arr data
            n (int build-length)]
        (loop [i 1, mn (aget arr 0), mx (aget arr 0)]
          (if (>= i n)
            [mn mx]
            (let [v (aget arr i)]
              (recur (inc i) (Math/min mn v) (Math/max mx v))))))

      :else nil)))

(def ^:private ^:const PUSH_SELECTIVITY_THRESHOLD
  "Maximum estimated selectivity at which the dynamic filter is worth
   pushing.  Cost model: `realize-mask` is ~5× cheaper per row than a
   hash-probe miss, so the push pays for itself only when at least
   ~1/5 of probe rows would be filtered out — i.e. selectivity ≤ 0.8.

   Lowered values push more aggressively (false-positive overhead
   risk on FK joins), raised values miss real wins on narrow-build
   joins.  See JOIN-Q1 / H2O-J1 trade-off discussion in F19 notes."
  0.8)

(defn- push-dynamic-filter!
  "Best-effort: derive [min,max] from the build-side join key and push
   `[probe-key :gte lo]` + `[probe-key :lte hi]` onto the probe-side
   scan's `:dynamic-filters` volatile.

   Gates:
   - `:inner` join-type only — LEFT/RIGHT/FULL outer joins must
     preserve unmatched probe rows, so a probe-side range filter
     would silently drop them.
   - Estimated probe-side selectivity of `[probe-key :range lo hi]`
     must be ≤ `PUSH_SELECTIVITY_THRESHOLD`.  Uses the planner's
     three-tier `estimate-selectivity` (zone-map → 128-sample →
     heuristic), so the gate is cheap (microseconds) regardless of
     probe shape.  This catches the common FK case where build's
     range covers probe entirely — sampling sees every value
     in-range, returns selectivity ≈ 1.0, and the push is skipped
     (cf. JOIN-Q1: 48 ms → ~5 ms when this gate is honoured).

   No-op when the probe subtree has no unique scan target, the build
   key isn't numeric, or the target scan has no volatile."
  [probe-side build-cols build-length on-pairs join-type]
  (when (= :inner join-type)
    (when-let [[probe-key build-key] (first on-pairs)]
      (when-let [target (dynamic-target-scan probe-side probe-key)]
        (when-let [vol (:dynamic-filters target)]
          (when-let [[lo hi] (key-bounds build-cols build-key (long build-length))]
            (let [sel (est/estimate-selectivity
                       [probe-key :range lo hi]
                       (:columns target))]
              (when (<= (double sel) PUSH_SELECTIVITY_THRESHOLD)
                (vreset! vol [[probe-key :gte lo] [probe-key :lte hi]])))))))))

;; ============================================================================
;; Execute dispatch
;; ============================================================================

(declare execute-node)

(defn- scan-preds
  "Concatenate static :predicates with the runtime contents of
   :dynamic-filters (a `volatile!` of a vec, or nil). Returns a (possibly
   empty) vec of raw preds — the upstream executor merges these into ctx
   :preds, where the standard `prepare-node-preds` pipeline picks them up
   alongside any preds carried by a downstream node."
  [node]
  (let [static  (:predicates node)
        dyn-vol (:dynamic-filters node)
        dyn     (when dyn-vol @dyn-vol)]
    (vec (concat static dyn))))

(defn- execute-scan [node]
  (let [preds (scan-preds node)]
    (cond-> {:columns (:columns node) :length (:length node)}
      (seq preds) (assoc :preds preds))))

(defn- execute-chunked-scan [node]
  ;; Zone-map pruning: when the optimizer pre-computed `:surviving-chunks`,
  ;; materialize only those chunks (already accounts for static preds).
  ;; Dynamic preds (set at execute-time) are NOT re-applied here — they
  ;; flow to downstream chunked operators via ctx :preds, which already
  ;; do per-chunk zone-map skipping on the combined predicate set.
  (let [columns   (:columns node)
        length    (:length node)
        surviving (:surviving-chunks node)
        preds     (scan-preds node)
        ctx (if surviving
              (let [pruned (x/materialize-columns-pruned columns surviving)
                    first-data (:data (val (first pruned)))
                    new-len (cond
                              (expr/long-array? first-data) (alength ^longs first-data)
                              (expr/double-array? first-data) (alength ^doubles first-data)
                              :else length)]
                {:columns pruned :length (long new-len)})
              {:columns columns :length length})]
    (cond-> ctx
      (seq preds) (assoc :preds preds))))

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

(defn- execute-filter [node]
  ;; Filters modify the column context (potentially adding mask columns)
  ;; but don't reduce rows — the actual filtering happens in the agg/project node.
  ;; Use prepare-node-preds so any preds the scan attached to ctx (static
  ;; lifted preds or dynamic-filter preds) are folded together with the
  ;; filter node's own preds and run through `prepare-preds` once.
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        actual-count (count-mask-hits preds columns length)]
    (cond-> {:columns columns :length length :preds preds}
      actual-count (assoc :actual-surviving-rows actual-count))))

;; --- Global aggregation strategies ------------------------------------------

(defn- walk-chunk-stats
  "Walk all chunk entries for one column once, accumulating sum/count/
   min/max in a single pass. Predicate-aware via `survives?`. Returns
   `[sum count min-val max-val]` as a typed tuple."
  [entries ^long n-chunks survives?]
  (loop [i (long 0) sum 0.0 cnt (long 0)
         mn Double/MAX_VALUE mx (- Double/MAX_VALUE)]
    (if (>= i n-chunks)
      [sum cnt mn mx]
      (if (survives? i)
        (let [^stratum.index.ChunkEntry entry (nth entries i)
              ^stratum.stats.ChunkStats cs (.stats entry)]
          (recur (inc i)
                 (+ sum (double (:sum cs)))
                 (+ cnt (long (:count cs)))
                 (Math/min mn (double (:min-val cs)))
                 (Math/max mx (double (:max-val cs)))))
        (recur (inc i) sum cnt mn mx)))))

(defn- execute-stats-only [node columnar?]
  (let [ctx     (execute-node (:input node))
        columns (ctx-columns ctx)
        length  (ctx-length ctx)
        aggs    (:aggs node)
        preds   (:predicates node)
        ;; F20: when preds are present they've been pre-validated by
        ;; the strategy picker as fully zone-classifying — every chunk
        ;; is :stats-only (counts wholesale) or :skip (ignored). For
        ;; the no-preds case, surviving = all chunks.
        zone-filters (when (seq preds) (gb/build-zone-filters preds))
        survives?    (if zone-filters
                       (let [pred-cols   (into #{} (map :col) zone-filters)
                             col-entries (into {}
                                               (keep (fn [[k info]]
                                                       (when (contains? pred-cols k)
                                                         [k (gb/collect-chunk-entries (:index info))])))
                                               columns)]
                         (fn [^long i]
                           (= :stats-only (gb/classify-chunk zone-filters col-entries i))))
                       (constantly true))
        ;; One chunk-walk per distinct column. Previously the code walked
        ;; chunks once per agg — fine for one-agg queries, but for
        ;; e.g. MIN(price)+AVG(price)+MAX(price) that's three identical
        ;; passes over the same chunk-entry vector. Materialize the
        ;; per-column accumulator once and project aggs from it.
        ref-cols   (into #{} (keep :col) aggs)
        col-stats  (into {}
                         (map (fn [col]
                                (let [entries  (gb/collect-chunk-entries
                                                (:index (get columns col)))
                                      n-chunks (count entries)
                                      [sum cnt mn mx] (walk-chunk-stats
                                                       entries n-chunks survives?)]
                                  [col {:sum    (double sum)
                                        :count  (long cnt)
                                        :min    (double mn)
                                        :max    (double mx)
                                        :long?  (= :int64 (:type (get columns col)))}])))
                         ref-cols)
        ;; COUNT(*) under preds: derive from any one column's accumulated
        ;; count. Without preds, it's just `length`. Without preds AND
        ;; without ref-cols (count-only with no col agg), we still need
        ;; a count from chunk entries — pick any indexed column.
        count-all  (if (seq preds)
                     (or (some-> (first ref-cols) col-stats :count)
                         (let [idx-col  (some (fn [[_ info]] (:index info)) columns)
                               entries  (gb/collect-chunk-entries idx-col)
                               n-chunks (count entries)
                               [_ cnt _ _] (walk-chunk-stats entries n-chunks survives?)]
                           cnt))
                     length)
        result-row
        (reduce
         (fn [row agg]
           (let [alias (keyword (or (:as agg) (:op agg)))]
             (if (= :count (:op agg))
               (assoc row alias (long count-all) :_count count-all)
               (let [{:keys [sum count min max long?]} (col-stats (:col agg))]
                 (assoc row alias
                        (case (:op agg)
                          :sum (if (zero? count) nil (if long? (long sum) sum))
                          :min (if (zero? count) nil (if long? (long min) min))
                          :max (if (zero? count) nil (if long? (long max) max))
                          :avg (if (zero? count) nil (/ sum (double count))))
                        :_count count)))))
         {}
         aggs)]
    ;; F21: apply linear recipes if any agg carries one.
    [(post/rewrite-row-with-recipes result-row aggs)]))

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

(defn- block-skip-combined-validity
  "AND the per-predicate-column validity bitmaps for the block-skip
   path. Returns nil for the all-valid fast path (route to the
   existing block-skip kernel)."
  ^longs [preds mat-cols ^long length]
  (let [bms (->> preds
                 (keep #(:validity (get mat-cols (first %))))
                 vec)]
    (when (seq bms)
      (let [bm-len (quot (+ length 63) 64)
            combined (long-array bm-len)]
        (System/arraycopy ^longs (first bms) 0 combined 0 bm-len)
        (doseq [^longs v (rest bms)]
          (dotimes [i bm-len]
            (aset combined i (bit-and (aget combined i) (aget v i)))))
        combined))))

(defn- execute-block-skip-count [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        pp (gb/prepare-pred-arrays preds mat-cols)
        validity (block-skip-combined-validity preds mat-cols length)
        ^doubles r
        (if validity
          ;; Validity present: bypass block-skip and use the flat-array
          ;; Nullable count kernel. Block-skip optimisation assumes
          ;; predicate min/max bounds determine match status; with
          ;; nulls present those bounds include the sentinel, breaking
          ;; the analysis. The Nullable kernel ANDs validity into the
          ;; per-row mask at every SIMD step instead.
          (ColumnOpsNullable/fusedSimdCountParallel
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
           ^longs validity
           (int length))
          (ColumnOpsExt/fusedSimdCountBlockSkipParallel
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
           (int length)))
        agg (or (:agg node) {:op :count :as nil})]
    (post/format-fused-result {:result 0.0 :count (long (aget r 1))} agg)))

(defn- execute-fused-multi-sum [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        rows (x/execute-fused-multi-sum preds (:aggs node) mat-cols length)]
    ;; F21: each agg may carry a `:linear-recipe` from the rewrite pass.
    (mapv #(post/rewrite-row-with-recipes % (:aggs node)) rows)))

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
                        ;; Compact-with-null-skip helper for the
                        ;; copy paths below. Round-3 agent P1: the
                        ;; old `(double (aget la i))` plain-cast
                        ;; bled `Long/MIN_VALUE` into the working
                        ;; buffer as -9.22e18, dragging percentile
                        ;; / median / approx-quantile output to a
                        ;; meaningless extreme.
                        compact-skip-nulls
                        (fn ^doubles []
                          (if is-long?
                            (let [la ^longs col-data
                                  n (alength la)
                                  d (double-array n)
                                  k (int-array 1)]
                              (dotimes [i n]
                                (let [v (aget la i)]
                                  (when-not (= v Long/MIN_VALUE)
                                    (aset d (aget k 0) (double v))
                                    (aset k 0 (inc (aget k 0))))))
                              (java.util.Arrays/copyOf d (aget k 0)))
                            (let [da ^doubles col-data
                                  n (alength da)
                                  d (double-array n)
                                  k (int-array 1)]
                              (dotimes [i n]
                                (let [v (aget da i)]
                                  (when-not (Double/isNaN v)
                                    (aset d (aget k 0) v)
                                    (aset k 0 (inc (aget k 0))))))
                              (java.util.Arrays/copyOf d (aget k 0)))))
                        result (case (:op agg)
                                 (:median :percentile)
                                 (if mask
                                   (if is-long?
                                     (ColumnOps/percentileFilteredLong ^longs col-data ^longs mask (int length) pct)
                                     (ColumnOps/percentileFiltered ^doubles col-data ^longs mask (int length) pct))
                                   (let [^doubles compacted (compact-skip-nulls)]
                                     (ColumnOps/percentile compacted (alength compacted) pct)))
                                 :approx-quantile
                                 (if mask
                                   (let [work (double-array cnt)
                                         pos (int-array 1)]
                                     (dotimes [i length]
                                       (when (== 1 (aget ^longs mask i))
                                         (let [v (if is-long?
                                                   (let [lv (aget ^longs col-data i)]
                                                     (when-not (= lv Long/MIN_VALUE) (double lv)))
                                                   (let [dv (aget ^doubles col-data i)]
                                                     (when-not (Double/isNaN dv) dv)))]
                                           (when (some? v)
                                             (aset work (aget pos 0) (double v))
                                             (aset pos 0 (inc (aget pos 0)))))))
                                     (ColumnOpsAnalytics/tdigestApproxQuantileParallel work (aget pos 0) pct 200.0))
                                   (let [^doubles compacted (compact-skip-nulls)]
                                     (ColumnOpsAnalytics/tdigestApproxQuantileParallel compacted (alength compacted) pct 200.0))))]
                    [alias result]))
                aggs))]))

(defn- execute-scalar-agg [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        mat-cols (cols/materialize-columns columns)
        row (gb/execute-scalar-aggs preds (:aggs node) mat-cols length)]
    ;; Apply F21 linear-recipe metadata if present (the rewrite pass
    ;; may have rewritten an `:expr` agg into a base agg + recipe).
    [(post/rewrite-row-with-recipes row (:aggs node))]))

;; --- Mixed-class split aggregation (PSplitAgg) ------------------------------
;;
;; The planner emits PSplitAgg when LGlobalAgg / LGroupBy contain aggs whose
;; physical strategies disagree (e.g. `min(x)` runs on SIMD, `median(x)` on
;; quickselect). Each child sub-plan computes its own subset on its best
;; operator; this executor runs them and merges results.

(defn- merge-global-rows
  "Merge single-row results from each child into one row, preserving
   the user-facing agg order from `agg-order`. The `:_count` field is
   identical across children (same predicates + scan), so any one is fine."
  [child-results agg-order]
  (let [merged (reduce merge {} (map first child-results))
        ;; Project user-aliased fields in agg-order, plus :_count if present.
        ordered (cond-> (reduce (fn [m a]
                                  (let [alias (or (:as a) (:op a))]
                                    (if (contains? merged alias)
                                      (assoc m alias (get merged alias))
                                      m)))
                                {}
                                agg-order)
                  (contains? merged :_count) (assoc :_count (get merged :_count)))]
    [ordered]))

(defn- row-group-key
  "Extract the group-key tuple from a result row."
  [row group-keys]
  (mapv #(get row %) group-keys))

(defn- merge-group-rows
  "Merge per-group rows from each child by group-key tuple. Each child
   returns the same set of groups (same predicates + scan + keys); the
   merge unions their agg fields."
  [child-results agg-order group-keys]
  (let [;; Index child 0 by group key to preserve its row order.
        first-child (first child-results)
        key-index   (mapv #(row-group-key % group-keys) first-child)
        ;; For each remaining child, build a key→row map.
        other-maps  (mapv (fn [rows]
                            (persistent!
                             (reduce (fn [m r]
                                       (assoc! m (row-group-key r group-keys) r))
                                     (transient {})
                                     rows)))
                          (rest child-results))]
    (mapv (fn [base-row k]
            (let [merged (reduce (fn [acc child-map]
                                   (let [r (get child-map k)]
                                     (if r (merge acc r) acc)))
                                 base-row
                                 other-maps)]
              ;; Project group keys + aggs in declared order.
              (cond-> (reduce (fn [m gk] (assoc m gk (get merged gk)))
                              {}
                              group-keys)
                true (into (keep (fn [a]
                                   (let [alias (or (:as a) (:op a))]
                                     (when (contains? merged alias)
                                       [alias (get merged alias)])))
                                 agg-order))
                (contains? merged :_count) (assoc :_count (get merged :_count)))))
          first-child
          key-index)))

(defn- execute-split-agg [node columnar?]
  ;; Each child is a complete physical sub-plan. We always run them
  ;; in row mode (columnar? = false) and merge maps; the outer projection
  ;; layer handles the columnar conversion if requested.
  ;;
  ;; Children execute IN PARALLEL: the original serial design assumed
  ;; sub-plans shared L3-cached input across passes, but mixed-class
  ;; workloads can have wildly different per-branch costs (e.g.
  ;; percentile O(N log N) sort vs stats-only O(chunks)). Sequential
  ;; execution then bills wall-time = Σ branches; parallel bills
  ;; max(branches), saving the cheap-branch cost in the common case and
  ;; cleanly protecting against per-branch first-touch pessimism (page
  ;; faults, JIT warmup) that would otherwise serialize. Branches read
  ;; the same scan input — only OS-page-cache / L3 bandwidth is shared.
  (let [children   (:children node)
        agg-order  (:agg-order node)
        group-keys (:group-keys node)
        child-results
        (if (= 1 (count children))
          [(execute-node (first children) false)]
          ;; Propagate *explain-collector* across the worker threads so
          ;; ANALYZE per-node timings continue to be recorded.
          (let [coll *explain-collector*
                fs   (mapv (fn [c]
                             (future
                               (binding [*explain-collector* coll]
                                 (execute-node c false))))
                           children)]
            (mapv deref fs)))]
    (if (nil? group-keys)
      (merge-global-rows child-results agg-order)
      (merge-group-rows child-results agg-order group-keys))))

;; --- Group-by strategies ----------------------------------------------------

(defn- execute-chunked-dense-group-by [node columnar?]
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds (:predicates node) ctx)
        group-keys (:group-keys node)
        aggs (:aggs node)
        ;; Try chunked first (streaming, zero-copy), fall back to materialized
        result
        (or (gb/execute-chunked-group-by preds aggs group-keys columns length columnar?)
            (let [mat-cols (cols/materialize-columns columns)
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
              (x/execute-group-by preds aggs group-keys mat-cols length columnar?)))]
    ;; F21: per-group recipe application (no-op when no agg has a recipe).
    (post/apply-recipes-to-results result aggs)))

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
    ;; F21: per-group recipe application (no-op when no agg has a recipe).
    (post/apply-recipes-to-results
     (if (and actual-rows (< actual-rows 1000))
       ;; Very few rows survive — reduce dense limit to save memory
       (binding [gb/*dense-group-limit* (max 10000 (* 10 (long actual-rows)))]
         (x/execute-group-by preds aggs group-keys mat-cols length columnar?))
       (x/execute-group-by preds aggs group-keys mat-cols length columnar?))
     aggs)))

(defn- execute-hash-group-by [node columnar?]
  ;; Same as dense — execute-group-by internally decides dense vs hash
  (execute-dense-group-by node columnar?))

(defn- execute-fused-extract-count
  "Run the fused EXTRACT(unit, col) GROUP BY → COUNT operator. The
   planner emits this when a `LGroupBy` over a single
   `PMaterializeExpr {:op #{:minute :hour :second :day-of-week}}`
   has only `:count` aggs and no filter. We pull the source column
   straight from the scan (skipping the materialization), call
   `ColumnOpsExt/fusedExtractCountDenseParallel`, then decode the
   per-key accumulator array into row maps or columnar output. The
   decode mirrors the legacy block at `q.clj:858-908`."
  [node columnar?]
  (let [scan-ctx  (execute-node (:input node))
        col-key   (:extract-col node)
        col-info  (get (:columns scan-ctx) col-key)
        ;; Long-typed source — either a materialized array or an
        ;; index that we materialize once. The planner's eligibility
        ;; gate guarantees the column is :int64.
        ^longs raw-col (or (:data col-info)
                           (when-let [idx (:index col-info)]
                             (index/idx-materialize-to-array idx)))
        op       (:extract-op node)
        op-const (case op
                   :minute      ColumnOpsExt/EXTRACT_MINUTE
                   :hour        ColumnOpsExt/EXTRACT_HOUR
                   :second      ColumnOpsExt/EXTRACT_SECOND
                   :day-of-week ColumnOpsExt/EXTRACT_DAY_OF_WEEK)
        aggs     (:aggs node)
        n-aggs   (int (count aggs))
        length   (long (or (:length scan-ctx) (alength raw-col)))
        ^"[[D" result-array
        (ColumnOpsExt/fusedExtractCountDenseParallel
         raw-col (int op-const) n-aggs (int length))
        max-key  (int (ColumnOpsExt/extractMaxKey (int op-const)))
        group-col-name (keyword (name op))
        agg-aliases    (mapv #(or (:as %) (:op %)) aggs)]
    (if columnar?
      (let [key-list (java.util.ArrayList.)
            _ (dotimes [k max-key]
                (let [^doubles accs (aget result-array k)]
                  (when (and accs (> (aget accs 1) 0.0))
                    (.add key-list (long k)))))
            n        (.size key-list)
            keys-arr (long-array n)
            _        (dotimes [i n] (aset keys-arr i (long (.get key-list i))))
            agg-arrs (mapv (fn [_a-idx]
                             (let [la (long-array n)]
                               (dotimes [i n]
                                 (let [k (long (.get key-list i))
                                       ^doubles accs (aget result-array (int k))]
                                   (aset la i (long (aget accs 1)))))
                               la))
                           (range n-aggs))]
        (-> {group-col-name keys-arr :n-rows n}
            (into (map vector agg-aliases agg-arrs))))
      (let [results (transient [])]
        (dotimes [k max-key]
          (let [^doubles accs (aget result-array k)]
            (when (and accs (> (aget accs 1) 0.0))
              (let [cnt (long (aget accs 1))
                    row (-> {group-col-name (long k)}
                            (into (map (fn [a] [a cnt]) agg-aliases))
                            (assoc :_count cnt))]
                (conj! results row)))))
        (persistent! results)))))

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
        ;; F19: derive build-key range and push it onto the probe-side
        ;; scan's `:dynamic-filters` volatile BEFORE we execute the
        ;; probe — gives the probe scan a chance to do zone-map skips.
        _ (push-dynamic-filter! (:probe-side node)
                                (ctx-columns build-ctx)
                                (ctx-length build-ctx)
                                (:on-pairs node)
                                (:join-type node))
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
  (let [right-ctx (execute-node (:right node))   ;; build (dim)
        ;; F19: push dynamic range from build (right) onto probe (left).
        _ (push-dynamic-filter! (:left node)
                                (ctx-columns right-ctx)
                                (ctx-length right-ctx)
                                (:on-pairs (:join-spec node))
                                (:type (:join-spec node)))
        left-ctx  (execute-node (:left node))    ;; probe (fact)
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

(defn- execute-asof-join [node]
  (let [left-ctx  (execute-node (:left node))
        right-ctx (execute-node (:right node))
        probe-mask (realize-mask left-ctx)
        build-mask (realize-mask right-ctx)
        probe-cols (cols/materialize-columns (ctx-columns left-ctx))
        probe-length (ctx-length left-ctx)
        build-cols (cols/materialize-columns (ctx-columns right-ctx))
        build-length (ctx-length right-ctx)
        spec {:join-type (:join-type node)
              :on-pairs (:on-pairs node)
              :match-condition (:match-condition node)}
        result (asof/execute-asof-join probe-cols probe-length
                                       build-cols build-length
                                       spec
                                       :probe-mask probe-mask
                                       :build-mask build-mask)]
    {:columns (:columns result) :length (long (:length result))}))

(defn- execute-fused-join-global-agg [node columnar?]
  (let [right-ctx (execute-node (:right node))   ;; build (dim)
        ;; F19: push dynamic range from build (right) onto probe (left).
        _ (push-dynamic-filter! (:left node)
                                (ctx-columns right-ctx)
                                (ctx-length right-ctx)
                                (:on-pairs (:join-spec node))
                                (:type (:join-spec node)))
        left-ctx  (execute-node (:left node))    ;; probe (fact)
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
  ;; ctx may already carry prepared `:preds` (from a `PSIMDFilter`/`PMaskFilter`
  ;; child that ran `prepare-preds`) or raw `:preds` (when the lifted scan
  ;; surfaces its `:predicates` field via ctx). `prepare-node-preds [] ctx`
  ;; normalises both: raw preds get the full prepare pipeline (string preds
  ;; → mask, dict resolution, SIMD/mask split); already-prepared preds round-
  ;; trip safely (the pipeline is idempotent for SIMD-eligible forms).
  (let [ctx (execute-node (:input node))
        [preds columns length] (prepare-node-preds [] ctx)]
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

;; --- SMCS recompose ----------------------------------------------------------

(defn- execute-recompose
  "Run the child aggregator and reassemble user-visible aggs from
   the SMCS-decomposed result. The mapping was produced by
   `smcs-decomposition` (planner pass); for each original-agg index
   it specifies either `:passthrough` (no work) or `:sum-mul-const-sub`
   (apply `c·SUM(a) − SUM_PRODUCT(a,b)` and drop the synthetic
   __sp_ alias). Columnar output keeps the per-row arrays intact and
   transforms in-place; row output remaps each row."
  [node columnar?]
  (let [child-result (execute-node (:input node) columnar?)
        mapping  (:mapping node)
        orig-aggs (:orig-aggs node)]
    (if columnar?
      (gb/recompose-columnar-results child-result orig-aggs mapping)
      (mapv #(gb/recompose-row-results % orig-aggs mapping) child-result))))

;; --- Expression materialization ----------------------------------------------

(defn- execute-materialize-expr [node]
  (let [ctx (execute-node (:input node))
        columns (ctx-columns ctx)
        length  (ctx-length ctx)
        mat-cols (cols/materialize-columns columns)
        col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
        cache  (java.util.HashMap.)
        target (or (:target node) :float64)
        ;; `:int64` target → use `eval-expr-to-long` so date-trunc /
        ;; date-add / extract ops emit a `long[]` directly. The
        ;; alternative (`eval-expr-vectorized` then double→long
        ;; cast) blocks the dense group-by all-long fast path and
        ;; reintroduces the round-trip the legacy code already
        ;; eliminated. Mirrors `q.clj:712-718`.
        arr (if (= target :int64)
              (expr/eval-expr-to-long (:expr node) col-arrays length cache)
              (expr/eval-expr-vectorized (:expr node) col-arrays length cache))]
    (assoc ctx :columns (assoc mat-cols (:col-name node)
                               {:type target :data arr}))))

;; --- Top-N pushdown ---------------------------------------------------------

(defn- execute-top-n-node
  "Run the streaming top-N pushdown by delegating to
   `stratum.query.top-n/execute-top-n`. The IR's `LTopN` node carries
   one or more `:order-specs`, the LIMIT, and the (LProject) items
   it absorbed (or nil for SELECT *). We assemble the synthetic
   query map that `top-n/execute-top-n` expects from those fields
   plus the input column context."
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
        ;; `:order-specs` is the canonical multi-key form (vec of
        ;; `[col dir]` pairs). Caller may have stored a single
        ;; bare-keyword spec from the legacy single-key path —
        ;; normalize back to that vector shape here.
        order-specs (let [os (:order-specs node)]
                      (cond
                        (and (vector? os) (every? vector? os)) os
                        (and (vector? os) (keyword? (first os))) [os]
                        (keyword? os) [[os :asc]]
                        :else (vec os)))]
    (top-n/execute-top-n
     {:order  (vec order-specs)
      :limit  (:limit node)
      :select select}
     columns)))

(defn- execute-head-node
  "Run the LIMIT-without-ORDER-BY pushdown — return the first N rows
   in scan order. Each output column is materialized via
   `cols/take-prefix-column`, which fetches only the prefix needed
   (a single chunk for typical small limits over a multi-million-row
   index) and never decodes the full column. Mirrors
   `execute-top-n-node` but skips the heap.

   Output is a vec of row maps (the LLimit's normal shape).
   Dict-encoded columns flow through with their dict intact, so
   downstream string decoding still works in the per-row reads."
  [node]
  (let [ctx     (execute-node (:input node))
        columns (ctx-columns ctx)
        length  (long (ctx-length ctx))
        n       (Math/min (long (:limit node)) length)
        items   (:select node)
        all-keys (vec (keys columns))
        out-cols (cond
                   (nil? items) all-keys
                   :else (let [refs (keep :ref items)]
                           (if (every? keyword? refs) (vec refs) all-keys)))
        ;; Materialize only the first N rows of each referenced column.
        prefixed (into {}
                       (map (fn [k] [k (cols/take-prefix-column (get columns k) n)]))
                       out-cols)
        ;; Per-key reader fn that decodes the i-th row, applying dict
        ;; lookup for dict-encoded string columns.
        reader  (fn [k]
                  (let [col (get prefixed k)
                        data (:data col)
                        dict (:dict col)
                        dict-string? (and dict (= :string (:dict-type col)))]
                    (cond
                      (and dict-string? (instance? (Class/forName "[J") data))
                      (fn [^long i]
                        (let [code (aget ^longs data i)]
                          (when (>= code 0)
                            (aget ^"[Ljava.lang.String;" dict (int code)))))
                      (instance? (Class/forName "[J") data)
                      (fn [^long i] (aget ^longs data i))
                      (instance? (Class/forName "[D") data)
                      (fn [^long i] (aget ^doubles data i))
                      (instance? (Class/forName "[Ljava.lang.String;") data)
                      (fn [^long i] (aget ^"[Ljava.lang.String;" data i))
                      :else (fn [^long _i] nil))))
        readers (into {} (map (juxt identity reader)) out-cols)]
    (mapv (fn [^long i]
            (into {} (map (fn [k] [k ((get readers k) i)])) out-cols))
          (range n))))

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
   Mirrors the legacy `q` window-having pushdown.
   `preds` are already normalized (by `build-logical-plan`)."
  [ctx preds]
  (let [length  (long (:length ctx))
        columns (:columns ctx)
        normalized preds
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

(def ^:dynamic *explain-collector*
  "When bound (to an atom holding a map), `execute-node` records per-node
   wall-clock time and output cardinality keyed by `(System/identityHashCode
   node)`. Bound by `explain-analyze-query`; nil at runtime in non-EXPLAIN
   paths so the overhead is a single nil check per node call."
  nil)

(defn- count-output
  "Best-effort row count for whatever `execute-node` returned.
   Column context → :length. Columnar result → :n-rows. Seq → count.
   Other shapes (rare) → 1."
  ^long [result]
  (cond
    (nil? result)                                              0
    (and (map? result) (contains? result :length))             (long (:length result))
    (and (map? result) (contains? result :n-rows))             (long (:n-rows result))
    (sequential? result)                                       (count result)
    :else                                                      1))

(declare execute-node-impl)

(defn execute-node
  "Execute a physical plan node, returning either a column context or result rows.

   When `*explain-collector*` is bound, records wall-clock duration and
   output row count per invocation. Sub-executors call back through this
   public arity, so every recursive call also gets timed. A parent's
   recorded time naturally includes its children's — matching Postgres /
   DuckDB EXPLAIN ANALYZE semantics (inclusive timing)."
  ([node] (execute-node node false))
  ([node columnar?]
   (if-let [coll *explain-collector*]
     (let [start  (System/nanoTime)
           result (execute-node-impl node columnar?)
           end    (System/nanoTime)]
       (swap! coll assoc (System/identityHashCode node)
              {:time-ns (- end start)
               :rows    (count-output result)})
       result)
     (execute-node-impl node columnar?))))

(defn- execute-node-impl
  "Inner dispatch. Sub-execute-* fns call back through `execute-node`
   for their children, so each child goes through the timing wrap."
  [node columnar?]
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
    (instance? PSplitAgg node)        (execute-split-agg node columnar?)

     ;; Group-by
    (instance? PChunkedDenseGroupBy node) (execute-chunked-dense-group-by node columnar?)
    (instance? PDenseGroupBy node)        (execute-dense-group-by node columnar?)
    (instance? PHashGroupBy node)         (execute-hash-group-by node columnar?)
    (instance? PFusedExtractCount node)   (execute-fused-extract-count node columnar?)

     ;; Join
    (instance? PHashJoin node)             (execute-hash-join node)
    (instance? PPerfectHashJoin node)      (execute-hash-join node) ;; same API, perfect hash is internal
    (instance? PBitmapSemiJoin node)       (execute-bitmap-semi-join-node node)
    (instance? PAsofJoin node)             (execute-asof-join node)
    (instance? PFusedJoinGroupAgg node)    (execute-fused-join-group-agg node columnar?)
    (instance? PFusedJoinGlobalAgg node)   (execute-fused-join-global-agg node columnar?)

     ;; Expression materialization
    (instance? PMaterializeExpr node) (execute-materialize-expr node)

     ;; SMCS post-aggregation recompose (planner-inserted wrapper)
    (instance? PRecompose node) (execute-recompose node columnar?)

     ;; Top-N pushdown — LTopN is recognized directly (no separate
     ;; physical record) and dispatched to the streaming primitive.
    (instance? LTopN node) (execute-top-n-node node)

     ;; LIMIT-without-ORDER-BY pushdown — same shape as LTopN, but
     ;; no order column: first N rows in scan order.
    (instance? LHead node) (execute-head-node node)

     ;; Anomaly score materialization (post-join). Resolves at the
     ;; right point in the pipeline so the iforest sees joined
     ;; columns. The plan's frontend has already rewritten the
     ;; surrounding query to reference the synthetic columns
     ;; produced here. Pure column-ctx → column-ctx — no row decode.
    (instance? LAnomaly node)
    (let [ctx       (execute-node (:input node))
          columns   (:columns ctx)
          length    (long (:length ctx))
          new-cols  (prep/materialize-anomaly (:expr->col node)
                                              columns length
                                              (:models node))]
      (assoc ctx :columns new-cols))

     ;; Post-join string-expression materialization. The frontend
     ;; has rewritten `:group` / aggs / `:select` to reference the
     ;; synthetic `__str_expr_N` columns produced here. Each item
     ;; runs `expr/eval-string-expr` against the post-join
     ;; (materialized) column ctx, so dict-encoded results are
     ;; sized to the joined row count.
    (instance? LStringMaterialize node)
    (let [ctx       (execute-node (:input node))
          columns   (:columns ctx)
          length    (long (:length ctx))
          mat-cols  (cols/materialize-columns columns)
          new-cols  (reduce
                     (fn [cs {:keys [col-name expr]}]
                       (let [entry (expr/eval-string-expr expr cs length)]
                         (assoc cs col-name entry)))
                     mat-cols
                     (:items node))]
      (assoc ctx :columns new-cols))

     ;; Set ops — UNION/INTERSECT/EXCEPT. The runtime path in
     ;; `stratum.query/q` short-circuits these before the planner
     ;; runs, but `compile-physical` / `explain-query` callers can
     ;; still hand us an `LSetOp`. Recurse via `q` so each
     ;; sub-query goes through its own planner pass and the result
     ;; combination matches the legacy semantics in `q.clj:372–386`.
    (instance? LSetOp node)
    (let [q-fn @(requiring-resolve 'stratum.query/q)
          sub-results (mapv q-fn (:queries node))]
      (case (:op node)
        :union
        (let [combined (vec (apply concat sub-results))]
          (if (:all? node)
            combined
            (vec (clojure.core/distinct combined))))
        :intersect
        (let [first-r (set (first sub-results))]
          (vec (reduce #(clojure.set/intersection %1 (set %2))
                       first-r (rest sub-results))))
        :except
        (let [first-r (first sub-results)
              to-remove (reduce (fn [acc r] (into acc r)) #{} (rest sub-results))]
          (vec (remove to-remove first-r)))))

     ;; Projection
    (instance? PProject node)  (execute-project node columnar?)

     ;; Post-processing
    (instance? PWindow node)   (execute-window node columnar? nil)
    (instance? PHaving node)   (execute-having node nil)
    (instance? PSort node)     (execute-sort node nil)
    (instance? PDistinct node) (execute-distinct node nil)
    (instance? PLimit node)    (execute-limit node nil)

    :else (throw (ex-info (str "Unknown physical node: " (type node)) {:node node}))))

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
        ;; Anomaly resolution. `[:anomaly-score …]` would otherwise
        ;; reach `normalize-expr` and throw "Unknown expression
        ;; operator". Two cases:
        ;;   - No join: resolve eagerly here. The pre-join columns
        ;;     are the only columns the iforest will ever see, so
        ;;     scoring at prepare time and inlining the synthetic
        ;;     column into `columns` is fine.
        ;;   - Join: defer to the executor's `LAnomaly` case so the
        ;;     iforest sees post-join columns. We still rewrite the
        ;;     query here (via `anomaly-spec`) so synthetic refs
        ;;     are visible to `prepare-query` / `build-logical-plan`,
        ;;     and we thread the spec + models through the cleaned
        ;;     query for `build-logical-plan` to attach to `LAnomaly`.
        models  (:_anomaly-models query)
        has-join? (seq (:join query))
        [query columns anomaly-spec]
        (cond
          (and (seq models) (not has-join?))
          (let [[q' cols'] (prep/resolve-anomaly-columns query columns length models)]
            [q' cols' nil])

          (and (seq models) has-join?)
          (let [{:keys [expr->col query]} (prep/anomaly-spec query)]
            [query columns {:expr->col expr->col :models models}])

          :else [query columns nil])
        {:keys [preds aggs group select columns columns-meta string-items]}
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
                           ::plan/pre-normalized? true)
                    (cond->
                     anomaly-spec
                      (assoc ::plan/anomaly-expr->col (:expr->col anomaly-spec)
                             ::plan/anomaly-models    (:models    anomaly-spec))
                      (seq string-items)
                      (assoc ::plan/string-items string-items)))]
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
   planner-native keys (`:plan-tree`, `:strategy`, `:plan-data`) and
   legacy-shape keys (`:n-rows`, `:columns`) so existing callers don't
   break.

   `:plan-data` is the structured data tree from `plan/plan->data`;
   render with `plan/render-text` or `plan/render-json` for alternate
   output formats."
  [query]
  (let [logical  (plan/build-logical-plan query)
        physical (plan/optimize logical)
        data     (plan/plan->data physical)
        [n-rows n-cols] (scan-summary physical)]
    {:plan-data data
     :plan-tree (plan/render-text data)
     :strategy  (keyword (.getSimpleName (class physical)))
     :n-rows    n-rows
     :columns   n-cols
     :query     (dissoc query :from)}))

(defn explain-analyze-query
  "Same as `explain-query`, but runs the query under instrumentation
   and merges per-node `actual-rows` + `time-ms` into the plan data.

   `:execution-time-ms` is the wall-clock time for the root execute call.
   Result map adds `:plan-data` (with timings) + `:plan-tree` (rendered
   with timings + footer)."
  [query]
  (let [logical   (plan/build-logical-plan query)
        physical  (plan/optimize logical)
        collector (atom {})
        start     (System/nanoTime)
        _         (binding [*explain-collector* collector]
                    (execute-node physical false))
        elapsed   (/ (double (- (System/nanoTime) start)) 1.0e6)
        [n-rows n-cols] (scan-summary physical)
        data      (-> (plan/plan->data physical)
                      (plan/merge-analyze-timings @collector)
                      (assoc :execution-time-ms elapsed))]
    {:plan-data data
     :plan-tree (plan/render-text data)
     :strategy  (keyword (.getSimpleName (class physical)))
     :n-rows    n-rows
     :columns   n-cols
     :execution-time-ms elapsed
     :query     (dissoc query :from)}))
