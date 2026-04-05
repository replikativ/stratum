(ns stratum.query.plan
  "Query plan construction and optimization.

   Entry points:
     (build-logical-plan query)        — normalized query map → logical IR tree
     (annotate plan)                   — attach column types, cardinality, index info
     (optimize plan)                   — logical IR → physical IR via passes
     (explain plan)                    — human-readable plan description

   Optimization passes (applied in order):
     1. predicate-pushdown    — move filters below joins/windows
     2. zone-map-annotation   — mark index scans with surviving chunks
     3. expr-materialization  — insert PMaterializeExpr for computed expressions
     4. strategy-selection    — replace logical nodes with physical nodes
     5. operator-fusion       — merge adjacent compatible physical nodes

   Each pass is a pure function: plan → plan."
  (:require [stratum.query.ir :as ir]
            [stratum.query.normalization :as norm]
            [stratum.query.columns :as cols]
            [stratum.query.predicate :as pred]
            [stratum.query.expression :as expr]
            [stratum.dataset :as dataset])
  (:import [stratum.query.ir
            LScan LFilter LJoin LGroupBy LGlobalAgg LProject
            LWindow LHaving LDistinct LSort LLimit LSetOp
            PScan PChunkedScan PSIMDFilter PMaskFilter
            PStatsOnlyAgg PFusedSIMDAgg PFusedSIMDCount
            PChunkedSIMDAgg PChunkedSIMDCount PBlockSkipCount
            PFusedMultiSum PPercentileAgg PScalarAgg
            PChunkedDenseGroupBy PDenseGroupBy PHashGroupBy
            PFusedExtractCount PBitmapSemiJoin PHashJoin
            PPerfectHashJoin PFusedJoinGroupAgg PFusedJoinGlobalAgg
            PProject PWindow PHaving PSort PDistinct PLimit
            PMaterializeExpr]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Plan construction — query map → logical IR
;; ============================================================================

(defn- normalize-select-item
  "Normalize a single SELECT item to {:name kw :ref kw :expr expr}."
  [item idx]
  (cond
    (keyword? item) {:name item :ref item}
    (vector? item)  (let [expr (norm/normalize-expr (vec item))]
                      {:name (keyword (str "__sel_" idx)) :expr expr})
    (map? item)     item
    :else           {:name (keyword (str "__sel_" idx)) :ref item}))

(defn- build-scan
  "Build an LScan from :from, handling both maps and IDataset."
  [from]
  (let [columns (if (satisfies? dataset/IDataset from)
                  (dataset/columns from)
                  (cols/prepare-columns from))
        length  (cols/get-column-length (val (first columns)))]
    (ir/->LScan columns length)))

(defn- build-join-tree
  "Wrap scan with join nodes for each join spec."
  [scan join-specs]
  (reduce
   (fn [left join-spec]
     (let [{:keys [with on type]} join-spec
           on-pairs (cond
                      ;; [:= :a :b]
                      (and (vector? on) (= := (first on)))
                      [[(second on) (nth on 2)]]
                      ;; [[:= :a :b] [:= :c :d]]
                      (and (vector? on) (every? vector? on))
                      (mapv (fn [pair] [(second pair) (nth pair 2)]) on)
                      :else (throw (ex-info "Unsupported join :on format" {:on on})))
           right-scan (build-scan with)]
       (ir/->LJoin (or type :inner) on-pairs left right-scan)))
   scan
   join-specs))

(defn build-logical-plan
  "Convert a query map into a logical IR tree.

   The tree is built bottom-up: scan → join → filter → aggregate/project →
   window → having → distinct → sort → limit.  This mirrors the SQL
   evaluation order and matches stratum's current pipeline."
  [{:keys [from join where select agg group having order
           limit offset distinct window
           result
           _set-op _union _having-only-keys _order-only-keys]
    :as query}]
  ;; Set operations are separate top-level constructs
  (if-let [set-op (or _set-op
                      (when _union
                        {:op :union
                         :queries (:queries _union)
                         :all? (:all? _union)}))]
    (ir/->LSetOp (:op set-op) (:queries set-op) (:all? set-op))

    (let [;; Normalize group/order to strip SQL table-qualifier namespaces
          group  (when group (mapv #(if (keyword? %) (norm/strip-ns %) %) group))
          order  (when order (mapv (fn [[c dir]] [(norm/strip-ns c) dir]) order))
          window (when window
                   (mapv (fn [ws]
                           (cond-> ws
                             (:col ws)          (update :col norm/strip-ns)
                             (:partition-by ws) (update :partition-by #(mapv norm/strip-ns %))
                             (:order-by ws)     (update :order-by #(mapv (fn [[c d]] [(norm/strip-ns c) d]) %))))
                         window))

          ;; 1. Leaf: data source
          scan (build-scan from)

          ;; 2. Joins (if any)
          joined (if (seq join)
                   (build-join-tree scan join)
                   scan)

          ;; 3. Normalize predicates, aggs, select items
          preds   (mapv norm/normalize-pred (or where []))
          aggs    (norm/auto-alias-aggs (mapv norm/normalize-agg (or agg [])))
          sel     (when (seq select)
                    (vec (map-indexed #(normalize-select-item %2 %1) select)))

          ;; 4. Filter
          filtered (if (seq preds)
                     (ir/->LFilter preds joined)
                     joined)

          ;; 5. Core operation: group-by / global-agg / project / passthrough
          core (cond
                 (seq group)
                 (ir/->LGroupBy group aggs filtered)

                 (seq aggs)
                 (ir/->LGlobalAgg aggs filtered)

                 (seq sel)
                 (ir/->LProject sel filtered)

                 :else filtered)

          ;; 6. Window functions
          windowed (if (seq window)
                     (ir/->LWindow window core)
                     core)

          ;; 7. Having
          having-node (if (seq having)
                        (ir/->LHaving having windowed)
                        windowed)

          ;; 8. Distinct
          distinct-node (if distinct
                          (ir/->LDistinct having-node)
                          having-node)

          ;; 9. Sort
          sorted (if (seq order)
                   (ir/->LSort order distinct-node)
                   distinct-node)

          ;; 10. Limit / offset
          limited (if (or limit offset)
                    (ir/->LLimit limit offset sorted)
                    sorted)]

      ;; Attach output format as metadata so execution knows row vs columnar
      (vary-meta limited assoc
                 ::output-format (or result :rows)
                 ::having-only-keys _having-only-keys
                 ::order-only-keys _order-only-keys))))

;; ============================================================================
;; Annotation — gather column metadata for optimizer decisions
;; ============================================================================

(defn- scan-col-types
  "Extract {col-kw -> :int64/:float64/:dict-string} from an LScan."
  [^stratum.query.ir.LScan scan]
  (into {}
        (map (fn [[k v]]
               [k (cond
                    (:dict v)                        :dict-string
                    (= :int64 (:type v))             :int64
                    (= :float64 (:type v))           :float64
                    (expr/long-array? (:data v))     :int64
                    (expr/double-array? (:data v))   :float64
                    (expr/string-array? (:data v))   :string
                    :else                            :unknown)]))
        (:columns scan)))

(defn- scan-index-cols
  "Set of column keywords backed by PersistentColumnIndex."
  [^stratum.query.ir.LScan scan]
  (into #{}
        (keep (fn [[k v]] (when (:index v) k)))
        (:columns scan)))

(defn- all-indices?
  "True if every column in the scan is index-backed."
  [^stratum.query.ir.LScan scan]
  (every? (fn [[_ v]] (:index v)) (:columns scan)))

(defn annotate
  "Attach type/cardinality/index metadata to plan nodes.
   Called once before optimization passes."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (if (instance? LScan node)
        (vary-meta node assoc
                   ::col-types   (scan-col-types node)
                   ::index-cols  (scan-index-cols node)
                   ::all-indices (all-indices? node))
        ;; Propagate annotations upward through unary nodes
        (if-let [child (ir/input-node node)]
          (vary-meta node merge (select-keys (meta child)
                                             [::col-types ::index-cols ::all-indices]))
          node)))))

;; ============================================================================
;; Pass 1: Predicate pushdown
;; ============================================================================

(defn- pred-columns
  "Set of column keywords referenced by a normalized predicate."
  [pred]
  (let [col (first pred)]
    (if (map? col)
      ;; Expression predicate like [:> {:op :mul :args [:a :b]} 1000]
      (into #{} (filter keyword?) (:args col))
      (if (keyword? col) #{col} #{}))))

(defn predicate-pushdown
  "Push LFilter predicates below LJoin when they reference only one side.

   Currently disabled for join pushdown: stratum's join execution doesn't
   support pre-filtered probe/build sides. Predicates stay above the join
   and get absorbed into the group-by/agg node's execution.

   TODO: Implement bitmap-semi-join or mask-based pre-filtering to enable
   predicate pushdown through joins."
  [plan]
  ;; For now, only push predicates within non-join subtrees.
  ;; Join-level pushdown requires the executor to apply masks before joining.
  plan)

;; ============================================================================
;; Pass 2: Zone-map annotation
;; ============================================================================

(defn zone-map-annotation
  "For LFilter → LScan chains where the scan is all-indices and predicates
   are simple comparisons, annotate the scan with surviving chunk ranges.

   Downstream strategy selection uses this to choose PChunkedScan over PScan."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (if (and (instance? LFilter node)
               (instance? LScan (:input node))
               (::all-indices (meta (:input node)))
               (every? #(not (map? (first %))) (:predicates node)))
        ;; Mark the scan; actual chunk computation deferred to execution time
        ;; (depends on index state which may change between plan and execute)
        (update node :input vary-meta assoc ::zone-map-eligible true)
        node))))

;; ============================================================================
;; Pass 3: Expression materialization
;; ============================================================================

(defn- rewrite-expr-aggs
  "For aggs with non-nil :expr, create PMaterializeExpr nodes and rewrite aggs.
   Returns [new-aggs mat-nodes]."
  [aggs]
  (loop [idx 0, new-aggs [], mat-nodes []]
    (if (>= idx (count aggs))
      [new-aggs mat-nodes]
      (let [agg (nth aggs idx)]
        (if-let [e (:expr agg)]
          (let [cn (keyword (str "__expr_" idx))]
            (recur (inc idx)
                   (conj new-aggs (-> agg (dissoc :expr) (assoc :col cn)))
                   (conj mat-nodes (ir/->PMaterializeExpr cn e :float64 nil))))
          (recur (inc idx) (conj new-aggs agg) mat-nodes))))))

(defn- rewrite-expr-group-keys
  "For group-keys that are expressions (not keywords), create PMaterializeExpr
   nodes and rewrite the keys. Returns [new-keys mat-nodes]."
  [group-keys]
  (loop [idx 0, new-keys [], mat-nodes []]
    (if (>= idx (count group-keys))
      [new-keys mat-nodes]
      (let [gk (nth group-keys idx)]
        (if (keyword? gk)
          (recur (inc idx) (conj new-keys gk) mat-nodes)
          (let [cn (keyword (str "__gk_expr_" idx))]
            (recur (inc idx)
                   (conj new-keys cn)
                   (conj mat-nodes (ir/->PMaterializeExpr cn gk :float64 nil)))))))))

(defn- chain-materialize
  "Chain PMaterializeExpr nodes on top of an input node (innermost first)."
  [mat-nodes input]
  (reduce (fn [in mat] (assoc mat :input in)) input mat-nodes))

(defn expr-materialization
  "Insert PMaterializeExpr nodes for expressions in agg :expr fields
   and non-keyword group-keys. After this pass, all agg :expr fields
   are nil and all group-keys are plain keywords. This decouples expression
   evaluation from the physical strategy choice, enabling SIMD paths for
   expression aggs that previously fell to scalar."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (cond
        (instance? LGlobalAgg node)
        (if (some :expr (:aggs node))
          (let [[new-aggs mat-nodes] (rewrite-expr-aggs (:aggs node))]
            (assoc node
                   :aggs new-aggs
                   :input (chain-materialize mat-nodes (:input node))))
          node)

        (instance? LGroupBy node)
        (let [has-expr-aggs? (some :expr (:aggs node))
              has-expr-keys? (some #(not (keyword? %)) (:group-keys node))]
          (if (or has-expr-aggs? has-expr-keys?)
            (let [[new-aggs agg-mats] (if has-expr-aggs?
                                        (rewrite-expr-aggs (:aggs node))
                                        [(:aggs node) []])
                  [new-keys key-mats] (if has-expr-keys?
                                        (rewrite-expr-group-keys (:group-keys node))
                                        [(:group-keys node) []])
                  all-mats (into agg-mats key-mats)]
              (assoc node
                     :aggs new-aggs
                     :group-keys new-keys
                     :input (chain-materialize all-mats (:input node))))
            node))

        :else node))))

;; ============================================================================
;; Pass 4: Strategy selection (logical → physical)
;; ============================================================================

(defn- select-global-agg-strategy
  "Choose physical strategy for LGlobalAgg.

   Priority (mirrors current routing but as explicit decisions):
   1. Unfiltered COUNT → short-circuit to length
   2. Stats-only → O(chunks) from chunk statistics
   3. Chunked SIMD → stream index chunks
   4. Block-skip COUNT → skip blocks via min/max on arrays
   5. Fused SIMD → single agg on arrays
   6. Fused multi-sum → multiple SUM-like aggs in one pass
   7. Multi-agg global → group-by with 0 groups
   8. Percentile → two-pass or t-digest
   9. Scalar fallback"
  [node scan preds]
  (let [aggs       (:aggs node)
        n-aggs     (count aggs)
        all-idx?   (::all-indices (meta scan))
        columns    (:columns scan)
        length     (:length scan)
        first-agg  (first aggs)
        no-preds?  (empty? preds)
        no-expr?   (nil? (:expr first-agg))
        simd-ok?   (pred/simd-eligible? preds aggs columns length)
        ;; After expr-materialization, agg :col may reference temp columns
        ;; that don't exist in the scan. Stats-only and chunked paths need
        ;; real index-backed columns.
        agg-cols-in-scan? (every? #(or (= :count (:op %))
                                       (contains? columns (:col %)))
                                  aggs)]
    (cond
      ;; 1. Unfiltered COUNT
      (and (= 1 n-aggs) (= :count (:op first-agg)) no-preds?)
      (ir/->PFusedSIMDCount [] scan)

      ;; 2. Stats-only (needs chunk statistics → real columns only)
      (and (seq aggs) no-preds? all-idx? agg-cols-in-scan?
           (every? #(and (#{:sum :min :max :avg :count} (:op %))
                         (nil? (:expr %))) aggs))
      (ir/->PStatsOnlyAgg aggs scan)

      ;; 3. Chunked SIMD (single agg, index-backed → real columns only)
      (and all-idx? agg-cols-in-scan? (= 1 n-aggs) simd-ok? no-expr?)
      (if (= :count (:op first-agg))
        (ir/->PChunkedSIMDCount preds scan)
        (ir/->PChunkedSIMDAgg preds first-agg scan))

      ;; 4. Block-skip COUNT on arrays
      (and (= 1 n-aggs) (= :count (:op first-agg)) (seq preds)
           (not all-idx?) simd-ok? no-expr?)
      (ir/->PBlockSkipCount preds scan)

      ;; 5. Fused SIMD (single agg, arrays)
      (and (= 1 n-aggs) simd-ok? no-expr?)
      (ir/->PFusedSIMDAgg preds first-agg scan)

      ;; 6. Multi-sum
      (and (seq aggs)
           (pred/multi-agg-simd-eligible? preds aggs columns length)
           (every? #(#{:sum :sum-product :count :avg} (:op %)) aggs)
           (<= (count (filterv #(not= :count (:op %)) aggs)) 4))
      (ir/->PFusedMultiSum preds aggs scan)

      ;; 7. Multi-agg global via group-by with 0 groups
      (and (seq aggs)
           (pred/multi-agg-simd-eligible? preds aggs columns length))
      (ir/->PDenseGroupBy preds [] aggs 1 scan)

      ;; 8. Percentile
      (and (seq aggs)
           (every? #(#{:median :percentile :approx-quantile} (:op %)) aggs))
      (ir/->PPercentileAgg preds aggs scan)

      ;; 9. Scalar fallback
      :else
      (ir/->PScalarAgg preds aggs scan))))

(defn- select-group-by-strategy
  "Choose physical strategy for LGroupBy."
  [node scan preds]
  (let [{:keys [group-keys aggs]} node
        columns  (:columns scan)
        length   (:length scan)
        all-idx? (::all-indices (meta scan))]
    ;; Chunked dense (streaming, zero-copy) is tried first at execution time
    ;; with fallback to materialized dense/hash. The plan records the intent;
    ;; the executor handles the try/fallback since chunked eligibility depends
    ;; on runtime column state (null detection, chunk alignment).
    ;;
    ;; For the initial planner we emit a logical-level PChunkedDenseGroupBy
    ;; with fallback annotation. A more advanced version would compute
    ;; max-key here and choose dense vs hash definitively.
    (if all-idx?
      (vary-meta
       (ir/->PChunkedDenseGroupBy preds group-keys aggs 0 scan)
       assoc ::fallback :dense-or-hash)
      (ir/->PDenseGroupBy preds group-keys aggs 0 scan))))

(defn- select-join-strategy
  "Choose physical strategy for LJoin.

   Checks fused join+group+agg eligibility, bitmap semi-join eligibility,
   and falls back to hash join."
  [join-node parent-node]
  (let [{:keys [join-type on-pairs left right]} join-node]
    ;; Fused join strategies are checked by operator-fusion pass (pass 5),
    ;; since they require knowledge of the parent GroupBy/GlobalAgg node.
    ;; Here we choose between bitmap-semi and hash join.
    ;;
    ;; Bitmap semi-join: applicable when the join result is only used for
    ;; filtering (no dim columns in SELECT/AGG/GROUP). Detected by checking
    ;; if all referenced columns come from the left (fact) side.
    ;; For now, emit PHashJoin and let fusion upgrade it.
    (ir/->PHashJoin join-type on-pairs right left)))

(defn- peel-materialize
  "Peel off PMaterializeExpr nodes from an input chain.
   Returns [mat-chain inner-node] where mat-chain is innermost-first."
  [node]
  (loop [n node, chain []]
    (if (instance? PMaterializeExpr n)
      (recur (:input n) (conj chain n))
      [chain n])))

(defn- rechain-materialize
  "Re-insert PMaterializeExpr chain on top of a base input."
  [mat-chain base-input]
  (reduce (fn [in mat] (assoc mat :input in)) base-input mat-chain))

(defn- peel-filter-scan
  "Extract predicates and scan from a (possibly filter-wrapped) input."
  [input]
  (cond
    (instance? LFilter input)     [(:predicates input) (:input input)]
    (instance? PSIMDFilter input)  [(:predicates input) (:input input)]
    (instance? PMaskFilter input)  [(:predicates input) (:input input)]
    :else                          [[] input]))

(defn strategy-selection
  "Replace logical nodes with physical nodes based on data characteristics.

   Walks the plan bottom-up. Each logical node becomes a physical node
   (or stays logical if no strategy applies yet, e.g. post-processing)."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (cond
        ;; LScan → PScan or PChunkedScan (preserve annotations)
        (instance? LScan node)
        (let [m (meta node)]
          (if (::zone-map-eligible m)
            (with-meta (ir/->PChunkedScan (:columns node) (:length node) nil) m)
            (with-meta (ir/->PScan (:columns node) (:length node)) m)))

        ;; LGlobalAgg — peel through PMaterializeExpr to find filter/scan
        (instance? LGlobalAgg node)
        (let [[mat-chain inner] (peel-materialize (:input node))
              [preds scan] (peel-filter-scan inner)
              phys (select-global-agg-strategy node scan preds)]
          (if (seq mat-chain)
            (assoc phys :input (rechain-materialize mat-chain (:input phys)))
            phys))

        ;; LGroupBy — same peeling
        (instance? LGroupBy node)
        (let [[mat-chain inner] (peel-materialize (:input node))
              [preds scan] (peel-filter-scan inner)
              phys (select-group-by-strategy node scan preds)]
          (if (seq mat-chain)
            (assoc phys :input (rechain-materialize mat-chain (:input phys)))
            phys))

        ;; LJoin
        (instance? LJoin node)
        (select-join-strategy node nil)

        ;; LFilter without a parent agg (standalone filter, e.g. filter-only query)
        ;; Strategy selection for filter-under-agg is handled above.
        (instance? LFilter node)
        (let [preds (:predicates node)
              input (:input node)
              columns (when (or (instance? PScan input)
                                (instance? PChunkedScan input))
                        (:columns input))
              length  (when columns
                        (:length input))
              [simd non-simd] (if columns
                                (pred/split-preds preds columns)
                                [[] preds])]
          (cond
            ;; All SIMD-eligible
            (and (seq simd) (empty? non-simd))
            (ir/->PSIMDFilter preds nil input)

            ;; Mixed: mask for non-SIMD, SIMD for rest
            (seq non-simd)
            (let [mask-node (ir/->PMaskFilter non-simd nil input)]
              (if (seq simd)
                (ir/->PSIMDFilter simd nil mask-node)
                mask-node))

            :else node))

        ;; Post-processing: logical → physical 1:1
        (instance? LProject node)
        (ir/->PProject (:items node) (:input node))

        (instance? LWindow node)
        (ir/->PWindow (:specs node) (:input node))

        (instance? LHaving node)
        (ir/->PHaving (:predicates node) (:input node))

        (instance? LSort node)
        (ir/->PSort (:order-specs node) nil nil (:input node))

        (instance? LDistinct node)
        (ir/->PDistinct (:input node))

        (instance? LLimit node)
        (ir/->PLimit (:limit node) (:offset node) (:input node))

        :else node))))

;; ============================================================================
;; Pass 5: Operator fusion
;; ============================================================================

(defn- fused-join-group-agg?
  "Check if a join+group+agg subtree can fuse into a single Java pass.
   Requires: INNER join, all group keys from dim side, all agg cols from fact side,
   no predicates on either side, supported agg ops, no expression aggs."
  [group-node join-node]
  (let [supported #{:sum :count :min :max :avg :sum-product}
        aggs (:aggs group-node)
        group-keys (:group-keys group-node)
        preds (:predicates group-node)
        ;; Fused path can't handle pre-filtered probe side
        probe-is-scan? (or (instance? PScan (:probe-side join-node))
                           (instance? PChunkedScan (:probe-side join-node)))]
    (and (= :inner (:join-type join-node))
         (empty? preds)
         probe-is-scan?
         (seq group-keys)
         (every? keyword? group-keys)
         (seq aggs)
         (every? #(supported (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs))))

(defn- fused-join-global-agg?
  "Check if a join+global-agg subtree can fuse."
  [agg-node join-node]
  (let [supported #{:sum :count :min :max :avg :sum-product}
        aggs (:aggs agg-node)
        preds (:predicates agg-node)
        probe-is-scan? (or (instance? PScan (:probe-side join-node))
                           (instance? PChunkedScan (:probe-side join-node)))]
    (and (#{:inner :left} (:join-type join-node))
         (empty? (or preds []))
         probe-is-scan?
         (seq aggs)
         (every? #(supported (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs))))

(defn operator-fusion
  "Merge adjacent physical nodes into fused operators where beneficial.

   Patterns:
   - PDenseGroupBy/PHashGroupBy over PHashJoin → PFusedJoinGroupAgg
   - PFusedSIMDAgg/PScalarAgg over PHashJoin → PFusedJoinGlobalAgg
   - PFusedExtractCount from date-extract group-by + all-count aggs

   This pass discovers fusion opportunities that the cascading cond
   hard-coded as top-level eligibility checks."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (cond
        ;; Pattern 1: GroupBy over HashJoin → FusedJoinGroupAgg
        (and (or (instance? PDenseGroupBy node) (instance? PHashGroupBy node))
             (instance? PHashJoin (:input node)))
        (let [join (:input node)]
          (if (fused-join-group-agg? node join)
            (ir/->PFusedJoinGroupAgg
             {:on-pairs (:on-pairs join) :type (:join-type join)}
             (:group-keys node) (:aggs node)
             (:probe-side join)   ;; left = fact
             (:build-side join))  ;; right = dim
            node))

        ;; Pattern 2: GlobalAgg over HashJoin → FusedJoinGlobalAgg
        (and (or (instance? PFusedSIMDAgg node) (instance? PScalarAgg node)
                 (instance? PFusedMultiSum node))
             (instance? PHashJoin (:input node)))
        (let [join (:input node)
              aggs (or (:aggs node) (when (:agg node) [(:agg node)]))]
          (if (fused-join-global-agg?
               {:aggs aggs :predicates (:predicates node)} join)
            (ir/->PFusedJoinGlobalAgg
             {:on-pairs (:on-pairs join) :type (:join-type join)}
             aggs
             (:probe-side join)
             (:build-side join))
            node))

        :else node))))

;; ============================================================================
;; Composite: full optimization pipeline
;; ============================================================================

(defn optimize
  "Run all optimization passes on a logical plan, producing a physical plan."
  [plan]
  (-> plan
      annotate
      predicate-pushdown
      zone-map-annotation
      expr-materialization
      strategy-selection
      operator-fusion))

;; ============================================================================
;; Plan explanation (for debugging and EXPLAIN output)
;; ============================================================================

(defn- node-name [node]
  (let [cls (.getSimpleName (class node))]
    cls))

(defn- node-detail [node]
  (cond
    (instance? PStatsOnlyAgg node)
    (str "aggs=" (mapv :op (:aggs node)))

    (instance? PFusedSIMDAgg node)
    (str "agg=" (:op (:agg node)) " preds=" (count (:predicates node)))

    (instance? PChunkedDenseGroupBy node)
    (str "groups=" (:group-keys node) " aggs=" (mapv :op (:aggs node)))

    (instance? PDenseGroupBy node)
    (str "groups=" (:group-keys node) " aggs=" (mapv :op (:aggs node))
         " max-key=" (:max-key node))

    (instance? PHashGroupBy node)
    (str "groups=" (:group-keys node) " aggs=" (mapv :op (:aggs node)))

    (instance? PScan node)
    (str "cols=" (vec (keys (:columns node))) " len=" (:length node))

    (instance? PChunkedScan node)
    (str "cols=" (vec (keys (:columns node))) " len=" (:length node)
         " zone-pruned=" (some? (:surviving-chunks node)))

    (instance? PMaterializeExpr node)
    (str "col=" (:col-name node) " expr=" (:expr node))

    :else ""))

(defn explain
  "Return a human-readable string describing the physical plan tree.

   (println (explain (optimize (build-logical-plan query))))"
  ([plan] (explain plan 0))
  ([plan depth]
   (let [indent (apply str (repeat (* 2 depth) \space))
         line   (str indent (node-name plan)
                     (let [d (node-detail plan)]
                       (when (seq d) (str "  " d))))
         children (cond
                    (instance? LJoin plan)
                    [(str indent "  left:\n" (explain (:left plan) (+ depth 2)))
                     (str indent "  right:\n" (explain (:right plan) (+ depth 2)))]

                    (instance? PHashJoin plan)
                    [(str indent "  build:\n" (explain (:build-side plan) (+ depth 2)))
                     (str indent "  probe:\n" (explain (:probe-side plan) (+ depth 2)))]

                    (ir/input-node plan)
                    [(explain (ir/input-node plan) (inc depth))]

                    :else [])]
     (clojure.string/join "\n" (cons line children)))))
