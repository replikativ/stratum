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
            [stratum.query.estimate :as est]
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
  "Set of column keywords referenced by a normalized predicate (recursive)."
  [pred]
  (let [op (second pred)]
    (case op
      :or (into #{} (mapcat pred-columns) (subvec pred 2))
      (:in :not-in :fn) (let [c (first pred)] (if (keyword? c) #{c} #{}))
      (let [col (first pred)]
        (if (map? col)
          ;; Expression predicate like [:> {:op :mul :args [:a :b]} 1000]
          (into #{} (filter keyword?) (:args col))
          (if (keyword? col) #{col} #{}))))))

(defn- classify-pred-by-side
  "Classify a predicate as :left, :right, or :cross based on which join side
   its columns belong to."
  [pred left-cols right-cols]
  (let [cols (pred-columns pred)]
    (cond
      (empty? cols)                       :cross  ;; can't determine → keep above
      (every? #(contains? left-cols %) cols)  :left
      (every? #(contains? right-cols %) cols) :right
      :else                                   :cross)))

(defn- scan-columns
  "Get the set of column keywords reachable from a subtree.
   Recurses through LFilter and LJoin so that predicate pushdown works
   through chained joins (e.g. fact → join1 → join2)."
  [node]
  (cond
    (instance? LScan node)   (set (keys (:columns node)))
    (instance? LFilter node) (recur (:input node))
    (instance? LJoin node)   (let [l (scan-columns (:left node))
                                   r (scan-columns (:right node))]
                               (when (and l r)
                                 (into l r)))
    :else nil))

(defn- pushdown-once
  "Single pass: push LFilter predicates below LJoin when they reference only one side."
  [plan]
  (ir/walk-plan plan
                (fn [node]
                  (if (and (instance? LFilter node)
                           (instance? LJoin (:input node)))
                    (let [join (:input node)
                          left-cols  (scan-columns (:left join))
                          right-cols (scan-columns (:right join))]
                      (if (and left-cols right-cols)
                        (let [classified (group-by #(classify-pred-by-side % left-cols right-cols)
                                                   (:predicates node))
                              left-preds  (vec (get classified :left []))
                              right-preds (vec (get classified :right []))
                              cross-preds (vec (get classified :cross []))
                  ;; Wrap join children with pushed-down filters
                              new-left  (if (seq left-preds)
                                          (ir/->LFilter left-preds (:left join))
                                          (:left join))
                              new-right (if (seq right-preds)
                                          (ir/->LFilter right-preds (:right join))
                                          (:right join))
                              new-join (assoc join :left new-left :right new-right)]
              ;; Keep cross-side predicates above the join (or remove filter if all pushed)
                          (if (seq cross-preds)
                            (ir/->LFilter cross-preds new-join)
                            new-join))
            ;; Can't determine sides → leave as-is
                        node))
                    node))))

(defn predicate-pushdown
  "Push LFilter predicates below LJoin when they reference only one side.

   Left-only predicates → wrap join's left (probe) child in LFilter.
   Right-only predicates → wrap join's right (build) child in LFilter.
   Cross-side predicates stay above the join.

   Runs to fixpoint so that predicates pushed to a join's left child
   (which may itself be a join) get pushed further down on the next pass.
   Bounded by join depth (typically 2-3)."
  [plan]
  (loop [p plan, n 0]
    (let [p' (pushdown-once p)]
      (if (or (= p p') (>= n 10))
        p'
        (recur p' (inc n))))))

;; ============================================================================
;; Pass 1.5: Join reordering (DP-based)
;; ============================================================================

(defn- extract-join-chain
  "Extract a left-deep chain of LJoin nodes into [fact-node, dims-vec].
   dims-vec is in join order (innermost = first joined)."
  [node]
  (loop [n node, dims-rev []]
    (if (instance? LJoin n)
      (recur (:left n)
             (conj dims-rev {:node (:right n)
                             :on-pairs (:on-pairs n)
                             :join-type (:join-type n)}))
      [n (vec (rseq dims-rev))])))

(defn- estimate-node-rows
  "Estimate the output row count of a logical plan node."
  ^long [node]
  (cond
    (instance? LScan node)
    (long (:length node))

    (instance? LFilter node)
    (let [child (:input node)]
      (if (instance? LScan child)
        (est/estimate-output-rows (:predicates node) (:columns child) (:length child))
        (long (or (:length child) 1000000))))

    :else
    (long (or (:length node) 1000000))))

(defn- dim-join-selectivity
  "Estimate what fraction of fact rows survive joining with a dim.
   For FK→PK: selectivity ≈ filtered_dim_rows / total_dim_rows."
  ^double [{:keys [node]}]
  (let [total (double
               (cond
                 (instance? LFilter node)
                 (let [inner (:input node)]
                   (if (instance? LScan inner) (:length inner) (estimate-node-rows node)))
                 (instance? LScan node) (:length node)
                 :else (estimate-node-rows node)))
        filtered (double (estimate-node-rows node))]
    (if (zero? total) 1.0 (min 1.0 (/ filtered total)))))

(defn- compute-join-deps
  "For each dim, compute the set of dim indices it depends on.
   Dim j depends on dim i if j's probe-side join key comes from dim i (not from fact).
   This handles snowflake schemas where dim2 joins on a column from dim1."
  [fact-cols dims]
  (let [n (count dims)
        dim-cols (mapv (fn [d] (or (scan-columns (:node d)) #{})) dims)]
    (mapv (fn [j]
            (let [probe-keys (set (map first (:on-pairs (nth dims j))))]
              (into #{}
                    (keep (fn [i]
                            (when (and (not= i j)
                                       (some (fn [k]
                                               (and (not (contains? fact-cols k))
                                                    (contains? (nth dim-cols i) k)))
                                             probe-keys))
                              i)))
                    (range n))))
          (range n))))

(defn- dp-join-order
  "DP join ordering for dim tables. Returns optimal index ordering.
   Minimizes total probe cost = sum of rows entering each join step.
   Respects column dependencies (snowflake schemas)."
  [^long fact-rows fact-cols dims]
  (let [n (count dims)
        sels (mapv dim-join-selectivity dims)
        deps (compute-join-deps fact-cols dims)]
    (if (<= n 1)
      (vec (range n))
      (let [full (dec (bit-shift-left 1 n))
            dp   (object-array (inc full))]
        (aset dp 0 {:cost 0.0 :order [] :rows (double fact-rows)})
        (doseq [mask (range 1 (inc full))]
          (let [best (reduce
                       (fn [best i]
                         (if (zero? (bit-and mask (bit-shift-left 1 i)))
                           best
                           (let [prev-mask (bit-xor mask (bit-shift-left 1 i))
                                 i-deps (nth deps i)]
                             ;; Check deps: all dependencies must be in prev-mask
                             (if (not (every? #(pos? (bit-and prev-mask (bit-shift-left 1 %))) i-deps))
                               best
                               (let [prev (aget dp (int prev-mask))]
                                 (when prev
                                   (let [new-cost (+ (double (:cost prev)) (double (:rows prev)))
                                         new-rows (* (double (:rows prev)) (double (nth sels i)))]
                                     (if (or (nil? best) (< new-cost (double (:cost best))))
                                       {:cost new-cost :order (conj (:order prev) i) :rows new-rows}
                                       best))))))))
                       nil
                       (range n))]
            (aset dp (int mask) best)))
        (or (:order (aget dp (int full)))
            (vec (range n)))))))

(defn- rebuild-join-chain
  "Rebuild a left-deep join tree from fact-node and reordered dims."
  [fact-node dims order]
  (reduce
    (fn [left idx]
      (let [{:keys [node on-pairs join-type]} (nth dims idx)]
        (ir/->LJoin join-type on-pairs left node)))
    fact-node
    order))

(defn join-reorder
  "Reorder left-deep INNER join chains for optimal execution cost.
   Uses DP to minimize total probe cost. Puts most selective dims first.
   Respects column dependencies for snowflake schemas.
   Only reorders chains of all-INNER joins (outer joins stay in place)."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (if (instance? LJoin node)
        (let [[fact-node dims] (extract-join-chain node)]
          (if-let [fact-cols (and (>= (count dims) 2)
                                  (every? #(= :inner (:join-type %)) dims)
                                  (scan-columns fact-node))]
            (let [order (dp-join-order (estimate-node-rows fact-node) fact-cols dims)]
              (if (= order (vec (range (count dims))))
                node
                (rebuild-join-chain fact-node dims order)))
            node))
        node))))

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

   Uses selectivity estimates to make cost-aware decisions:
   - Very selective preds (< 5%) → prefer block-skip COUNT over full SIMD
   - Low selectivity index scans → prefer chunked SIMD (skip whole chunks)
   - Otherwise follows capability-based priority cascade.

   Priority (capability gated, cost-informed):
   1. Unfiltered COUNT → short-circuit to length
   2. Stats-only → O(chunks) from chunk statistics
   3. Chunked SIMD → stream index chunks (favored for selective preds)
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
                                  aggs)
        ;; Cost-aware: estimate predicate selectivity
        selectivity (when (seq preds)
                      (est/estimate-combined-selectivity preds columns))
        est-rows    (when selectivity
                      (est/estimate-output-rows preds columns length))
        very-selective? (and selectivity (< (double selectivity) 0.05))]
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
      ;; Cost-aware: chunked SIMD is especially good for selective preds on
      ;; index columns because zone maps can skip entire chunks.
      (and all-idx? agg-cols-in-scan? (= 1 n-aggs) simd-ok? no-expr?)
      (if (= :count (:op first-agg))
        (ir/->PChunkedSIMDCount preds scan)
        (ir/->PChunkedSIMDAgg preds first-agg scan))

      ;; 4. Block-skip COUNT on arrays
      ;; Cost-aware: prefer block-skip when selectivity < 5% — can skip entire
      ;; blocks using min/max stats on materialized arrays.
      (and (= 1 n-aggs) (= :count (:op first-agg)) (seq preds)
           (not all-idx?) simd-ok? no-expr?)
      (ir/->PBlockSkipCount preds scan)

      ;; 4b. Cost-aware: for very selective non-COUNT single-agg on arrays,
      ;; block-skip then SIMD on survivors would be faster than full SIMD.
      ;; We don't have a fused block-skip+agg path yet, so this is a future
      ;; optimization. For now, fall through to full SIMD.

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
  "Choose physical strategy for LGroupBy.

   Cost-aware decisions:
   - Estimated output rows inform dense-vs-hash: if predicates are very
     selective, fewer rows enter the group-by → dense array more likely viable.
   - For index-backed columns, chunked dense is preferred (zero-copy streaming)
     with fallback to materialized dense/hash at execution time."
  [node scan preds]
  (let [{:keys [group-keys aggs]} node
        columns  (:columns scan)
        length   (:length scan)
        all-idx? (::all-indices (meta scan))
        ;; Cost-aware: estimate rows entering the group-by after predicates
        selectivity (when (seq preds)
                      (est/estimate-combined-selectivity preds columns))
        est-rows    (if selectivity
                      (est/estimate-output-rows preds columns length)
                      length)]
    (if all-idx?
      (vary-meta
       (ir/->PChunkedDenseGroupBy preds group-keys aggs 0 scan)
       assoc ::fallback :dense-or-hash
       ::estimated-rows est-rows
       ::selectivity (or selectivity 1.0))
      (vary-meta
       (ir/->PDenseGroupBy preds group-keys aggs 0 scan)
       assoc ::estimated-rows est-rows
       ::selectivity (or selectivity 1.0)))))

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

(defn- scan-or-filter-scan?
  "True if node is a PScan/PChunkedScan, or a chain of filters wrapping one.
   Recurses through PSIMDFilter/PMaskFilter chains (e.g. mixed SIMD + non-SIMD
   preds produce PSIMDFilter → PMaskFilter → PScan)."
  [node]
  (cond
    (or (instance? PScan node)
        (instance? PChunkedScan node))
    true

    (or (instance? PSIMDFilter node)
        (instance? PMaskFilter node))
    (recur (:input node))

    :else false))

(defn- extract-scan-cols
  "Walk through filter chains to the underlying scan and return its column key set."
  [node]
  (cond
    (or (instance? PScan node) (instance? PChunkedScan node))
    (set (keys (:columns node)))

    (or (instance? PSIMDFilter node) (instance? PMaskFilter node))
    (recur (:input node))

    :else nil))

(defn- fused-join-group-agg?
  "Check if a join+group+agg subtree can fuse into a single Java pass.
   Requires: INNER join, all group keys from dim side, all agg cols from fact side,
   supported agg ops, no expression aggs. Filter-wrapped scans on either side are
   allowed — the executor realizes pushed-down predicates as masks."
  [group-node join-node]
  (let [supported #{:sum :count :min :max :avg :sum-product}
        aggs (:aggs group-node)
        group-keys (:group-keys group-node)
        preds (:predicates group-node)
        probe-ok? (scan-or-filter-scan? (:probe-side join-node))
        build-cols (extract-scan-cols (:build-side join-node))]
    (and (= :inner (:join-type join-node))
         (empty? preds)
         probe-ok?
         (seq group-keys)
         (every? keyword? group-keys)
         ;; Group keys must all come from the build (dim) side
         build-cols
         (every? build-cols group-keys)
         (seq aggs)
         (every? #(supported (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs))))

(defn- fused-join-global-agg?
  "Check if a join+global-agg subtree can fuse.
   Filter-wrapped scans on either side are allowed — the executor realizes
   pushed-down predicates as masks."
  [agg-node join-node]
  (let [supported #{:sum :count :min :max :avg :sum-product}
        aggs (:aggs agg-node)
        preds (:predicates agg-node)
        probe-ok? (scan-or-filter-scan? (:probe-side join-node))]
    (and (#{:inner :left} (:join-type join-node))
         (empty? (or preds []))
         probe-ok?
         (seq aggs)
         (every? #(supported (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs))))

(defn- node-referenced-cols
  "Collect the set of column keywords referenced by a node (not recursing into children).
   Used to determine if a join's build-side columns are needed by the parent."
  [node]
  (cond
    (instance? PProject node)
    (into #{} (keep (fn [item]
                      (or (:ref item)
                          (when-let [e (:expr item)]
                            nil)  ;; expression items may reference multiple cols
                          (:name item))))
          (:items node))

    (or (instance? PDenseGroupBy node) (instance? PHashGroupBy node)
        (instance? PChunkedDenseGroupBy node))
    (into (set (:group-keys node))
          (keep :col (:aggs node)))

    (or (instance? PFusedSIMDAgg node) (instance? PFusedSIMDCount node)
        (instance? PScalarAgg node) (instance? PFusedMultiSum node))
    (into #{} (keep :col (or (:aggs node)
                             (when-let [a (:agg node)] [a]))))

    :else nil))  ;; nil = unknown, don't optimize

(defn- bitmap-semi-join-eligible?
  "Check if a PHashJoin can be replaced with PBitmapSemiJoin.
   Requires:
   - INNER join
   - Single-column join key
   - Parent node doesn't reference any build-side (dim) columns except the join key"
  [join-node parent-cols]
  (and parent-cols
       (= :inner (:join-type join-node))
       (= 1 (count (:on-pairs join-node)))
       (let [build-cols (extract-scan-cols (:build-side join-node))
             join-key-right (second (first (:on-pairs join-node)))]
         (and build-cols
              ;; No build-side columns (other than join key) used by parent
              (empty? (disj (clojure.set/intersection build-cols parent-cols)
                            join-key-right))))))

(defn operator-fusion
  "Merge adjacent physical nodes into fused operators where beneficial.

   Patterns:
   - PDenseGroupBy/PHashGroupBy over PHashJoin → PFusedJoinGroupAgg
   - PFusedSIMDAgg/PScalarAgg over PHashJoin → PFusedJoinGlobalAgg
   - Any node over PHashJoin where dim cols unused → PBitmapSemiJoin

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
            ;; Fused join failed — try bitmap semi-join
                        (let [parent-cols (node-referenced-cols node)]
                          (if (bitmap-semi-join-eligible? join parent-cols)
                            (assoc node :input
                                   (ir/->PBitmapSemiJoin
                                    {:on-pairs (:on-pairs join) :type (:join-type join)
                                     :build-side (:build-side join)}
                                    (:probe-side join)))
                            node))))

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
            ;; Fused join failed — try bitmap semi-join
                        (let [parent-cols (node-referenced-cols node)]
                          (if (bitmap-semi-join-eligible? join parent-cols)
                            (assoc node :input
                                   (ir/->PBitmapSemiJoin
                                    {:on-pairs (:on-pairs join) :type (:join-type join)
                                     :build-side (:build-side join)}
                                    (:probe-side join)))
                            node))))

        ;; Pattern 3: Project over HashJoin → try bitmap semi-join
                    (and (instance? PProject node)
                         (instance? PHashJoin (:input node)))
                    (let [join (:input node)
                          parent-cols (node-referenced-cols node)]
                      (if (bitmap-semi-join-eligible? join parent-cols)
                        (assoc node :input
                               (ir/->PBitmapSemiJoin
                                {:on-pairs (:on-pairs join) :type (:join-type join)
                                 :build-side (:build-side join)}
                                (:probe-side join)))
                        node))

                    :else node))))

;; ============================================================================
;; Pass 6: Statistics propagation
;; ============================================================================

(defn- propagate-est-rows
  "Estimate output rows for a physical node based on its children's estimates."
  ^long [node]
  (cond
    ;; Leaf scans
    (or (instance? PScan node) (instance? PChunkedScan node))
    (long (:length node))

    ;; Filters reduce rows
    (or (instance? PSIMDFilter node) (instance? PMaskFilter node))
    (let [child-rows (long (or (::estimated-rows (meta (:input node)))
                               (:length (:input node))
                               1000000))
          columns (when-let [inp (:input node)]
                    (:columns inp))
          preds (:predicates node)]
      (if (and columns (seq preds))
        (est/estimate-output-rows preds columns child-rows)
        child-rows))

    ;; Joins: FK→PK heuristic
    (instance? PHashJoin node)
    (let [probe-rows (long (or (::estimated-rows (meta (:probe-side node)))
                               (:length (:probe-side node))
                               1000000))
          build-total (long (or (:length (:build-side node)) 1000000))
          build-rows (long (or (::estimated-rows (meta (:build-side node)))
                               build-total))
          sel (min 1.0 (/ (double build-rows) (max 1.0 (double build-total))))]
      (max 1 (long (* probe-rows sel))))

    ;; Global agg always produces 1 row
    (or (instance? PStatsOnlyAgg node)
        (instance? PFusedSIMDAgg node) (instance? PFusedSIMDCount node)
        (instance? PChunkedSIMDAgg node) (instance? PChunkedSIMDCount node)
        (instance? PBlockSkipCount node) (instance? PFusedMultiSum node)
        (instance? PPercentileAgg node) (instance? PScalarAgg node)
        (instance? PFusedJoinGlobalAgg node))
    1

    ;; Fused join+group: use metadata if present
    (instance? PFusedJoinGroupAgg node)
    (long (or (::estimated-rows (meta node)) 1000))

    ;; Pass-through: inherit from child
    :else
    (long (or (::estimated-rows (meta (ir/input-node node)))
              (when-let [c (ir/input-node node)] (:length c))
              1000000))))

(defn stats-propagation
  "Propagate estimated row counts through the physical plan tree.
   Attaches ::estimated-rows metadata to each node."
  [plan]
  (ir/walk-plan plan
    (fn [node]
      (let [est (propagate-est-rows node)]
        (vary-meta node assoc ::estimated-rows est)))))

;; ============================================================================
;; Pass 7: Column pruning
;; ============================================================================

(defn- collect-all-refs
  "Collect all column keywords referenced anywhere in the plan tree."
  [plan]
  (let [refs (volatile! (transient #{}))]
    (ir/walk-plan plan
      (fn [node]
        ;; Predicates
        (when-let [preds (:predicates node)]
          (doseq [p preds
                  c (pred-columns p)]
            (vswap! refs conj! c)))
        ;; Group keys
        (when-let [gks (:group-keys node)]
          (doseq [gk gks] (when (keyword? gk) (vswap! refs conj! gk))))
        ;; Aggs
        (when-let [aggs (:aggs node)]
          (doseq [a aggs]
            (when-let [c (:col a)] (vswap! refs conj! c))
            (when-let [cs (:cols a)] (doseq [c cs] (when (keyword? c) (vswap! refs conj! c))))))
        (when-let [agg (:agg node)]
          (when-let [c (:col agg)] (vswap! refs conj! c)))
        ;; Project items
        (when-let [items (:items node)]
          (doseq [item items]
            (when-let [r (:ref item)] (vswap! refs conj! r))
            (when-let [n (:name item)] (vswap! refs conj! n))))
        ;; Join on-pairs
        (when-let [on (:on-pairs node)]
          (doseq [[l r] on] (vswap! refs conj! l) (vswap! refs conj! r)))
        (when-let [js (:join-spec node)]
          (doseq [[l r] (:on-pairs js)] (vswap! refs conj! l) (vswap! refs conj! r)))
        ;; Window specs
        (when-let [specs (:specs node)]
          (doseq [s specs]
            (when-let [c (:col s)] (vswap! refs conj! c))
            (when-let [pb (:partition-by s)] (doseq [c pb] (vswap! refs conj! c)))
            (when-let [ob (:order-by s)] (doseq [[c _] ob] (vswap! refs conj! c)))))
        ;; Sort
        (when-let [os (:order-specs node)]
          (doseq [[c _] os] (vswap! refs conj! c)))
        ;; Extract
        (when-let [ec (:extract-col node)]
          (vswap! refs conj! ec))
        ;; Materialized expressions
        (when-let [e (:expr node)]
          (when (map? e)
            (doseq [a (:args e)] (when (keyword? a) (vswap! refs conj! a)))))
        node))
    (persistent! @refs)))

(defn column-pruning
  "Remove unreferenced columns from scan nodes to reduce memory and I/O."
  [plan]
  (let [all-refs (collect-all-refs plan)]
    (ir/walk-plan plan
      (fn [node]
        (if (or (instance? PScan node) (instance? PChunkedScan node))
          (let [cols (:columns node)
                pruned (select-keys cols all-refs)]
            (if (< (count pruned) (count cols))
              (assoc node :columns pruned)
              node))
          node)))))

;; ============================================================================
;; Composite: full optimization pipeline
;; ============================================================================

(defn optimize
  "Run all optimization passes on a logical plan, producing a physical plan."
  [plan]
  (-> plan
      annotate
      predicate-pushdown
      join-reorder
      zone-map-annotation
      expr-materialization
      strategy-selection
      operator-fusion
      stats-propagation
      column-pruning))

;; ============================================================================
;; Plan explanation (for debugging and EXPLAIN output)
;; ============================================================================

(defn- node-name [node]
  (let [cls (.getSimpleName (class node))]
    cls))

(defn- est-detail
  "Append selectivity/estimated-rows from node metadata if present."
  [node base]
  (let [m (meta node)
        sel (::selectivity m)
        est (::estimated-rows m)]
    (cond-> base
      sel (str " sel=" (format "%.3f" (double sel)))
      est (str " est-rows=" est))))

(defn- node-detail [node]
  (est-detail node
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

                (instance? PFusedJoinGroupAgg node)
                (str "groups=" (:group-keys node) " aggs=" (mapv :op (:aggs node))
                     " join=" (:join-spec node))

                (instance? PFusedJoinGlobalAgg node)
                (str "aggs=" (mapv :op (:aggs node)) " join=" (:join-spec node))

                (instance? PMaterializeExpr node)
                (str "col=" (:col-name node) " expr=" (:expr node))

                :else "")))

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

                    (instance? PFusedJoinGroupAgg plan)
                    [(str indent "  fact:\n" (explain (:left plan) (+ depth 2)))
                     (str indent "  dim:\n" (explain (:right plan) (+ depth 2)))]

                    (instance? PFusedJoinGlobalAgg plan)
                    [(str indent "  fact:\n" (explain (:left plan) (+ depth 2)))
                     (str indent "  dim:\n" (explain (:right plan) (+ depth 2)))]

                    (instance? PBitmapSemiJoin plan)
                    (let [bs (get-in plan [:join-spec :build-side])]
                      (cond-> [(explain (:input plan) (inc depth))]
                        bs (conj (str indent "  dim:\n" (explain bs (+ depth 2))))))

                    (ir/input-node plan)
                    [(explain (ir/input-node plan) (inc depth))]

                    :else [])]
     (clojure.string/join "\n" (cons line children)))))
