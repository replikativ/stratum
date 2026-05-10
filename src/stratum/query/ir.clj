(ns stratum.query.ir
  "Intermediate representations for the query planner.

   Two layers:
     Logical IR  — describes WHAT the query computes (scan, filter, group, join …).
                   Built directly from the normalized query map.
     Physical IR — describes HOW to execute each logical op (SIMD, chunked, hash …).
                   Produced by optimization passes that inspect data characteristics.

   Both are plain defrecords with an :input (or :left/:right) field pointing
   at the child node, forming a tree.  Optimization passes walk this tree
   bottom-up, replacing logical nodes with physical ones.")

;; ============================================================================
;; Logical IR — query semantics, no execution decisions
;; ============================================================================

(defrecord LScan
  ;; Leaf node: columnar data source.
  ;; columns — {kw -> {:type :int64/:float64 :data array :index idx :dict str[]}}
  ;; length  — row count (long)
           [columns length])

(defrecord LFilter
  ;; Row-level predicates (AND-combined).
  ;; predicates — vec of normalized preds: [col-kw op & args]
           [predicates input])

(defrecord LJoin
  ;; Equi-join between left (fact) and right (dimension).
  ;; join-type — :inner | :left | :right | :full
  ;; on-pairs  — [[left-col right-col] ...]
  ;; right     — LScan of dimension columns
           [join-type on-pairs left right])

(defrecord LAsofJoin
  ;; ASOF join: equality keys partition the search; the inequality column
  ;; picks the closest match per probe row.
  ;; join-type        — :inner | :left
  ;; on-pairs         — [[left-col right-col] ...]   (zero or more equality preds)
  ;; match-condition  — [op left-col right-col]      (op ∈ #{:>= :> :<= :<})
  ;; left/right       — child plan nodes
           [join-type on-pairs match-condition left right])

(defrecord LGroupBy
  ;; Group-by aggregation.
  ;; group-keys — vec of keywords or normalized exprs
  ;; aggs       — vec of normalized aggs {:op :col :cols :expr :as}
           [group-keys aggs input])

(defrecord LGlobalAgg
  ;; Aggregation without grouping.
  ;; aggs — vec of normalized aggs
           [aggs input])

(defrecord LProject
  ;; Column projection (SELECT without aggregation).
  ;; items — vec of {:name kw :ref kw :expr normalized-expr}
           [items input])

(defrecord LWindow
  ;; Window functions applied to input rows or grouped output.
  ;; specs — vec of {:op :col :as :partition-by :order-by}
           [specs input])

(defrecord LHaving
  ;; Post-aggregation filter.
  ;; predicates — vec of normalized preds on agg aliases
           [predicates input])

(defrecord LDistinct [input])

(defrecord LSort
  ;; order-specs — [[col-kw :asc/:desc] ...]
           [order-specs input])

(defrecord LLimit [limit offset input])

(defrecord LTopN
  ;; Streaming `ORDER BY col [DESC] [, col2 [DESC] ...] LIMIT N` over
  ;; one or more numeric columns. The optimizer recognizes the
  ;; LLimit-over-LSort shape and rewrites to this; the executor
  ;; delegates to `stratum.query.top-n/execute-top-n`, which keeps a
  ;; fixed-size heap and fetches only the surviving rows. `select`
  ;; is the (already normalized) projection (or nil for SELECT *)
  ;; so the rewrite can happen below the LProject layer.
  ;;
  ;; `order-specs` is a vec of `[col dir]` pairs (or bare `col` for
  ;; ASC). The heap walks them in declared order; mixed
  ;; `:asc`/`:desc` is supported. Cross-column comparison uses the
  ;; same `key-double` cast as the single-key path (loses precision
  ;; only for int64 values >2^53, which doesn't occur in typical
  ;; timestamps or dict-IDs).
           [order-specs limit select input])

(defrecord LHead
  ;; LIMIT N without ORDER BY — return the first N rows in scan
  ;; order. The optimizer recognizes the LLimit-over-LScan
  ;; (optionally wrapped in LProject) shape and rewrites to this;
  ;; the executor materializes only the first N rows of each scan
  ;; column via `cols/take-prefix-column`, so a `SELECT * LIMIT 3`
  ;; over a multi-GB index touches a single chunk per column instead
  ;; of decoding the entire dataset. `select` is the (already
  ;; normalized) projection (or nil for SELECT *), mirroring
  ;; `LTopN`.
           [limit select input])

(defrecord LSetOp
  ;; Union/Intersect/Except over sub-queries.
  ;; op      — :union | :intersect | :except
  ;; queries — vec of sub-query maps (each built into its own plan)
  ;; all?    — boolean (UNION ALL)
           [op queries all?])

(defrecord LAnomaly
  ;; Materialize iforest-derived synthetic columns post-join.
  ;; expr->col — map from `[:anomaly-* model …]` expression vector
  ;;             to the synthetic column keyword the surrounding
  ;;             query has been rewritten to reference.
  ;; models    — `{model-name → trained-model}` carried through to
  ;;             the executor (anomaly is an enrichment, so models
  ;;             aren't available via column metadata).
  ;; input     — child plan whose output column ctx the executor
  ;;             scores against.
           [expr->col models input])

(defrecord LStringMaterialize
  ;; Evaluate string-producing expressions (`:upper`, `:lower`,
  ;; `:concat`, `:trim`, …) against the post-join column ctx and
  ;; inject the dict-encoded results as synthetic columns. The
  ;; surrounding query has already been rewritten so `:group` /
  ;; aggs / `:select` reference the synthetic column keywords.
  ;; items — vec of `{:col-name :expr}` pairs (col-name is a
  ;;         `__str_expr_N` keyword; expr is a normalized
  ;;         expression map).
           [items input])

;; ============================================================================
;; Annotations — attached to logical nodes by analysis passes
;; ============================================================================
;;
;; Rather than baking analysis results into the records we use metadata:
;;
;;   (vary-meta node assoc
;;     ::col-types    {:price :float64 :qty :int64}
;;     ::index-cols   #{:shipdate :orderkey}
;;     ::cardinality  {:region 5 :nation 25}
;;     ::has-nulls    #{:discount})
;;
;; Passes read these annotations to make strategy decisions.

;; ============================================================================
;; Physical IR — concrete execution strategy per node
;; ============================================================================
;;
;; Each physical record replaces one (or several fused) logical nodes.
;; The executor dispatches on record type — no routing cond needed.

;; --- Scan strategies --------------------------------------------------------

(defrecord PScan
  ;; Materialized scan: all columns as arrays.
  ;; predicates       — vec of normalized scan-attached preds. Folded
  ;;                    in by `lift-filters-to-scan` from `PSIMDFilter`
  ;;                    / `PMaskFilter` wrappers so every scan-adjacent
  ;;                    filter has a single attachment point.
  ;; dynamic-filters  — `volatile!` of a vec of preds attached at
  ;;                    execution time (e.g. `execute-hash-join`
  ;;                    derives a build-key range and pushes it onto
  ;;                    the probe scan, mirroring DuckDB's
  ;;                    `DynamicTableFilterSet`). Default `nil`.
           [columns length predicates dynamic-filters])

(defrecord PChunkedScan
  ;; Streaming scan: columns stay as PersistentColumnIndex, processed per-chunk.
  ;; surviving-chunks — nil (all) or vec of chunk indices after zone-map pruning.
  ;; predicates       — same role as on `PScan`; the executor merges
  ;;                    them into the chunk-level zone-map evaluation.
  ;; dynamic-filters  — same as on `PScan`; `volatile!`-mutable.
           [columns length surviving-chunks predicates dynamic-filters])

;; --- Filter strategies ------------------------------------------------------

(defrecord PSIMDFilter
  ;; SIMD-eligible predicates compiled to Java fused filter.
  ;; prep — {:n-long :long-pred-types :long-cols :long-lo :long-hi
  ;;          :n-dbl  :dbl-pred-types  :dbl-cols  :dbl-lo  :dbl-hi}
           [predicates prep input])

(defrecord PMaskFilter
  ;; Non-SIMD predicates compiled to a long[] mask column via eval.
  ;; mask-fn — fn(length) -> long[]
           [predicates mask-fn input])

;; --- Global aggregation strategies ------------------------------------------

(defrecord PStatsOnlyAgg
  ;; O(chunks) aggregation from chunk-level statistics. No materialization.
  ;; Requires: all columns from indices, simple aggs.
  ;; predicates — vec of preds. When non-empty, the strategy picker
  ;;              has verified every chunk classifies via zone maps as
  ;;              `:stats-only` or `:skip` (no partial chunks); the
  ;;              executor accumulates stats only for surviving chunks.
           [predicates aggs input])

(defrecord PFusedSIMDAgg
  ;; Single fused SIMD filter+aggregate on materialized arrays.
  ;; Targets: ColumnOps/fusedSimdParallel, fusedFilterAggregate
           [predicates agg input])

(defrecord PFusedSIMDCount
  ;; JIT-isolated COUNT path (avoids JIT poisoning from aggType switch).
  ;; Targets: ColumnOps/fusedSimdCountParallel.
  ;; `agg` carries the normalized COUNT spec so the result is keyed by
  ;; the user's `:as` alias (e.g. `SELECT COUNT(*) AS cnt → :cnt`).
           [predicates agg input])

(defrecord PChunkedSIMDAgg
  ;; Stream index chunks with SIMD filter+aggregate.
  ;; Targets: ColumnOpsChunkedSimd methods
           [predicates agg input])

(defrecord PChunkedSIMDCount
  ;; JIT-isolated chunked COUNT.
  ;; Targets: ColumnOpsExt/fusedSimdChunkedCountParallel.
  ;; `agg` carries the COUNT spec so the result honors the user's
  ;; `:as` alias (parallel to PFusedSIMDCount).
           [predicates agg input])

(defrecord PBlockSkipCount
  ;; Block-skip COUNT on arrays using min/max statistics.
  ;; Targets: ColumnOpsExt/fusedSimdCountBlockSkipParallel.
  ;; `agg` carries the COUNT spec so the result honors the user's
  ;; `:as` alias (parallel to PFusedSIMDCount / PChunkedSIMDCount).
           [predicates agg input])

(defrecord PFusedMultiSum
  ;; Single-pass multiple SUM/AVG/COUNT aggs (<= 4 non-COUNT).
  ;; Targets: ColumnOpsExt fusedSimdMultiSumParallel
           [predicates aggs input])

(defrecord PPercentileAgg
  ;; Two-pass percentile/median/approx-quantile.
  ;; Targets: ColumnOps/percentile, ColumnOpsAnalytics/tdigestApproxQuantileParallel
           [predicates aggs input])

(defrecord PScalarAgg
  ;; Clojure scalar loop for complex/unsupported agg combinations.
           [predicates aggs input])

(defrecord PSplitAgg
  ;; Mixed-strategy aggregation: partitions aggregates by physical class
  ;; (fast SIMD / percentile / variance / scalar) and runs each class on
  ;; its optimal sub-plan, then merges results.
  ;;
  ;; children   — vec of physical sub-plan nodes (one per class)
  ;; agg-order  — original aggs in declared order (for column projection
  ;;              and to preserve user-facing field order)
  ;; group-keys — nil for global agg; vec of group keys for GROUP BY merge
           [children agg-order group-keys])

;; --- Group-by strategies ----------------------------------------------------

(defrecord PChunkedDenseGroupBy
  ;; Stream index chunks with dense accumulator array. Zero-copy.
  ;; Targets: ColumnOpsChunked/fusedGroupAggregateDenseChunkedParallel
  ;; Requires: all columns from indices, key-space <= dense-limit
           [predicates group-keys aggs max-key input])

(defrecord PDenseGroupBy
  ;; Materialized dense group-by with direct array indexing.
  ;; Targets: ColumnOps/fusedFilterGroupAggregateDenseParallel
           [predicates group-keys aggs max-key input])

(defrecord PHashGroupBy
  ;; Hash-based group-by for large/sparse key spaces.
  ;; Targets: ColumnOps/fusedFilterGroupAggregateParallel
           [predicates group-keys aggs input])

(defrecord PFusedExtractCount
  ;; Fused date extraction + COUNT dense group-by. No intermediate array.
  ;; Targets: ColumnOpsExt/fusedExtractCountDenseParallel
           [extract-op extract-col aggs input])

;; --- Join strategies --------------------------------------------------------

(defrecord PBitmapSemiJoin
  ;; Semi-join materialized as a mask column, injected as a predicate.
  ;; After this, the join is eliminated and the mask acts as a filter.
           [join-spec input])

(defrecord PHashJoin
  ;; Standard hash join: build on dim side, probe on fact side.
  ;; Targets: ColumnOpsExt/hashJoinBuild + hashJoinProbe
           [join-type on-pairs build-side probe-side])

(defrecord PAsofJoin
  ;; ASOF join via radix-partition + per-partition sort + two-pointer merge.
  ;; Targets: stratum.internal.ColumnOpsAsof
  ;; join-type        — :inner | :left
  ;; on-pairs         — [[left-col right-col] ...]
  ;; match-condition  — [op left-col right-col]
  ;; left/right       — child plan nodes (left is probe, right is build)
           [join-type on-pairs match-condition left right])

(defrecord PPerfectHashJoin
  ;; Direct-array-indexing join when build-key range is small.
  ;; Targets: ColumnOpsExt/perfectHashJoinBuild + perfectJoinProbeInner
           [join-type on-pairs min-key key-range build-side probe-side])

(defrecord PFusedJoinGroupAgg
  ;; Single Java pass: probe + gather + group-by + aggregate.
  ;; Requires: single INNER join, group on dim cols, agg on fact cols, no WHERE.
           [join-spec group-keys aggs left right])

(defrecord PFusedJoinGlobalAgg
  ;; Single Java pass: probe + accumulate (no group-by).
           [join-spec aggs left right])

;; --- Post-processing (same logical and physical) ----------------------------

(defrecord PProject [items input])
(defrecord PWindow  [specs input])
(defrecord PHaving  [predicates input])
(defrecord PSort    [order-specs limit offset input])
(defrecord PDistinct [input])
(defrecord PLimit   [limit offset input])

(defrecord PTopN
  ;; Physical counterpart of LTopN. The executor delegates to
  ;; `stratum.query.top-n/execute-top-n` after recovering the column
  ;; context from the input scan.
           [order-spec limit select input])

;; --- Expression materialization (inserted by passes) ------------------------

(defrecord PMaterializeExpr
  ;; Pre-compute an expression into a temp column.
  ;; col-name — generated keyword for the temp column
  ;; expr     — normalized expression to evaluate
  ;; target   — :int64 | :float64 | :dict-string
           [col-name expr target input])

;; ============================================================================
;; Utilities
;; ============================================================================

(defn logical?
  "True if node is a logical IR node (not yet physical)."
  [node]
  (or (instance? LScan node) (instance? LFilter node) (instance? LJoin node)
      (instance? LAsofJoin node)
      (instance? LGroupBy node) (instance? LGlobalAgg node) (instance? LProject node)
      (instance? LWindow node) (instance? LHaving node) (instance? LDistinct node)
      (instance? LSort node) (instance? LLimit node) (instance? LSetOp node)
      (instance? LAnomaly node) (instance? LStringMaterialize node)
      (instance? LHead node)))

(defn input-node
  "Returns the input child of a unary node, or nil for leaves/joins."
  [node]
  (cond
    (instance? LScan node)     nil
    (instance? LSetOp node)    nil
    (instance? LJoin node)     nil ;; use :left/:right directly
    (instance? LAsofJoin node) nil
    :else                      (:input node)))

(defn map-input
  "Replace the :input child of a unary node. For joins, use map-join-children."
  [node f]
  (if-let [child (:input node)]
    (assoc node :input (f child))
    node))

(defn map-children
  "Apply f to all children of a node (unary or binary)."
  [node f]
  (cond
    (instance? LJoin node)
    (-> node (update :left f) (update :right f))

    (instance? LAsofJoin node)
    (-> node (update :left f) (update :right f))

    (instance? PAsofJoin node)
    (-> node (update :left f) (update :right f))

    (instance? PHashJoin node)
    (-> node (update :build-side f) (update :probe-side f))

    (instance? PPerfectHashJoin node)
    (-> node (update :build-side f) (update :probe-side f))

    (instance? PFusedJoinGroupAgg node)
    (-> node (update :left f) (update :right f))

    (instance? PFusedJoinGlobalAgg node)
    (-> node (update :left f) (update :right f))

    (instance? PBitmapSemiJoin node)
    (-> node
        (update :input f)
        (update-in [:join-spec :build-side] f))

    (instance? PSplitAgg node)
    (update node :children (fn [cs] (mapv f cs)))

    (:input node)
    (update node :input f)

    :else node))

(defn walk-plan
  "Bottom-up transform of a plan tree. f receives each node after its
   children have been transformed."
  [node f]
  (f (map-children node #(walk-plan % f))))
