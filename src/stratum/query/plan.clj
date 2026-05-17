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
            [stratum.query.group-by :as gb]
            [stratum.query.execution :as x-cols]
            [stratum.dataset :as dataset])
  (:import [stratum.query.ir
            LScan LFilter LJoin LAsofJoin LGroupBy LGlobalAgg LProject
            LWindow LHaving LDistinct LSort LLimit LSetOp
            PScan PChunkedScan PSIMDFilter PMaskFilter
            PStatsOnlyAgg PFusedSIMDAgg PFusedSIMDCount
            PChunkedSIMDAgg PChunkedSIMDCount PBlockSkipCount
            PFusedMultiSum PPercentileAgg PScalarAgg PSplitAgg
            PChunkedDenseGroupBy PDenseGroupBy PHashGroupBy
            PFusedExtractCount PBitmapSemiJoin PHashJoin
            PPerfectHashJoin PAsofJoin PFusedJoinGroupAgg PFusedJoinGlobalAgg
            PProject PWindow PHaving PSort PDistinct PLimit
            PMaterializeExpr PRecompose]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Plan construction — query map → logical IR
;; ============================================================================

;; `normalize-select-item` lives in `stratum.query.execution` and
;; handles :as / literals / expressions / keywords. We delegate to it
;; instead of duplicating the cases here.

(defn- build-scan
  "Build an LScan from :from, handling both maps and IDataset."
  [from]
  (let [columns (if (satisfies? dataset/IDataset from)
                  (dataset/columns from)
                  (cols/prepare-columns from))
        length  (cols/get-column-length (val (first columns)))]
    (ir/->LScan columns length)))

(def ^:private asof-ops #{:>= :> :<= :<})

(defn- parse-on-preds
  "Normalize a `:on` spec into a flat vec of `[op left-col right-col]`
   tuples. Accepts `nil` / empty, a single `[op a b]`, or a vec of
   `[op a b]` clauses. Table-qualified namespace prefixes (`:t1/a`,
   `:t2/x`) are stripped so callers can compare against the
   unqualified column-map keys.

   ASOF joins fold equality (`:=`) and inequality (`:>=` / `:>` /
   `:<=` / `:<`) predicates into the same `:on` slot;
   `split-asof-on` separates them when needed. Non-ASOF joins
   accept only `:=` (enforced in `build-join-tree`)."
  [on]
  (cond
    (nil? on) []
    (and (vector? on) (keyword? (first on)) (= 3 (count on)))
    [[(first on) (norm/strip-ns (second on)) (norm/strip-ns (nth on 2))]]
    (and (vector? on) (every? vector? on))
    (mapv (fn [pred]
            (when-not (and (= 3 (count pred)) (keyword? (first pred)))
              (throw (ex-info "Join :on predicate must be [op left-col right-col]"
                              {:predicate pred})))
            [(first pred) (norm/strip-ns (second pred)) (norm/strip-ns (nth pred 2))])
          on)
    (and (sequential? on) (empty? on)) []
    :else (throw (ex-info "Unsupported join :on format" {:on on}))))

(defn- split-asof-on
  "Split parsed ON predicates into equality keys and the single inequality.
   Returns {:on-pairs [[l r] ...] :match-condition [op l r]}.
   Throws if the inequality count is not exactly 1."
  [preds join-spec]
  (let [eq (filterv #(= := (first %)) preds)
        ineq (filterv #(contains? asof-ops (first %)) preds)
        other (filterv #(and (not= := (first %))
                             (not (contains? asof-ops (first %)))) preds)]
    (when (seq other)
      (throw (ex-info (str "ASOF join :on supports only := and inequality operators "
                           "(#{:>= :> :<= :<}); got " (mapv first other))
                      {:join-spec join-spec :unsupported-ops (mapv first other)})))
    (when-not (= 1 (count ineq))
      (throw (ex-info (str "ASOF join :on must contain exactly one inequality predicate "
                           "(#{:>= :> :<= :<}), got " (count ineq))
                      {:join-spec join-spec :inequality-count (count ineq)})))
    {:on-pairs (mapv (fn [[_ l r]] [l r]) eq)
     :match-condition (first ineq)}))

(defn- build-join-tree
  "Wrap scan with join nodes for each join spec. Supports
   equi-joins (`:inner` / `:left` / `:right` / `:full`) and ASOF
   joins (`:asof` / `:asof-left`).

   For ASOF joins, the inequality (one of `#{:>= :> :<= :<}`) lives
   inside `:on` alongside the equality predicates — the same shape
   DuckDB's SQL form uses. `split-asof-on` separates them into
   `:on-pairs` and `:match-condition` on the `LAsofJoin` IR node.

   Table-qualified `:on` keys (e.g. `:t1/a`) are stripped by
   `parse-on-preds` so they match the unqualified column-map keys."
  [scan join-specs]
  (reduce
   (fn [left join-spec]
     (let [{:keys [with on type]} join-spec
           jt (or type :inner)
           right-scan (build-scan with)
           preds (parse-on-preds on)]
       (cond
         (#{:asof :asof-left} jt)
         (let [{:keys [on-pairs match-condition]} (split-asof-on preds join-spec)
               jt' (if (= jt :asof-left) :left :inner)]
           (ir/->LAsofJoin jt' on-pairs match-condition left right-scan))

         :else
         (do
           (when (empty? preds)
             (throw (ex-info "Join :on is required for non-ASOF joins" {:join-spec join-spec})))
           (when-let [bad (seq (filter #(not= := (first %)) preds))]
             (throw (ex-info (str "Non-equality predicate in :on clause is not supported for "
                                  jt " join. Use :type :asof for inequality matches. Got: "
                                  (mapv first bad))
                             {:join-spec join-spec :bad-ops (mapv first bad)})))
           (ir/->LJoin jt (mapv (fn [[_ l r]] [l r]) preds) left right-scan)))))
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

    (let [;; Normalize group keys: strip SQL ns from plain keywords, and
          ;; recognize the `[:as expr alias]` form (matching the existing
          ;; `:select` / `:agg` alias convention) so a non-keyword group
          ;; expression can be given a stable user-facing result-map key.
          ;; Without `:as`, expressions still work but the result key is
          ;; the planner's internal synthetic — documented as "use :as if
          ;; you need to read it back."
          group  (when group
                   (mapv (fn [g]
                           (cond
                             (keyword? g)
                             (norm/strip-ns g)

                             (and (vector? g) (= :as (first g)) (= 3 (count g)))
                             (let [[_ expr alias] g]
                               {:expr expr :as alias})

                             :else g))
                         group))
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

          ;; 2b. Anomaly score materialization (post-join, pre-filter).
          ;; The frontend (`prepare-and-build`) has rewritten
          ;; `:where`/`:select`/etc. so they reference synthetic
          ;; columns; the actual scoring runs at LAnomaly execute
          ;; time so post-join columns are visible to iforest.
          ;; For the no-join case, the frontend already resolved
          ;; eagerly and `::anomaly-expr->col` is empty.
          anomaly-expr->col (::anomaly-expr->col query)
          anomaly-models    (::anomaly-models query)
          joined (if (seq anomaly-expr->col)
                   (ir/->LAnomaly anomaly-expr->col anomaly-models joined)
                   joined)

          ;; 2c. Post-join string-expression materialization. The
          ;; frontend has rewritten `:group` / aggs / `:select` to
          ;; reference synthetic `__str_expr_N` columns; the actual
          ;; `eval-string-expr` calls run at `LStringMaterialize`
          ;; execute time, with the joined column ctx in scope.
          ;; `:string-items` is empty for non-join queries (passes
          ;; 5a/5b ran eagerly in `prepare-query` and folded the
          ;; results into the columns map).
          string-items (::string-items query)
          joined (if (seq string-items)
                   (ir/->LStringMaterialize string-items joined)
                   joined)

          ;; 3. Normalize predicates, aggs, select items.
          ;;    When called via `executor/run-query` the lowering step
          ;;    (prepare-query) has already normalized preds / aggs and
          ;;    set `::pre-normalized?` on the query map; in that case
          ;;    skip re-normalization (it isn't idempotent).
          pre-norm? (::pre-normalized? query)
          preds   (if pre-norm?
                    (or where [])
                    (mapv norm/normalize-pred (or where [])))
          aggs    (if pre-norm?
                    (or agg [])
                    (norm/auto-alias-aggs (mapv norm/normalize-agg (or agg []))))
          sel     (when (seq select)
                    (if pre-norm?
                      (vec select)
                      (vec (map-indexed #(x-cols/normalize-select-item %2 %1) select))))

          ;; 4. Filter
          filtered (if (seq preds)
                     (ir/->LFilter preds joined)
                     joined)

          ;; 5. Core operation: group-by / global-agg / passthrough.
          ;; Project is held back when a window is present so the
          ;; window can see all source columns (SQL evaluation order:
          ;; window functions logically run after GROUP BY / HAVING
          ;; but before SELECT projection).
          window? (seq window)
          core (cond
                 (seq group)
                 (ir/->LGroupBy group aggs filtered)

                 (seq aggs)
                 (ir/->LGlobalAgg aggs filtered)

                 (and (seq sel) (not window?))
                 (ir/->LProject sel filtered)

                 :else filtered)

          ;; 6. Window functions (run before any deferred projection).
          windowed (if window?
                     (ir/->LWindow window core)
                     core)

          ;; 6b. Deferred projection: when select coexisted with
          ;; window, apply LProject now so window-output columns are
          ;; visible to the projection. Window-output columns
          ;; (`:as` of each spec) are auto-appended to the select if
          ;; the user didn't list them explicitly. When no select
          ;; was given at all, we synthesize one that exposes both
          ;; base scan columns and window outputs (mirrors
          ;; q.clj:807-826).
          windowed (cond
                     (and window? (seq sel))
                     (let [existing (set (map :name sel))
                           extra    (keep (fn [ws]
                                            (let [a (:as ws)]
                                              (when-not (contains? existing a)
                                                {:name a :ref a})))
                                          window)
                           sel'     (into sel extra)]
                       (ir/->LProject sel' windowed))

                     (and window? (not (seq sel)) (not (seq aggs)) (not (seq group)))
                     (let [columns (:columns scan)
                           base-items (mapv (fn [k] {:name k :ref k})
                                            (remove #{:__mask} (keys columns)))
                           win-items  (mapv (fn [ws]
                                              {:name (:as ws) :ref (:as ws)})
                                            window)]
                       (ir/->LProject (into base-items win-items) windowed))

                     :else windowed)

          ;; 7. Having
          ;; Normalize predicates here so every consumer (planner
          ;; rewrites, executor, post/apply-having) can rely on the
          ;; `[col op & args]` shape. `normalize-pred` is idempotent,
          ;; so we always run it — `prepare-query` does NOT cover
          ;; HAVING in its `pre-normalized?` flag (only `:where` and
          ;; `:agg`), and we want the pushdown classifier to read
          ;; the canonical form unconditionally.
          having-preds (mapv norm/normalize-pred (or having []))
          having-node (if (seq having-preds)
                        (ir/->LHaving having-preds windowed)
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
  "Set of column keywords referenced by a normalized predicate (recursive).

   Includes both the LHS column (`first pred`) and any keyword RHS
   args (col-vs-col predicates like `[:_valid_from :lt :_valid_to]`).
   Without scanning RHS args, column-pruning trims the RHS column
   from the projected col-arrays map, and `eval-pred-scalar`'s
   `resolve-arg` then NPEs on `(aget ^doubles other i)` because
   `(get col-arrays :_valid_to)` returned nil. Copilot review #3."
  [pred]
  (let [op (second pred)]
    (case op
      :or (into #{} (mapcat pred-columns) (subvec pred 2))
      (:in :not-in :fn) (let [c (first pred)] (if (keyword? c) #{c} #{}))
      (let [col (first pred)
            base (cond
                   (map? col)     (into #{} (filter keyword?) (:args col))
                   (keyword? col) #{col}
                   :else          #{})
            ;; Scan RHS args (everything from index 2 onward) for
            ;; keyword column refs — `:lt`/`:gt`/`:eq`/etc accept
            ;; a literal OR a column ref on the RHS; `:range` and
            ;; `:not-range` accept two such args.
            args (subvec pred 2)]
        (into base (filter keyword?) args)))))

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
   Recurses through LFilter and join nodes so that predicate pushdown works
   through chained joins (e.g. fact → join1 → join2)."
  [node]
  (cond
    (instance? LScan node)     (set (keys (:columns node)))
    (instance? LFilter node)   (recur (:input node))
    (instance? LJoin node)     (let [l (scan-columns (:left node))
                                     r (scan-columns (:right node))]
                                 (when (and l r)
                                   (into l r)))
    (instance? LAsofJoin node) (let [l (scan-columns (:left node))
                                     r (scan-columns (:right node))]
                                 (when (and l r)
                                   (into l r)))
    :else nil))

(defn- pushdown-once
  "Single pass: push LFilter predicates below LJoin / LAsofJoin
   when they reference only one side. Outer joins constrain the
   rewrite:
     - LEFT  join: push left-side preds (preserves NULL-padded rows
                   on the right); right-side preds CANNOT be pushed
                   because LEFT preserves left rows that have no
                   right match.
     - RIGHT join: symmetric — push right, not left.
     - FULL  join: neither side can be pushed.
   Only INNER joins allow both. ASOF joins behave like LEFT
   (`:asof-left`) or INNER (`:asof`) for pushdown purposes."
  [plan]
  (ir/walk-plan plan
                (fn [node]
                  (if (and (instance? LFilter node)
                           (or (instance? LJoin (:input node))
                               (instance? LAsofJoin (:input node))))
                    (let [join (:input node)
                          jt   (:join-type join)
                          left-cols  (scan-columns (:left join))
                          right-cols (scan-columns (:right join))]
                      (if (and left-cols right-cols)
                        (let [classified (group-by #(classify-pred-by-side % left-cols right-cols)
                                                   (:predicates node))
                              left-pushable?  (#{:inner :left}  jt)
                              right-pushable? (#{:inner :right} jt)
                              left-preds  (if left-pushable?
                                            (vec (get classified :left []))
                                            [])
                              right-preds (if right-pushable?
                                            (vec (get classified :right []))
                                            [])
                              ;; Anything we can't push stays above the join.
                              kept-preds  (vec (concat
                                                (when-not left-pushable?
                                                  (get classified :left []))
                                                (when-not right-pushable?
                                                  (get classified :right []))
                                                (get classified :cross [])))
                  ;; Wrap join children with pushed-down filters
                              new-left  (if (seq left-preds)
                                          (ir/->LFilter left-preds (:left join))
                                          (:left join))
                              new-right (if (seq right-preds)
                                          (ir/->LFilter right-preds (:right join))
                                          (:right join))
                              new-join (assoc join :left new-left :right new-right)]
                          (if (seq kept-preds)
                            (ir/->LFilter kept-preds new-join)
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
;; Pass 1b: HAVING → WHERE pushdown (filter through aggregate)
;; ============================================================================

(defn having-to-where-pushdown
  "Push `LHaving` predicates that only reference `LGroupBy` group
   columns (not aggregate aliases) down as pre-aggregate `LFilter`
   preds. Mirrors DuckDB's `FilterPushdown::PushdownAggregate`
   (`src/optimizer/pushdown/pushdown_aggregate.cpp`):

     HAVING g > 5  (where g is in GROUP BY)  →  WHERE g > 5
     HAVING SUM(x) > 100                       stays on LHaving

   The user's HAVING clause is the SQL spec point for filtering
   post-aggregate; the planner is free to evaluate any predicate
   pre-aggregate when it doesn't depend on the aggregate output.
   Pushing such predicates eliminates rows before they enter the
   group-by hash table, which is the same constraint-as-early-as-
   possible principle as predicate-pushdown-through-join.

   Constraints:
     - Only `LHaving` directly above `LGroupBy` (skips `LWindow` /
       `LProject` between them — their reference resolution is more
       subtle; the existing `window-having-pushdown` handles the
       window case separately).
     - Skips `LGlobalAgg` (no group columns to filter on, matching
       DuckDB's `aggr.groups.empty()` short-circuit).
     - Only keyword group keys count (expression group keys
       reference `__gk_expr_N` synthetic columns that
       `expr-materialization` introduces later in the pipeline).
     - When a downstream `LFilter` already wraps the `LGroupBy`'s
       input (the WHERE clause), the pushed predicates merge into
       that filter — keeps the IR shape clean."
  [plan]
  (ir/walk-plan
   plan
   (fn [node]
     (if (and (instance? LHaving node)
              (instance? LGroupBy (:input node)))
       (let [groupby     (:input node)
             group-cols  (set (filter keyword? (:group-keys groupby)))
             classify    (fn [pred]
                           (let [cols (pred-columns pred)]
                             (if (and (seq cols) (every? group-cols cols))
                               :pushable
                               :kept)))
             grouped     (group-by classify (:predicates node))
             pushable    (vec (get grouped :pushable []))
             kept        (vec (get grouped :kept []))]
         (if (empty? pushable)
           node
           (let [child     (:input groupby)
                 new-child (if (instance? LFilter child)
                             (ir/->LFilter
                              (into (vec (:predicates child)) pushable)
                              (:input child))
                             (ir/->LFilter pushable child))
                 new-gb    (assoc groupby :input new-child)]
             (if (empty? kept)
               new-gb
               (ir/->LHaving kept new-gb)))))
       node))))

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
   nodes and rewrite the keys. Returns [new-keys mat-nodes].

   Normalizes the expression with `norm/normalize-expr` so the
   downstream `eval-expr-vectorized` sees `{:op ... :args ...}` form
   rather than a raw `[:op ...]` vector (which it doesn't accept).

   The target type is `:int64` for group-by expressions: group keys
   are discrete by definition, the executor's `eval-expr-to-long`
   path returns `long[]` directly for date-trunc/date-add/extract
   ops (skipping the `long → double → long` round-trip), and
   downstream dense group-by accumulators dispatch on the column's
   `:int64` type for the all-long fast path. Mirrors the legacy
   block at `q.clj:700-718`. Closes the CB-Q43 perf gap (date-trunc
   minute group-by was 40× slower under the planner because the
   `:float64` round-trip blocked the long fast-path)."
  [group-keys]
  (loop [idx 0, new-keys [], mat-nodes []]
    (if (>= idx (count group-keys))
      [new-keys mat-nodes]
      (let [gk (nth group-keys idx)]
        (cond
          (keyword? gk)
          (recur (inc idx) (conj new-keys gk) mat-nodes)

          ;; `{:expr ... :as ...}` alias map (parsed at build-logical-plan
          ;; time from `[:as expr alias]`). User-supplied alias becomes
          ;; the column name — visible in result maps, matching the
          ;; same convention `:select` / `:agg` use for `[:as ...]`.
          (and (map? gk) (contains? gk :expr) (contains? gk :as))
          (let [{:keys [expr as]} gk
                normalized (if (map? expr) expr (norm/normalize-expr expr))]
            (recur (inc idx)
                   (conj new-keys as)
                   (conj mat-nodes (ir/->PMaterializeExpr as normalized :int64 nil))))

          :else
          (let [cn (keyword (str "__gk_expr_" idx))
                normalized (if (map? gk) gk (norm/normalize-expr gk))]
            (recur (inc idx)
                   (conj new-keys cn)
                   (conj mat-nodes (ir/->PMaterializeExpr cn normalized :int64 nil)))))))))

(defn- chain-materialize
  "Chain PMaterializeExpr nodes on top of an input node (innermost first)."
  [mat-nodes input]
  (reduce (fn [in mat] (assoc mat :input in)) input mat-nodes))

;; ============================================================================
;; Pass: linear-agg-rewrite (F21-A)
;; ----------------------------------------------------------------------------
;; Algebraic rewrite for `SUM/AVG/MIN/MAX` on a linear expression of a
;; single column: `s·x + o`. Eliminates the temp materialised column
;; (the otherwise-mandatory `PMaterializeExpr` shim) by reducing the
;; agg's `:expr` to `nil`, computing the agg on the bare column, and
;; reconstructing the user-visible value at decode time from a recipe
;; attached as agg metadata.
;;
;; Recipes (each agg is keyed by the user's alias):
;;   SUM(s·x + o)         → s·SUM(x) + o·count
;;   AVG(s·x + o)         → s·AVG(x) + o
;;   MIN/MAX(s·x + o)     → s·MIN/MAX(x) + o      (s ≥ 0)
;;                          s·MAX/MIN(x) + o      (s < 0; op flips)
;;
;; First-cut scope: `LGlobalAgg` only (single global agg). `LGroupBy`
;; is deferred to F21-B (the per-group recipe needs threading through
;; group-by decode).
;; ============================================================================

(defn- match-linear-expr
  "Recognise an expression of the form `s·x + o` over a single column
   `x` and numeric constants `s,o`. Returns `{:col k :scale s :offset o}`
   or nil. `s,o` are returned as `double` so downstream arithmetic stays
   in float space (the chunked SIMD agg path returns double accumulators)."
  [expr]
  (when (and (map? expr)
             (vector? (:args expr))
             (= 2 (count (:args expr))))
    (let [op (:op expr)
          [a b] (:args expr)
          col-num (cond
                    (and (keyword? a) (number? b)) [a (double b) :col-first]
                    (and (number? a) (keyword? b)) [b (double a) :num-first]
                    :else nil)]
      (when col-num
        (let [[col c order] col-num]
          (case op
            :add  {:col col :scale 1.0  :offset c}
            :sub  (case order
                    :col-first {:col col :scale 1.0  :offset (- c)}
                    :num-first {:col col :scale -1.0 :offset c})
            :mul  {:col col :scale c    :offset 0.0}
            :div  (when (and (= order :col-first) (not (zero? c)))
                    {:col col :scale (/ 1.0 c) :offset 0.0})
            nil))))))

(defn- linear-recipe
  "Given the original agg `op` and a linear-expr match, return
   `{:op base-op :recipe …}` describing the base agg to compute and
   the post-arithmetic to apply. Returns nil when the rewrite is
   not safe (e.g. `:scale 0` collapses to a constant — handled by a
   different path)."
  [op {:keys [scale offset] :as match}]
  (when (and match (not (zero? (double scale))))
    (case op
      :sum {:base-op :sum :recipe (assoc match :reassemble :sum)}
      :avg {:base-op :avg :recipe (assoc match :reassemble :avg)}
      :min {:base-op (if (neg? (double scale)) :max :min)
            :recipe  (assoc match :reassemble :min-max)}
      :max {:base-op (if (neg? (double scale)) :min :max)
            :recipe  (assoc match :reassemble :min-max)}
      nil)))

(defn- try-rewrite-linear-agg
  "Attempt to rewrite a single agg map. Returns either the original
   agg unchanged or a new agg with `:expr` cleared and the recipe
   attached as `^:linear-recipe` metadata that downstream decoders
   read via `(:linear-recipe (meta agg))`.

   Pins `:as` to the original op (or the user's existing alias) so
   the result key stays the user's, even though MIN/MAX with negative
   scale internally flips the agg `:op`."
  [agg]
  (let [op   (:op agg)
        expr (:expr agg)]
    (or (when (and expr (#{:sum :avg :min :max} op))
          (when-let [match (match-linear-expr expr)]
            (when-let [{:keys [base-op recipe]} (linear-recipe op match)]
              (let [as (or (:as agg) op)]
                (with-meta
                  (-> agg (dissoc :expr) (assoc :op base-op :col (:col match) :as as))
                  {:linear-recipe recipe})))))
        agg)))

(defn smcs-decomposition
  "Rewrite `LGlobalAgg` and `LGroupBy` aggs of the form `SUM(a*(c-b))`
   into `c*SUM(a) - SUM_PRODUCT(a,b)` and wrap the node in `PRecompose`.

   Runs BEFORE `expr-materialization` so the compound expression never
   gets materialized as a temp column — the decomposed aggs reference
   `a` and `b` directly, which on indexed inputs keeps the L2-resident
   chunked group-by streaming path eligible. `execute-chunked-group-by`
   would do the same rewrite itself, but only sees aggs after the
   planner has replaced their `:expr` with `:col` refs to the
   materialized temp column — too late for the rewrite to fire."
  [plan]
  (ir/walk-plan plan
                (fn [node]
                  (cond
                    (or (instance? LGlobalAgg node)
                        (instance? LGroupBy node))
                    (if-let [decomp (gb/decompose-smcs-aggs (:aggs node))]
                      (ir/->PRecompose
                       (:mapping decomp)
                       (:aggs node)
                       (assoc node :aggs (:aggs decomp)))
                      node)
                    :else node))))

(defn linear-agg-rewrite
  "Rewrite `LGlobalAgg` and `LGroupBy` aggs of the form
   `SUM/AVG/MIN/MAX(s·x + o)` into the base agg on `x` plus a recipe
   stored on agg metadata, eliminating the otherwise-required
   `PMaterializeExpr` shim. The recipe is consumed at decode time:
     - global agg paths: `format-fused-result` /
       `rewrite-row-with-recipes`
     - group-by paths: `apply-recipes-to-rows` on the per-group rows"
  [plan]
  (ir/walk-plan plan
                (fn [node]
                  (cond
                    (or (instance? LGlobalAgg node)
                        (instance? LGroupBy node))
                    (let [orig-aggs (:aggs node)
                          rewritten (mapv try-rewrite-linear-agg orig-aggs)]
                      (if (= rewritten orig-aggs)
                        node
                        (assoc node :aggs rewritten)))
                    :else node))))

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

(def ^:private long-null-sentinel
  "The null sentinel `Long/MIN_VALUE` cast to double, the form
   `prepare-query` produces when rewriting `IS-NULL` on `:int64` cols
   into `:eq` for the SIMD path.  ChunkStats min/max exclude nulls so
   zone maps can't reliably classify chunks against this sentinel."
  (double Long/MIN_VALUE))

(defn- pred-targets-null-sentinel?
  "True if `pred` compares against the long-null sentinel. Defensive
   guard for user-supplied predicates like `[:col :eq Long/MIN_VALUE]`:
   ChunkStats omit nulls, so any zone-map classification of such a pred
   is wrong (an all-null chunk has min/max set to non-null sentinels and
   `zone-may-contain` says \"no\", but every row matches the sentinel).
   F20 skips stats-only promotion in this case.

   Internal IS-NULL/IS-NOT-NULL no longer emit this shape — the
   `rewrite-null-preds` pass was removed for SQL 3VL correctness and
   IS-NULL now routes through the compiled-mask path."
  [pred]
  (and (vector? pred) (>= (count pred) 3)
       (#{:eq :neq} (second pred))
       (let [v (nth pred 2)]
         (and (number? v) (== (double v) long-null-sentinel)))))

(defn- preds-classify-all-chunks?
  "True iff every chunk classifies via zone maps as `:stats-only` or
   `:skip` for the given preds — i.e. no partial-match chunks exist.
   When this holds we can skip the SIMD path entirely and accumulate
   the answer from `ChunkStats` alone (F20). Conservative: returns
   false when any column is non-indexed, when no zone filters can be
   built (e.g. preds on non-numeric cols), when any pred targets the
   null sentinel (zone maps unreliable for nulls), or when any
   chunk would need data inspection."
  [preds columns]
  (and (seq preds)
       (every? :index (vals columns))   ; all index-backed
       ;; Null sentinel preds bypass zone-map classification — see
       ;; pred-targets-null-sentinel? doc.
       (not (some pred-targets-null-sentinel? preds))
       (let [zone-filters (gb/build-zone-filters preds)
             ;; Reject expr-LHS preds (their `:col` is a map, not a
             ;; keyword we can look up in `columns`) and any pred that
             ;; build-zone-filters didn't lift.
             pred-cols (into #{} (keep (fn [zf] (let [c (:col zf)]
                                                  (when (keyword? c) c))))
                             zone-filters)]
         (and (= (count pred-cols) (count preds))
              (let [col-entries (into {}
                                      (keep (fn [[k info]]
                                              (when (contains? pred-cols k)
                                                [k (gb/collect-chunk-entries (:index info))])))
                                      columns)
                    n-chunks (some-> col-entries first val count)]
                (and n-chunks
                     (every? #(let [c (gb/classify-chunk zone-filters col-entries %)]
                                (or (= :skip c) (= :stats-only c)))
                             (range n-chunks))))))))

;; ----------------------------------------------------------------------------
;; Mixed-agg partitioning (PSplitAgg)
;; ----------------------------------------------------------------------------
;;
;; Several aggregate ops need a fundamentally different physical operator
;; (e.g. `median` needs a sort/quickselect pass, `count-distinct` needs a
;; HashSet, `variance` needs Welford). When the user mixes these with
;; SIMD-friendly aggs (`sum/min/max/avg/count`) on a single LGlobalAgg or
;; LGroupBy, the legacy planner picks one operator for the whole list and
;; falls all the way down to `PScalarAgg` — a per-row Clojure reduce that
;; is 6–10× slower than running each agg on its best operator. The split
;; pass partitions the aggregate list by physical-strategy class, plans
;; each subset with the regular strategy chooser, and emits a `PSplitAgg`
;; that the executor runs and merges. See DuckDB / ClickHouse for the
;; analogous design: each aggregate function declares its own state and
;; update routine; the engine dispatches per-agg over the same column data.

(defn- agg-class
  "Classify an aggregate by its physical-strategy compatibility.
   Aggs in the same class share a single physical sub-plan; aggs in
   different classes must run on separate sub-plans."
  [agg]
  (case (:op agg)
    (:median :percentile :approx-quantile) :percentile
    (:sum :min :max :avg :count :sum-product :count-non-null) :fast
    :scalar))

(defn- partition-aggs-by-class
  "Group aggs by class, preserving in-class declared order.
   Returns a vec of [class agg-subset] pairs (stable across runs)."
  [aggs]
  ;; Use an ordered traversal so result order is stable.
  (let [grouped (reduce (fn [acc a]
                          (let [c (agg-class a)]
                            (update acc c (fnil conj []) a)))
                        {}
                        aggs)]
    ;; Stable ordering: :fast first (cheapest), then :percentile, then :scalar
    (->> [:fast :percentile :scalar]
         (keep (fn [c] (when-let [as (get grouped c)] [c as])))
         vec)))

(defn- select-global-agg-strategy
  "Choose physical strategy for LGlobalAgg.

   Uses selectivity estimates to make cost-aware decisions:
   - Very selective preds (< 5%) → prefer block-skip COUNT over full SIMD
   - Low selectivity index scans → prefer chunked SIMD (skip whole chunks)
   - Otherwise follows capability-based priority cascade.

   Priority (capability gated, cost-informed):
   0. Mixed-class aggs → split into per-class sub-plans (PSplitAgg)
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
        agg-cols-in-scan? (every? (fn [a]
                                    (or (= :count (:op a))
                                        (and (:col a) (contains? columns (:col a)))
                                        (and (:cols a)
                                             (every? #(contains? columns %) (:cols a)))))
                                  aggs)
        ;; Cost-aware: estimate predicate selectivity
        selectivity (when (seq preds)
                      (est/estimate-combined-selectivity preds columns))
        est-rows    (when selectivity
                      (est/estimate-output-rows preds columns length))
        very-selective? (and selectivity (< (double selectivity) 0.05))
        ;; 0. Mixed-class split detection.
        ;; If aggs span >1 class, each class would force a different
        ;; physical plan. Without splitting, the cascade below collapses
        ;; the whole list to PScalarAgg (the slowest fallback). Splitting
        ;; runs each class on its best operator and merges single rows.
        partitions (partition-aggs-by-class aggs)]
    (cond
      ;; 0. Mixed classes — split into per-class sub-plans
      (> (count partitions) 1)
      (let [child-plans (mapv (fn [[_cls agg-subset]]
                                (select-global-agg-strategy
                                 (assoc node :aggs agg-subset) scan preds))
                              partitions)]
        (ir/->PSplitAgg child-plans aggs nil))

      ;; 1. Unfiltered COUNT
      (and (= 1 n-aggs) (= :count (:op first-agg)) no-preds?)
      (ir/->PFusedSIMDCount [] first-agg scan)

      ;; 2. Stats-only (needs chunk statistics → real columns only).
      ;; SUM and AVG read sum/sum-sq from ChunkStats. Sources whose stats
      ;; do not carry those (e.g. parquet-dataset row groups — parquet
      ;; metadata has min/max/count/null-count but not sum) advertise
      ;; :stats-sum-incomplete? on the column map; in that case SUM and
      ;; AVG must fall through to the SIMD path.
      ;;
      ;; F20 extension: even with predicates, when zone maps fully
      ;; classify every chunk as `:stats-only` or `:skip` (no partial
      ;; chunks need data inspection), we still answer in O(chunks)
      ;; from the surviving chunks' stats alone.
      (and (seq aggs) all-idx? agg-cols-in-scan?
           (every? (fn [a]
                     (and (#{:sum :min :max :avg :count} (:op a))
                          (nil? (:expr a))
                          (or (not (#{:sum :avg} (:op a)))
                              (not (:stats-sum-incomplete?
                                    (get columns (:col a)))))))
                   aggs)
           (or no-preds?
               (preds-classify-all-chunks? preds columns)))
      (ir/->PStatsOnlyAgg (if no-preds? [] preds) aggs scan)

      ;; 3. Chunked SIMD (single agg, index-backed → real columns only)
      ;; Cost-aware: chunked SIMD is especially good for selective preds on
      ;; index columns because zone maps can skip entire chunks.
      (and all-idx? agg-cols-in-scan? (= 1 n-aggs) simd-ok? no-expr?)
      (if (= :count (:op first-agg))
        (ir/->PChunkedSIMDCount preds first-agg scan)
        (ir/->PChunkedSIMDAgg preds first-agg scan))

      ;; 4. Block-skip COUNT on arrays
      ;; Cost-aware: prefer block-skip when selectivity < 5% — can skip entire
      ;; blocks using min/max stats on materialized arrays.
      (and (= 1 n-aggs) (= :count (:op first-agg)) (seq preds)
           (not all-idx?) simd-ok? no-expr?)
      (ir/->PBlockSkipCount preds first-agg scan)

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
     with fallback to materialized dense/hash at execution time.

   Mixed-class aggs are split into per-class sub-plans (PSplitAgg) — same
   principle as the global-agg path. This keeps median/percentile out of
   the slow Clojure fallback in `execute-group-by` when mixed with
   SIMD-friendly aggs."
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
                      length)
        partitions  (partition-aggs-by-class aggs)]
    (cond
      ;; Mixed classes — split into per-class group-by sub-plans
      (> (count partitions) 1)
      (let [child-plans (mapv (fn [[_cls agg-subset]]
                                (select-group-by-strategy
                                 (assoc node :aggs agg-subset) scan preds))
                              partitions)]
        (vary-meta
         (ir/->PSplitAgg child-plans aggs group-keys)
         assoc ::estimated-rows est-rows
         ::selectivity (or selectivity 1.0)))

      all-idx?
      (vary-meta
       (ir/->PChunkedDenseGroupBy preds group-keys aggs 0 scan)
       assoc ::fallback :dense-or-hash
       ::estimated-rows est-rows
       ::selectivity (or selectivity 1.0))

      :else
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

(def ^:private fused-extract-ops
  "Subset of single-arg date-extract ops the Java fused path handles
   (`ColumnOpsExt/fusedExtractCountDenseParallel`). `:day-of-week`
   uses Hinnant's civil-date arithmetic; the rest are O(1) modular.
   `:date-trunc` and full timestamps stay on the materialize +
   dense-group-by path because they don't share the small bounded
   key space the fast-path assumes."
  #{:minute :hour :second :day-of-week})

(defn- try-fused-extract-count
  "Return a `PFusedExtractCount` node if the LGroupBy shape is the
   canonical 'EXTRACT(unit, col) GROUP BY → COUNT' fused-fast-path
   shape, else nil. Mirrors the legacy `fused-extract?` gate at
   `q.clj:680-696`. The expression has already been lifted to a
   `PMaterializeExpr` by `expr-materialization`, so we walk back
   through it to the underlying scan and emit the fused operator
   that absorbs the extraction. Closes the CB-Q19 10× gap."
  [{:keys [group-keys aggs input]}]
  (let [[mat-chain inner] (peel-materialize input)
        [preds scan] (peel-filter-scan inner)]
    (when (and (= 1 (count group-keys))
               (= 1 (count mat-chain))
               (seq aggs)
               (every? #(= :count (:op %)) aggs)
               (empty? preds)
               (or (instance? PScan scan) (instance? PChunkedScan scan)))
      (let [mat (first mat-chain)
            gk  (first group-keys)
            e   (:expr mat)]
        (when (and (= (:col-name mat) gk)
                   (map? e)
                   (contains? fused-extract-ops (:op e))
                   (= 1 (count (:args e)))
                   (keyword? (first (:args e))))
          (let [src-col  (first (:args e))
                col-info (get (:columns scan) src-col)]
            (when (and col-info
                       (or (= :int64 (:type col-info))
                           (expr/long-array? (:data col-info))))
              (vary-meta
               (ir/->PFusedExtractCount (:op e) src-col aggs scan)
               assoc ::estimated-rows (:length scan)
               ::selectivity 1.0))))))))

(defn strategy-selection
  "Replace logical nodes with physical nodes based on data characteristics.

   Walks the plan bottom-up. Each logical node becomes a physical node
   (or stays logical if no strategy applies yet, e.g. post-processing)."
  [plan]
  (ir/walk-plan plan
                (fn [node]
                  (cond
        ;; LScan → PScan or PChunkedScan (preserve annotations).
        ;; Each scan gets its own `volatile!` for `:dynamic-filters`
        ;; so a downstream `execute-hash-join` can push a runtime
        ;; range predicate at execute-time without mutating the IR
        ;; node itself. The volatile defaults to `nil` (no dynamic
        ;; preds) and the scan executor treats `nil` as "no preds".
                    (instance? LScan node)
                    (let [m (meta node)]
                      (if (::zone-map-eligible m)
                        (with-meta (ir/->PChunkedScan (:columns node) (:length node)
                                                      nil [] (volatile! nil)) m)
                        (with-meta (ir/->PScan (:columns node) (:length node)
                                               [] (volatile! nil)) m)))

        ;; LGlobalAgg — peel through PMaterializeExpr to find filter/scan
                    (instance? LGlobalAgg node)
                    (let [[mat-chain inner] (peel-materialize (:input node))
                          [preds scan] (peel-filter-scan inner)
                          phys (select-global-agg-strategy node scan preds)]
                      (if (seq mat-chain)
                        (assoc phys :input (rechain-materialize mat-chain (:input phys)))
                        phys))

        ;; LGroupBy — try fused-extract+count first, otherwise peel
        ;; the materialize/filter chain and pick a regular strategy.
        ;; The fused operator absorbs the extraction so the matching
        ;; PMaterializeExpr is intentionally dropped from the chain.
                    (instance? LGroupBy node)
                    (or (try-fused-extract-count node)
                        (let [[mat-chain inner] (peel-materialize (:input node))
                              [preds scan] (peel-filter-scan inner)
                              phys (select-group-by-strategy node scan preds)]
                          (if (seq mat-chain)
                            (assoc phys :input (rechain-materialize mat-chain (:input phys)))
                            phys)))

        ;; LJoin
                    (instance? LJoin node)
                    (select-join-strategy node nil)

        ;; LAsofJoin → PAsofJoin (single strategy for now; revisit if we add NLJ alt)
                    (instance? LAsofJoin node)
                    (ir/->PAsofJoin (:join-type node) (:on-pairs node)
                                    (:match-condition node)
                                    (:left node) (:right node))

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
   - Parent node doesn't reference ANY build-side (dim) columns —
     including the join key. The semi-join discards the right side
     after building a presence-bitmap, so a parent SELECT that
     projects `t2.x` (the join key) would lose its right-side
     binding even though `t2.x = t1.a` in matched rows. Mirrors the
     `(not has-select?)` clause of the legacy
     `query.join/bitmap-semi-join-eligible?`."
  [join-node parent-cols]
  (and parent-cols
       (= :inner (:join-type join-node))
       (= 1 (count (:on-pairs join-node)))
       (let [build-cols (extract-scan-cols (:build-side join-node))]
         (and build-cols
              ;; No build-side columns at all referenced by parent.
              (empty? (clojure.set/intersection build-cols parent-cols))))))

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

    ;; Joins: NDV-based cardinality.
    ;; Output = probe_rows × build_rows / max(probe_ndv, build_ndv)
    ;; for the (first) join key. Falls back to the FK→PK heuristic
    ;; (`min(1.0, build_rows/build_total)`) when a side's scan or
    ;; key column isn't reachable, so multi-key / non-trivial join
    ;; chains keep producing a sensible number.
    (instance? PHashJoin node)
    (let [probe-rows  (long (or (::estimated-rows (meta (:probe-side node)))
                                (:length (:probe-side node))
                                1000000))
          build-total (long (or (:length (:build-side node)) 1000000))
          build-rows  (long (or (::estimated-rows (meta (:build-side node)))
                                build-total))
          [probe-key build-key] (first (:on-pairs node))
          probe-scan (let [n (:probe-side node)]
                       (cond (instance? PScan n) n
                             (instance? PChunkedScan n) n
                             (instance? PSIMDFilter n) (:input n)
                             (instance? PMaskFilter n) (:input n)
                             :else nil))
          build-scan (let [n (:build-side node)]
                       (cond (instance? PScan n) n
                             (instance? PChunkedScan n) n
                             (instance? PSIMDFilter n) (:input n)
                             (instance? PMaskFilter n) (:input n)
                             :else nil))
          probe-col (when probe-scan (get-in probe-scan [:columns probe-key]))
          build-col (when build-scan (get-in build-scan [:columns build-key]))
          probe-ndv (when probe-col (est/estimate-ndv probe-col probe-rows))
          build-ndv (when build-col (est/estimate-ndv build-col build-rows))]
      (if (and probe-ndv build-ndv)
        (max 1 (long (/ (* (double probe-rows) (double build-rows))
                        (double (max (long probe-ndv) (long build-ndv))))))
        (let [sel (min 1.0 (/ (double build-rows) (max 1.0 (double build-total))))]
          (max 1 (long (* probe-rows sel))))))

    ;; ASOF: each probe row matches ≤1 build row. LEFT preserves all probes;
    ;; INNER drops unmatched. Without selectivity stats we keep probe count.
    (instance? PAsofJoin node)
    (long (or (::estimated-rows (meta (:left node)))
              (:length (:left node))
              1000000))

    ;; Global agg always produces 1 row
    (or (instance? PStatsOnlyAgg node)
        (instance? PFusedSIMDAgg node) (instance? PFusedSIMDCount node)
        (instance? PChunkedSIMDAgg node) (instance? PChunkedSIMDCount node)
        (instance? PBlockSkipCount node) (instance? PFusedMultiSum node)
        (instance? PPercentileAgg node) (instance? PScalarAgg node)
        (instance? PFusedJoinGlobalAgg node))
    1

    ;; PSplitAgg: 1 row when no group keys (global); otherwise inherit
    ;; from a child plan (all children produce the same group cardinality).
    (instance? PSplitAgg node)
    (if (nil? (:group-keys node))
      1
      (long (or (::estimated-rows (meta (first (:children node))))
                1000000)))

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

(defn- collect-expr-refs!
  "Walk a normalized expression tree (possibly nested) and conj!
   every column keyword reference into `refs` (a volatile transient)."
  [refs e]
  (cond
    (keyword? e) (vswap! refs conj! e)
    (map? e)
    (do
      (doseq [a (:args e)]
        (collect-expr-refs! refs a))
      (when-let [branches (:branches e)]
        (doseq [b branches]
          (collect-expr-refs! refs (:val b))
          (when-let [p (:pred b)]
            ;; pred form `[op-args...]`; conj keyword args
            (doseq [x p] (when (keyword? x) (vswap! refs conj! x)))))))
    (sequential? e)
    (doseq [a e] (collect-expr-refs! refs a))))

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
                      (doseq [gk gks]
                        (cond
                          (keyword? gk) (vswap! refs conj! gk)
                          (map? gk)     (collect-expr-refs! refs gk)
                          (sequential? gk) (collect-expr-refs! refs gk))))
        ;; Aggs (post-materialization the :expr field is gone, but
        ;; before that pass — and for paths that don't lift — we may
        ;; still see :expr embedded in an agg.)
                    (when-let [aggs (:aggs node)]
                      (doseq [a aggs]
                        (when-let [c (:col a)] (vswap! refs conj! c))
                        (when-let [cs (:cols a)] (doseq [c cs] (when (keyword? c) (vswap! refs conj! c))))
                        (when-let [e (:expr a)] (collect-expr-refs! refs e))))
                    (when-let [agg (:agg node)]
                      (when-let [c (:col agg)] (vswap! refs conj! c))
                      (when-let [cs (:cols agg)] (doseq [c cs] (when (keyword? c) (vswap! refs conj! c))))
                      (when-let [e (:expr agg)] (collect-expr-refs! refs e)))
        ;; Project items: each may have :ref OR :expr. Walk both.
                    (when-let [items (:items node)]
                      (doseq [item items]
                        (when-let [r (:ref item)] (vswap! refs conj! r))
                        (when-let [n (:name item)] (vswap! refs conj! n))
                        (when-let [e (:expr item)] (collect-expr-refs! refs e))))
        ;; Join on-pairs
                    (when-let [on (:on-pairs node)]
                      (doseq [[l r] on] (vswap! refs conj! l) (vswap! refs conj! r)))
                    (when-let [js (:join-spec node)]
                      (doseq [[l r] (:on-pairs js)] (vswap! refs conj! l) (vswap! refs conj! r)))
        ;; ASOF match-condition columns
                    (when-let [mc (:match-condition node)]
                      (vswap! refs conj! (nth mc 1))
                      (vswap! refs conj! (nth mc 2)))
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
        ;; Materialized expressions (PMaterializeExpr top-level :expr)
                    (when-let [e (:expr node)]
                      (collect-expr-refs! refs e))
        ;; LTopN — order column + projection items. When :select is
        ;; nil (SELECT *) the streaming top-N fetches every scan
        ;; column at the surviving rows, so we conj every column of
        ;; the input LScan to keep column-pruning from dropping them.
        ;; LAnomaly — every column referenced by an anomaly
        ;; expression's arguments is live AFTER the join, so flag
        ;; them here so column-pruning keeps them on the build /
        ;; probe scans. Both short form (`[:anomaly-score "model"]`,
        ;; which falls through to the model's `:feature-names`) and
        ;; long form (`[:anomaly-score "model" e1 e2]`) are walked.
                    (when (instance? stratum.query.ir.LAnomaly node)
                      (doseq [[anom-expr _col-name] (:expr->col node)]
                        (let [explicit-args (drop 2 anom-expr)]
                          (if (seq explicit-args)
                            (doseq [a explicit-args]
                              (cond
                                (keyword? a) (vswap! refs conj! a)
                                (sequential? a) (collect-expr-refs! refs a)))
                            ;; Short form — pull feature names from the
                            ;; model. Models map keys are strings; the
                            ;; anomaly expression's second slot is the
                            ;; model name (string or keyword).
                            (let [model-name (let [m (second anom-expr)]
                                               (if (string? m) m (name m)))
                                  model      (get (:models node) model-name)]
                              (doseq [k (:feature-names model)]
                                (when (keyword? k)
                                  (vswap! refs conj! k))))))))
        ;; LStringMaterialize — every column referenced by a
        ;; deferred string expression is live post-join.
                    (when (instance? stratum.query.ir.LStringMaterialize node)
                      (doseq [{:keys [expr]} (:items node)]
                        (collect-expr-refs! refs expr)))
                    (when (instance? stratum.query.ir.LTopN node)
                      (doseq [spec (:order-specs node)]
                        (let [[c _] (if (vector? spec) spec [spec :asc])]
                          (when (keyword? c) (vswap! refs conj! c))))
                      (if-let [items (:select node)]
                        (doseq [item items]
                          (when-let [r (:ref item)] (vswap! refs conj! r))
                          (when-let [e (:expr item)] (collect-expr-refs! refs e)))
                        ;; SELECT *: top-N fetches every column from
                        ;; the input scan. The input may be either an
                        ;; LScan (rewrite ran before strategy-selection)
                        ;; or PScan/PChunkedScan (column-pruning is
                        ;; running later in the pipeline).
                        (let [in (:input node)]
                          (when (or (instance? stratum.query.ir.LScan in)
                                    (instance? stratum.query.ir.PScan in)
                                    (instance? stratum.query.ir.PChunkedScan in))
                            (doseq [k (keys (:columns in))]
                              (vswap! refs conj! k))))))
        ;; LHead — same shape as LTopN minus the order column. The
        ;; head executor materializes a prefix of every column it
        ;; touches, so the live-set must include either the
        ;; explicit `:select` items or the full scan column set
        ;; (SELECT *).
                    (when (instance? stratum.query.ir.LHead node)
                      (if-let [items (:select node)]
                        (doseq [item items]
                          (when-let [r (:ref item)] (vswap! refs conj! r))
                          (when-let [e (:expr item)] (collect-expr-refs! refs e)))
                        (let [in (:input node)]
                          (when (or (instance? stratum.query.ir.LScan in)
                                    (instance? stratum.query.ir.PScan in)
                                    (instance? stratum.query.ir.PChunkedScan in))
                            (doseq [k (keys (:columns in))]
                              (vswap! refs conj! k))))))
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
;; Pass: lift-filters-to-scan
;; ----------------------------------------------------------------------------
;; Final-stage pass that folds `PSIMDFilter` wrappers sitting DIRECTLY
;; above a scan into the scan's `:predicates` field.  This unifies
;; stratum's filter representation with DuckDB's
;; `LogicalGet::table_filters`: SIMD-eligible scan-adjacent filters
;; have a single attachment point (the scan node), which (a) lets
;; dynamic filters from joins target a stable spot at execute-time
;; and (b) avoids each downstream operator needing to special-case
;; the wrapper.
;;
;; **Why not `PMaskFilter`?**  `PMaskFilter` carries preds that
;; require the `prepare-preds` pipeline (string equality on raw
;; `String[]`, dict resolution, computed-expression preds) — they
;; need to be rewritten into a `__mask` column before they can be
;; evaluated.  Lifting them to the scan strips that contract: the
;; scan's `:predicates` would surface as raw preds via ctx and
;; downstream consumers (e.g. `execute-hash-join`'s `realize-mask`)
;; would try to compile them as numeric → string→long cast crash.
;; Keeping `PMaskFilter` as a wrapper preserves its
;; prepare-preds-via-`execute-filter` semantics; F19/F20/F21 don't
;; lose anything because they only consume numeric preds anyway
;; (F19's join-key range is numeric by gate; F20's zone-map
;; classification needs numeric preds; F21 operates on agg `:expr`,
;; not filter wrappers).
;; ============================================================================

(defn lift-filters-to-scan
  "Fold scan-adjacent `PSIMDFilter` wrappers into the scan's
   `:predicates` field.  `PMaskFilter` wrappers are left in place
   (see pass docstring above).

   Resolves dict-encoded string equality at lift time
   (`[col :eq \"foo\"]` → `[col :eq dict-code]`) so the lifted preds
   are safe for downstream consumers that don't go through
   `prepare-preds` again (e.g. `execute-hash-join`'s `realize-mask`).
   `simd-pred?` happily classifies `:eq` with a string argument as
   SIMD-eligible (it gates on op + column existence, not value
   type), so without this resolution we'd crash trying to compile a
   string into long-cast bytecode."
  [plan]
  (ir/walk-plan
   plan
   (fn [node]
     (let [is-filter? (instance? PSIMDFilter node)
           input      (when is-filter? (:input node))
           scan?      (or (instance? PScan input)
                          (instance? PChunkedScan input))]
       (if (and is-filter? scan?)
         (let [merged   (vec (concat (:predicates input) (:predicates node)))
               resolved (pred/resolve-dict-equality-preds merged (:columns input))]
           (assoc input :predicates resolved))
         node)))))

;; ============================================================================
;; Pass: top-N pushdown rewrite
;; ============================================================================

(def ^:dynamic *top-n-limit*
  "Maximum LIMIT value at which the planner rewrites LLimit-over-LSort
   into LTopN. Above this the legacy materialize-and-sort path runs.
   Mirrors `stratum.query.top-n/*top-n-limit*`."
  1024)

(defn- peel-project
  "If the node is `(LProject items LScan)`, return `[items LScan]`,
   otherwise `[nil node]`. Lets top-N capture an explicit SELECT
   projection while keeping the underlying scan for column lookup."
  [node]
  (cond
    (and (instance? stratum.query.ir.LProject node)
         (instance? stratum.query.ir.LScan (:input node)))
    [(:items node) (:input node)]

    (instance? stratum.query.ir.LScan node)
    [nil node]

    :else
    [nil nil]))

(defn- top-n-eligible-input?
  "An input below LSort qualifies if it's a plain scan or a project
   directly over a scan. The legacy `query.top-n/top-n-eligible?`
   gate requires `(empty? where)`, so anything with predicates
   (LFilter), joins, or aggregates leaves the materialize-and-sort
   path running."
  [node]
  (let [[_ scan] (peel-project node)]
    (some? scan)))

(defn- top-n-order-eligible?
  "Every ORDER BY column must be primitive numeric (`:int64` or
   `:float64`) and not a dict-string. The streaming heap supports
   any number of keys (multi-key compared in declared order, mixed
   asc/desc allowed) — same gate as `query.top-n/top-n-eligible?`."
  [order-specs scan-cols]
  (and (>= (count order-specs) 1)
       (every? (fn [spec]
                 (let [[col _dir] (if (vector? spec) spec [spec :asc])]
                   (and (keyword? col)
                        (let [c (get scan-cols col)]
                          (and c
                               (#{:int64 :float64} (:type c))
                               (not= :string (:dict-type c)))))))
               order-specs)))

(defn- find-scan-cols
  "Return the `:columns` map of the leaf LScan reachable through an
   optional LProject layer."
  [node]
  (let [[_ scan] (peel-project node)]
    (when scan (:columns scan))))

(def ^:dynamic *head-limit*
  "Maximum LIMIT value at which the planner rewrites a top-level
   `LLimit` (no `ORDER BY`, no filter, no aggregate) over a scan
   into `LHead`. Above this we fall through to the regular path
   (which materializes the whole scan via `PProject` /
   `materialize-columns`); for tiny LIMITs we'd rather pay one
   chunk fetch per column than decode every row group."
  100000)

(defn top-n-rewrite
  "Rewrite `LLimit { limit N input: LSort [single-spec] subtree }`
   into a single `LTopN`, when N ≤ *top-n-limit* and the input is
   filter/scan only. The executor delegates LTopN to
   `query.top-n/execute-top-n`, which streams the order column,
   keeps a fixed-size heap, and fetches just the surviving rows.

   Only applied to top-level LLimit (no other rewrites between LSort
   and LLimit) — anything more elaborate stays on the legacy
   materialize-and-sort path."
  [plan]
  (let [limit-node? (instance? stratum.query.ir.LLimit plan)
        sort-node   (when limit-node? (:input plan))
        sort?       (and sort-node (instance? stratum.query.ir.LSort sort-node))
        sort-input  (when sort? (:input sort-node))
        eligible-input? (and sort? (top-n-eligible-input? sort-input))
        scan-cols   (when eligible-input? (find-scan-cols sort-input))
        order-ok?   (and scan-cols
                         (top-n-order-eligible? (:order-specs sort-node)
                                                scan-cols))
        no-offset?  (and limit-node?
                         (or (nil? (:offset plan)) (zero? (long (:offset plan)))))
        small?      (and limit-node?
                         (some? (:limit plan))
                         (<= 0 (long (:limit plan)))
                         (<= (long (:limit plan)) (long *top-n-limit*)))]
    (if (and limit-node? sort? eligible-input? order-ok? no-offset? small?)
      ;; Capture any LProject between LSort and the scan: top-N's
      ;; row-fetch can apply the projection itself, dropping the
      ;; LProject so the LScan keeps every column the projection
      ;; references.
      (let [[items scan] (peel-project sort-input)]
        (with-meta
          (ir/->LTopN (vec (:order-specs sort-node))
                      (long (:limit plan))
                      items
                      scan)
          (meta plan)))
      plan)))

(defn head-rewrite
  "Rewrite `LLimit { limit N input: LScan }` (optionally with an
   intermediate `LProject` of bare column refs) into a single
   `LHead`, when N ≤ `*head-limit*` and there's no `ORDER BY`,
   `WHERE`, aggregate, join or window between the limit and the
   scan. The executor materializes only the first N rows of each
   referenced column, so `SELECT * FROM huge LIMIT 3` over an
   index-backed scan touches a single chunk per column instead of
   decoding the entire dataset.

   This is the LIMIT-without-ORDER-BY counterpart to
   `top-n-rewrite`. Bigger LIMITs (above `*head-limit*`) fall
   through to the regular `PProject + PLimit` path. The rewrite
   runs before `top-n-rewrite` would pick up an `LLimit (LSort)`,
   so it never competes with that pass — this only fires when
   there is no `LSort` in between."
  [plan]
  (let [limit-node? (instance? stratum.query.ir.LLimit plan)
        input       (when limit-node? (:input plan))
        ;; Eligible input shapes: bare LScan, or LProject items
        ;; (all bare column refs) over LScan. Anything else (sort,
        ;; filter, join, aggregate, window) means the LIMIT is
        ;; semantically tied to row order or count after that
        ;; operator, so we leave it on the regular path.
        [items scan] (when input (peel-project input))
        eligible?   (and limit-node?
                         scan
                         (instance? stratum.query.ir.LScan scan)
                         (or (nil? items)
                             (every? (fn [it]
                                       (and (:ref it) (keyword? (:ref it))
                                            (not (:expr it))))
                                     items)))
        no-offset?  (and limit-node?
                         (or (nil? (:offset plan)) (zero? (long (:offset plan)))))
        small?      (and limit-node?
                         (some? (:limit plan))
                         (<= 0 (long (:limit plan)))
                         (<= (long (:limit plan)) (long *head-limit*)))]
    (if (and eligible? no-offset? small?)
      (with-meta
        (ir/->LHead (long (:limit plan)) items scan)
        (meta plan))
      plan)))

;; ============================================================================
;; Pass: window-having pushdown
;; ============================================================================

(defn window-having-pushdown
  "Rewrite `LHaving (LProject items (LWindow specs input))` →
   `LProject items (LHaving preds (LWindow specs input))` when:
   - all `:items` are bare `{:name x :ref x}` column refs (no
     `:expr` projection items that materialize new columns), AND
   - every having predicate references a column produced by the
     window (output names) or already present in the LWindow input.

   Mirrors the legacy `query.clj` window-having pushdown
   (q.clj:758-816): evaluating the having against raw column arrays
   before LProject avoids materializing every input row when only
   a tiny fraction survive the post-window filter (e.g. ROW_NUMBER
   <= 2). H2O-Q8 (6M → 120K) is the canonical win."
  [plan]
  (ir/walk-plan
   plan
   (fn [node]
     (if (and (instance? LHaving node)
              (instance? LProject (:input node))
              (instance? LWindow (:input (:input node))))
       (let [proj    (:input node)
             win     (:input proj)
             items   (:items proj)
             ;; LHaving's `:predicates` are already normalized at
             ;; `build-logical-plan` time, so `pred-columns` can read
             ;; them directly.
             preds   (:predicates node)
             ;; Only push when project is trivial (no expr items)
             simple? (every? (fn [it]
                               (and (:ref it)
                                    (keyword? (:ref it))
                                    (not (:expr it))))
                             items)
             win-out  (set (map :as (:specs win)))
             win-in   (loop [n (:input win)]
                        (cond
                          (instance? LScan n)   (set (keys (:columns n)))
                          (instance? LFilter n) (recur (:input n))
                          :else                 #{}))
             visible  (clojure.set/union win-out win-in)
             pred-cols (mapcat pred-columns preds)
             refs-window? (some win-out pred-cols)
             refs-only-visible?
             (every? #(contains? visible %) pred-cols)]
         (if (and simple? refs-window? refs-only-visible?)
           (ir/->LProject items
                          (ir/->LHaving preds win))
           node))
       node))))

;; ============================================================================
;; Composite: full optimization pipeline
;; ============================================================================

(defn optimize
  "Run all optimization passes on a logical plan, producing a physical plan.
   Top-level metadata (e.g. ::output-format, ::order-only-keys) is
   preserved across passes since individual rewrites may drop it."
  [plan]
  (let [m (meta plan)
        out (-> plan
                annotate
                predicate-pushdown
                ;; HAVING → WHERE runs immediately after WHERE pushdown
                ;; so any predicates we lift below LGroupBy can in turn
                ;; be pushed further down (through joins, etc.) by the
                ;; next pass that touches `LFilter`. Currently the only
                ;; such pass is `predicate-pushdown` itself, but its
                ;; fixpoint loop already exited; re-running it would be
                ;; correct but rarely useful (HAVING preds reference
                ;; group columns from `LGroupBy`, which is above any
                ;; join). Position is for symmetry with DuckDB's
                ;; FilterPushdown.
                having-to-where-pushdown
                join-reorder
                zone-map-annotation
                ;; Window-having pushdown runs before expr-materialization
                ;; so the rewritten `LHaving (LWindow ...)` shape is the
                ;; one strategy-selection sees.
                window-having-pushdown
                ;; Algebraic rewrite of `SUM(a·(c−b))` → `c·SUM(a) −
                ;; SUM_PRODUCT(a,b)`, wrapped in PRecompose for output
                ;; reassembly. Runs BEFORE expr-materialization so the
                ;; SMCS pattern matches its `:expr` shape (after that
                ;; pass, exprs are gone). Disjoint from linear-agg-rewrite:
                ;; linear matches `s·x + o`, SMCS matches `a·(c−b)`.
                smcs-decomposition
                ;; Algebraic rewrite of `SUM/AVG/MIN/MAX(s·col + o)` → base
                ;; agg on `col` + decode-time recipe. Runs BEFORE
                ;; expr-materialization so the temp-column shim is
                ;; bypassed entirely (no extra column materialisation,
                ;; SIMD long path stays eligible).
                linear-agg-rewrite
                expr-materialization
                ;; Top-N rewrite runs BEFORE strategy-selection so it operates
                ;; on logical IR (`LLimit (LSort scan)`) rather than physical
                ;; (`PLimit (PSort scan)`). Also runs BEFORE column-pruning so
                ;; the `LScan` referenced by `LTopN` keeps all output columns
                ;; — the streaming top-N fetches each surviving row's full
                ;; column set, not just the order column.
                top-n-rewrite
                ;; Head rewrite is the LIMIT-without-ORDER-BY counterpart.
                ;; Same constraints (runs before strategy-selection so it
                ;; sees `LLimit (LScan)` / `LLimit (LProject LScan)`, and
                ;; before column-pruning so the LScan keeps every column
                ;; the head fetches).
                head-rewrite
                strategy-selection
                operator-fusion
                stats-propagation
                column-pruning
                ;; Final pass: any `PSIMDFilter`/`PMaskFilter` wrapper
                ;; that didn't get absorbed by strategy-selection or
                ;; operator-fusion (e.g. standalone `SELECT * WHERE p`
                ;; with no agg) and that sits DIRECTLY above a scan
                ;; gets folded into the scan's `:predicates` field.
                ;; Provides the single attachment point F19 needs for
                ;; runtime dynamic filters from joins. See pass for
                ;; details.
                lift-filters-to-scan)]
    (if (and m (instance? clojure.lang.IObj out))
      (vary-meta out merge m)
      out)))

;; ============================================================================
;; Plan explanation (for debugging and EXPLAIN output)
;;
;; The pipeline is:
;;
;;   physical-plan  --plan->data-->  data tree  --render-text-->  Postgres-style string
;;                                                --render-json-->  DuckDB-shape JSON data
;;
;; The data tree is a recursive map keyed by:
;;   :op           — operator class simple name (string)
;;   :est-rows     — estimated cardinality (long or nil)
;;   :sel          — selectivity (double or nil)
;;   :actual-rows  — actual row count (long or nil; ANALYZE only)
;;   :time-ms      — wall-clock millis (double or nil; ANALYZE only)
;;   :details      — vec of [label string-value] pairs (in display order)
;;   :children     — vec of child data nodes
;;   :child-tags   — vec of role labels parallel to :children (e.g. ["build" "probe"])
;;                   nil for unary nodes / parallel branches.
;; ============================================================================

(defn- format-val
  "Format a literal predicate value for display."
  [v]
  (cond
    (nil? v)     "NULL"
    (string? v)  (str "'" v "'")
    (keyword? v) (name v)
    (set? v)     (str "(" (clojure.string/join ", "
                                               (mapv format-val
                                                     (sort-by str v)))
                      ")")
    :else        (str v)))

(declare format-col-or-expr)

(defn- format-col-or-expr
  "Format an LHS that may be a keyword column or a normalized expr map."
  [x]
  (cond
    (keyword? x) (name x)
    (map? x)
    (let [{:keys [op args]} x
          arity-2-infix {:add "+" :sub "-" :mul "*" :div "/" :mod "%"}]
      (cond
        (contains? arity-2-infix op)
        (str "(" (clojure.string/join (str " " (arity-2-infix op) " ")
                                      (mapv format-col-or-expr args))
             ")")
        :else
        (str (name (or op :expr))
             "("
             (clojure.string/join ", " (mapv format-col-or-expr args))
             ")")))
    :else (pr-str x)))

(defn- format-pred
  "Format a normalized predicate vector to a SQL-like string."
  [pred]
  (let [op (second pred)]
    (case op
      :or         (str "("
                       (clojure.string/join " OR "
                                            (mapv format-pred (subvec (vec pred) 2)))
                       ")")
      :lt         (str "(" (format-col-or-expr (first pred)) " < "  (format-val (nth pred 2)) ")")
      :gt         (str "(" (format-col-or-expr (first pred)) " > "  (format-val (nth pred 2)) ")")
      :lte        (str "(" (format-col-or-expr (first pred)) " <= " (format-val (nth pred 2)) ")")
      :gte        (str "(" (format-col-or-expr (first pred)) " >= " (format-val (nth pred 2)) ")")
      :eq         (str "(" (format-col-or-expr (first pred)) " = "  (format-val (nth pred 2)) ")")
      :neq        (str "(" (format-col-or-expr (first pred)) " != " (format-val (nth pred 2)) ")")
      :range      (str "(" (format-col-or-expr (first pred))
                       " BETWEEN " (format-val (nth pred 2))
                       " AND "     (format-val (nth pred 3)) ")")
      :not-range  (str "(" (format-col-or-expr (first pred))
                       " NOT BETWEEN " (format-val (nth pred 2))
                       " AND "         (format-val (nth pred 3)) ")")
      :in         (str "(" (format-col-or-expr (first pred))
                       " IN " (format-val (nth pred 2)) ")")
      :not-in     (str "(" (format-col-or-expr (first pred))
                       " NOT IN " (format-val (nth pred 2)) ")")
      :like       (str "(" (format-col-or-expr (first pred)) " LIKE "       (format-val (nth pred 2)) ")")
      :not-like   (str "(" (format-col-or-expr (first pred)) " NOT LIKE "   (format-val (nth pred 2)) ")")
      :ilike      (str "(" (format-col-or-expr (first pred)) " ILIKE "      (format-val (nth pred 2)) ")")
      :not-ilike  (str "(" (format-col-or-expr (first pred)) " NOT ILIKE "  (format-val (nth pred 2)) ")")
      :is-null      (str "(" (format-col-or-expr (first pred)) " IS NULL)")
      :is-not-null  (str "(" (format-col-or-expr (first pred)) " IS NOT NULL)")
      :contains     (str "(" (format-col-or-expr (first pred)) " CONTAINS "    (format-val (nth pred 2)) ")")
      :starts-with  (str "(" (format-col-or-expr (first pred)) " STARTS WITH " (format-val (nth pred 2)) ")")
      :ends-with    (str "(" (format-col-or-expr (first pred)) " ENDS WITH "   (format-val (nth pred 2)) ")")
      :fn           (str "(" (format-col-or-expr (first pred)) " <fn>)")
      (pr-str pred))))

(defn- format-preds
  "Format a vector of predicates as a single SQL-like AND-conjunction."
  [preds]
  (if (= 1 (count preds))
    (format-pred (first preds))
    (str "("
         (clojure.string/join " AND " (mapv format-pred preds))
         ")")))

(defn- format-agg
  "Format a normalized aggregate map. Recognizes :as alias."
  [agg]
  (let [{:keys [op col cols p as expr]} agg
        body (cond
               expr             (str (name (or op :agg)) "(" (pr-str expr) ")")
               cols             (str (name op) "("
                                     (clojure.string/join ", "
                                                          (mapv format-col-or-expr cols))
                                     ")")
               (and p col)      (str (name op) "(" (format-col-or-expr col)
                                     ", " p ")")
               col              (str (name op) "(" (format-col-or-expr col) ")")
               (= :count op)    "count(*)"
               :else            (str (name op) "()"))]
    (if as (str body " AS " (name as)) body)))

(defn- format-aggs [aggs]
  (str "[" (clojure.string/join ", " (mapv format-agg aggs)) "]"))

(defn- format-on-pairs
  "Format `[[l r] ...]` join keys as `(l = r AND ...)`."
  [on-pairs]
  (clojure.string/join " AND "
                       (mapv (fn [[l r]] (str (format-col-or-expr l)
                                              " = "
                                              (format-col-or-expr r)))
                             on-pairs)))

(defn- format-match-cond
  "Format an asof match condition [op l r] as `l <op> r`."
  [[op l r]]
  (let [sym (case op
              :lt "<" :gt ">" :lte "<=" :gte ">="
              :eq "=" (name op))]
    (str (format-col-or-expr l) " " sym " " (format-col-or-expr r))))

;; ---- Detail-entry helpers (each returns [label value] or nil) -------------

(defn- d-filter [preds] (when (seq preds) ["Filter" (format-preds preds)]))
(defn- d-group-keys [ks] (when (seq ks) ["Group Keys" (pr-str (vec ks))]))
(defn- d-aggs [aggs] (when (seq aggs) ["Aggregates" (format-aggs aggs)]))
(defn- d-agg  [agg]  (when agg ["Aggregate" (format-agg agg)]))
(defn- d-columns [cols] (when (seq cols) ["Columns" (pr-str (vec (keys cols)))]))
(defn- d-length  [len]  (when len ["Length" (str len)]))
(defn- d-zone-pruned [n]
  (when (instance? PChunkedScan n)
    ["Zone Pruned" (if (:surviving-chunks n) "yes" "no")]))

(defn- node-details
  "Return a vec of [label value] entries for a physical node's labeled
   sub-info lines, in display order. Nils are dropped."
  [node]
  (->>
   (cond
     (instance? PScan node)
     [(d-columns (:columns node))
      (d-length  (:length node))
      (d-filter  (:predicates node))]

     (instance? PChunkedScan node)
     [(d-columns (:columns node))
      (d-length  (:length node))
      (d-zone-pruned node)
      (d-filter  (:predicates node))]

     (or (instance? PSIMDFilter node) (instance? PMaskFilter node))
     [(d-filter (:predicates node))]

     (instance? PStatsOnlyAgg node)
     [(d-aggs (:aggs node))
      (d-filter (:predicates node))]

     (or (instance? PFusedSIMDAgg node)
         (instance? PFusedSIMDCount node)
         (instance? PChunkedSIMDAgg node)
         (instance? PChunkedSIMDCount node)
         (instance? PBlockSkipCount node))
     [(d-agg (:agg node))
      (d-filter (:predicates node))]

     (instance? PFusedMultiSum node)
     [(d-aggs (:aggs node))
      (d-filter (:predicates node))]

     (instance? PPercentileAgg node)
     [(d-aggs (:aggs node))
      (d-filter (:predicates node))]

     (instance? PScalarAgg node)
     [(d-aggs (:aggs node))
      (d-filter (:predicates node))]

     (instance? PSplitAgg node)
     [(d-aggs (:agg-order node))
      (d-group-keys (:group-keys node))
      ["Strategies" (str (count (:children node)))]]

     (instance? PChunkedDenseGroupBy node)
     [(d-group-keys (:group-keys node))
      (d-aggs (:aggs node))
      ["Max Key" (str (:max-key node))]
      (d-filter (:predicates node))]

     (instance? PDenseGroupBy node)
     [(d-group-keys (:group-keys node))
      (d-aggs (:aggs node))
      ["Max Key" (str (:max-key node))]
      (d-filter (:predicates node))]

     (instance? PHashGroupBy node)
     [(d-group-keys (:group-keys node))
      (d-aggs (:aggs node))
      (d-filter (:predicates node))]

     (instance? PFusedExtractCount node)
     [["Extract" (str (name (:extract-op node))
                      "(" (format-col-or-expr (:extract-col node)) ")")]
      (d-aggs (:aggs node))]

     (instance? PHashJoin node)
     [["Join Type" (name (:join-type node))]
      ["Hash Cond" (format-on-pairs (:on-pairs node))]]

     (instance? PPerfectHashJoin node)
     [["Join Type" (name (:join-type node))]
      ["Hash Cond" (format-on-pairs (:on-pairs node))]
      ["Min Key" (str (:min-key node))]
      ["Key Range" (str (:key-range node))]]

     (instance? PAsofJoin node)
     [["Join Type" (name (:join-type node))]
      ["Equi Keys" (if (seq (:on-pairs node))
                     (format-on-pairs (:on-pairs node))
                     "(none)")]
      ["Match Cond" (format-match-cond (:match-condition node))]]

     (instance? PBitmapSemiJoin node)
     (let [js (:join-spec node)]
       [["Probe Key" (format-col-or-expr (:probe-key js))]
        ["Build Key" (format-col-or-expr (:build-key js))]])

     (instance? PFusedJoinGroupAgg node)
     [["Join" (pr-str (dissoc (:join-spec node) :build-side))]
      (d-group-keys (:group-keys node))
      (d-aggs (:aggs node))]

     (instance? PFusedJoinGlobalAgg node)
     [["Join" (pr-str (dissoc (:join-spec node) :build-side))]
      (d-aggs (:aggs node))]

     (instance? PProject node)
     [["Output" (pr-str (mapv (fn [i] (or (:as i) (:expr i) i)) (:items node)))]]

     (instance? PWindow node)
     [["Specs" (pr-str (mapv #(select-keys % [:fn :partition-by :order-by :as])
                             (:specs node)))]]

     (instance? PHaving node)
     [(d-filter (:predicates node))]

     (instance? PSort node)
     (let [{:keys [order-specs limit offset]} node]
       [["Order" (pr-str order-specs)]
        (when limit ["Limit" (str limit)])
        (when offset ["Offset" (str offset)])])

     (instance? PLimit node)
     [(when (:limit node) ["Limit" (str (:limit node))])
      (when (:offset node) ["Offset" (str (:offset node))])]

     (instance? PMaterializeExpr node)
     [["Column" (name (:col-name node))]
      ["Expr"   (format-col-or-expr (:expr node))]
      (when (:target node) ["Target" (name (:target node))])]

     (instance? PRecompose node)
     [["SMCS Recompose"
       (let [n (count (filter #(= :sum-mul-const-sub (:type (val %)))
                              (:mapping node)))]
         (str n " agg(s)"))]]

     (instance? LScan node)
     [(d-columns (:columns node))
      (d-length  (:length node))
      (d-filter  (:predicates node))]

     (instance? LFilter node)
     [(d-filter (:predicates node))]

     (instance? LJoin node)
     [["Join Type" (name (:join-type node))]
      (when (seq (:on-pairs node))
        ["Cond" (format-on-pairs (:on-pairs node))])]

     (instance? LAsofJoin node)
     [["Join Type" (name (:join-type node))]
      (when (seq (:on-pairs node))
        ["Equi Keys" (format-on-pairs (:on-pairs node))])
      (when (:match-condition node)
        ["Match Cond" (format-match-cond (:match-condition node))])]

     (instance? LGroupBy node)
     [(d-group-keys (:group-keys node))
      (d-aggs (:aggs node))]

     (instance? LGlobalAgg node)
     [(d-aggs (:aggs node))]

     :else
     [])
   (filterv some?)))

(defn- node-children
  "Return a [children-vec child-tags-vec] pair for a plan node.
   child-tags-vec is nil for plain unary nodes."
  [node]
  (cond
    (instance? LJoin node)
    [[(:left node) (:right node)] ["left" "right"]]

    (instance? LAsofJoin node)
    [[(:left node) (:right node)] ["left" "right"]]

    (instance? PAsofJoin node)
    [[(:left node) (:right node)] ["probe" "build"]]

    (instance? PHashJoin node)
    [[(:build-side node) (:probe-side node)] ["build" "probe"]]

    (instance? PPerfectHashJoin node)
    [[(:build-side node) (:probe-side node)] ["build" "probe"]]

    (instance? PFusedJoinGroupAgg node)
    [[(:left node) (:right node)] ["fact" "dim"]]

    (instance? PFusedJoinGlobalAgg node)
    [[(:left node) (:right node)] ["fact" "dim"]]

    (instance? PBitmapSemiJoin node)
    (let [bs (get-in node [:join-spec :build-side])]
      (if bs
        [[(:input node) bs] ["input" "dim"]]
        [[(:input node)] nil]))

    (instance? PSplitAgg node)
    [(vec (:children node)) nil]

    :else
    (if-let [c (ir/input-node node)]
      [[c] nil]
      [[] nil])))

(defn plan->data
  "Convert a physical (or logical) plan tree into a serializable data
   tree of `node-info` maps. See namespace doc above for shape.

   `:node-id` is the System/identityHashCode of the physical node — used
   by `merge-analyze-timings` to look up per-node ANALYZE measurements.
   It is not rendered in text or JSON output."
  [plan]
  (let [m (meta plan)
        [kids tags] (node-children plan)]
    {:op          (.getSimpleName (class plan))
     :node-id     (System/identityHashCode plan)
     :est-rows    (::estimated-rows m)
     :sel         (::selectivity m)
     :details     (node-details plan)
     :children    (mapv plan->data kids)
     :child-tags  tags}))

(defn merge-analyze-timings
  "Given a plan data tree and a `{node-identity-hash -> {:time-ns N :rows N}}`
   collector map, return the data tree with `:actual-rows` and `:time-ms`
   attached on every node where a measurement was recorded."
  [data timings]
  (let [m (get timings (:node-id data))
        merged-kids (mapv #(merge-analyze-timings % timings) (:children data))]
    (cond-> (assoc data :children merged-kids)
      m (assoc :actual-rows (:rows m)
               :time-ms (/ (double (:time-ns m)) 1.0e6)))))

;; ---- Text renderer (Postgres style) ---------------------------------------

(defn- pad ^String [^long n]
  (apply str (repeat n \space)))

(defn- cost-suffix
  "Render `(est-rows=N sel=S.SSS actual-rows=M time=T.TTms)` Postgres-style.
   Returns empty string when there's nothing to show."
  [{:keys [est-rows sel actual-rows time-ms]}]
  (let [parts (cond-> []
                est-rows    (conj (str "est-rows=" est-rows))
                sel         (conj (format "sel=%.3f" (double sel)))
                actual-rows (conj (str "actual-rows=" actual-rows))
                time-ms     (conj (format "time=%.3fms" (double time-ms))))]
    (if (seq parts)
      (str "  (" (clojure.string/join " " parts) ")")
      "")))

(defn- render-data-lines
  "Returns a vec of strings for a data tree at the given depth.
   `tag` is an optional role label (e.g. \"build\") for branching parents."
  [data depth tag]
  (let [op-col   (+ 2 (* 6 (long depth)))
        sub-col  (+ op-col 2)
        head-sym (cond
                   (zero? depth)        (pad 1)
                   (some? tag)          (str (pad (- op-col 4)) "->  ")
                   :else                (str (pad (- op-col 4)) "->  "))
        head-tag (if tag (str "[" tag "] ") "")
        head     (str head-sym head-tag (:op data) (cost-suffix data))
        details  (mapv (fn [[label value]]
                         (str (pad sub-col) label ": " value))
                       (:details data))
        kids     (:children data)
        ktags    (:child-tags data)
        child-lines
        (vec
         (mapcat (fn [i child]
                   (render-data-lines child
                                      (inc depth)
                                      (when ktags (nth ktags i))))
                 (range) kids))]
    (into [head] (into details child-lines))))

(defn render-text
  "Render a plan data tree to a Postgres-style indented multi-line string.

   If `data` contains `:execution-time-ms` at the root (set by the
   ANALYZE entry point), appends a Postgres-style `Execution Time:` line."
  [data]
  (let [body (clojure.string/join "\n" (render-data-lines data 0 nil))
        et   (:execution-time-ms data)]
    (if et
      (str body "\n Execution Time: " (format "%.3f ms" (double et)))
      body)))

;; ---- JSON renderer (DuckDB-compatible shape) ------------------------------

(defn- data->json-node
  "Convert a plan data node to a DuckDB-shape map:
     {\"name\" op, \"children\" [...], \"extra_info\" {...}}
   Internal numeric keys use the `__...__` convention DuckDB uses."
  [{:keys [op est-rows sel actual-rows time-ms details children child-tags] :as data}]
  (let [extra (cond-> (reduce (fn [m [k v]] (assoc m k v)) {} details)
                est-rows    (assoc "__estimated_cardinality__" est-rows)
                sel         (assoc "__selectivity__" sel)
                actual-rows (assoc "__cardinality__" actual-rows)
                time-ms     (assoc "__timing__" time-ms))
        kids (mapv (fn [i child]
                     (let [json-child (data->json-node child)]
                       (if child-tags
                         (assoc json-child "role" (nth child-tags i))
                         json-child)))
                   (range) children)]
    {"name" op
     "children" kids
     "extra_info" extra}))

(defn render-json
  "Render a plan data tree as a Clojure data structure matching
   DuckDB's JSON EXPLAIN shape: a one-element vector wrapping the root."
  [data]
  [(data->json-node data)])

;; ---- Public entry point ---------------------------------------------------

(defn explain
  "Return a Postgres-style human-readable string describing the plan tree.

   (println (explain (optimize (build-logical-plan query))))

   For structured access, use `plan->data` + `render-text`/`render-json`
   directly."
  [plan]
  (render-text (plan->data plan)))
