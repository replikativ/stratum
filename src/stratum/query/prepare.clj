(ns stratum.query.prepare
  "Lower a user-facing query map into engine-ready form.

   The legacy `q` body has always run a sequence of pre-passes inline
   before main execution. They lower expressions, string predicates,
   and dict-encoded equality predicates into plain temp-column refs so
   that the engine paths (chunked SIMD, group-by, etc.) only see flat
   shapes. The IR planner needs the same lowering — without it, raw
   Clojure vector expressions reach the executor and explode at
   `eval-expr-polymorphic`.

   This namespace extracts that pipeline into a single function so
   both the legacy `q` body and `stratum.query.executor/run-query` can
   converge on the same source of truth.

   Pipeline (mirrors legacy `q` lines ~502–625):

     0. Normalize predicates and aggregates.
     1. Pre-materialize string-producing predicate expressions
        (e.g. `LOWER(name) = 'bob'`) into dict-encoded temp columns.
     2. Pre-materialize numeric predicate expressions
        (e.g. `[:> [:* :a :b] 1000]`) into double[] temp columns.
     3. Materialize string predicates (LIKE / CONTAINS) into mask
        columns referenced as `[:_mask_N :eq 1]`.
     4. Resolve dict-encoded equality predicates by replacing the
        right-hand string/keyword with its dict-id.
     5. Pre-materialize string-producing expressions appearing in
        SELECT / GROUP BY / aggs into dict-encoded temp columns.

   The function returns the cleaned (preds, aggs, group, select,
   columns) plus the `columns-meta` map the caller should bind to
   `expr/*columns-meta*`. The caller decides whether to apply zone-map
   pruning (legacy-only) or pass the columns through to the planner."
  (:require [stratum.query.normalization :as norm]
            [stratum.query.expression :as expr]
            [stratum.query.predicate :as pred]
            [stratum.query.execution :as x]
            [stratum.iforest :as iforest]))

(set! *warn-on-reflection* true)

(defn- pre-materialize-string-pred-exprs
  "Pass 1: lift string-producing expressions out of predicate LHS
   positions into dict-encoded temp columns."
  [preds columns length]
  (if (some #(and (map? (first %))
                  (expr/string-producing-expr? (first %)))
            preds)
    (let [counter (atom 0)]
      (reduce (fn [[ps cs] pred]
                (let [col-ref (first pred)]
                  (if (and (map? col-ref) (expr/string-producing-expr? col-ref))
                    (let [n         (swap! counter inc)
                          col-name  (keyword (str "__pred_str_" n))
                          col-entry (expr/eval-string-expr col-ref cs length)]
                      [(conj ps (assoc pred 0 col-name))
                       (assoc cs col-name col-entry)])
                    [(conj ps pred) cs])))
              [[] columns]
              preds))
    [preds columns]))

(defn- pre-materialize-numeric-pred-exprs
  "Pass 2: lift numeric expressions out of predicate LHS positions
   into double[] temp columns."
  [preds columns length]
  (if (some #(map? (first %)) preds)
    (x/materialize-computed-preds preds columns length)
    [preds columns]))

(defn- materialize-string-preds
  "Pass 3: rewrite LIKE / CONTAINS / STARTS_WITH / ENDS_WITH predicates
   into mask columns referenced via plain equality."
  [preds columns length]
  (if (some #(pred/string-pred-ops (second %)) preds)
    (let [mat-cols (x/materialize-columns columns)]
      (pred/materialize-string-preds preds mat-cols length))
    [preds columns]))

(defn- resolve-dict-equality
  "Pass 4: where the right-hand side is a string/keyword and the
   predicate is dict-resolvable, replace the literal with its dict-id."
  [preds columns]
  (let [has-dict-pred?
        (some #(and (pred/dict-resolvable-ops (second %))
                    (>= (count %) 3)
                    (let [v (nth % 2)]
                      (or (string? v) (keyword? v))))
              preds)]
    (if has-dict-pred?
      (pred/resolve-dict-equality-preds
       (mapv (fn [pred]
               (if (and (>= (count pred) 3) (keyword? (nth pred 2)))
                 (assoc pred 2 (str (nth pred 2)))
                 pred))
             preds)
       columns)
      preds)))

(defn- compile-non-simd-preds-to-mask
  "Pass 4b: OR / IN / NOT-IN / :fn predicates aren't SIMD-routable,
   so the legacy compiles them into a single mask column referenced
   as `[:__mask :eq 1]`. Only materializes the columns that the
   non-SIMD preds actually reference, leaving the rest index-backed
   so chunked SIMD paths stay eligible."
  [preds columns length]
  (let [[simd-preds non-simd-preds] (pred/split-preds preds columns)]
    (if (seq non-simd-preds)
      (let [pred-col-keys
            (let [ks (atom #{})]
              (letfn [(walk [p]
                        (let [op (second p)]
                          (case op
                            :or (doseq [sub (subvec p 2)] (walk sub))
                            :fn (swap! ks conj (first p))
                            (:in :not-in) (swap! ks conj (first p))
                            (when (keyword? (first p)) (swap! ks conj (first p))))))]
                (doseq [p non-simd-preds] (walk p)))
              @ks)
            partial-mat
            (reduce (fn [cols k]
                      (if-let [c (get cols k)]
                        (assoc cols k (x/materialize-column c))
                        cols))
                    columns pred-col-keys)
            mask-fn (pred/compile-pred-mask non-simd-preds partial-mat)
            mask-arr (mask-fn length)]
        [(conj simd-preds [:__mask :eq 1])
         (assoc partial-mat :__mask {:type :int64 :data mask-arr})])
      [simd-preds columns])))

(defn- pre-materialize-string-group-agg-exprs
  "Pass 5a (no-join eager path): lift string-producing expressions
   out of GROUP BY and aggregate slots into dict-encoded temp
   columns. Mirrors the legacy `expr/materialize-string-exprs`
   call in `q`. Used only when no join is present — see
   `string-expr-spec-group-agg` for the deferred (post-join)
   counterpart."
  [group aggs select columns length]
  (let [;; A group key may be: keyword | bare expr (sequential) |
        ;; aliased expr ({:expr e :as a}). Pull the raw expression out
        ;; for the string-producing check; non-expressions skip.
        group-expr (fn [g]
                     (cond
                       (map? g)        (:expr g)
                       (sequential? g) g
                       :else           nil))
        has-string-expr?
        (or (some #(when-let [e (group-expr %)]
                     (expr/string-producing-expr? (norm/normalize-expr (vec e))))
                  group)
            (some #(and (:expr %)
                        (expr/string-producing-expr? (:expr %)))
                  aggs))]
    (if has-string-expr?
      (expr/materialize-string-exprs group aggs select columns length)
      [group aggs select columns])))

(defn- normalize-and-materialize-select-items
  "Pass 5b (no-join eager path): normalize SELECT items via
   `x/normalize-select-item`, then lift any `:expr` slot whose
   normalized expression is string-producing into a dict-encoded
   temp column. Mirrors the legacy block at q.clj:722-740."
  [select columns length]
  (when (seq select)
    (let [items (vec (map-indexed #(x/normalize-select-item %2 %1) select))
          has-string-expr?
          (some #(and (:expr %) (expr/string-producing-expr? (:expr %)))
                items)]
      (if has-string-expr?
        (let [counter (atom 0)]
          (reduce (fn [[its cs] sel]
                    (if (and (:expr sel) (expr/string-producing-expr? (:expr sel)))
                      (let [n         (swap! counter inc)
                            col-name  (keyword (str "__sel_str_" n))
                            col-entry (expr/eval-string-expr (:expr sel) cs length)]
                        [(conj its (-> sel (dissoc :expr) (assoc :ref col-name)))
                         (assoc cs col-name col-entry)])
                      [(conj its sel) cs]))
                  [[] columns]
                  items))
        [items columns]))))

;; ============================================================================
;; Static string-expr lowering (deferred materialization for join queries)
;; ============================================================================
;;
;; When a join is present we can't run `eval-string-expr` here — it would
;; size the materialized column to the pre-join row count, breaking after
;; the join. The two functions below mirror the eager passes above but
;; only REWRITE the slots and emit a runtime spec; the actual evaluation
;; happens at `LStringMaterialize` execute time, with a post-join column
;; ctx in scope.
;;
;; Spec items have the shape `{:col-name kw :expr normalized-expr}` and
;; the synthetic name is `__str_expr_N` for both group/agg and select
;; sources (a single counter is threaded through so names don't collide).

(defn- string-expr-spec-group-agg
  "Walk `:group` and `:agg :expr` slots for string-producing expressions,
   replace each with a fresh `__str_expr_N` keyword and emit the matching
   `{:col-name :expr}` spec entry. Returns
   `{:group :aggs :select :items :next-counter}`. `select` is passed
   through unchanged — pass 5b's own deferred variant handles SELECT
   string exprs."
  [group aggs select start-counter]
  (let [counter (atom (long (or start-counter 0)))
        items   (java.util.ArrayList.)
        synth   (fn [expr]
                  (swap! counter inc)
                  (let [col-name (keyword (str "__str_expr_" @counter))]
                    (.add items {:col-name col-name :expr expr})
                    col-name))
        new-group (mapv (fn [g]
                          (cond
                            (keyword? g) g

                            ;; Alias-wrapped expression: keep the alias
                            ;; on the synthetic temp column when string-
                            ;; producing, so the downstream IR carries
                            ;; the user-facing name into the result.
                            (map? g)
                            (let [e (:expr g)
                                  norm-g (norm/normalize-expr (vec e))]
                              (if (expr/string-producing-expr? norm-g)
                                (synth norm-g)
                                g))

                            :else
                            (let [norm-g (norm/normalize-expr (vec g))]
                              (if (expr/string-producing-expr? norm-g)
                                (synth norm-g)
                                g))))
                        group)
        new-aggs  (mapv (fn [agg]
                          (if-let [e (:expr agg)]
                            (if (expr/string-producing-expr? e)
                              (-> agg (dissoc :expr) (assoc :col (synth e)))
                              agg)
                            agg))
                        aggs)]
    {:group         new-group
     :aggs          new-aggs
     :select        select
     :items         (vec items)
     :next-counter  @counter}))

(defn- string-expr-spec-select
  "Walk normalized SELECT items for `:expr` slots whose expression is
   string-producing, replace each with a `:ref` to a synthetic
   `__str_expr_N` column, and emit the matching spec entry. Reuses
   the counter from `string-expr-spec-group-agg` so names stay unique
   across both passes."
  [select-items start-counter]
  (when (seq select-items)
    (let [counter (atom (long (or start-counter 0)))
          items   (java.util.ArrayList.)
          new-items
          (mapv (fn [sel]
                  (if (and (:expr sel) (expr/string-producing-expr? (:expr sel)))
                    (let [n        (swap! counter inc)
                          col-name (keyword (str "__str_expr_" n))]
                      (.add items {:col-name col-name :expr (:expr sel)})
                      (-> sel (dissoc :expr) (assoc :ref col-name)))
                    sel))
                select-items)]
      {:select       new-items
       :items        (vec items)
       :next-counter @counter})))

(defn prepare-query
  "Lower `query` against `columns` (length=`length`) into engine-ready
   form. Returns
     {:preds <preds>            ; flat predicates referencing real cols
      :aggs <normalized-aggs>   ; aggs whose :col may point at temps
      :group <group>            ; group keys, possibly mixed
      :select <select>          ; normalized select items (or nil)
      :columns <columns>        ; possibly grew by temp-column entries
      :columns-meta <map>}      ; dict-info for `expr/*columns-meta*`

   The query map's other slots (`:order`, `:limit`, `:distinct`,
   `:window`, `:having`, `:join`, `:from`) are not touched here.

   When `:join` is non-empty the predicate-lowering passes (numeric
   expr, string pred, dict eq, non-SIMD mask) are skipped — the
   filter applies after the join, when the joined columns are in
   scope, so the executor's per-filter `prepare-preds` runs them
   at filter time instead. Predicates are still normalized."
  [{:keys [where agg group select join]} columns length]
  (let [has-join? (boolean (seq join))
        ;; Recognize the `[:as expr alias]` shape on `:group` and rewrite
        ;; into the canonical `{:expr expr :as alias}` marker map so the
        ;; downstream `normalize-expr` calls (string-producing-expr?
        ;; checks) can handle them uniformly via `(:expr g)`. Plain
        ;; keywords and bare expressions pass through unchanged.
        group (when group
                (mapv (fn [g]
                        (if (and (vector? g) (= :as (first g)) (= 3 (count g)))
                          {:expr (nth g 1) :as (nth g 2)}
                          g))
                      group))
        preds (mapv norm/normalize-pred (or where []))
        aggs  (norm/auto-alias-aggs (mapv norm/normalize-agg (or agg [])))
        [preds columns] (if has-join?
                          [preds columns]
                          (pre-materialize-string-pred-exprs preds columns length))
        [preds columns] (if has-join?
                          [preds columns]
                          (pre-materialize-numeric-pred-exprs preds columns length))
        [preds columns] (if has-join?
                          [preds columns]
                          (materialize-string-preds preds columns length))
        preds            (if has-join?
                           preds
                           (resolve-dict-equality preds columns))
        [preds columns] (if has-join?
                          [preds columns]
                          (compile-non-simd-preds-to-mask preds columns length))
        ;; Pass 5a — string-producing exprs in :group / aggs :expr.
        ;; No-join: materialize eagerly into `columns` (legacy
        ;; behaviour). Join: emit a deferred spec; the planner
        ;; inserts an `LStringMaterialize` node post-join and the
        ;; executor evaluates against the joined column ctx.
        [group aggs select columns string-items-1]
        (if has-join?
          (let [{:keys [group aggs select items]}
                (string-expr-spec-group-agg group aggs select 0)]
            [group aggs select columns items])
          (let [[g a s c] (pre-materialize-string-group-agg-exprs
                           group aggs select columns length)]
            [g a s c nil]))
        ;; Pass 5b — string-producing :expr items in SELECT.
        ;; Same split: eager when no join, deferred otherwise.
        [select-items columns string-items-2]
        (if has-join?
          (let [normalized (when (seq select)
                             (vec (map-indexed #(x/normalize-select-item %2 %1) select)))]
            (if-let [{:keys [select items]}
                     (string-expr-spec-select normalized (count string-items-1))]
              [select columns items]
              [normalized columns nil]))
          (let [r (normalize-and-materialize-select-items select columns length)]
            (if r
              [(first r) (second r) nil]
              [nil columns nil])))
        string-items (into (or string-items-1 []) (or string-items-2 []))
        columns-meta (into {} (keep (fn [[k v]] (when (or (:dict v) (:temporal-unit v)) [k v]))) columns)]
    {:preds preds
     :aggs aggs
     :group group
     :select select-items
     :columns columns
     :columns-meta columns-meta
     :string-items string-items}))

;; ============================================================================
;; Anomaly model resolution
;;
;; `ANOMALY_SCORE / _PREDICT / _PROBA / _CONFIDENCE` reach the engine
;; as expression vectors (`[:anomaly-score "model" e1 e2]`). They aren't
;; understood by `normalize-expr` — the legacy `q` body resolves them
;; before any other normalization by:
;;   1. collecting every anomaly expression in the query
;;   2. evaluating its arguments against the (post-join) column map
;;   3. running the appropriate iforest fn → a synthetic double[]
;;      column keyed by `:__<op>_<model>`
;;   4. rewriting the query slots so the anomaly expr becomes a plain
;;      column keyword reference.
;; The functions below are the same logic, lifted out of `q` so the IR
;; planner's `run-query` path can call them too.
;; ============================================================================

(def anomaly-ops
  #{:anomaly-score :anomaly-predict :anomaly-proba :anomaly-confidence})

(def ^:private anomaly-op->fn
  {:anomaly-score      iforest/score
   :anomaly-predict    iforest/predict
   :anomaly-proba      iforest/predict-proba
   :anomaly-confidence iforest/predict-confidence})

(defn- anomaly-expr? [form]
  (and (vector? form) (seq form) (contains? anomaly-ops (first form))))

(defn collect-anomaly-exprs
  "Walk a nested structure and return the set of anomaly expression
   vectors found in it."
  [form]
  (cond
    (anomaly-expr? form) #{form}
    (sequential? form)   (reduce into #{} (map collect-anomaly-exprs form))
    :else                #{}))

(defn rewrite-anomaly-exprs
  "Replace anomaly expressions with the synthetic column keywords
   `expr->col` maps them to."
  [form expr->col]
  (cond
    (contains? expr->col form) (get expr->col form)
    (vector? form)             (mapv #(rewrite-anomaly-exprs % expr->col) form)
    :else                      form))

(defn- select-alias-map
  "Build a map from anomaly expression → alias keyword for aliased
   SELECT items (`[:as [:anomaly-score …] :alias]`)."
  [select-items]
  (into {}
        (keep (fn [item]
                (when (and (sequential? item) (= :as (first item))
                           (sequential? (second item))
                           (contains? anomaly-ops (first (second item))))
                  [(second item) (nth item 2)])))
        select-items))

(defn- anomaly-synthetic-col
  "Synthetic column keyword `__<op>_<model-name>` for an anomaly
   expression. Deterministic from the expression alone, so the same
   expression always lifts to the same column name (and equal
   expressions in different clauses share the same materialization)."
  [anom-expr]
  (let [op         (first anom-expr)
        model-name (second anom-expr)
        model-name (if (string? model-name) model-name (name model-name))]
    (keyword (str "__" (name op) "_" model-name))))

(defn anomaly-spec
  "Static analysis of an anomaly query. Walks `:select`, `:where`,
   `:having`, `:order` to find every `[:anomaly-* model …]`
   expression and returns

     {:expr->col   {anom-expr → synthetic-col-keyword}
      :query       <query with anomaly exprs replaced by refs>}

   The synthetic column for each expression is named and the query
   is rewritten in-place — but no scoring runs. Both the no-join
   `resolve-anomaly-columns` (which scores immediately) and the
   join path (which defers via `LAnomaly`) share this front-end so
   the synthetic naming and the rewrite stay in lockstep."
  [query]
  (let [exprs (reduce into #{}
                      (map #(collect-anomaly-exprs (get query %))
                           [:select :where :having :order]))]
    (if (empty? exprs)
      {:expr->col {} :query query}
      (let [aliases   (select-alias-map (:select query))
            expr->col (into {}
                            (map (juxt identity anomaly-synthetic-col))
                            exprs)
            ;; ORDER BY may reference the alias the SELECT gave to an
            ;; anomaly expr — `expr->alias` keeps that working.
            expr->alias    (into {}
                                 (keep (fn [[e _]]
                                         (when-let [a (get aliases e)]
                                           [e a])))
                                 expr->col)
            expr->col-other (merge expr->col expr->alias)
            rewritten      (-> query
                               (assoc :select (rewrite-anomaly-exprs (:select query) expr->col))
                               (cond->
                                (:where query)  (assoc :where  (rewrite-anomaly-exprs (:where query) expr->col))
                                (:having query) (assoc :having (rewrite-anomaly-exprs (:having query) expr->col))
                                (:order query)  (assoc :order  (rewrite-anomaly-exprs (:order query) expr->col-other))))]
        {:expr->col expr->col :query rewritten}))))

(defn- score-anomaly-expr
  "Run the iforest computation for one anomaly expression against
   the materialized `col-arrays` and return the resulting `double[]`."
  [anom-expr models col-arrays length cache]
  (let [op            (first anom-expr)
        model-name    (let [m (second anom-expr)]
                        (if (string? m) m (name m)))
        model         (get models model-name)]
    (when-not model
      (throw (ex-info (str "Unknown model: " model-name)
                      {:model     model-name
                       :available (keys models)})))
    (let [feature-names (:feature-names model)
          explicit-args (vec (drop 2 anom-expr))
          short-form?   (empty? explicit-args)
          data
          (if short-form?
            (into {}
                  (map (fn [k]
                         (let [v (get col-arrays k)]
                           (when-not v
                             (throw (ex-info (str "Model feature " k
                                                  " not found in columns. "
                                                  "Use long form or ensure "
                                                  "column is available.")
                                             {:feature   k
                                              :available (vec (keys col-arrays))})))
                           [k v])))
                  feature-names)
            (do
              (when (not= (count explicit-args) (count feature-names))
                (throw (ex-info (str "ANOMALY_SCORE arity mismatch: model '"
                                     model-name "' has " (count feature-names)
                                     " features but " (count explicit-args)
                                     " arguments were provided")
                                {:model     model-name
                                 :expected  (count feature-names)
                                 :actual    (count explicit-args)
                                 :features  feature-names})))
              (into {}
                    (map-indexed
                     (fn [i arg]
                       (let [feat-name (nth feature-names i)
                             norm-arg  (if (and (vector? arg) (not (keyword? arg)))
                                         (norm/normalize-expr arg)
                                         arg)
                             arr       (if (keyword? norm-arg)
                                         (get col-arrays norm-arg)
                                         (expr/eval-expr-vectorized norm-arg col-arrays length cache))]
                         [feat-name arr])))
                    explicit-args)))
          compute-fn (get anomaly-op->fn op)]
      (compute-fn model data))))

(defn materialize-anomaly
  "Score every anomaly expression in `expr->col` against the
   `columns` map and return a new columns map with one synthetic
   `{:type :float64 :data <double[]>}` entry per expression. The
   inputs (and length) are evaluated against the materialized form
   of the columns so this works equally well on the no-join
   prepare-time call and the post-join `LAnomaly` execute-time
   call."
  [expr->col columns ^long length models]
  (if (empty? expr->col)
    columns
    (let [mat-cols   (x/materialize-columns columns)
          col-arrays (into {} (map (fn [[k v]] [k (:data v)])) mat-cols)
          cache      (java.util.HashMap.)]
      (reduce-kv
       (fn [cs anom-expr col-name]
         (let [arr (score-anomaly-expr anom-expr models col-arrays length cache)]
           (assoc cs col-name {:type :float64 :data arr})))
       mat-cols
       expr->col))))

(defn resolve-anomaly-columns
  "Resolve anomaly expressions eagerly. Materializes columns,
   evaluates the expression arguments, scores via iforest, and
   injects the result as a synthetic column. Rewrites query clauses
   to reference that synthetic column.

     Short form: `[:anomaly-score \"model\"]`         — uses model's
                                                        feature names
     Long form:  `[:anomaly-score \"model\" e1 e2]`   — evaluates
                                                        each expr

   Used by the no-join planner path (the source columns are already
   in scope at prepare-time). The join path defers materialization
   to the executor's `LAnomaly` case via `anomaly-spec` +
   `materialize-anomaly`. Returns `[query columns]` (passed through
   unchanged when no anomaly expressions are present)."
  [query columns length models]
  (let [{:keys [expr->col query]} (anomaly-spec query)]
    (if (empty? expr->col)
      [query columns]
      [query (materialize-anomaly expr->col columns length models)])))
