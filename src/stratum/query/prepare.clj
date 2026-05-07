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
            [stratum.query.execution :as x]))

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
  "Pass 5a: lift string-producing expressions out of GROUP BY and
   aggregate slots into dict-encoded temp columns. Mirrors the legacy
   `expr/materialize-string-exprs` call in `q`."
  [group aggs select columns length]
  (let [has-string-expr?
        (or (some #(and (sequential? %)
                        (expr/string-producing-expr? (norm/normalize-expr (vec %))))
                  group)
            (some #(and (:expr %)
                        (expr/string-producing-expr? (:expr %)))
                  aggs))]
    (if has-string-expr?
      (expr/materialize-string-exprs group aggs select columns length)
      [group aggs select columns])))

(defn- normalize-and-materialize-select-items
  "Pass 5b: normalize SELECT items via x/normalize-select-item, then
   lift any `:expr` slot whose normalized expression is
   string-producing into a dict-encoded temp column. Mirrors the
   legacy block at q.clj:722-740."
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
        [group aggs select columns]
        (pre-materialize-string-group-agg-exprs group aggs select columns length)
        ;; Normalize select items + lift string-producing :expr items
        ;; into temp columns.
        [select-items columns]
        (let [r (normalize-and-materialize-select-items select columns length)]
          (or r [nil columns]))
        columns-meta (into {} (keep (fn [[k v]] (when (:dict v) [k v]))) columns)]
    {:preds preds
     :aggs aggs
     :group group
     :select select-items
     :columns columns
     :columns-meta columns-meta}))
