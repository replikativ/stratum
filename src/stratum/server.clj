(ns stratum.server
  "PostgreSQL wire protocol server for Stratum.

   Provides a SQL interface to Stratum's SIMD-accelerated columnar engine.
   Standard PostgreSQL clients (psql, DBeaver, JDBC, Python) can connect
   and execute SQL queries.

   Usage:
     ;; Start server
     (def srv (start {:port 5432}))

     ;; Register tables
     (register-table! srv \"orders\" {:price (double-array [...])
                                      :quantity (long-array [...])})

     ;; Stop server
     (stop srv)

   CLI:
     clj -M:server
     clj -M:server --demo --port 5432"
  (:require [stratum.query :as q]
            [stratum.sql :as sql]
            [stratum.dataset :as dataset]
            [stratum.index :as idx]
            [stratum.files :as files]
            [stratum.iforest :as iforest]
            [stratum.server.state :as srv-state]
            [stratum.util.decimal :as decimal]
            [clojure.string :as str])
  (:import [stratum.internal PgWireServer PgWireServer$QueryHandler PgWireServer$QueryHandlerFactory PgWireServer$QueryResult]
           [java.util Random]
           [java.io File])
  (:gen-class))

(set! *warn-on-reflection* true)

;; Forward declarations for durable-SQL-table helpers (defined further below
;; near `make-handler-factory`). Needed because `execute-sql` — defined
;; earlier in the file for historical reasons — calls into them.
(declare make-durable-sql-table! durable-cols? resync-durable-table!
         hydrate-sql-tables! hydrate-models! hydrate-live-tables!
         hydrate-enums!
         empty-col-array columns->descriptors
         durable-branch durable-meta
         resolve-enum-columns validate-enum-rows!
         coerce-rows-for-decimals check-unsigned-rows!)

;; ============================================================================
;; File path validation
;; ============================================================================

(defn- validate-file-path
  "Validate a file path for the server context to prevent path traversal.
   Rejects paths containing '..' and ensures the resolved path is within data-dir."
  [^String path ^String data-dir]
  (when (re-find #"(^|[/\\])\.\.[/\\]|^\.\.$|(^|[/\\])\.\.$" path)
    (throw (ex-info (str "Path traversal not allowed: " path)
                    {:path path :reason :path-traversal})))
  (when (.startsWith path "/")
    (throw (ex-info (str "Absolute paths not allowed from server context: " path)
                    {:path path :reason :absolute-path})))
  (let [canonical (.getCanonicalPath (File. (str data-dir File/separator path)))
        dir-canonical (.getCanonicalPath (File. ^String data-dir))]
    (when-not (.startsWith canonical (str dir-canonical File/separator))
      (throw (ex-info (str "Path escapes data directory: " path)
                      {:path path :data-dir data-dir :reason :outside-data-dir}))))
  path)

;; ============================================================================
;; Table Registry
;; ============================================================================

(defn- encode-table-columns
  "Pre-encode table columns: auto-detect types and dict-encode String[]
   columns. **Preserves the column-map's metadata** — callers register
   bitemporal tables via `(with-meta {...} {:bitemporal {...}})` and
   the metadata is the canonical source of truth for axis configuration
   read by `bitemporal-valid-cfg` / `bitemporal-ds-meta`. Dropping it
   here would silently demote any SQL DML on the table to the
   valid-only or columns-inferred path."
  [columns]
  (cond-> (into {}
                (map (fn [[col-name col-data]]
                       (let [k (if (keyword? col-name) col-name (keyword col-name))
                             encoded (q/encode-column col-data)]
                         [k encoded])))
                columns)
    (meta columns) (with-meta (meta columns))))

;; ----------------------------------------------------------------------------
;; Row-level helpers — uniform across raw-array tables (CREATE TABLE) and
;; encoded index-backed tables (register-table! with stratum indexes).
;; ----------------------------------------------------------------------------

(defn- col-row-count
  "Length of a single column entry in the table-registry. Handles raw
   `[J` / `[D` / `[Ljava.lang.String;` arrays AND encoded
   `{:source :index :index PSI ...}` / `{:type ... :data <arr>}` maps."
  ^long [col]
  (cond
    (nil? col) 0
    (instance? (Class/forName "[J") col) (alength ^longs col)
    (instance? (Class/forName "[D") col) (alength ^doubles col)
    (instance? (Class/forName "[Ljava.lang.String;") col)
    (alength ^"[Ljava.lang.String;" col)
    (and (map? col) (= :index (:source col))) (idx/idx-length (:index col))
    (and (map? col) (:data col))
    (let [arr (:data col)]
      (cond
        (instance? (Class/forName "[J") arr) (alength ^longs arr)
        (instance? (Class/forName "[D") arr) (alength ^doubles arr)
        :else 0))
    :else 0))

(defn- index-backed-cols?
  "True when every column entry is encoded with `:source :index` —
   i.e. the table was registered through stratum's columnar engine
   and DML must route through `idx-*!` primitives instead of array
   rebuilds."
  [cols]
  (and (seq cols)
       (every? (fn [[_k v]] (and (map? v) (= :index (:source v))))
               cols)))

(defn- read-cell
  "Read the value of `col-name` at row `i` from a table-registry entry.
   Returns nil for NULL sentinels (Long/MIN_VALUE / NaN / nil String).
   Mirrors the per-column dispatch used by DML predicate evaluation but
   covers both raw arrays AND encoded index columns."
  [existing col-name ^long i]
  (let [col (get existing col-name)]
    (cond
      (instance? (Class/forName "[J") col)
      (let [v (aget ^longs col i)]
        (when-not (= v Long/MIN_VALUE) v))

      (instance? (Class/forName "[D") col)
      (let [v (aget ^doubles col i)]
        (when-not (Double/isNaN v) v))

      (instance? (Class/forName "[Ljava.lang.String;") col)
      (aget ^"[Ljava.lang.String;" col i)

      (and (map? col) (= :index (:source col)))
      (let [index (:index col)
            datatype (:type col)
            dict (:dict col)]
        (cond
          dict
          (let [code (idx/idx-get-long index i)]
            (when-not (= code Long/MIN_VALUE)
              (aget ^"[Ljava.lang.String;" dict (int code))))

          (= :int64 datatype)
          (let [v (idx/idx-get-long index i)]
            (when-not (= v Long/MIN_VALUE) v))

          (= :float64 datatype)
          (let [v (idx/idx-get-double index i)]
            (when-not (Double/isNaN v) v))

          :else nil))

      :else nil)))

(defn- nil-safe-arith
  "SQL 3-valued logic for `:+/:-/:*/:/` applied to two
   already-evaluated operands. Returns nil if either operand is
   nil (NULL propagation); also returns nil on `:/` by zero.
   Shared by every per-row arithmetic evaluator below
   (eval-dml-predicate, the UPSERT eval-val, the UPDATE eval-row,
   …) — copilot review #2 + agent-discovered duplicates."
  [op l r]
  (when (and (some? l) (some? r))
    (case op
      :+ (+ (double l) (double r))
      :- (- (double l) (double r))
      :* (* (double l) (double r))
      :/ (when-not (zero? (double r))
           (/ (double l) (double r)))
      nil)))

(defn- eval-dml-predicate
  "Evaluate the DML-style predicate list (each pred is `[op col val]`
   or `[op pred ...]` for combinators) against row `i` of a column-map.
   Returns truthy/falsy. Reusable across UPDATE / DELETE / UPSERT
   branches; works on both raw-array and index-backed columns via
   `read-cell`.

   F-021: covers logical combinators (`:and`/`:or`/`:not`), set ops
   (`:in`/`:not-in`), `:between`/`:not-between`, and the LIKE
   family (`:like`/`:not-like`/`:contains`/`:starts-with`/`:ends-with`).
   Pre-fix these all fell to the `false` default → DML silently
   matched no rows."
  [existing where ^long i]
  (letfn [(eval-val [e]
            (cond
              (keyword? e) (read-cell existing e i)
              (number? e)  e
              (string? e)  e
              (nil? e)     nil
              (vector? e)
              (nil-safe-arith (first e)
                              (eval-val (nth e 1))
                              (eval-val (nth e 2)))))
          (like-match [s pattern escape]
            (when (and (some? s) (some? pattern))
              ;; Shares the LIKE→regex compiler with the SIMD path so the
              ;; two can never disagree; `escape` (4th pred slot) honors the
              ;; SQL `LIKE ... ESCAPE` clause. DOTALL: `_`/`%` span newlines.
              (let [esc (int (if escape
                               (int (.charAt ^String (str escape) 0))
                               -1))
                    re  (java.util.regex.Pattern/compile
                         (stratum.internal.ColumnOpsString/likeToRegex (str pattern) esc)
                         java.util.regex.Pattern/DOTALL)]
                (.matches (.matcher re (str s))))))
          (eval1 [pred]
            (case (first pred)
              :and  (every? eval1 (rest pred))
              :or   (boolean (some eval1 (rest pred)))
              :not  (not (eval1 (second pred)))
              :=    (let [l (eval-val (nth pred 1))
                          r (eval-val (nth pred 2))]
                      (and (some? l) (some? r) (= l r)))
              (:!= :<>) (let [l (eval-val (nth pred 1))
                              r (eval-val (nth pred 2))]
                          (and (some? l) (some? r) (not= l r)))
              :>    (let [l (eval-val (nth pred 1))
                          r (eval-val (nth pred 2))]
                      (and (some? l) (some? r) (> (double l) (double r))))
              :<    (let [l (eval-val (nth pred 1))
                          r (eval-val (nth pred 2))]
                      (and (some? l) (some? r) (< (double l) (double r))))
              :>=   (let [l (eval-val (nth pred 1))
                          r (eval-val (nth pred 2))]
                      (and (some? l) (some? r) (>= (double l) (double r))))
              :<=   (let [l (eval-val (nth pred 1))
                          r (eval-val (nth pred 2))]
                      (and (some? l) (some? r) (<= (double l) (double r))))
              :is-null     (nil? (eval-val (nth pred 1)))
              :is-not-null (some? (eval-val (nth pred 1)))
              :in     (let [l (eval-val (nth pred 1))
                            xs (nth pred 2)]
                        (and (some? l) (contains? (set xs) l)))
              :not-in (let [l (eval-val (nth pred 1))
                            xs (nth pred 2)
                            has-null? (some nil? xs)]
                        ;; PG 3VL: NOT IN with NULL in the set is UNKNOWN → false.
                        (and (some? l) (not has-null?) (not (contains? (set xs) l))))
              :between (let [v (eval-val (nth pred 1))
                             lo (eval-val (nth pred 2))
                             hi (eval-val (nth pred 3))]
                         (and (some? v) (some? lo) (some? hi)
                              (>= (double v) (double lo))
                              (<= (double v) (double hi))))
              :not-between (let [v (eval-val (nth pred 1))
                                 lo (eval-val (nth pred 2))
                                 hi (eval-val (nth pred 3))]
                             (and (some? v) (some? lo) (some? hi)
                                  (or (< (double v) (double lo))
                                      (> (double v) (double hi)))))
              :like     (like-match (eval-val (nth pred 1)) (nth pred 2) (nth pred 3 nil))
              :not-like (let [v (eval-val (nth pred 1))]
                          ;; NOT LIKE NULL → UNKNOWN → false (3VL).
                          (and (some? v) (not (like-match v (nth pred 2) (nth pred 3 nil)))))
              :contains    (let [v (eval-val (nth pred 1)) p (str (nth pred 2))]
                             (and (some? v) (.contains (str v) p)))
              :starts-with (let [v (eval-val (nth pred 1)) p (str (nth pred 2))]
                             (and (some? v) (.startsWith (str v) p)))
              :ends-with   (let [v (eval-val (nth pred 1)) p (str (nth pred 2))]
                             (and (some? v) (.endsWith (str v) p)))
              false))]
    (every? eval1 where)))

(defn- delete-via-index-backend
  "Apply a DELETE to a fully-index-backed table. Builds a sorted-desc
   list of row indices to delete, then fans `dataset/ds-delete-rows!`
   across a transient wrapper. Returns the new column map + delete
   count.

   `existing` — the registry's column map for `table` (must be
                fully index-backed; callers gate on `index-backed-cols?`).
   `where`    — predicate list, or nil to delete all rows.
   `meta`     — original metadata to re-attach on the result map."
  [^String table existing where meta]
  (let [first-col (val (first existing))
        n-rows    (col-row-count first-col)
        ;; Phase 1: scan once, collect row indices matching `where`.
        delete-idxs
        (if (nil? where)
          (vec (range n-rows))
          (vec (filter #(eval-dml-predicate existing where %)
                       (range n-rows))))]
    (if (empty? delete-idxs)
      {:new-cols existing :n-deleted 0}
      ;; Phase 2: wrap as dataset, drop rows via the transient primitive.
      (let [ds (dataset/make-dataset existing {:name table})
            mutated (-> ds
                        transient
                        (dataset/ds-delete-rows! delete-idxs)
                        persistent!)
            new-cols (dataset/columns mutated)
            new-cols (if meta (with-meta new-cols meta) new-cols)]
        {:new-cols new-cols :n-deleted (count delete-idxs)}))))

(defn- bitemporal-axis-cfg
  "Infer one axis (`:valid` or `:system`) of a bitemporal config from
   a table-registry column map. Prefers metadata (`:bitemporal {axis-kw
   {...}}`); falls back to the SQL:2011 convention column names
   (`_valid_from`/`_valid_to`, `_system_from`/`_system_to`). Returns
   `nil` when neither source yields the axis."
  [existing meta axis-kw]
  (let [bt-meta (:bitemporal meta)
        [from-col to-col] (case axis-kw
                            :valid  [:_valid_from  :_valid_to]
                            :system [:_system_from :_system_to])]
    (or (get bt-meta axis-kw)
        (when (and (contains? existing from-col)
                   (contains? existing to-col))
          {:from-col from-col :to-col to-col :unit :micros}))))

(defn- bitemporal-valid-cfg
  "Convenience: infer just the `:valid` axis for a table. Same
   precedence rule as `bitemporal-axis-cfg`."
  [existing meta]
  (bitemporal-axis-cfg existing meta :valid))

(defn- bitemporal-ds-meta
  "Build the dataset-level `:bitemporal` metadata used to construct an
   ad-hoc dataset from a table-registry column map for SQL DML
   lowering. **Preserves the `:system` axis** — either from metadata
   or from the SQL:2011 column-name convention — so the SCD2-on-both-
   axes surgery in `dataset/upsert!` / `retract!` /
   `bounded-update!` fires.

   `valid` is the inferred valid-axis config (typically from
   `bitemporal-valid-cfg`); it overrides any `:valid` in the table's
   existing bitemporal metadata. The `:system` axis is inferred via
   `bitemporal-axis-cfg`."
  [existing meta valid]
  (let [bt   (or (:bitemporal meta) {})
        sys  (bitemporal-axis-cfg existing meta :system)]
    {:bitemporal (cond-> (assoc bt :valid valid)
                   sys (assoc :system sys))}))

(defn- with-bitemporal-dataset
  "Run a SQL DML surgery against a registered bitemporal table. Wraps
   `existing` in a transient stratum dataset with correct bitemporal
   metadata (preserving the `:system` axis when present), invokes
   `surgery-fn` to apply the mutation, persists, and returns the new
   column-map with the table-registry metadata re-applied.

   `surgery-fn` is `(fn [tds valid-cfg] mutated-transient-tds)`. It
   receives the transient dataset AND the resolved `:valid` axis
   config (useful for per-helper error messages or column-name
   lookups). Returning the mutated transient is the contract.

   `verb-name` is the SQL verb used in the not-a-bitemporal-table
   error (`'INSERT FOR PORTION OF VALID_TIME'`, etc.).

   Centralises the boilerplate that was duplicated across the three
   `*-portion-via-index-backend` builders: valid-axis discovery,
   throw-on-missing, ds-meta construction (with `:system` axis
   preservation), make-dataset, transient → mutate → persistent,
   and metadata re-application."
  [^String table existing meta verb-name surgery-fn]
  (let [valid (bitemporal-valid-cfg existing meta)
        _ (when-not valid
            (throw (ex-info (str verb-name " requires a :valid axis on the table")
                            {:table table :existing-cols (vec (keys existing))})))
        ds-meta (bitemporal-ds-meta existing meta valid)
        ds (dataset/make-dataset existing {:name table :metadata ds-meta})
        mutated (-> ds transient (surgery-fn valid) persistent!)
        new-cols (dataset/columns mutated)]
    (cond-> new-cols meta (with-meta meta))))

(defn- insert-portion-via-index-backend
  "Apply a SQL:2011 `INSERT … FOR PORTION OF VALID_TIME FROM x TO y`
   on an index-backed bitemporal table. Each row in `rows` (positional
   per `col-list`, which must be supplied since the period fills in
   the temporal cols) is `append!`'d with `:valid-from` / `:valid-to`
   in tx-meta so `append!`'s axis auto-stamping picks them up.

   The user MUST supply an explicit column list naming all the
   non-temporal columns; the temporal cols are filled from the
   period. If `col-list` is nil (positional VALUES with no column
   names) we throw — the temporal columns are positionally ambiguous."
  [^String table existing col-list rows period meta]
  (when-not col-list
    (let [valid (bitemporal-valid-cfg existing meta)]
      (throw (ex-info (str "INSERT FOR PORTION OF VALID_TIME requires an explicit "
                           "column list: `INSERT INTO t (col1, col2) VALUES (...)` — "
                           "the period fills in "
                           (or (:from-col valid) "_valid_from") " / "
                           (or (:to-col valid) "_valid_to"))
                      {:table table}))))
  ;; The period is meant to fill the axis columns; if the user
  ;; ALSO names them in col-list, the row-map would carry an
  ;; explicit value and `append!`'s tx-meta default-merge wouldn't
  ;; overwrite it — FOR PORTION OF would be silently ignored.
  ;; Reject so the user's confused intent surfaces. (Copilot
  ;; review-3 P1.)
  (let [valid-cfg (bitemporal-valid-cfg existing meta)
        axis-cols (when valid-cfg
                    #{(:from-col valid-cfg) (:to-col valid-cfg)})
        clash (when axis-cols (filterv axis-cols col-list))]
    (when (seq clash)
      (throw (ex-info (str "INSERT … FOR PORTION OF VALID_TIME column list "
                           "must not include the valid-time axis columns — "
                           "the period fills them. Got " (vec clash) " in col-list.")
                      {:table table
                       :axis-cols axis-cols
                       :col-list col-list
                       :overlap (set clash)}))))
  (let [new-cols (with-bitemporal-dataset
                   table existing meta "INSERT FOR PORTION OF VALID_TIME"
                   (fn [tds _valid]
                     (reduce (fn [t row-vals]
                               (dataset/append! t (zipmap col-list row-vals)
                                                {:valid-from (:from period)
                                                 :valid-to   (:to period)}))
                             tds rows)))]
    {:new-cols new-cols :n-inserted (count rows)}))

(defn- update-portion-via-index-backend
  "Apply a SQL:2011 `UPDATE … FOR PORTION OF VALID_TIME FROM x TO y
   SET col=val WHERE p` on an index-backed bitemporal table by
   lowering to `dataset/bounded-update!`. The :set assignments are
   evaluated as literal values (no expressions — sub-expressions in
   SET are a P2 future addition)."
  [^String table existing where assignments period meta]
  (let [set-map (reduce (fn [m {:keys [col expr]}]
                          (when-not (or (number? expr) (string? expr) (nil? expr))
                            (throw (ex-info "UPDATE FOR PORTION OF VALID_TIME only supports literal SET values"
                                            {:assignment {:col col :expr expr}
                                             :hint "expression evaluation in SET is deferred"})))
                          (assoc m col expr))
                        {}
                        assignments)
        new-cols (with-bitemporal-dataset
                   table existing meta "UPDATE FOR PORTION OF VALID_TIME"
                   (fn [tds _valid]
                     (dataset/bounded-update! tds {:where (or where []) :set set-map}
                                              {:valid-from (:from period)
                                               :valid-to   (:to period)})))]
    {:new-cols new-cols
     ;; SQL clients read `UPDATE N` as "N rows participated in the
     ;; surgery." We report the pre-mutation WHERE-match count, NOT
     ;; the number of physical slices the bounded-update produced
     ;; (which can be larger after a 3-way split). This matches
     ;; Postgres's semantic for partitioned-table UPDATEs where the
     ;; physical row count is divorced from the logical "rows
     ;; affected." Callers needing physical counts should diff
     ;; row-count(before) vs row-count(after).
     :n-updated (count (filter #(eval-dml-predicate existing where %)
                               (range (col-row-count (val (first existing))))))}))

(defn- delete-portion-via-index-backend
  "Apply a SQL:2011 `DELETE … FOR PORTION OF VALID_TIME FROM x TO y`
   to an index-backed table. Lowers to `retract!` on a transient
   stratum dataset with `:valid-from` + `:valid-to` in tx-meta —
   `retract!` then performs the bounded surgery (truncate / shift /
   split / drop) per overlapping row.

   The bitemporal axis config is inferred from the column-map's
   metadata; if the user registered a table without bitemporal
   metadata, we infer reasonable defaults from the column names
   (`_valid_from` + `_valid_to`). Throws if neither convention is
   discoverable."
  [^String table existing where period meta]
  (let [{vf-val :from vt-val :to} period
        before-count (col-row-count (val (first existing)))
        new-cols (with-bitemporal-dataset
                   table existing meta "DELETE FOR PORTION OF VALID_TIME"
                   (fn [tds _valid]
                     (dataset/retract! tds {:where (or where [])}
                                       {:valid-from vf-val :valid-to vt-val})))
        after-count (col-row-count (val (first new-cols)))]
    ;; "DELETE N" tag reports rows physically removed. With bounded
    ;; retract, splits ADD rows (one tail per split), which doesn't
    ;; map cleanly to a "DELETE n" count. We report
    ;; max(0, before-after) so a pure-shift or pure-truncate reports
    ;; 0, a drop reports the count, and a split reports a smaller
    ;; number than reality. Refinement deferred until a real SQL
    ;; client cares about the exact semantics here.
    {:new-cols new-cols
     :n-deleted (max 0 (- before-count after-count))}))

;; ============================================================================
;; Query Handler
;; ============================================================================

(defn- json-write-str
  "Minimal JSON writer for explain output. Handles maps, vectors, strings,
   numbers, booleans, nil. Indented pretty-print."
  ([x] (json-write-str x 0))
  ([x ^long depth]
   (let [ind (apply str (repeat (* 2 depth) \space))
         ind1 (apply str (repeat (* 2 (inc depth)) \space))
         escape (fn [^String s]
                  (-> s
                      (.replace "\\" "\\\\")
                      (.replace "\"" "\\\"")
                      (.replace "\n" "\\n")
                      (.replace "\r" "\\r")
                      (.replace "\t" "\\t")))]
     (cond
       (nil? x)     "null"
       (true? x)    "true"
       (false? x)   "false"
       (string? x)  (str "\"" (escape x) "\"")
       (number? x)  (str x)
       (keyword? x) (str "\"" (escape (name x)) "\"")
       (map? x)
       (if (empty? x)
         "{}"
         (str "{\n"
              (str/join ",\n"
                        (mapv (fn [[k v]]
                                (str ind1 (json-write-str (cond
                                                            (keyword? k) (name k)
                                                            :else (str k))
                                                          0)
                                     ": "
                                     (json-write-str v (inc depth))))
                              x))
              "\n" ind "}"))
       (sequential? x)
       (if (empty? x)
         "[]"
         (str "[\n"
              (str/join ",\n"
                        (mapv (fn [item] (str ind1 (json-write-str item (inc depth))))
                              x))
              "\n" ind "]"))
       :else (str "\"" (escape (str x)) "\"")))))

(defn- explain-rows
  "Build pgwire `QueryResult` rows for an EXPLAIN result.

   `plan-result` is what `q/explain` returns; `format` is :text or :json.
   For :text, each line of `:plan-tree` is its own row (matches Postgres'
   per-line `QUERY PLAN` column). For :json, a single row with the
   pretty-printed JSON document."
  [plan-result format]
  (let [lines (case format
                :json [(json-write-str (:plan-json plan-result))]
                :text (str/split-lines (str (:plan-tree plan-result))))
        rows  (mapv (fn [l] (into-array String [l])) lines)]
    (PgWireServer$QueryResult.
     (into-array String ["QUERY PLAN"])
     (int-array [25])
     (into-array (Class/forName "[Ljava.lang.String;") rows)
     (str "EXPLAIN"))))

(defn- format-explain-result
  "Pgwire formatter for an EXPLAIN parsed result.

   `parsed-explain` is the `:explain` value from `sql/parse-sql`:
     {:options {:analyze? bool :format :text/:json}
      :inner   {:query <map>} | {:system true :tag <str>}}"
  [parsed-explain table-registry]
  (let [{:keys [options inner]} parsed-explain
        {:keys [analyze? format] :or {format :text}} options]
    (cond
      ;; System query — show its tag in a single QUERY PLAN row
      (:system inner)
      (let [tag (or (:tag inner) "SYSTEM")
            row (into-array String [(str "System query: " tag)])]
        (PgWireServer$QueryResult.
         (into-array String ["QUERY PLAN"])
         (int-array [25])
         (into-array (Class/forName "[Ljava.lang.String;") [row])
         (str "EXPLAIN")))

      (:query inner)
      (let [query   (:query inner)
            ;; Resolve any string-keyed table references in the query
            ;; against the live registry, matching the path the
            ;; non-EXPLAIN dispatch takes.
            opts    {:analyze? (boolean analyze?)
                     :format   (or format :text)}
            result  (q/explain query opts)]
        (explain-rows (cond-> result
                        (and (= :json format) (not (:plan-json result)))
                        (assoc :plan-json (stratum.query.plan/render-json
                                           (:plan-data result))))
                      format))

      :else
      (PgWireServer$QueryResult. "EXPLAIN: malformed parsed result"))))

(def ^:private model-type-map
  "Extensible registry of model types for CREATE MODEL.
   Each entry maps an uppercase type name to {:train-fn, :default-opts}."
  {"ISOLATION_FOREST" {:train-fn    iforest/train
                       :default-opts {:n-trees 100 :sample-size 256 :seed 42}}})

(defn- attach-anomaly-models
  "Look up anomaly models referenced in the query and attach them as :_anomaly-models.
   Does NOT compute scores — that happens post-join inside query.clj."
  [query registry]
  (let [models (get registry "__models__")]
    (if (seq models)
      (assoc query :_anomaly-models models)
      query)))

(defn- resolve-live-tables
  "Resolve any live table entries in the registry by loading fresh from storage."
  [raw-registry]
  (into {}
        (map (fn [[k v]]
               (if (and (map? v) (:__live v))
                 (let [ds (dataset/load (:store v) (:branch v))]
                   [k (encode-table-columns (dataset/columns ds))])
                 [k v])))
        raw-registry))

;; Matches file path references in SQL that should be auto-loaded:
;;   read_csv('path.csv')          read_parquet('path.parquet')
;;   FROM 'path.csv'               FROM 'path.parquet'
;;   FROM "path.csv"               FROM "path.parquet"
(def ^:private file-ref-patterns
  [;; read_csv('path') / read_parquet('path')
   #"(?i)\b(read_csv|read_parquet)\s*\(\s*'([^']+)'\s*\)"
   ;; FROM 'path.csv'
   #"(?i)(?<=\bFROM\s)'([^']+\.(?:csv|parquet))'"
   ;; FROM "path.csv"
   #"(?i)(?<=\bFROM\s)\"([^\"]+\.(?:csv|parquet))\""])

(defn- resolve-file-refs
  "Detect file path references in SQL and load them as indexed datasets.
   Returns [rewritten-sql extra-registry] where each file reference has
   been replaced with a synthetic table name __file_N__ and the loaded
   column map added to extra-registry.

   Requires data-dir to be set; no-ops when nil."
  [^String sql data-dir]
  (if-not data-dir
    [sql {}]
    (let [counter   (volatile! 0)
          extra-reg (volatile! {})
          result    (volatile! sql)]
      ;; Pre-fix (round-4 agent P1 #4.3): each pattern only ran
      ;; `re-find` ONCE — a query with two `read_csv` calls had
      ;; the second left as raw SQL → JSqlParser confusion. Loop
      ;; while any occurrence remains.
      ;; read_csv/read_parquet function syntax
      (loop []
        (when-let [m (re-find #"(?i)\b(read_csv|read_parquet)\s*\(\s*'([^']+)'\s*\)" @result)]
          (let [[full-match _func path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace-first @result full-match synthetic))
            (recur))))
      ;; FROM 'path.csv' (single-quoted)
      (loop []
        (when-let [m (re-find #"(?i)\bFROM\s+'([^']+\.(?:csv|parquet))'" @result)]
          (let [[full-match path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace-first @result full-match
                                               (str "FROM " synthetic)))
            (recur))))
      ;; FROM "path.csv" (double-quoted — JSqlParser strips quotes, becomes table name)
      (loop []
        (when-let [m (re-find #"(?i)\bFROM\s+\"([^\"]+\.(?:csv|parquet))\"" @result)]
          (let [[full-match path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace-first @result full-match
                                               (str "FROM " synthetic)))
            (recur))))
      [@result @extra-reg])))

(def ^:dynamic *session-settings*
  "Per-handler session-state atom. Bound by `make-handler-factory`
   to a fresh atom for each PgWire connection, so
   `SET datahike.clock_time = …` from one client doesn't leak into
   another client's session. Falls back to the global registry
   `__settings__` map when not bound (REPL / single-session use).
   Round-4 agent P1 #1.2."
  nil)

(defn- session-clock-time-millis
  "Read the clock-time pin from per-session atom (if bound) or the
   global registry. Per-session shadows global."
  [table-registry-atom]
  (or (when *session-settings*
        (get @*session-settings* :clock-time-millis))
      (get-in @table-registry-atom ["__settings__" :clock-time-millis])))

(defn- store-clock-time-millis!
  "Write the clock-time pin to per-session atom (if bound) — keeps
   the setting scoped to one PgWire connection. Falls back to
   global registry for non-handler invocations (REPL)."
  [table-registry-atom value]
  (if *session-settings*
    (if (= value :clear)
      (swap! *session-settings* dissoc :clock-time-millis)
      (swap! *session-settings* assoc :clock-time-millis value))
    (if (= value :clear)
      (swap! table-registry-atom update "__settings__" dissoc :clock-time-millis)
      (swap! table-registry-atom assoc-in
             ["__settings__" :clock-time-millis] value))))

(defn- execute-sql
  "Execute a SQL statement against the registry and return a QueryResult.

   `SET datahike.clock_time = …` is honored as a session-scoped
   binding via the dynamic `*session-settings*` atom (bound per
   PgWire connection by the handler factory) — falls back to the
   shared registry `__settings__` map when not bound. Every
   subsequent execute-sql call runs inside a `binding` form that
   pins `dataset/*clock-time-millis*` to that value, so DML
   defaults (`:valid-from` / `:system-from` when omitted) become
   repeatable. `SET datahike.clock_time = DEFAULT` clears the
   binding.

   The 3-arity overload runs without a durable store — equivalent to
   `store = nil`. Kept so existing tests and programmatic callers in
   `stratum.api` continue to work unchanged."
  ([sql table-registry-atom data-dir-atom]
   (execute-sql sql table-registry-atom data-dir-atom nil))
  ([sql table-registry-atom data-dir-atom store]
   (try
     (let [raw-registry @table-registry-atom
           registry     (resolve-live-tables raw-registry)
           [sql extra]  (resolve-file-refs sql @data-dir-atom)
           registry     (merge registry extra)
           parsed       (sql/parse-sql sql registry)
           settings (:settings parsed)]
       (when settings
         (cond
           (= :clear (:clock-time-millis settings))
           (store-clock-time-millis! table-registry-atom :clear)
           (number? (:clock-time-millis settings))
           (store-clock-time-millis! table-registry-atom (:clock-time-millis settings))))
       (binding [dataset/*clock-time-millis*
                 (or (session-clock-time-millis table-registry-atom)
                     dataset/*clock-time-millis*)]
         (cond
            ;; EXPLAIN [(ANALYZE)] [(FORMAT JSON)] query
           (:explain parsed)
           (format-explain-result (:explain parsed) @table-registry-atom)

            ;; System query (SET, SHOW, VERSION, etc.)
           (:system parsed)
           (sql/format-results parsed)

            ;; DDL (CREATE TABLE, INSERT INTO, UPDATE, DELETE)
            ;; Coarse server-scoped lock on the registry atom for
            ;; the whole DDL/DML dispatch. Pre-fix (round-4 agent
            ;; P0), every DML branch did
            ;; `(let [existing (get @reg table)] ... (swap! reg
            ;; assoc table new-cols))` — two concurrent writes on
            ;; the same table each captured the same `existing`,
            ;; computed disjoint `new-cols`, and the second swap!
            ;; clobbered the first → silent lost writes under
            ;; multi-connection PgWire. Coarse lock is the
            ;; conservative-correct fix; per-table lock map is a
            ;; future perf optimization.
           (:ddl parsed)
           (locking table-registry-atom
             (let [{:keys [op table columns rows assignments where
                           conflict-cols action from table-alias] :as ddl} (:ddl parsed)]
               (case op
                 :create-type-enum
                 (let [{:keys [type-name values or-replace?]} ddl
                       existing (get-in @table-registry-atom ["__enums__" type-name])]
                   (when (and existing (not or-replace?))
                     (throw (ex-info (str "Type already exists: " type-name
                                          " (use OR REPLACE to overwrite)")
                                     {:type-name type-name})))
                   (let [oid    (if (and existing store (= values (:values-ordered existing)))
                                  (:oid existing)
                                  (if store
                                    (srv-state/allocate-oid! store)
                                   ;; No durable store: allocate a session-local OID.
                                   ;; Counter lives in the registry's `__enum-oid__` slot.
                                    (let [next (-> (swap! table-registry-atom
                                                          update "__enum-oid__"
                                                          (fnil inc srv-state/FIRST-USER-OID))
                                                   (get "__enum-oid__"))]
                                      (long next))))
                         record {:values-ordered (vec values) :oid (long oid)}]
                     (swap! table-registry-atom assoc-in ["__enums__" type-name] record)
                     (when store
                       (srv-state/put! store :enums type-name record)))
                   (PgWireServer$QueryResult/empty "CREATE TYPE"))

                 :create-table
                 (do
                  ;; Resolve enum-typed columns against the registry
                  ;; before the storage shape is decided. An unknown
                  ;; type that doesn't match any registered enum is
                  ;; an error (no silent fall-through to :string —
                  ;; the user typed something we don't recognise).
                   (let [enums (get @table-registry-atom "__enums__")
                         columns (resolve-enum-columns columns enums)]
                     (if store
                      ;; Durable path (restart-safety R2 + R2.5): every
                      ;; column type — int64, float64, dict-string,
                      ;; enum, temporal — lands as an index-backed
                      ;; dataset, syncs to a per-table branch, and
                      ;; persists the binding. INSERT/UPDATE/DELETE
                      ;; re-syncs via resync-durable-table! after each
                      ;; statement; string columns encode through the
                      ;; column's dict, extending it as needed.
                       (let [durable-cols (make-durable-sql-table! store table columns)]
                         (swap! table-registry-atom assoc table durable-cols))
                      ;; No :store — original heap path. The startup
                      ;; warning told the user this is session-scoped.
                       (let [cols (into {}
                                        (map (fn [{:keys [name type]}]
                                               [(keyword name) (empty-col-array type)]))
                                        columns)
                             schema (columns->descriptors columns)
                             cols (if (seq schema)
                                    (with-meta cols {:column-schema schema})
                                    cols)]
                         (swap! table-registry-atom assoc table cols))))
                   (PgWireServer$QueryResult/empty "CREATE TABLE"))

                 :drop-table
                 (do (swap! table-registry-atom dissoc table)
                     (PgWireServer$QueryResult/empty "DROP TABLE"))

                 :create-model
                 (let [{:keys [model-name model-type options training-sql]} ddl
                       type-config (get model-type-map model-type)]
                   (when-not type-config
                     (throw (ex-info (str "Unknown model type: " model-type
                                          ". Available: " (str/join ", " (keys model-type-map)))
                                     {:model-type model-type})))
                   (let [;; Execute the training SELECT to get data
                         parsed-training (sql/parse-sql training-sql registry)
                         _ (when (:error parsed-training)
                             (throw (ex-info (str "Error in training query: " (:error parsed-training))
                                             {:sql training-sql})))
                         training-result (q/q (:query parsed-training))
                         training-cols (q/results->columns training-result)
                        ;; Merge defaults with user options
                         train-opts (merge (:default-opts type-config)
                                           options
                                           {:from training-cols})
                        ;; Train the model
                         train-fn (:train-fn type-config)
                         model (assoc (train-fn train-opts) :model-type model-type)]
                     (swap! table-registry-atom assoc-in ["__models__" model-name] model)
                    ;; R3: durable model registry. Trained models are
                    ;; plain Clojure data (per `iforest.clj`), so
                    ;; Konserve serialisation is just a write. On
                    ;; restart `hydrate-models!` reinstates them.
                     (when store
                       (srv-state/put! store :models model-name model))
                     (println (str "Created model '" model-name "' (" model-type ") with "
                                   (:n-features model) " features"))
                     (PgWireServer$QueryResult/empty "CREATE MODEL")))

                 :drop-model
                 (let [{:keys [model-name if-exists?]} ddl
                       models (get @table-registry-atom "__models__")]
                   (when (and (not if-exists?) (not (get models model-name)))
                     (throw (ex-info (str "Model not found: " model-name)
                                     {:model model-name
                                      :available (keys models)})))
                   (swap! table-registry-atom update "__models__" dissoc model-name)
                   (when store
                     (srv-state/delete! store :models model-name))
                   (PgWireServer$QueryResult/empty "DROP MODEL"))

                 :insert
                 (let [existing (get @table-registry-atom table)
                       _        (when-not existing
                                  (throw (ex-info (str "Table not found: " table) {:table table})))
                      ;; Auto-fill column list for durable tables when
                      ;; INSERT INTO t VALUES (...) omits the explicit
                      ;; column list. The declaration order is recorded
                      ;; in :durable-binding so positional VALUES keep
                      ;; binding to the right columns post-restart.
                       ddl (if (and (not (:columns ddl))
                                    (some-> existing meta :durable-binding :column-order))
                             (assoc ddl :columns
                                    (-> existing meta :durable-binding :column-order))
                             ddl)
                      ;; Step 5: DECIMAL columns store int64 unscaled
                      ;; values. Coerce any BigDecimal/Long/Double/String
                      ;; literal in each row to the column's unscaled
                      ;; long form before the storage path sees it.
                      ;; No-op when the table has no DECIMAL columns.
                       col-order (or (:columns ddl) (vec (keys existing)))
                       schema    (:column-schema (meta existing))
                      ;; Step 6: range-check unsigned columns BEFORE the
                      ;; decimal coercion. Out-of-range values must surface
                      ;; before any partial conversion lands.
                       rows      (check-unsigned-rows! rows col-order schema table)
                       rows      (coerce-rows-for-decimals rows col-order schema table)
                       ddl       (assoc ddl :rows rows)]
                  ;; ENUM validation: for each enum-typed column,
                  ;; every inserted label must be in the declared set.
                  ;; Runs before any storage path so partial-insert
                  ;; can't leave the table in a half-validated state.
                   (validate-enum-rows!
                    table col-order rows (meta existing)
                    (get @table-registry-atom "__enums__"))
                   (cond
                ;; INSERT … FOR PORTION OF VALID_TIME on a fully
                ;; index-backed bitemporal table lowers to append!
                ;; with vf/vt stamped from the period.
                     (and (:period ddl)
                          (= :valid_time (:axis (:period ddl)))
                          (index-backed-cols? existing))
                     (let [{:keys [new-cols n-inserted]}
                           (insert-portion-via-index-backend
                            table existing (:columns ddl) rows (:period ddl) (meta existing))]
                       (swap! table-registry-atom assoc table new-cols)
                       (PgWireServer$QueryResult/empty (str "INSERT 0 " n-inserted)))

                ;; Plain INSERT on an index-backed bitemporal table
                ;; with explicit column list lowers to append! so
                ;; the user can supply vf/vt directly. When valid is
                ;; present, build metadata via `bitemporal-ds-meta`
                ;; so the :system axis is preserved — without it,
                ;; `dataset/append!` won't auto-stamp _system_from/_to
                ;; and SCD2-on-system silently breaks (caught by
                ;; copilot review #1 on PR #27).
                     (and (index-backed-cols? existing) (:columns ddl))
                     (let [valid (bitemporal-valid-cfg existing (meta existing))
                           ds-meta (when valid
                                     (bitemporal-ds-meta existing (meta existing) valid))
                           ds (dataset/make-dataset existing
                                                    (cond-> {:name table}
                                                      ds-meta (assoc :metadata ds-meta)))
                           mutated (reduce (fn [tds row-vals]
                                             (dataset/append! tds (zipmap (:columns ddl) row-vals)))
                                           (transient ds)
                                           rows)
                          ;; R2: when the table was created with :store,
                          ;; sync the sealed dataset to its bound branch
                          ;; so INSERT survives restart.
                           sealed (let [s (persistent! mutated)]
                                    (if (and store (durable-cols? existing))
                                      (dataset/sync! s store
                                                     (-> existing meta :durable-binding :branch))
                                      s))
                           new-cols (dataset/columns sealed)
                           new-cols (if-let [m (meta existing)] (with-meta new-cols m) new-cols)]
                       (swap! table-registry-atom assoc table new-cols)
                       (PgWireServer$QueryResult/empty (str "INSERT 0 " (count rows))))

                ;; Index-backed table without explicit column list — reject
                ;; rather than silently mismatch positions against an
                ;; encoded column map (whose iteration order isn't guaranteed
                ;; to match a SQL `CREATE TABLE` order).
                     (index-backed-cols? existing)
                     (throw (ex-info
                             (str "INSERT on a stratum-index-backed table requires an explicit "
                                  "column list: `INSERT INTO " table " (col1, col2, ...) VALUES (...)`")
                             {:table table}))

                     :else
                     (do
                       (let [col-keys (vec (keys existing))
                             n-existing (if-let [first-col (get existing (first col-keys))]
                                          (cond
                                            (instance? (Class/forName "[J") first-col)
                                            (alength ^longs first-col)
                                            (instance? (Class/forName "[D") first-col)
                                            (alength ^doubles first-col)
                                            (instance? (Class/forName "[Ljava.lang.String;") first-col)
                                            (alength ^"[Ljava.lang.String;" first-col)
                                            :else 0)
                                          0)
                             n-new (count rows)
                             n-total (+ n-existing n-new)
                             new-cols
                             (into {}
                                   (map-indexed
                                    (fn [ci col-key]
                                      (let [old-arr (get existing col-key)]
                                        [col-key
                                         (cond
                                           (instance? (Class/forName "[J") old-arr)
                                           (let [arr (long-array n-total)]
                                             (System/arraycopy ^longs old-arr 0 arr 0 n-existing)
                                             (dotimes [r n-new]
                                               (let [v (nth (nth rows r) ci)]
                                                 (aset arr (+ n-existing r)
                                                       (long (if (nil? v) Long/MIN_VALUE v)))))
                                             arr)

                                           (instance? (Class/forName "[D") old-arr)
                                           (let [arr (double-array n-total)]
                                             (System/arraycopy ^doubles old-arr 0 arr 0 n-existing)
                                             (dotimes [r n-new]
                                               (let [v (nth (nth rows r) ci)]
                                                 (aset arr (+ n-existing r)
                                                       (double (if (nil? v) Double/NaN v)))))
                                             arr)

                                           (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                                           (let [arr (make-array String n-total)]
                                             (System/arraycopy ^"[Ljava.lang.String;" old-arr 0
                                                               arr 0 n-existing)
                                             (dotimes [r n-new]
                                               (aset ^"[Ljava.lang.String;" arr (+ n-existing r)
                                                     (let [v (nth (nth rows r) ci)]
                                                       (when (some? v) (str v)))))
                                             arr))]))
                                    col-keys))
                             new-cols (with-meta new-cols (meta existing))]
                         (swap! table-registry-atom assoc table new-cols))
                       (PgWireServer$QueryResult/empty
                        (str "INSERT 0 " (count rows))))))

                 :upsert
                 (let [existing (get @table-registry-atom table)]
                   (when-not existing
                     (throw (ex-info (str "Table not found: " table) {:table table})))
              ;; UPSERT (INSERT … ON CONFLICT) combined with FOR
              ;; PORTION OF VALID_TIME has subtle semantics — the
              ;; conflict target needs to be evaluated *per slice*,
              ;; not per (positional) row. Reject explicitly rather
              ;; than silently doing the wrong thing.
                   (when (:period ddl)
                     (throw (ex-info
                             (str "INSERT … ON CONFLICT … FOR PORTION OF VALID_TIME "
                                  "is not supported. Use INSERT FOR PORTION OF + "
                                  "UPDATE FOR PORTION OF as separate statements, "
                                  "or DELETE FOR PORTION OF + INSERT FOR PORTION OF.")
                             {:table table
                              :period (:period ddl)})))
                   (when (and (index-backed-cols? existing) (nil? (:period ddl)))
                     (throw (ex-info
                             (str "INSERT … ON CONFLICT on stratum-index-backed tables "
                                  "is not yet supported. Use plain INSERT + UPDATE separately.")
                             {:table table})))
                   (let [col-keys (vec (keys existing))
                         n-existing (let [first-col (get existing (first col-keys))]
                                      (cond
                                        (instance? (Class/forName "[J") first-col) (alength ^longs first-col)
                                        (instance? (Class/forName "[D") first-col) (alength ^doubles first-col)
                                        (instance? (Class/forName "[Ljava.lang.String;") first-col)
                                        (alength ^"[Ljava.lang.String;" first-col)
                                        :else 0))
                        ;; For each row, find if a conflicting row exists
                         conflict-col-indices (mapv #(.indexOf ^java.util.List col-keys %) conflict-cols)
                         get-val (fn [arr ^long i]
                                   (cond
                                     (instance? (Class/forName "[J") arr)
                                     (let [v (aget ^longs arr (int i))]
                                       (when-not (= v Long/MIN_VALUE) v))
                                     (instance? (Class/forName "[D") arr)
                                     (let [v (aget ^doubles arr (int i))]
                                       (when-not (Double/isNaN v) v))
                                     (instance? (Class/forName "[Ljava.lang.String;") arr)
                                     (aget ^"[Ljava.lang.String;" arr (int i))))
                         set-val (fn [arr ^long i v]
                                   (cond
                                     (instance? (Class/forName "[J") arr)
                                     (aset ^longs arr (int i) (long (if (nil? v) Long/MIN_VALUE v)))
                                     (instance? (Class/forName "[D") arr)
                                     (aset ^doubles arr (int i) (double (if (nil? v) Double/NaN v)))
                                     (instance? (Class/forName "[Ljava.lang.String;") arr)
                                     (aset ^"[Ljava.lang.String;" arr (int i)
                                           (when (some? v) (str v)))))
                        ;; Process each row: find conflict or insert
                         result (reduce
                                 (fn [{:keys [cols n-rows n-inserted n-updated]} row]
                                   (let [;; Extract conflict key values from this row
                                         row-conflict-vals (mapv #(nth row (int %)) conflict-col-indices)
                                        ;; Find matching existing row
                                         match-idx (loop [i (int 0)]
                                                     (when (< i n-rows)
                                                       (let [existing-vals
                                                             (mapv (fn [cc]
                                                                     (get-val (get cols cc) i))
                                                                   conflict-cols)]
                                                         (if (= row-conflict-vals existing-vals)
                                                           i
                                                           (recur (inc i))))))]
                                     (if match-idx
                                      ;; Conflict found
                                       (if (= action :do-update)
                                        ;; Apply assignments with EXCLUDED = new row values
                                         (let [excluded (zipmap col-keys row)]
                                           (doseq [{:keys [col expr]} assignments]
                                             (let [arr (get cols col)
                                                   eval-val (fn eval-val [e]
                                                              (cond
                                                                (keyword? e)
                                                                (if (= "excluded" (namespace e))
                                                                  (get excluded (keyword (name e)))
                                                                  (get-val (get cols e) match-idx))
                                                                (number? e) e
                                                                (string? e) e
                                                                (nil? e) nil
                                                                (vector? e)
                                                                (nil-safe-arith (first e)
                                                                                (eval-val (nth e 1))
                                                                                (eval-val (nth e 2)))))
                                                   v (eval-val expr)]
                                               (set-val arr match-idx v)))
                                           {:cols cols :n-rows n-rows
                                            :n-inserted n-inserted :n-updated (inc n-updated)})
                                        ;; DO NOTHING
                                         {:cols cols :n-rows n-rows
                                          :n-inserted n-inserted :n-updated n-updated})
                                      ;; No conflict — append row
                                       (let [new-n (inc n-rows)
                                             new-cols
                                             (into {}
                                                   (map-indexed
                                                    (fn [ci col-key]
                                                      (let [old-arr (get cols col-key)
                                                            new-arr
                                                            (cond
                                                              (instance? (Class/forName "[J") old-arr)
                                                              (let [arr (long-array new-n)]
                                                                (System/arraycopy ^longs old-arr 0 arr 0 n-rows)
                                                                (aset arr n-rows
                                                                      (long (let [v (nth row ci)]
                                                                              (if (nil? v) Long/MIN_VALUE v))))
                                                                arr)
                                                              (instance? (Class/forName "[D") old-arr)
                                                              (let [arr (double-array new-n)]
                                                                (System/arraycopy ^doubles old-arr 0 arr 0 n-rows)
                                                                (aset arr n-rows
                                                                      (double (let [v (nth row ci)]
                                                                                (if (nil? v) Double/NaN v))))
                                                                arr)
                                                              (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                                                              (let [arr (make-array String new-n)]
                                                                (System/arraycopy ^"[Ljava.lang.String;" old-arr 0
                                                                                  arr 0 n-rows)
                                                                (aset ^"[Ljava.lang.String;" arr n-rows
                                                                      (let [v (nth row ci)]
                                                                        (when (some? v) (str v))))
                                                                arr))]
                                                        [col-key new-arr]))
                                                    col-keys))]
                                         {:cols new-cols :n-rows new-n
                                          :n-inserted (inc n-inserted) :n-updated n-updated}))))
                                 {:cols existing :n-rows n-existing
                                  :n-inserted 0 :n-updated 0}
                                 rows)]
                     (swap! table-registry-atom assoc table
                            (with-meta (:cols result) (meta existing)))
                     (PgWireServer$QueryResult/empty
                      (str "INSERT 0 " (:n-inserted result)))))

                 :update
                 (let [existing (get @table-registry-atom table)]
                   (when-not existing
                     (throw (ex-info (str "Table not found: " table) {:table table})))
                   (cond
                ;; UPDATE … FOR PORTION OF VALID_TIME on a fully
                ;; index-backed bitemporal table lowers to
                ;; dataset/bounded-update! (SQL:2011 non-sequenced UPDATE).
                     (and (:period ddl)
                          (= :valid_time (:axis (:period ddl)))
                          (index-backed-cols? existing))
                     (let [{:keys [new-cols n-updated]}
                           (update-portion-via-index-backend
                            table existing where assignments
                            (:period ddl) (meta existing))]
                       (swap! table-registry-atom assoc table new-cols)
                       (PgWireServer$QueryResult/empty (str "UPDATE " n-updated)))

                     :else
                     (do
                       (let [;; FROM table for UPDATE FROM (joined updates)
                             from-existing (when from
                                             (let [ft (get @table-registry-atom (:table from))]
                                               (when-not ft
                                                 (throw (ex-info (str "FROM table not found: " (:table from))
                                                                 {:table (:table from)})))
                                               ft))
                        ;; Helper to get value from a typed array at index i
                             get-arr-val (fn [arr ^long i]
                                           (cond
                                             (instance? (Class/forName "[J") arr)
                                             (let [v (aget ^longs arr (int i))]
                                               (when-not (= v Long/MIN_VALUE) v))
                                             (instance? (Class/forName "[D") arr)
                                             (let [v (aget ^doubles arr (int i))]
                                               (when-not (Double/isNaN v) v))
                                             (instance? (Class/forName "[Ljava.lang.String;") arr)
                                             (aget ^"[Ljava.lang.String;" arr (int i))))
                        ;; Helper to set value in a typed array
                             set-arr-val (fn [arr ^long i v]
                                           (cond
                                             (instance? (Class/forName "[J") arr)
                                             (aset ^longs arr (int i) (long (if (nil? v) Long/MIN_VALUE v)))
                                             (instance? (Class/forName "[D") arr)
                                             (aset ^doubles arr (int i) (double (if (nil? v) Double/NaN v)))
                                             (instance? (Class/forName "[Ljava.lang.String;") arr)
                                             (aset ^"[Ljava.lang.String;" arr (int i)
                                                   (when (some? v) (str v)))))
                        ;; Resolve column from target table, then from FROM table
                        ;; Handles namespaced keywords: :table/col → specific table,
                        ;; :col → target first, then FROM
                             from-table-name (when from (:table from))
                             from-alias (when from (:alias from))
                             resolve-col (fn [e target-table from-table target-i from-i]
                                           (let [target-i (long target-i)
                                                 from-i (long from-i)
                                                 ns (namespace e)
                                                 col-kw (keyword (name e))]
                                             (if ns
                                          ;; Qualified: resolve to specific table
                                               (if (or (= ns table) (= ns table-alias))
                                                 (when-let [arr (get target-table col-kw)]
                                                   (get-arr-val arr target-i))
                                                 (when (and from-table (>= from-i 0))
                                                   (when-let [arr (get from-table col-kw)]
                                                     (get-arr-val arr from-i))))
                                          ;; Unqualified: target first, then FROM
                                               (or (when-let [arr (get target-table e)]
                                                     (get-arr-val arr target-i))
                                                   (when (and from-table (>= from-i 0))
                                                     (when-let [arr (get from-table e)]
                                                       (get-arr-val arr from-i)))))))
                        ;; Evaluate expression for a row pair (target-i, from-i)
                             eval-row (fn eval-row [e target-table from-table target-i from-i]
                                        (cond
                                          (keyword? e) (resolve-col e target-table from-table target-i from-i)
                                          (number? e) e
                                          (string? e) e
                                          (nil? e) nil
                                          (vector? e)
                                          (nil-safe-arith (first e)
                                                          (eval-row (nth e 1) target-table from-table target-i from-i)
                                                          (eval-row (nth e 2) target-table from-table target-i from-i))))
                        ;; Evaluate predicate for a row pair
                             eval-pred-row (fn [pred target-table from-table target-i from-i]
                                             (let [ev (fn [e] (eval-row e target-table from-table target-i from-i))]
                                               (case (first pred)
                                                 := (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                      (and (some? l) (some? r) (= l r)))
                                                 (:!= :<>) (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                             (and (some? l) (some? r) (not= l r)))
                                                 :> (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                      (and (some? l) (some? r) (> (double l) (double r))))
                                                 :< (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                      (and (some? l) (some? r) (< (double l) (double r))))
                                                 :>= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                       (and (some? l) (some? r) (>= (double l) (double r))))
                                                 :<= (let [l (ev (nth pred 1)) r (ev (nth pred 2))]
                                                       (and (some? l) (some? r) (<= (double l) (double r))))
                                                 :is-null (nil? (ev (nth pred 1)))
                                                 :is-not-null (some? (ev (nth pred 1)))
                                                 false)))
                             col-keys (vec (keys existing))
                             n-rows (let [first-col (get existing (first col-keys))]
                                      (cond
                                        (instance? (Class/forName "[J") first-col) (alength ^longs first-col)
                                        (instance? (Class/forName "[D") first-col) (alength ^doubles first-col)
                                        (instance? (Class/forName "[Ljava.lang.String;") first-col)
                                        (alength ^"[Ljava.lang.String;" first-col)
                                        :else 0))
                             n-from (when from-existing
                                      (let [first-col (val (first from-existing))]
                                        (cond
                                          (instance? (Class/forName "[J") first-col) (alength ^longs first-col)
                                          (instance? (Class/forName "[D") first-col) (alength ^doubles first-col)
                                          (instance? (Class/forName "[Ljava.lang.String;") first-col)
                                          (alength ^"[Ljava.lang.String;" first-col)
                                          :else 0)))
                        ;; Build match mask + from-row index for UPDATE FROM
                             match (boolean-array n-rows)
                             from-match (int-array n-rows -1) ;; index into FROM table, -1 = no match
                             _ (if (nil? where)
                                 (java.util.Arrays/fill match true)
                                 (if from-existing
                              ;; UPDATE FROM: for each target row, find first matching FROM row
                                   (dotimes [i n-rows]
                                     (loop [j (int 0)]
                                       (when (< j (int n-from))
                                         (if (every? #(eval-pred-row % existing from-existing (long i) (long j)) where)
                                           (do (aset match i true)
                                               (aset from-match i j))
                                           (recur (inc j))))))
                              ;; Simple UPDATE: evaluate predicates against target table only
                                   (dotimes [i n-rows]
                                     (aset match i
                                           (boolean
                                            (every? #(eval-pred-row % existing nil (long i) -1) where))))))
                        ;; Count matches
                             n-matched (loop [i (int 0) c (int 0)]
                                         (if (>= i n-rows) c
                                             (recur (inc i) (if (aget match i) (inc c) c))))
                        ;; Apply assignments to matching rows
                             new-cols
                             (reduce
                              (fn [cols {:keys [col expr]}]
                                (let [arr (get cols col)]
                                  (when-not arr
                                    (throw (ex-info (str "Column not found: " col)
                                                    {:column col :table table})))
                                  (dotimes [i n-rows]
                                    (when (aget match i)
                                      (let [from-i (long (aget from-match i))
                                            v (eval-row expr existing from-existing (long i) from-i)]
                                        (set-arr-val arr i v))))
                                  (assoc cols col arr)))
                              existing
                              assignments)]
                         (swap! table-registry-atom assoc table
                                (with-meta new-cols (meta existing)))
                         (PgWireServer$QueryResult/empty (str "UPDATE " n-matched))))))

                 :delete
                 (let [existing (get @table-registry-atom table)]
                   (when-not existing
                     (throw (ex-info (str "Table not found: " table) {:table table})))
              ;; Index-backed tables (registered via stratum's columnar
              ;; engine — `register-table!` with `idx/index-from-seq`
              ;; columns) route through `ds-delete-rows!` (no period)
              ;; or `retract!` (with FOR PORTION OF VALID_TIME period)
              ;; instead of the array rebuild path below. The arrays
              ;; path is preserved unchanged for CREATE TABLE statements.
                   (if (index-backed-cols? existing)
                ;; `ERASE FROM …` bypasses
                ;; the bounded-retract path and always physically purges
                ;; via `ds-delete-rows!` — the distinct verb makes the
                ;; "destroys across both axes" intent explicit. Plain
                ;; DELETE today also routes to ds-delete-rows! on
                ;; index-backed tables; ERASE just hardens the contract.
                     (let [period (:period ddl)
                           erase? (:erase? ddl)
                           {:keys [new-cols n-deleted]}
                           (cond
                             erase?
                             (delete-via-index-backend
                              table existing where (meta existing))

                             (and period (= :valid_time (:axis period)))
                             (delete-portion-via-index-backend
                              table existing where period (meta existing))

                             :else
                             (delete-via-index-backend
                              table existing where (meta existing)))
                           tag (if erase? "ERASE" "DELETE")]
                       (swap! table-registry-atom assoc table new-cols)
                       (PgWireServer$QueryResult/empty (str tag " " n-deleted)))
                     (let [col-keys (vec (keys existing))
                           n-rows (let [first-col (get existing (first col-keys))]
                                    (cond
                                      (instance? (Class/forName "[J") first-col) (alength ^longs first-col)
                                      (instance? (Class/forName "[D") first-col) (alength ^doubles first-col)
                                      (instance? (Class/forName "[Ljava.lang.String;") first-col)
                                      (alength ^"[Ljava.lang.String;" first-col)
                                      :else 0))
                        ;; Build delete mask (true = delete this row)
                           delete-mask (boolean-array n-rows)
                           _ (if (nil? where)
                               (java.util.Arrays/fill delete-mask true)
                        ;; `read-cell` (used by eval-dml-predicate)
                        ;; handles both raw arrays AND :source :index
                        ;; columns, so the inline duplicate that
                        ;; previously lived here only covered raw
                        ;; arrays and silently NPE'd on NULL
                        ;; arithmetic (copilot review #2). Route
                        ;; through the canonical evaluator.
                               (dotimes [i n-rows]
                                 (aset delete-mask i
                                       (boolean (eval-dml-predicate existing where i)))))
                        ;; Count deletes and gather surviving indices
                           n-deleted (loop [i (int 0) c (int 0)]
                                       (if (>= i n-rows) c
                                           (recur (inc i) (if (aget delete-mask i) (inc c) c))))
                           n-surviving (- n-rows n-deleted)
                        ;; Rebuild columns with surviving rows
                           new-cols
                           (into {}
                                 (map (fn [col-key]
                                        (let [old-arr (get existing col-key)]
                                          [col-key
                                           (cond
                                             (instance? (Class/forName "[J") old-arr)
                                             (let [arr (long-array n-surviving)]
                                               (loop [i (int 0) j (int 0)]
                                                 (when (< i n-rows)
                                                   (if (aget delete-mask i)
                                                     (recur (inc i) j)
                                                     (do (aset arr j (aget ^longs old-arr i))
                                                         (recur (inc i) (inc j))))))
                                               arr)

                                             (instance? (Class/forName "[D") old-arr)
                                             (let [arr (double-array n-surviving)]
                                               (loop [i (int 0) j (int 0)]
                                                 (when (< i n-rows)
                                                   (if (aget delete-mask i)
                                                     (recur (inc i) j)
                                                     (do (aset arr j (aget ^doubles old-arr i))
                                                         (recur (inc i) (inc j))))))
                                               arr)

                                             (instance? (Class/forName "[Ljava.lang.String;") old-arr)
                                             (let [arr (make-array String n-surviving)]
                                               (loop [i (int 0) j (int 0)]
                                                 (when (< i n-rows)
                                                   (if (aget delete-mask i)
                                                     (recur (inc i) j)
                                                     (do (aset ^"[Ljava.lang.String;" arr j
                                                               (aget ^"[Ljava.lang.String;" old-arr i))
                                                         (recur (inc i) (inc j))))))
                                               arr))]))
                                      col-keys))]
                       (swap! table-registry-atom assoc table
                              (with-meta new-cols (meta existing)))
                       (PgWireServer$QueryResult/empty (str "DELETE " n-deleted))))))))  ;; end locking DDL/DML dispatch

            ;; Parse/translation error
           (:error parsed)
           (PgWireServer$QueryResult. ^String (:error parsed))

            ;; Normal query — execute via Stratum engine
           (:query parsed)
           (let [query (:query parsed)
                  ;; Attach anomaly models to query map — scoring happens post-join in query.clj
                 query (attach-anomaly-models query registry)
                 result (q/q query)
                 result (if-let [post-aggs (:_post-aggs query)]
                          (sql/apply-post-aggs result post-aggs)
                          result)
                 result (if-let [sel-cols (:_select-columns query)]
                          (sql/apply-select-columns result sel-cols)
                          result)
                ;; Step 4a + 5c: build the column-meta map from the
                ;; live registry (declared columns) and overlay
                ;; per-query output-meta (aggregations preserve their
                ;; input column's DECIMAL precision/scale, etc.).
                ;; Computed columns / CASTs / expressions still fall
                ;; back to value-based inference.
                 column-meta (merge (sql/collect-column-meta @table-registry-atom)
                                    (sql/output-column-meta query @table-registry-atom))]
             (sql/format-results result column-meta))

           :else
           (PgWireServer$QueryResult. "Internal error: unexpected parse result"))))
     (catch Exception e
       (PgWireServer$QueryResult. (str (.getMessage e)))))))

;; ----------------------------------------------------------------------------
;; Durable SQL-table machinery (restart-safety R2)
;;
;; SQL `CREATE TABLE` over a server with `:store` configured creates a real
;; empty index-backed dataset, syncs it to a dedicated branch (`sql/<name>`),
;; persists the binding under [:server :sql-tables <name>], and tags the
;; in-registry column map with a `:durable-binding` Clojure metadata marker.
;; INSERT/UPDATE/DELETE/UPSERT on a marked table re-sync via the index-backed
;; path after each statement. Without `:store`, the original heap-only path
;; runs unchanged.
;; ----------------------------------------------------------------------------

(defn- empty-col-array
  "Produce a zero-length array for an empty SQL CREATE TABLE column."
  [type]
  (case type
    :int64   (long-array 0)
    :float64 (double-array 0)
    :string  (make-array String 0)))

(defn- durable-branch
  "Branch name for a SQL-created table. Per-table to keep them isolated
   from user-managed branches."
  ^String [^String table-name]
  (str "sql/" table-name))

(defn- coerce-row-for-decimals
  "Pre-INSERT row transformation: for each column tagged `:decimal?`
   in the table's `:column-schema` metadata, convert the row's value
   (Long/Double/String/BigDecimal) to the int64 unscaled
   representation at the column's declared scale. Non-decimal cells
   pass through unchanged.

   Returns the new row. `col-order` is the parallel list of column
   keywords for the row's positional values."
  [row col-order column-schema table-name]
  (if-not (some #(get-in column-schema [% :decimal?]) col-order)
    row
    (mapv (fn [col-kw v]
            (if-let [spec (get column-schema col-kw)]
              (if (:decimal? spec)
                (when (some? v)
                  (decimal/bigdec->unscaled-long
                   (decimal/coerce->bigdec v)
                   (:scale spec)
                   (str table-name "." (name col-kw))))
                v)
              v))
          col-order
          row)))

(defn- coerce-rows-for-decimals
  "Map `coerce-row-for-decimals` over a sequence of rows. No-op when
   the table has no DECIMAL columns."
  [rows col-order column-schema table-name]
  (if-not (some #(get-in column-schema [% :decimal?]) col-order)
    rows
    (mapv #(coerce-row-for-decimals % col-order column-schema table-name) rows)))

;; ---------------------------------------------------------------------------
;; Step 6 / 8a: Unsigned integer range-checking on INSERT.
;;
;; UTINYINT (u8), USMALLINT (u16), and UINTEGER (u32) fit comfortably
;; inside an int64 with positive arithmetic. UBIGINT (u64) requires
;; BigInteger range checks because the upper bound 2^64-1 exceeds
;; Long/MAX_VALUE; we store the value as its bit-reinterpreted long
;; (so 2^64-1 becomes -1L, etc.) and rely on `Long.toUnsignedString`
;; on the read side.

(def ^:private ^java.math.BigInteger U64-MAX
  (.subtract (.shiftLeft java.math.BigInteger/ONE 64) java.math.BigInteger/ONE))

(defn- ^:private unsigned-max
  "Inclusive upper bound for an unsigned-width column. Returns long for
   widths ≤ 32 and BigInteger for width 64."
  [^long width]
  (case width
    8  255
    16 65535
    32 4294967295
    64 U64-MAX))

(defn- ^:private coerce-to-bigint
  "Best-effort coercion of an INSERT value to BigInteger for range
   checking. Returns nil if the value isn't an integer at all."
  ^java.math.BigInteger [v]
  (cond
    (instance? java.math.BigInteger v) v
    (instance? java.math.BigDecimal v) (.toBigIntegerExact ^java.math.BigDecimal v)
    (instance? Long v)    (java.math.BigInteger/valueOf (long v))
    (instance? Integer v) (java.math.BigInteger/valueOf (long v))
    (instance? Short v)   (java.math.BigInteger/valueOf (long v))
    (instance? Byte v)    (java.math.BigInteger/valueOf (long v))
    (string? v) (try (java.math.BigInteger. ^String v)
                     (catch NumberFormatException _ nil))
    :else nil))

(defn- check-unsigned-row!
  "Validate that every unsigned column's value in `row` fits its
   declared range. Throws ex-info with sqlstate 22003 (numeric value out
   of range) when a value falls outside [0, max] for its width. NULL
   values pass through (range constraint applies only to non-NULL).

   For UBIGINT (width 64), values > Long/MAX_VALUE are rewritten in
   place as their bit-reinterpreted long (via BigInteger.longValue())
   so the column's int64 backing stores the correct unsigned bit
   pattern. Smaller widths leave the value unchanged."
  [row col-order column-schema table-name]
  (mapv (fn [col-kw v]
          (if-let [width (get-in column-schema [col-kw :unsigned-width])]
            (if (nil? v)
              v
              (let [n (coerce-to-bigint v)
                    max-val (unsigned-max width)]
                (cond
                  (nil? n)
                  (throw (ex-info (str "Value " v " is not a valid integer for "
                                       table-name "." (name col-kw)
                                       " (U" (case (long width)
                                               8 "TINY" 16 "SMALL"
                                               32 "" 64 "BIG")
                                       "INT)")
                                  {:sqlstate "22003"
                                   :column col-kw :value v}))
                  (or (neg? (.signum ^java.math.BigInteger n))
                      (pos? (.compareTo ^java.math.BigInteger n
                                        ^java.math.BigInteger
                                        (if (instance? java.math.BigInteger max-val)
                                          max-val
                                          (java.math.BigInteger/valueOf (long max-val))))))
                  (throw (ex-info (str "Value " n " out of range for "
                                       table-name "." (name col-kw)
                                       " (u" width "; valid range 0.."
                                       max-val ")")
                                  {:sqlstate "22003"
                                   :column col-kw :value n :width width}))
                  ;; UBIGINT: rewrite to the bit-reinterpreted long so
                  ;; storage stays int64. .longValue() truncates the
                  ;; high bits — fine because we've already validated
                  ;; the BigInteger fits in 64 bits.
                  (= 64 width) (.longValue ^java.math.BigInteger n)
                  :else v)))
            v))
        col-order
        row))

(defn- check-unsigned-rows!
  "Validate every row against the table's unsigned-width columns. No-op
   when the table has no unsigned columns."
  [rows col-order column-schema table-name]
  (if-not (some #(get-in column-schema [% :unsigned-width]) col-order)
    rows
    (mapv #(check-unsigned-row! % col-order column-schema table-name) rows)))

(defn- columns->descriptors
  "Reduce a DDL :columns vector into a {col-kw <descriptor>} schema map.
   Used to compose the side-schema attached as Clojure metadata so
   downstream INSERT/UPDATE handlers can see per-column policy
   without re-reading the parser output. Captures `:temporal-unit`
   (DATE/TIMESTAMP precision), `:enum-of` (declared enum name),
   `:decimal? :precision :scale` (step 5 — DECIMAL/NUMERIC tagging),
   and `:unsigned-width` (step 6 — UTINYINT/USMALLINT/UINTEGER)."
  [columns]
  (into {}
        (keep (fn [{:keys [name temporal-unit enum-of decimal? precision scale
                           unsigned-width]}]
                (when (or temporal-unit enum-of decimal? unsigned-width)
                  [(keyword name)
                   (cond-> {}
                     temporal-unit  (assoc :temporal-unit temporal-unit)
                     enum-of        (assoc :enum-of enum-of)
                     decimal?       (assoc :decimal? true
                                           :precision precision
                                           :scale scale)
                     unsigned-width (assoc :unsigned-width unsigned-width))])))
        columns))

(defn- durable-meta
  "Build the registry-meta map for a durable SQL table."
  [branch column-order column-schema]
  (let [binding (cond-> {:branch branch}
                  (seq column-order) (assoc :column-order column-order))]
    (cond-> {:durable-binding binding}
      (seq column-schema) (assoc :column-schema column-schema))))

(defn- make-durable-sql-table!
  "Create an empty index-backed dataset, sync to `store` under
   `sql/<table-name>`, persist the [:server :sql-tables <name>] record, and
   return the index-backed column map (with :durable-binding + optional
   :column-schema metadata) ready for the registry atom.

   The column declaration order is recorded in `:durable-binding
   :column-order` so positional `INSERT INTO t VALUES (...)` keeps
   mapping the right values to the right columns even after a restart
   (where the index-backed dataset's map iteration order is otherwise
   unspecified)."
  [store table-name columns]
  (let [branch       (durable-branch table-name)
        column-order (mapv (comp keyword :name) columns)
        empty-cols   (into {} (map (fn [{:keys [name type]}]
                                     [(keyword name) (empty-col-array type)]))
                           columns)
        side-schema  (columns->descriptors columns)
        ds           (-> (dataset/make-dataset empty-cols {:name table-name})
                         dataset/ensure-indexed
                         (dataset/sync! store branch))
        new-cols     (dataset/columns ds)]
    (srv-state/put! store :sql-tables table-name
                    {:branch         branch
                     :column-order   column-order
                     :temporal-units side-schema})
    (with-meta new-cols (durable-meta branch column-order side-schema))))

(defn- durable-cols?
  "True when `cols` was produced by `make-durable-sql-table!` (or an INSERT/
   UPDATE that preserved the marker)."
  [cols]
  (some-> (meta cols) :durable-binding))

(defn- resync-durable-table!
  "Re-sync `cols` (already index-backed in the index-backed-INSERT path) to
   the bound branch on `store`. Returns the refreshed index-backed column
   map with metadata preserved.

   Use after a DML statement on a durable table — the caller has already
   built a new dataset shape via the existing append!/edit path; this
   walks it through `make-dataset → ensure-indexed → sync!` so the
   chunk-level changes land in Konserve."
  [store table-name cols]
  (let [branch (-> cols meta :durable-binding :branch)
        ds    (-> (dataset/make-dataset cols {:name table-name})
                  dataset/ensure-indexed
                  (dataset/sync! store branch))]
    (with-meta (dataset/columns ds) (meta cols))))

(defn- hydrate-sql-tables!
  "On server start with a configured store, load every persisted
   [:server :sql-tables <name>] dataset HEAD into the in-memory registry.
   Returns the count of tables loaded."
  [store registry-atom]
  (let [records (srv-state/list-section store :sql-tables)]
    (doseq [[table-name {:keys [branch column-order temporal-units]}] records]
      (try
        (let [ds     (dataset/load store branch)
              loaded (dataset/columns ds)]
          (swap! registry-atom assoc table-name
                 (with-meta loaded (durable-meta branch column-order temporal-units))))
        (catch Throwable t
          (println (str "WARNING: failed to hydrate SQL table '" table-name
                        "' from branch '" branch "': " (.getMessage t))))))
    (count records)))

(defn- hydrate-models!
  "On server start with a configured store, reinstall every persisted
   trained model into the in-memory registry's `__models__` slot.
   Returns the count of models loaded."
  [store registry-atom]
  (let [records (srv-state/list-section store :models)]
    (doseq [[model-name model] records]
      (swap! registry-atom assoc-in ["__models__" model-name] model))
    (count records)))

(defn- hydrate-enums!
  "On server start with a configured store, reinstall every persisted
   ENUM declaration into the in-memory registry's `__enums__` slot.
   Returns the count of enums loaded. The value-set + OID round-trips
   without modification — `resolve-enum-columns` consults this map
   when a CREATE TABLE column names an enum type, and INSERT
   validation reads it to reject unknown labels."
  [store registry-atom]
  (let [records (srv-state/list-section store :enums)]
    (doseq [[enum-name record] records]
      (swap! registry-atom assoc-in ["__enums__" enum-name] record))
    (count records)))

(defn- validate-enum-rows!
  "Reject INSERT/UPSERT/UPDATE rows whose enum-typed column values
   aren't in the declared label set. Throws with column + offending
   value on first violation — pre-empts any storage write so a
   partial-insert can't leave the table half-validated. NULL is
   permitted (SQL standard); column-level NOT NULL is a separate
   axis."
  [table-name col-order rows table-meta enums]
  (let [col-schema (:column-schema table-meta)
        enum-cols  (vec
                    (keep-indexed
                     (fn [idx col-kw]
                       (when-let [enum-name (get-in col-schema [col-kw :enum-of])]
                         (let [values (-> enums (get enum-name) :values-ordered set)]
                           {:idx       idx
                            :col       col-kw
                            :enum-name enum-name
                            :values    values})))
                     col-order))]
    (when (seq enum-cols)
      (doseq [[row-idx row] (map-indexed vector rows)
              {:keys [idx col enum-name values]} enum-cols]
        (let [v (nth row idx nil)]
          (when (and (some? v) (not (contains? values v)))
            (throw (ex-info (str "invalid input value for enum "
                                 enum-name ": " (pr-str v))
                            {:table     table-name
                             :column    col
                             :row       row-idx
                             :enum      enum-name
                             :value     v
                             :allowed   values}))))))))

(defn- resolve-enum-columns
  "Walk the DDL :columns vector and rewrite any column whose
   `:type` is `:unknown` to a string column tagged with the
   resolved enum metadata. Throws a clear error if the named type
   isn't in `enums` — silent fall-through to `:string` (the old
   behaviour) would mask user typos."
  [columns enums]
  (mapv (fn [{:keys [type type-name] :as col}]
          (if (= type :unknown)
            (if-let [enum (get enums type-name)]
              (-> col
                  (assoc :type :string)
                  (assoc :enum-of type-name)
                  (assoc :enum-values (:values-ordered enum))
                  (dissoc :type-name))
              (throw (ex-info (str "Unknown column type: " type-name
                                   " (no CREATE TYPE declares it)")
                              {:type-name type-name
                               :column    (:name col)
                               :available (keys enums)})))
            col))
        columns))

(defn- hydrate-live-tables!
  "On server start with a configured store, reinstall every persisted
   live-table binding pointing at this server's store. Live tables that
   pointed at a *different* Konserve store can't be reinstalled
   automatically (we don't serialize foreign store handles) — those
   require the user to call `register-live-table!` again at start.
   Returns the count of bindings restored."
  [store registry-atom]
  (let [records (srv-state/list-section store :live-tables)]
    (doseq [[table-name {:keys [branch]}] records]
      (swap! registry-atom assoc table-name
             {:__live true :store store :branch branch}))
    (count records)))

(defn- make-handler-factory
  "Create a PgWireServer.QueryHandlerFactory. Each call to .create() returns
   an independent QueryHandler with its own per-connection transaction state.

   The 2-arity overload runs without a durable store — convenient for
   tests that drive the handler manually without going through
   `server/start`."
  ([table-registry-atom data-dir-atom]
   (make-handler-factory table-registry-atom data-dir-atom nil))
  ([table-registry-atom data-dir-atom store]
   (reify PgWireServer$QueryHandlerFactory
     (create [_]
      ;; Per-handler atoms: tx-state for transaction status (in-tx /
      ;; aborted) and session-settings for `SET datahike.clock_time`
      ;; — both scoped to ONE PgWire connection so cross-session
      ;; leakage can't happen. Round-4 agent P1 #1.2 lifted
      ;; `__settings__` from the shared registry to a per-connection
      ;; atom; the dynamic var `*session-settings*` is bound around
      ;; every `execute-sql` so the SET handler writes here, not the
      ;; global.
       (let [tx-state (atom {:in-tx false :aborted false})
             session-settings (atom {})]
         (reify PgWireServer$QueryHandler
           (execute [_ sql]
             (binding [*session-settings* session-settings]
               (let [{:keys [in-tx aborted]} @tx-state]
                 (cond
                ;; Reject non-tx commands when transaction is in error state
                   (and aborted (not (re-find #"(?i)^\s*(ROLLBACK|COMMIT|END)" sql)))
                   (-> (PgWireServer$QueryResult. "current transaction is aborted, commands ignored until end of transaction block")
                       (.withTxStatus \E))
                ;; BEGIN — start transaction
                   (re-find #"(?i)^\s*BEGIN" sql)
                   (do (swap! tx-state assoc :in-tx true :aborted false)
                       (-> ^PgWireServer$QueryResult (execute-sql sql table-registry-atom data-dir-atom store)
                           (.withTxStatus \T)))
                ;; COMMIT / END — commit transaction
                   (re-find #"(?i)^\s*(COMMIT|END)\s*$" sql)
                   (do (reset! tx-state {:in-tx false :aborted false})
                       (-> ^PgWireServer$QueryResult (execute-sql sql table-registry-atom data-dir-atom store)
                           (.withTxStatus \I)))
                ;; ROLLBACK — roll back transaction
                   (re-find #"(?i)^\s*ROLLBACK" sql)
                   (do (reset! tx-state {:in-tx false :aborted false})
                       (-> ^PgWireServer$QueryResult (execute-sql sql table-registry-atom data-dir-atom store)
                           (.withTxStatus \I)))
                ;; Normal command — execute and propagate tx status
                   :else
                   (let [^PgWireServer$QueryResult qr (execute-sql sql table-registry-atom data-dir-atom store)]
                     (cond
                       (and in-tx (.error qr))
                       (do (swap! tx-state assoc :aborted true)
                           (.withTxStatus qr \E))
                       in-tx
                       (.withTxStatus qr \T)
                       :else
                       qr))))))))))))  ;; one extra `)` for the per-handler (binding …) plus the new 3-arity arity wrapper

;; ============================================================================
;; Public API
;; ============================================================================

(defn start
  "Start a PgWire server.

   Options:
     :port     — TCP port to listen on (default 5432)
     :data-dir — Directory for per-file index stores used when SQL references
                 file paths (read_csv/read_parquet or FROM 'file.csv').
                 When set, files are materialized as Stratum indices on first
                 access and cached with mtime-based invalidation.
                 Defaults to nil (files are read as plain arrays, no cache).
     :store    — Konserve store for durable server state (SQL CREATE TABLE,
                 ENUM, trained models, live-table bindings). When set, DDL
                 and DML survive restart; without it, all server state is
                 session-scoped and a startup warning is logged.
                 Future phases of the restart-safety project route
                 INSERT/UPDATE/DELETE through `dataset/sync!` against this
                 store; phase R1 (this commit) only installs the schema
                 singleton and hydrates an empty cache. See
                 `stratum.server.state` for the Konserve key layout.

   Returns a server map with :server, :registry, :data-dir, :store, and
   :port keys."
  ([] (start {}))
  ([{:keys [port host data-dir store] :or {port 5432 host "127.0.0.1"}}]
   (when store
     (srv-state/ensure-schema! store))
   (let [registry (atom {})
         _ (if store
             ;; Hydrate the in-memory cache from the durable store.
             ;; Each persisted SQL table is loaded as an index-backed
             ;; column map with the original :durable-binding metadata
             ;; so subsequent DML re-syncs back to the same branch.
             (let [n-tables (hydrate-sql-tables! store registry)
                   n-models (hydrate-models! store registry)
                   n-live   (hydrate-live-tables! store registry)
                   n-enums  (hydrate-enums! store registry)]
               (println (format "Hydrated server state: %d SQL tables, %d models, %d live tables, %d enums"
                                n-tables n-models n-live n-enums)))
             (do
               (println "WARNING: stratum.server/start invoked without :store —")
               (println "         SQL CREATE TABLE / ENUM / MODEL state is session-scoped")
               (println "         and will NOT survive a restart. Pass a Konserve store")
               (println "         via :store for durability.")))
         data-dir-atom (atom data-dir)
         factory  (make-handler-factory registry data-dir-atom store)
         server   (PgWireServer. (int port) ^String host ^PgWireServer$QueryHandlerFactory factory)]
     (.start server)
     {:server   server
      :registry registry
      :data-dir data-dir-atom
      :store    store
      :port     port})))

(defn stop
  "Stop a PgWire server and release the registry so the GC can
   reclaim any konserve stores held under `:__live` entries.

   Pre-fix (round-4 agent P1 #5.2), `stop` only called
   `(.stop server)` on the underlying Java PgWire server; the
   `:registry` atom kept references to every live store + every
   `__file_*__` synthetic entry, pinning konserve handles until
   the next GC cycle (or never, in tests that hold onto the
   server map). Now: clear the registry to drop those refs.

   konserve doesn't expose an explicit `close` for the common
   file/memory backends, so we don't actively close handles;
   dropping the references is sufficient for the JVM to reclaim
   the underlying file descriptors via finalizer / phantom-ref."
  [{:keys [^PgWireServer server registry]}]
  (when server
    (.stop server))
  (when registry
    (reset! registry {})))

(defn register-table!
  "Register a table with the server.

   table-name — String name (used in FROM clause)
   columns    — Map of {col-name data} where data is:
                - long[] / double[] / String[] — raw arrays
                - PersistentColumnIndex — Stratum index
                - {:type T :data arr} — pre-encoded column

   String[] columns are automatically dictionary-encoded for SIMD group-by."
  [{:keys [registry]} table-name columns]
  (let [encoded (encode-table-columns columns)]
    (swap! registry assoc table-name encoded)
    (println (str "Registered table '" table-name "' with "
                  (count encoded) " columns"))))

(defn register-live-table!
  "Register a storage-backed table that resolves fresh on each SQL query.
   PgWire clients always see the current branch HEAD.

   store  — konserve store containing the dataset
   branch — branch name to resolve HEAD from (e.g., \"main\")

   When `server-map` carries a `:store` (the server's durable
   state store) AND the supplied `store` is identical to it
   (same Konserve handle), the binding is persisted under
   [:server :live-tables <table-name>] and reinstalled on next
   restart. A live-table backed by a *different* Konserve store
   is registered for the current session only; the user must
   re-register after restart (a warning is printed). Future v2
   may add a labeled multi-store registry."
  [{:keys [registry store] :as _server-map} table-name live-store branch]
  (swap! registry assoc table-name {:__live true :store live-store :branch branch})
  (cond
    (and store (identical? store live-store))
    (srv-state/put! store :live-tables table-name {:branch branch})

    store
    (println (str "WARNING: live table '" table-name "' uses a Konserve "
                  "store that is not the server's :store — the binding is "
                  "session-scoped and will be lost on restart.")))
  (println (str "Registered live table '" table-name "' on branch '" branch "'")))

(defn index-file!
  "Pre-index a file and register it under table-name.
   Reads the file (CSV or Parquet), materializes all columns as
   PersistentColumnIndex, persists to a per-file konserve store at:
     <data-dir>/<filename>.stratum/
   then registers the indexed dataset in the server table registry.

   On subsequent calls (including server restarts) the cached index is
   reused unless the source file has been modified (mtime check).

   server     — server map from start
   table-name — String name to use in FROM clause
   file-path  — absolute or relative path to .csv or .parquet file
   data-dir   — directory to store per-file indexes (created if needed)"
  [{:keys [registry]} table-name ^String file-path ^String data-dir]
  (.mkdirs (File. data-dir))
  (println (str "Indexing '" file-path "' as table '" table-name "' ..."))
  (let [ds      (files/load-or-index-file! file-path data-dir)
        encoded (encode-table-columns (dataset/columns ds))]
    (swap! registry assoc table-name encoded)
    (println (str "  ✓ " (dataset/row-count ds) " rows, "
                  (count encoded) " columns → "
                  (files/file-store-dir data-dir file-path)))))

(defn unregister-table!
  "Remove a table from the server. When the server has a `:store`
   configured and the table was a durable live-table binding, the
   persisted [:server :live-tables <name>] record is removed too."
  [{:keys [registry store]} table-name]
  (swap! registry dissoc table-name)
  (when store
    (srv-state/delete! store :live-tables table-name)))

(defn register-model!
  "Register a trained isolation forest model with the server.
   The model can then be used in SQL: ANOMALY_SCORE('model_name', col1, col2, ...)

   model-name — String name for the model
   model      — trained model from stratum.iforest/train

   When the server was started with `:store`, the model is also
   persisted under [:server :models <model-name>] and reinstalled on
   subsequent restarts."
  [{:keys [registry store]} model-name model]
  (swap! registry assoc-in ["__models__" model-name] model)
  (when store
    (srv-state/put! store :models model-name model))
  (println (str "Registered model '" model-name "' with "
                (:n-features model) " features")))

(defn list-tables
  "List all registered table names."
  [{:keys [registry]}]
  (keys @registry))

;; ============================================================================
;; Demo Data Generation
;; ============================================================================

(defn- generate-demo-tables
  "Generate synthetic demo tables for --demo mode."
  []
  (let [rng (Random. 42)
        n 100000
        ;; lineitem table (TPC-H inspired)
        shipdate (long-array n)
        discount (double-array n)
        quantity (long-array n)
        price (double-array n)
        tax (double-array n)
        returnflag (make-array String n)
        linestatus (make-array String n)
        rf-vals ["N" "R" "A"]
        ls-vals ["O" "F"]
        base-date 946684800 ;; 2000-01-01 epoch seconds
        _ (dotimes [i n]
            (aset shipdate i (+ base-date (* (.nextInt rng 1461) 86400))) ;; 4 years range
            (aset discount i (* (.nextInt rng 11) 0.01))
            (aset quantity i (long (inc (.nextInt rng 50))))
            (aset price i (+ 1.0 (* (.nextDouble rng) 999.0)))
            (aset tax i (* (.nextInt rng 9) 0.01))
            (aset ^"[Ljava.lang.String;" returnflag i
                  (nth rf-vals (.nextInt rng 3)))
            (aset ^"[Ljava.lang.String;" linestatus i
                  (nth ls-vals (.nextInt rng 2))))
        ;; taxi table (NYC Taxi inspired)
        fare (double-array n)
        tip (double-array n)
        total (double-array n)
        passengers (long-array n)
        payment-type (make-array String n)
        pickup-hour (long-array n)
        pt-vals ["Cash" "Credit" "No Charge" "Dispute"]
        _ (dotimes [i n]
            (let [f (+ 2.5 (* (.nextDouble rng) 97.5))
                  t (if (zero? (.nextInt rng 3)) 0.0 (* f (.nextDouble rng) 0.3))]
              (aset fare i f)
              (aset tip i t)
              (aset total i (+ f t))
              (aset passengers i (long (inc (.nextInt rng 6))))
              (aset ^"[Ljava.lang.String;" payment-type i
                    (nth pt-vals (.nextInt rng 4)))
              (aset pickup-hour i (long (.nextInt rng 24)))))
        ;; Inject 50 anomalies: extremely high fares, zero tips, late-night
        _ (dotimes [j 50]
            (let [i (- n 50 (- j))]
              (aset fare i (+ 500.0 (* (.nextDouble rng) 500.0)))
              (aset tip i 0.0)
              (aset total i (aget fare i))
              (aset passengers i 1)
              (aset ^"[Ljava.lang.String;" payment-type i "Cash")
              (aset pickup-hour i (long (+ 2 (.nextInt rng 3))))))]
    {"lineitem" {:shipdate shipdate :discount discount :quantity quantity
                 :price price :tax tax :returnflag returnflag :linestatus linestatus}
     "taxi" {:fare_amount fare :tip_amount tip :total_amount total
             :passenger_count passengers :payment_type payment-type
             :pickup_hour pickup-hour}}))

;; ============================================================================
;; CLI Entry Point
;; ============================================================================

(defn- default-data-dir []
  (let [xdg (System/getenv "XDG_DATA_HOME")
        home (System/getProperty "user.home")]
    (if xdg
      (str xdg "/stratum")
      (str home "/.local/share/stratum"))))

(defn- parse-args
  "Parse CLI arguments into a map.

   --port N          TCP port (default 5432)
   --host ADDR       Bind address (default 127.0.0.1, use 0.0.0.0 for all interfaces)
   --data-dir DIR    Directory for file index stores (default: ~/.local/share/stratum)
   --index NAME:FILE Pre-index FILE and register as table NAME
   --demo            Load synthetic demo tables"
  [args]
  (loop [args (seq args)
         result {:port     5432
                 :host     "127.0.0.1"
                 :demo     false
                 :data-dir (or (System/getenv "STRATUM_DATA_DIR") (default-data-dir))
                 :indexes  []}]
    (if-not args
      result
      (let [arg (first args)]
        (cond
          (= arg "--demo")
          (recur (next args) (assoc result :demo true))

          (= arg "--port")
          (recur (nnext args) (assoc result :port (Integer/parseInt (second args))))

          (= arg "--host")
          (recur (nnext args) (assoc result :host (second args)))

          (= arg "--data-dir")
          (recur (nnext args) (assoc result :data-dir (second args)))

          ;; --index name:path  or  --index path  (derives name from filename stem)
          (= arg "--index")
          (let [spec   (second args)
                [n p]  (if (str/includes? spec ":") (str/split spec #":" 2) [nil spec])
                name   (or n (let [f (.getName (File. ^String p))
                                   i (.lastIndexOf f ".")]
                               (if (> i 0) (.substring f 0 i) f)))]
            (recur (nnext args) (update result :indexes conj {:name name :path p})))

          ;; Legacy: bare number as port
          (re-matches #"\d+" arg)
          (recur (next args) (assoc result :port (Integer/parseInt arg)))

          :else
          (recur (next args) result))))))

(defn -main
  "Start the PgWire server from command line.

   Usage:
     java -jar stratum.jar [options]
     clj -M:server [options]

   Options:
     --port N          TCP port (default 5432)
     --host ADDR       Bind address (default 127.0.0.1, use 0.0.0.0 for all interfaces)
     --data-dir DIR    Directory for per-file index stores
                       (default: $STRATUM_DATA_DIR or ~/.local/share/stratum)
     --index NAME:FILE Pre-index FILE and register as table NAME.
                       NAME can be omitted — derived from filename stem.
                       Can be repeated for multiple files.
     --demo            Load synthetic demo tables (lineitem, taxi)"
  [& args]
  (let [{:keys [port host demo data-dir indexes]} (parse-args args)
        srv (start {:port port :host host :data-dir data-dir})]

    ;; Pre-index any --index FILE specs
    (when (seq indexes)
      (println (str "Indexing files (store: " data-dir ") ..."))
      (.mkdirs (File. ^String data-dir))
      (doseq [{:keys [name path]} indexes]
        (index-file! srv name path data-dir)))

    (when demo
      (println "Loading demo tables...")
      (let [tables (generate-demo-tables)]
        (doseq [[table-name columns] tables]
          (register-table! srv table-name columns))
        ;; Train anomaly model on taxi numeric features
        (let [taxi (get tables "taxi")
              model (iforest/train {:from (select-keys taxi [:fare_amount :tip_amount :total_amount
                                                             :passenger_count :pickup_hour])
                                    :n-trees 100 :sample-size 256 :seed 42
                                    :contamination 0.01})]
          (register-model! srv "taxi_anomaly" model))))

    ;; JIT warmup — exercise all Java hot paths so the C2 compiler sees every
    ;; code shape before real queries arrive, preventing deoptimization cliffs.
    (let [t0 (System/nanoTime)]
      (q/jit-warmup!)
      (println (format "JIT warmup complete (%.1fs)" (/ (- (System/nanoTime) t0) 1e9))))

    (println)
    (println (str "Stratum SQL server ready on " host ":" port))
    (println (str "Connect with: psql -h " (if (= host "0.0.0.0") "localhost" host) " -p " port " -U stratum"))
    (println (str "File index store: " data-dir))

    (when (seq indexes)
      (println)
      (println "Pre-indexed tables:")
      (doseq [{:keys [name path]} indexes]
        (println (str "  " name " → " path))))

    (when demo
      (println)
      (println "Demo tables loaded: lineitem (100K rows), taxi (100K rows)")
      (println "Anomaly model 'taxi_anomaly' trained on taxi (fare_amount, tip_amount, total_amount, passenger_count, pickup_hour)")
      (println)
      (println "Try:")
      (println "  SELECT payment_type, AVG(tip_amount), COUNT(*) FROM taxi GROUP BY payment_type;")
      (println "  SELECT returnflag, linestatus, SUM(price * discount) FROM lineitem GROUP BY returnflag, linestatus;")
      (println "  EXPLAIN SELECT SUM(price) FROM lineitem WHERE quantity < 24;")
      (println)
      (println "Anomaly detection:")
      (println "  SELECT fare_amount, tip_amount, pickup_hour, ANOMALY_SCORE('taxi_anomaly', fare_amount, tip_amount, total_amount, passenger_count, pickup_hour) AS score FROM taxi WHERE ANOMALY_SCORE('taxi_anomaly', fare_amount, tip_amount, total_amount, passenger_count, pickup_hour) > 0.7 LIMIT 20;"))

    (println)
    (println "Ad-hoc file queries (auto-indexed on first access):")
    (println "  SELECT * FROM read_csv('/path/to/data.csv') LIMIT 10;")
    (println "  SELECT region, SUM(amount) FROM 'sales.csv' GROUP BY region;")
    (println)
    (println "Press Ctrl+C to stop.")
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (stop srv))))
    @(promise)))
