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
            [stratum.files :as files]
            [stratum.iforest :as iforest]
            [clojure.string :as str])
  (:import [stratum.internal PgWireServer PgWireServer$QueryHandler PgWireServer$QueryResult]
           [java.util Random]
           [java.io File])
  (:gen-class))

(set! *warn-on-reflection* true)

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
  "Pre-encode table columns: auto-detect types and dict-encode String[] columns."
  [columns]
  (into {}
        (map (fn [[col-name col-data]]
               (let [k (if (keyword? col-name) col-name (keyword col-name))
                     encoded (q/encode-column col-data)]
                 [k encoded])))
        columns))

;; ============================================================================
;; Query Handler
;; ============================================================================

(defn- format-explain-result
  "Format an EXPLAIN result as a text table for pgwire."
  [plan]
  (let [lines [(str "Strategy: " (name (:strategy plan)))
               (str "Data source: " (name (or (:data-source plan) :unknown)))
               (str "Rows: " (:n-rows plan))
               (str "Columns: " (:columns plan))
               (str "Predicates: " (:count (:predicates plan)) " total")
               (when (seq (:aggregates plan))
                 (str "Aggregates: " (pr-str (:aggregates plan))))
               (when (seq (:group-by plan))
                 (str "Group by: " (pr-str (:group-by plan))))
               (when (:join plan)
                 (str "Joins: " (:count (:join plan))))]
        lines (remove nil? lines)
        rows (mapv (fn [l] (into-array String [l])) lines)]
    (PgWireServer$QueryResult.
     (into-array String ["QUERY PLAN"])
     (int-array [25])
     (into-array (Class/forName "[Ljava.lang.String;") rows)
     (str "EXPLAIN"))))

(def ^:private anomaly-ops
  "Set of anomaly detection operations resolved from SQL."
  #{:anomaly-score :anomaly-predict :anomaly-proba :anomaly-confidence})

(def ^:private anomaly-op->fn
  "Map anomaly operation to iforest function."
  {:anomaly-score      iforest/score
   :anomaly-predict    iforest/predict
   :anomaly-proba      iforest/predict-proba
   :anomaly-confidence iforest/predict-confidence})

(defn- resolve-anomaly-expressions
  "Resolve anomaly detection expressions in :select.
   Computes scores/predictions using the registered model and injects as columns in :from."
  [query registry]
  (if-let [select-items (:select query)]
    (let [anomaly-selects (filter (fn [item]
                                    (and (sequential? item)
                                         (or (contains? anomaly-ops (first item))
                                             (and (= :as (first item))
                                                  (sequential? (second item))
                                                  (contains? anomaly-ops (first (second item)))))))
                                  select-items)]
      (if (empty? anomaly-selects)
        query
        (reduce
         (fn [q sel]
           (let [expr (if (= :as (first sel)) (second sel) sel)
                 op (first expr)
                 model-name (second expr)
                 model-name (if (string? model-name) model-name (name model-name))
                 model (get-in registry ["__models__" model-name])]
             (if model
               (let [feature-names (:feature-names model)
                     from (:from q)
                     data (select-keys from feature-names)
                     compute-fn (get anomaly-op->fn op)
                     result (compute-fn model data)
                     col-name (keyword (str "__" (name op) "_" model-name))]
                 (-> q
                     (assoc-in [:from col-name] result)
                     (update :select (fn [sels]
                                       (mapv (fn [s]
                                               (if (= s sel)
                                                 (if (= :as (first sel))
                                                   [:as col-name (nth sel 2)]
                                                   col-name)
                                                 s))
                                             sels)))))
               (throw (ex-info (str "Unknown model: " model-name)
                               {:model model-name
                                :available (keys (get registry "__models__"))})))))
         query anomaly-selects)))
    query))

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
      ;; read_csv/read_parquet function syntax
      (let [m (re-find #"(?i)\b(read_csv|read_parquet)\s*\(\s*'([^']+)'\s*\)" @result)]
        (when m
          (let [[full-match _func path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace @result full-match synthetic)))))
      ;; FROM 'path.csv' (single-quoted)
      (let [m (re-find #"(?i)\bFROM\s+'([^']+\.(?:csv|parquet))'" @result)]
        (when m
          (let [[full-match path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace @result full-match
                                         (str "FROM " synthetic))))))
      ;; FROM "path.csv" (double-quoted — JSqlParser strips quotes, becomes table name)
      (let [m (re-find #"(?i)\bFROM\s+\"([^\"]+\.(?:csv|parquet))\"" @result)]
        (when m
          (let [[full-match path] m
                _         (validate-file-path path data-dir)
                synthetic (str "__file_" (vswap! counter inc) "__")
                ds        (files/load-or-index-file! path data-dir)]
            (vswap! extra-reg assoc synthetic (encode-table-columns (dataset/columns ds)))
            (vreset! result (str/replace @result full-match
                                         (str "FROM " synthetic))))))
      [@result @extra-reg])))

(defn- make-query-handler
  "Create a PgWireServer.QueryHandler that routes SQL to Stratum."
  [table-registry-atom data-dir-atom]
  (reify PgWireServer$QueryHandler
    (execute [_ sql]
      (try
        (let [raw-registry @table-registry-atom
              registry     (resolve-live-tables raw-registry)
              [sql extra]  (resolve-file-refs sql @data-dir-atom)
              registry     (merge registry extra)
              parsed       (sql/parse-sql sql registry)]
          (cond
            ;; EXPLAIN query — show execution plan
            (:explain parsed)
            (let [explain-data (:explain parsed)]
              (if (map? explain-data)
                (if (:strategy explain-data)
                  ;; Already a plan (system query)
                  (format-explain-result explain-data)
                  ;; It's a query map — run explain on it
                  (format-explain-result (q/explain {:from (:from explain-data)
                                                     :where (:where explain-data)
                                                     :agg (:agg explain-data)
                                                     :group (:group explain-data)
                                                     :select (:select explain-data)})))
                (PgWireServer$QueryResult. "EXPLAIN: unexpected result")))

            ;; System query (SET, SHOW, VERSION, etc.)
            (:system parsed)
            (sql/format-results parsed)

            ;; DDL (CREATE TABLE, INSERT INTO, UPDATE, DELETE)
            (:ddl parsed)
            (let [{:keys [op table columns rows assignments where
                          conflict-cols action from table-alias] :as ddl} (:ddl parsed)]
              (case op
                :create-table
                (do
                  (let [cols (into {}
                                   (map (fn [{:keys [name type]}]
                                          [(keyword name)
                                           (case type
                                             :int64   (long-array 0)
                                             :float64 (double-array 0)
                                             :string  (make-array String 0))]))
                                   columns)]
                    (swap! table-registry-atom assoc table cols))
                  (PgWireServer$QueryResult/empty "CREATE TABLE"))

                :drop-table
                (do (swap! table-registry-atom dissoc table)
                    (PgWireServer$QueryResult/empty "DROP TABLE"))

                :insert
                (let [existing (get @table-registry-atom table)]
                  (when-not existing
                    (throw (ex-info (str "Table not found: " table) {:table table})))
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
                               col-keys))]
                    (swap! table-registry-atom assoc table new-cols))
                  (PgWireServer$QueryResult/empty
                   (str "INSERT 0 " (count rows))))

                :upsert
                (let [existing (get @table-registry-atom table)]
                  (when-not existing
                    (throw (ex-info (str "Table not found: " table) {:table table})))
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
                                                               (case (first e)
                                                                 :+ (+ (double (eval-val (nth e 1)))
                                                                       (double (eval-val (nth e 2))))
                                                                 :- (- (double (eval-val (nth e 1)))
                                                                       (double (eval-val (nth e 2))))
                                                                 :* (* (double (eval-val (nth e 1)))
                                                                       (double (eval-val (nth e 2))))
                                                                 :/ (/ (double (eval-val (nth e 1)))
                                                                       (double (eval-val (nth e 2))))
                                                                 nil)))
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
                    (swap! table-registry-atom assoc table (:cols result))
                    (PgWireServer$QueryResult/empty
                     (str "INSERT 0 " (:n-inserted result)))))

                :update
                (let [existing (get @table-registry-atom table)]
                  (when-not existing
                    (throw (ex-info (str "Table not found: " table) {:table table})))
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
                                     (case (first e)
                                       :+ (+ (double (eval-row (nth e 1) target-table from-table target-i from-i))
                                             (double (eval-row (nth e 2) target-table from-table target-i from-i)))
                                       :- (- (double (eval-row (nth e 1) target-table from-table target-i from-i))
                                             (double (eval-row (nth e 2) target-table from-table target-i from-i)))
                                       :* (* (double (eval-row (nth e 1) target-table from-table target-i from-i))
                                             (double (eval-row (nth e 2) target-table from-table target-i from-i)))
                                       :/ (/ (double (eval-row (nth e 1) target-table from-table target-i from-i))
                                             (double (eval-row (nth e 2) target-table from-table target-i from-i)))
                                       nil)))
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
                    (swap! table-registry-atom assoc table new-cols)
                    (PgWireServer$QueryResult/empty (str "UPDATE " n-matched))))

                :delete
                (let [existing (get @table-registry-atom table)]
                  (when-not existing
                    (throw (ex-info (str "Table not found: " table) {:table table})))
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
                            (dotimes [i n-rows]
                              (aset delete-mask i
                                    (boolean
                                     (every? (fn [pred]
                                               (let [eval-val (fn eval-val [e]
                                                                (cond
                                                                  (keyword? e)
                                                                  (let [arr (get existing e)]
                                                                    (cond
                                                                      (instance? (Class/forName "[J") arr)
                                                                      (let [v (aget ^longs arr i)]
                                                                        (when-not (= v Long/MIN_VALUE) v))
                                                                      (instance? (Class/forName "[D") arr)
                                                                      (let [v (aget ^doubles arr i)]
                                                                        (when-not (Double/isNaN v) v))
                                                                      (instance? (Class/forName "[Ljava.lang.String;") arr)
                                                                      (aget ^"[Ljava.lang.String;" arr i)))
                                                                  (number? e) e
                                                                  (string? e) e
                                                                  (nil? e) nil
                                                                  (vector? e)
                                                                  (case (first e)
                                                                    :+ (+ (double (eval-val (nth e 1))) (double (eval-val (nth e 2))))
                                                                    :- (- (double (eval-val (nth e 1))) (double (eval-val (nth e 2))))
                                                                    :* (* (double (eval-val (nth e 1))) (double (eval-val (nth e 2))))
                                                                    :/ (/ (double (eval-val (nth e 1))) (double (eval-val (nth e 2))))
                                                                    nil)))]
                                                 (case (first pred)
                                                   := (let [l (eval-val (nth pred 1))
                                                            r (eval-val (nth pred 2))]
                                                        (and (some? l) (some? r) (= l r)))
                                                   (:!= :<>) (let [l (eval-val (nth pred 1))
                                                                   r (eval-val (nth pred 2))]
                                                               (and (some? l) (some? r) (not= l r)))
                                                   :> (let [l (eval-val (nth pred 1))
                                                            r (eval-val (nth pred 2))]
                                                        (and (some? l) (some? r)
                                                             (> (double l) (double r))))
                                                   :< (let [l (eval-val (nth pred 1))
                                                            r (eval-val (nth pred 2))]
                                                        (and (some? l) (some? r)
                                                             (< (double l) (double r))))
                                                   :>= (let [l (eval-val (nth pred 1))
                                                             r (eval-val (nth pred 2))]
                                                         (and (some? l) (some? r)
                                                              (>= (double l) (double r))))
                                                   :<= (let [l (eval-val (nth pred 1))
                                                             r (eval-val (nth pred 2))]
                                                         (and (some? l) (some? r)
                                                              (<= (double l) (double r))))
                                                   :is-null (nil? (eval-val (nth pred 1)))
                                                   :is-not-null (some? (eval-val (nth pred 1)))
                                                   false)))
                                             where)))))
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
                    (swap! table-registry-atom assoc table new-cols)
                    (PgWireServer$QueryResult/empty (str "DELETE " n-deleted))))))

            ;; Parse/translation error
            (:error parsed)
            (PgWireServer$QueryResult. ^String (:error parsed))

            ;; Normal query — execute via Stratum engine
            (:query parsed)
            (let [query (:query parsed)
                  ;; Resolve ANOMALY_SCORE expressions: inject model scores as computed columns
                  query (resolve-anomaly-expressions query registry)
                  result (q/q query)
                  result (if-let [post-aggs (:_post-aggs query)]
                           (sql/apply-post-aggs result post-aggs)
                           result)]
              (sql/format-results result))

            :else
            (PgWireServer$QueryResult. "Internal error: unexpected parse result")))
        (catch Exception e
          (PgWireServer$QueryResult. (str (.getMessage e))))))))

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

   Returns a server map with :server, :registry, :data-dir, and :port keys."
  ([] (start {}))
  ([{:keys [port host data-dir] :or {port 5432 host "127.0.0.1"}}]
   (let [registry (atom {})
         data-dir-atom (atom data-dir)
         handler  (make-query-handler registry data-dir-atom)
         server   (PgWireServer. (int port) ^String host handler)]
     (.start server)
     {:server   server
      :registry registry
      :data-dir data-dir-atom
      :port     port})))

(defn stop
  "Stop a PgWire server."
  [{:keys [^PgWireServer server]}]
  (when server
    (.stop server)))

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
   branch — branch name to resolve HEAD from (e.g., \"main\")"
  [{:keys [registry]} table-name store branch]
  (swap! registry assoc table-name {:__live true :store store :branch branch})
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
  "Remove a table from the server."
  [{:keys [registry]} table-name]
  (swap! registry dissoc table-name))

(defn register-model!
  "Register a trained isolation forest model with the server.
   The model can then be used in SQL: ANOMALY_SCORE('model_name', col1, col2, ...)

   model-name — String name for the model
   model      — trained model from stratum.iforest/train"
  [{:keys [registry]} model-name model]
  (swap! registry assoc-in ["__models__" model-name] model)
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
              (aset pickup-hour i (long (.nextInt rng 24)))))]
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
          (register-table! srv table-name columns))))

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
      (println)
      (println "Try:")
      (println "  SELECT payment_type, AVG(tip_amount), COUNT(*) FROM taxi GROUP BY payment_type;")
      (println "  SELECT returnflag, linestatus, SUM(price * discount) FROM lineitem GROUP BY returnflag, linestatus;")
      (println "  EXPLAIN SELECT SUM(price) FROM lineitem WHERE quantity < 24;"))

    (println)
    (println "Ad-hoc file queries (auto-indexed on first access):")
    (println "  SELECT * FROM read_csv('/path/to/data.csv') LIMIT 10;")
    (println "  SELECT region, SUM(amount) FROM 'sales.csv' GROUP BY region;")
    (println)
    (println "Press Ctrl+C to stop.")
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (stop srv))))
    @(promise)))
