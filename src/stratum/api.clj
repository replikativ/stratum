(ns stratum.api
  "Top-level API for Stratum — SIMD-accelerated columnar analytics.

   This is the single namespace most users need to import.

   Usage:
     (require '[stratum.api :as st])

     ;; Query with Stratum DSL
     (st/q {:from {:price prices :qty quantities}
            :where [[:< :qty 24]]
            :agg [[:sum [:* :price :qty]]]})

     ;; Query with SQL
     (st/q \"SELECT SUM(price * qty) FROM orders WHERE qty < 24\"
           {\"orders\" {:price prices :qty quantities}})

     ;; Import data
     (st/from-csv \"data/orders.csv\")
     (st/from-parquet \"data/orders.parquet\")

     ;; Start SQL server
     (def srv (st/start-server {:port 5432}))
     (st/register-table! srv \"orders\" {:price prices :qty quantities})
     (st/stop-server srv)"
  (:refer-clojure :exclude [name sync load resolve])
  (:require [stratum.query :as query]
            [stratum.query.normalization :as norm]
            [stratum.sql :as sql]
            [stratum.server :as server]
            [stratum.index :as index]
            [stratum.dataset :as dataset]
            [stratum.storage :as storage]
            [stratum.csv :as csv]
            [stratum.parquet :as parquet]
            [stratum.iforest :as iforest]))

(defn- encode-table
  "Encode all columns in a table map via query/encode-column.
   Handles StratumDataset values by extracting their columns."
  [table]
  (cond
    ;; StratumDataset → extract columns (already normalized)
    (satisfies? dataset/IDataset table)
    (dataset/columns table)

    ;; Already encoded column map
    (every? #(and (map? (val %)) (:type (val %))) table)
    table

    ;; Encode each column
    :else
    (into {}
          (map (fn [[k v]]
                 [(if (keyword? k) k (keyword (clojure.core/name k)))
                  (query/encode-column v)]))
          table)))

(defn- resolve-temporal-from
  "Resolve :from when it's a string/keyword table reference + temporal opts."
  [from opts]
  (if (and (:store opts) (or (string? from) (keyword? from)))
    (dataset/resolve (:store opts)
                     (if (keyword? from) (clojure.core/name from) from)
                     opts)
    from))

(defn q
  "Query Stratum's analytical engine. Accepts either:

   - Query map:               (q {:from {:x arr} :agg [[:sum :x]]})
   - Query map + temporal opts: (q {:from \"trades\" :agg [[:sum :price]]}
                                   {:store s :branch \"main\"})
   - SQL string + table map:  (q \"SELECT SUM(x) FROM t\" {\"t\" {:x arr}})
   - SQL string + tables + opts: (q \"SELECT ...\" {\"t\" data} {:store s})

   Temporal opts (map with :store key):
     :store     - konserve store (required for temporal resolution)
     :as-of     - UUID of specific commit to query
     :branch    - branch name to query HEAD of
     :as-of-tx  - Datahike tx id for floor lookup

   See stratum.query/q for comprehensive query map documentation.

   Returns results as vector of maps (default) or columnar
   (with :result :columns in query map)."
  ([query-or-sql]
   (if (string? query-or-sql)
     (throw (ex-info "SQL queries require a table map as second argument"
                     {:sql query-or-sql}))
     (query/q query-or-sql)))
  ([query-or-sql tables-or-opts]
   (if (string? query-or-sql)
     ;; SQL path: second arg is table registry
     (let [registry (into {}
                          (map (fn [[k v]]
                                 [(if (keyword? k) (clojure.core/name k) (str k))
                                  (encode-table v)]))
                          tables-or-opts)
           parsed (sql/parse-sql query-or-sql registry)]
       (cond
         (:error parsed)
         (throw (ex-info (:error parsed) {:sql query-or-sql}))

         (:system parsed)
         (:result parsed)

         (:query parsed)
         (let [result (query/q (:query parsed))]
           (if-let [post (:_post-aggs (:query parsed))]
             (sql/apply-post-aggs result post)
             result))

         :else
         (throw (ex-info "Unexpected parse result" {:sql query-or-sql :parsed parsed}))))
     ;; DSL path: second arg is temporal opts
     (let [resolved-from (resolve-temporal-from (:from query-or-sql) tables-or-opts)]
       (query/q (assoc query-or-sql :from resolved-from)))))
  ([sql tables opts]
   ;; SQL path with separate tables and temporal opts
   ;; Resolve storage-backed and StratumDataset table entries before building registry
   (let [resolved-tables
         (into {}
               (map (fn [[k v]]
                      (let [table-key (if (keyword? k) (clojure.core/name k) (str k))]
                        (cond
                          ;; StratumDataset value → extract columns
                          (satisfies? dataset/IDataset v)
                          [table-key (encode-table v)]

                          ;; String/keyword ref → resolve from storage
                          (and (:store opts) (or (string? v) (keyword? v)))
                          [table-key (encode-table
                                      (dataset/columns
                                       (dataset/resolve (:store opts)
                                                        (if (keyword? v) (clojure.core/name v) v)
                                                        opts)))]

                          ;; Regular column map
                          :else
                          [table-key (encode-table v)]))))
               tables)
         parsed (sql/parse-sql sql resolved-tables)]
     (cond
       (:error parsed)
       (throw (ex-info (:error parsed) {:sql sql}))

       (:system parsed)
       (:result parsed)

       (:query parsed)
       (let [result (query/q (:query parsed))]
         (if-let [post (:_post-aggs (:query parsed))]
           (sql/apply-post-aggs result post)
           result))

       :else
       (throw (ex-info "Unexpected parse result" {:sql sql :parsed parsed}))))))

(defn explain
  "Show execution plan without running the query.

   Accepts the same arguments as `q`. Returns a map describing
   the execution strategy that would be used."
  ([query-or-sql]
   (if (string? query-or-sql)
     (throw (ex-info "SQL queries require a table map as second argument"
                     {:sql query-or-sql}))
     (query/explain query-or-sql)))
  ([sql tables]
   (let [registry (into {}
                        (map (fn [[k v]]
                               [(if (keyword? k) (clojure.core/name k) (str k))
                                (encode-table v)]))
                        tables)
         parsed (sql/parse-sql sql registry)]
     (cond
       (:error parsed)
       {:error (:error parsed) :sql sql}

       (:system parsed)
       {:strategy :system :tag (:tag parsed)}

       (:query parsed)
       (query/explain (:query parsed))

       :else
       {:error "Unexpected parse result"}))))

;; ============================================================================
;; Re-exports for convenience
;; ============================================================================

(def from-csv
  "Read a CSV file into a Stratum column map.
   See stratum.csv/from-csv for options."
  csv/from-csv)

(def from-parquet
  "Read a Parquet file into a Stratum column map.
   See stratum.parquet/from-parquet for options."
  parquet/from-parquet)

(def from-maps
  "Convert a sequence of maps to a Stratum column map.
   See stratum.csv/from-maps for details."
  csv/from-maps)

(def start-server
  "Start a PostgreSQL-compatible SQL server.
   See stratum.server/start for options."
  server/start)

(def stop-server
  "Stop a running SQL server."
  server/stop)

(def register-table!
  "Register a table with the SQL server.
   See stratum.server/register-table! for details."
  server/register-table!)

(def index-from-seq
  "Create a PersistentColumnIndex from a sequence.
   See stratum.index/index-from-seq for details."
  index/index-from-seq)

(def encode-column
  "Pre-encode a column (String[] → dict-encoded, long[]/double[] → passthrough).
   See stratum.query/encode-column for details."
  query/encode-column)

;; ============================================================================
;; Query Normalization (for Datahike / programmatic integration)
;; ============================================================================

(def normalize-pred
  "Normalize a predicate to internal form.
   Accepts both keyword [:< :col 5] and symbol '(< :col 5) operators.
   See stratum.query.normalization/normalize-pred for full documentation."
  norm/normalize-pred)

(def normalize-agg
  "Normalize an aggregation spec to internal form.
   Accepts both keyword [:sum :col] and symbol '(sum :col) operators.
   See stratum.query.normalization/normalize-agg for full documentation."
  norm/normalize-agg)

(def normalize-expr
  "Normalize an arithmetic expression to internal form.
   Converts [:* :a :b] to {:op :mul :args [:a :b]}.
   See stratum.query.normalization/normalize-expr for full documentation."
  norm/normalize-expr)

(def results->columns
  "Convert a vector of result maps to a column map {:col array ... :n-rows N}.
   See stratum.query/results->columns."
  query/results->columns)

(def tuples->columns
  "Convert positional tuples + column-names vector into a column map.
   Type-inferred: integers→long[], floats→double[], else→String[].
   See stratum.query/tuples->columns."
  query/tuples->columns)

(def columns->tuples
  "Convert a column map into a vector of positional tuples.
   See stratum.query/columns->tuples."
  query/columns->tuples)

;; ============================================================================
;; Anomaly Detection (Isolation Forest)
;; ============================================================================

(def train-iforest
  "Train an isolation forest for anomaly detection.
   See stratum.iforest/train for options."
  iforest/train)

(def iforest-score
  "Score rows using a trained isolation forest.
   Returns double[] of anomaly scores [0,1], higher = more anomalous.
   See stratum.iforest/score."
  iforest/score)

(def iforest-predict
  "Binary anomaly prediction (1=anomaly, 0=normal).
   Requires :contamination in training or explicit {:threshold t}.
   See stratum.iforest/predict."
  iforest/predict)

(def iforest-predict-proba
  "Anomaly probability [0,1] normalized from training distribution.
   See stratum.iforest/predict-proba."
  iforest/predict-proba)

(def iforest-predict-confidence
  "Prediction confidence [0,1] based on tree agreement.
   See stratum.iforest/predict-confidence."
  iforest/predict-confidence)

(def iforest-rotate
  "Rotate oldest trees with new ones for online adaptation.
   See stratum.iforest/rotate-forest."
  iforest/rotate-forest)

(def register-model!
  "Register an isolation forest model with the SQL server.
   See stratum.server/register-model!."
  server/register-model!)

;; ============================================================================
;; Dataset Persistence
;; ============================================================================

(def make-dataset
  "Create a StratumDataset from a column map.
   See stratum.dataset/make-dataset for options."
  dataset/make-dataset)

(def name
  "Get dataset name (string)."
  dataset/ds-name)

(def row-count
  "Get total number of rows."
  dataset/row-count)

(def columns
  "Get normalized columns map for query execution."
  dataset/columns)

(def column-names
  "Get sequence of column keywords."
  dataset/column-names)

(def schema
  "Get schema map: {col-kw {:type :int64|:float64 :nullable? boolean}}."
  dataset/schema)

(def sync!
  "Atomically persist dataset + all indices to storage.
   Returns new dataset with commit metadata.
   See stratum.dataset/sync!."
  dataset/sync!)

(def load
  "Load dataset from storage by branch name or commit UUID.
   Returns StratumDataset with all index columns restored.
   See stratum.dataset/load."
  dataset/load)

(def fork
  "O(1) fork — all index columns forked via idx-fork.
   See stratum.dataset/fork."
  dataset/fork)

(def gc!
  "Mark-and-sweep GC from dataset branch heads.
   See stratum.storage/gc!."
  storage/gc!)

(def resolve
  "Resolve a temporal reference to a StratumDataset.
   See stratum.dataset/resolve."
  dataset/resolve)

(def with-metadata
  "Return a new dataset with updated metadata map.
   See stratum.dataset/with-metadata."
  dataset/with-metadata)

(def ensure-indexed
  "Convert all array-backed columns to index-backed PersistentColumnIndex.
   See stratum.dataset/ensure-indexed."
  dataset/ensure-indexed)

(def with-parent
  "Return a new dataset with parent commit-info from another dataset.
   See stratum.dataset/with-parent."
  dataset/with-parent)

(def register-live-table!
  "Register a storage-backed table that resolves fresh on each SQL query.
   See stratum.server/register-live-table!."
  server/register-live-table!)

(def index-file!
  "Pre-index a CSV or Parquet file and register it as a named table.
   Materializes all columns as PersistentColumnIndex with zone maps,
   persisted to <data-dir>/<filename>.stratum/. Mtime-based cache.
   See stratum.server/index-file!."
  server/index-file!)
