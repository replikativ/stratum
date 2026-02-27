(ns stratum.specification
  "Shared specification for Stratum API.

  This namespace holds all semantic information so that individual bindings
  (Clojure API, SQL interface, future Java/Python) can be derived from it.

  Following the Datahike/Proximum pattern — the spec is purely declarative
  about semantics, not about how each binding should look.

  Each operation has:
    :args                     - malli function schema [:=> [:cat ...] ret] or [:function [...]]
    :ret                      - malli schema for return value
    :doc                      - documentation string
    :impl                     - symbol pointing to implementation function
    :categories               - semantic grouping tags (vector of keywords)
    :stability                - API maturity (:alpha, :beta, :stable)
    :referentially-transparent? - true if pure (no side effects, deterministic)"
  (:require [malli.core :as m]
            [malli.error :as me]
            [stratum.dataset :as dataset]))

;; =============================================================================
;; Core Type Schemas
;; =============================================================================

(def SDoubleArray
  "Java double[] array."
  [:fn {:error/message "must be a double[]"}
   #(instance? (Class/forName "[D") %)])

(def SLongArray
  "Java long[] array."
  [:fn {:error/message "must be a long[]"}
   #(instance? (Class/forName "[J") %)])

(def SStringArray
  "Java String[] array."
  [:fn {:error/message "must be a String[]"}
   #(instance? (Class/forName "[Ljava.lang.String;") %)])

(def SEncodedColumn
  "Pre-encoded column {:type T :data arr :dict String[] (optional)}.
   Also accepts index-mode: {:type T :index PersistentColumnIndex :dict ...}."
  [:map {:closed false}
   [:type [:enum :int64 :float64]]
   [:data {:optional true} :any]])

(def SIndex
  "PersistentColumnIndex."
  [:fn {:error/message "must be a PersistentColumnIndex"}
   #(= "stratum.index.PersistentColumnIndex" (.getName (class %)))])

(def SColumnData
  "Column data: double[], long[], String[], PersistentColumnIndex,
   pre-encoded {:type T :data arr}, or Clojure sequential (auto-converted)."
  [:or {:error/message "must be double[], long[], String[], PersistentColumnIndex, {:type :data} map, or sequential collection"}
   SDoubleArray SLongArray SStringArray SIndex SEncodedColumn
   sequential?])

(def SColumnMap
  "Map of keyword column names to column data."
  [:map-of {:error/message "must be a map of keyword → column data (double[], long[], String[])"}
   :keyword SColumnData])

;; =============================================================================
;; Expression & Reference Types
;; =============================================================================

(def agg-op-set
  "All recognized aggregate operators (keywords only).
   Symbol forms (e.g. 'sum) are normalized to keywords by the query engine before dispatch."
  #{:sum :count :count-non-null :avg :min :max :stddev :stddev-pop :variance :variance-pop :corr
    :sum-product :count-distinct :median :percentile :approx-quantile})

(def pred-op-set
  "All recognized predicate operators (keywords only).
   Symbol forms (e.g. '< '=) are normalized to keywords by the query engine before dispatch."
  #{:< :> :<= :>= := :!= :not= :between :range :not-range
    :like :not-like :ilike :not-ilike :contains :starts-with :ends-with
    :in :not-in :is-null :is-not-null :or :and :not})

(def expr-op-set
  "All recognized expression operators (keywords only).
   Symbol forms are normalized to keywords by the query engine before dispatch."
  #{:+ :- :* :/ :mod :% :abs :sqrt :log :ln :log10 :exp :pow
    :round :floor :ceil :sign :signum
    :year :month :day :hour :minute :second :day-of-week :week-of-year
    :date-trunc :date-add :date-diff :epoch-days :epoch-seconds
    :coalesce :nullif :greatest :least :lower :upper :substr :length :trim :replace :concat
    :cast :case})

(def SExpr
  "Column reference, literal, or expression vector.
   - :keyword  — column reference
   - number    — literal value
   - string    — literal string
   - vector/list starting with operator — expression"
  [:or :keyword number? :string
   [:and vector?
    [:fn {:error/message "expression vector must start with a keyword or symbol"}
     #(let [f (first %)] (or (keyword? f) (symbol? f)))]]
   list?])

;; =============================================================================
;; Aggregate Specifications
;; =============================================================================

(def SAggSpec
  "Single aggregation spec.
   Recognized shapes:
     [:count]                          — count all rows
     [:sum :col]                       — single column op
     [:corr :col1 :col2]              — two column op
     [:percentile :col 0.95]          — parameterized op
     [:as [:sum :col] :alias]         — aliased aggregate
     '(sum :col)                      — Datahike-style list form"
  [:and
   [:or {:error/message "aggregate must be a vector or list, e.g. [:sum :col] or '(sum :col)"}
    vector? list?]
   [:fn {:error/message
         (str "aggregate must start with a known operator: "
              ":sum :count :count-non-null :avg :min :max :stddev :stddev-pop :variance :variance-pop :corr "
              ":count-distinct :median :percentile :approx-quantile :as")}
    #(let [op (first %)
           kw (if (symbol? op) (keyword (name op)) op)]
       (contains? (conj agg-op-set :as) kw))]])

;; =============================================================================
;; Predicate Specifications
;; =============================================================================

(def SPredSpec
  "Single predicate spec.
   Recognized shapes:
     [:< :col 10]                     — comparison
     [:between :col 5 15]             — range
     [:in :col 1 2 3]                — set membership
     [:like :col \"%pattern%\"]         — string matching
     [:is-null :col]                  — null check
     [:or [:< :a 1] [:> :b 2]]       — disjunction
     [:not [:= :x 0]]                — negation"
  [:and
   [:or {:error/message "predicate must be a vector or list, e.g. [:< :col 10] or '(< :col 10)"}
    vector? list?]
   [:fn {:error/message
         (str "predicate must start with an operator: "
              ":< :> :<= :>= := :!= :between :in :not-in "
              ":like :ilike :is-null :is-not-null :or :and :not")}
    #(let [op (first %)
           kw (if (symbol? op) (keyword (name op)) op)]
       (or (contains? pred-op-set kw)
           ;; Also allow expression predicates: [:< [:* :a :b] 100]
           (contains? expr-op-set kw)))]])

;; =============================================================================
;; Query Types
;; =============================================================================

(def SOrderSpec
  "Order-by specification: [:col :asc/:desc] or :col."
  [:or :keyword
   [:tuple :keyword [:enum :asc :desc]]])

(def SJoinType
  "Join type."
  [:enum :inner :left :right :full])

(def SJoinOn
  "Join condition: [:= :left-col :right-col] or vector of such."
  [:or
   [:tuple [:= :=] :keyword :keyword]
   [:vector {:min 1} [:tuple [:= :=] :keyword :keyword]]])

(def SJoinSpec
  "Single join specification."
  [:map {:closed false}
   [:with SColumnMap]
   [:on SJoinOn]
   [:type {:optional true :default :inner} SJoinType]])

(def SWindowOp
  "Window function operator."
  [:enum :row-number :rank :dense-rank :ntile :percent-rank :cume-dist :sum :count :avg :min :max :lag :lead])

(def SWindowSpec
  "Window function specification.
   {:op :row-number :partition-by [:col1] :order-by [[:col2 :asc]] :as :rn}"
  [:map {:closed false}
   [:op SWindowOp]
   [:as :keyword]
   [:col {:optional true} SExpr]
   [:partition-by {:optional true} [:vector SExpr]]
   [:order-by {:optional true} [:vector SOrderSpec]]
   [:offset {:optional true} [:or :int :keyword]]
   [:default {:optional true} :any]
   [:frame {:optional true}
    [:map {:closed false}
     [:type [:enum :rows :range]]
     [:start [:or
              [:enum :unbounded-preceding :current-row :unbounded-following]
              [:tuple :int [:enum :preceding :following]]]]
     [:end [:or
            [:enum :unbounded-preceding :current-row :unbounded-following]
            [:tuple :int [:enum :preceding :following]]]]]]])

(def SResultFormat
  "Result output format."
  [:enum :rows :columns])

(def STemporalOpts
  "Temporal query options for resolving :from references via storage."
  [:map {:closed false}
   [:store {:optional true} :any]
   [:as-of {:optional true} :uuid]
   [:branch {:optional true} :string]
   [:as-of-tx {:optional true} :int]])

(def SQuery
  "Query map specification.
   Required: :from (StratumDataset, column map, or string/keyword table reference)
   Optional: :where :agg :group :having :select :order :limit :offset :distinct :join :result"
  [:map {:closed false}
   [:from {:error/message "query must have :from with a StratumDataset, column map, or table name"}
    [:or
     [:fn {:error/message "must be a StratumDataset"}
      #(satisfies? dataset/IDataset %)]
     SColumnMap
     :string
     :keyword]]
   [:where {:optional true}
    [:vector {:error/message ":where must be a vector of predicates"} SPredSpec]]
   [:agg {:optional true}
    [:vector {:error/message ":agg must be a vector of aggregate specs"} SAggSpec]]
   [:group {:optional true}
    [:vector {:error/message ":group must be a vector of column keywords or expressions"}
     [:or :keyword vector?]]]
   [:having {:optional true}
    [:vector {:error/message ":having must be a vector of predicates"} SPredSpec]]
   [:select {:optional true}
    [:vector {:error/message ":select must be a vector of column refs, expressions, or literals"}
     [:or :keyword vector? number? :string]]]
   [:order {:optional true}
    [:vector {:error/message ":order must be a vector of [:col :asc/:desc] or :col"}
     SOrderSpec]]
   [:limit {:optional true
            :error/message ":limit must be a non-negative integer"}
    nat-int?]
   [:offset {:optional true
             :error/message ":offset must be a non-negative integer"}
    nat-int?]
   [:distinct {:optional true} :boolean]
   [:join {:optional true}
    [:vector SJoinSpec]]
   [:window {:optional true}
    [:vector SWindowSpec]]
   [:result {:optional true} SResultFormat]])

;; =============================================================================
;; Isolation Forest Types
;; =============================================================================

(def SModel
  "Trained isolation forest model."
  [:map
   [:forest SLongArray]
   [:n-trees pos-int?]
   [:sample-size pos-int?]
   [:feature-names [:vector :keyword]]
   [:n-features pos-int?]
   [:contamination {:optional true} [:and number? [:> 0] [:<= 0.5]]]
   [:threshold {:optional true} number?]
   [:training-scores-min {:optional true} number?]
   [:training-scores-max {:optional true} number?]])

(def STrainOpts
  "Options for iforest/train."
  [:map {:error/message "train options must be a map with :from (StratumDataset or column map)"}
   [:from [:or
           [:fn {:error/message "must be a StratumDataset"}
            #(satisfies? dataset/IDataset %)]
           SColumnMap]]
   [:n-trees {:optional true :default 100
              :error/message ":n-trees must be a positive integer"}
    pos-int?]
   [:sample-size {:optional true :default 256
                  :error/message ":sample-size must be a positive integer"}
    pos-int?]
   [:seed {:optional true :default 42} int?]
   [:contamination {:optional true
                    :error/message ":contamination must be in (0, 0.5]"}
    [:and number? [:> 0] [:<= 0.5]]]])

(def SPredictOpts
  "Options for iforest/predict."
  [:map {:closed false}
   [:threshold {:optional true
                :error/message ":threshold must be a number"}
    number?]])

;; =============================================================================
;; SQL Types
;; =============================================================================

(def SSqlQuery
  "SQL query string."
  [:and :string [:fn {:error/message "must be a non-empty SQL string"} seq]])

(def STableRegistry
  "Map of table names to column maps."
  [:map-of {:error/message "table registry must be a map of string → column map"}
   :string SColumnMap])

;; =============================================================================
;; Malli Registry
;; =============================================================================

(def registry
  "Malli registry with all Stratum types."
  (merge
   (m/default-schemas)
   {:stratum/SDoubleArray   SDoubleArray
    :stratum/SLongArray     SLongArray
    :stratum/SStringArray   SStringArray
    :stratum/SColumnData    SColumnData
    :stratum/SColumnMap     SColumnMap
    :stratum/SModel         SModel
    :stratum/STrainOpts     STrainOpts
    :stratum/SPredictOpts   SPredictOpts
    :stratum/SAggSpec       SAggSpec
    :stratum/SPredSpec      SPredSpec
    :stratum/SQuery         SQuery
    :stratum/STemporalOpts  STemporalOpts
    :stratum/SSqlQuery      SSqlQuery
    :stratum/STableRegistry STableRegistry}))

;; =============================================================================
;; Validation Helpers
;; =============================================================================

(defn validate
  "Validate data against a schema. Returns true if valid."
  [schema data]
  (m/validate schema data {:registry registry}))

(defn explain
  "Explain validation failure. Returns nil if valid."
  [schema data]
  (m/explain schema data {:registry registry}))

(defn humanize
  "Human-readable validation error message."
  [schema data]
  (some-> (explain schema data) me/humanize))

(defn validate!
  "Validate or throw with humanized error message."
  [schema data context]
  (when-let [errors (humanize schema data)]
    (throw (ex-info (str "Invalid input: " (pr-str errors))
                    (merge {:errors errors :schema (m/form schema)} context)))))

;; =============================================================================
;; API Specification
;; =============================================================================

(def api-specification
  "Complete API specification for Stratum.

   Operation names become:
   - Clojure function names (stratum.api namespace)
   - SQL function names (ANOMALY_SCORE, ANOMALY_PREDICT, etc.)
   - Future: Java method names, HTTP routes"

  '{;; =========================================================================
    ;; Query Engine
    ;; =========================================================================

    q
    {:args [:function
            [:=> [:cat :stratum/SQuery] :any]
            [:=> [:cat :stratum/SSqlQuery :stratum/STableRegistry] :any]]
     :ret :any
     :categories [:query]
     :stability :stable
     :referentially-transparent? true
     :doc "Query Stratum. Accepts a query map or SQL string + table map."
     :impl stratum.api/q}

    explain
    {:args [:function
            [:=> [:cat :stratum/SQuery] :map]
            [:=> [:cat :stratum/SSqlQuery :stratum/STableRegistry] :map]]
     :ret :map
     :categories [:query :diagnostic]
     :stability :stable
     :referentially-transparent? true
     :doc "Show execution plan without running the query."
     :impl stratum.api/explain}

    ;; =========================================================================
    ;; Data Import
    ;; =========================================================================

    from-csv
    {:args [:function
            [:=> [:cat :string] :stratum/SColumnMap]
            [:=> [:cat :string :map] :stratum/SColumnMap]]
     :ret :stratum/SColumnMap
     :categories [:data :import]
     :stability :stable
     :referentially-transparent? true
     :doc "Read a CSV file into a Stratum column map."
     :impl stratum.csv/from-csv}

    from-parquet
    {:args [:=> [:cat :string] :stratum/SColumnMap]
     :ret :stratum/SColumnMap
     :categories [:data :import]
     :stability :stable
     :referentially-transparent? true
     :doc "Read a Parquet file into a Stratum column map."
     :impl stratum.parquet/from-parquet}

    from-maps
    {:args [:=> [:cat [:sequential :map]] :stratum/SColumnMap]
     :ret :stratum/SColumnMap
     :categories [:data :import]
     :stability :stable
     :referentially-transparent? true
     :doc "Convert a sequence of maps to a Stratum column map."
     :impl stratum.csv/from-maps}

    encode-column
    {:args [:=> [:cat :stratum/SColumnData] :stratum/SColumnData]
     :ret :stratum/SColumnData
     :categories [:data :encoding]
     :stability :stable
     :referentially-transparent? true
     :doc "Pre-encode a column (String[] → dict-encoded, long[]/double[] → passthrough)."
     :impl stratum.query/encode-column}

    ;; =========================================================================
    ;; SQL Server
    ;; =========================================================================

    start-server
    {:args [:function
            [:=> [:cat] :map]
            [:=> [:cat :map] :map]]
     :ret :map
     :categories [:server :lifecycle]
     :stability :stable
     :referentially-transparent? false
     :doc "Start a PostgreSQL-compatible SQL server."
     :impl stratum.server/start}

    stop-server
    {:args [:=> [:cat :map] :any]
     :ret :any
     :categories [:server :lifecycle]
     :stability :stable
     :referentially-transparent? false
     :doc "Stop a running SQL server."
     :impl stratum.server/stop}

    register-table!
    {:args [:=> [:cat :map :string :stratum/SColumnMap] :any]
     :ret :any
     :categories [:server :data]
     :stability :stable
     :referentially-transparent? false
     :doc "Register a table with the SQL server."
     :impl stratum.server/register-table!}

    register-model!
    {:args [:=> [:cat :map :string :stratum/SModel] :any]
     :ret :any
     :categories [:server :anomaly-detection]
     :stability :stable
     :referentially-transparent? false
     :doc "Register an isolation forest model with the SQL server."
     :impl stratum.server/register-model!}

    ;; =========================================================================
    ;; Anomaly Detection (Isolation Forest)
    ;; =========================================================================

    train-iforest
    {:args [:=> [:cat :stratum/STrainOpts] :stratum/SModel]
     :ret :stratum/SModel
     :categories [:anomaly-detection :training]
     :stability :stable
     :referentially-transparent? true
     :doc "Train an isolation forest for anomaly detection.

Options:
  :from          — map of keyword → double[]/long[] columns (required)
  :n-trees       — number of trees (default 100)
  :sample-size   — subsample size per tree (default 256)
  :seed          — random seed for reproducibility (default 42)
  :contamination — expected fraction of anomalies (0, 0.5], sets auto-threshold"
     :examples [{:desc "Basic training"
                 :code "(train-iforest {:from {:amount amounts :freq freqs}})"}
                {:desc "With contamination for auto-threshold"
                 :code "(train-iforest {:from data :contamination 0.05})"}]
     :impl stratum.iforest/train}

    iforest-score
    {:args [:=> [:cat :stratum/SModel :stratum/SColumnMap] :stratum/SDoubleArray]
     :ret :stratum/SDoubleArray
     :categories [:anomaly-detection :scoring]
     :stability :stable
     :referentially-transparent? true
     :doc "Score rows using a trained isolation forest.
Returns double[] of anomaly scores in [0, 1], higher = more anomalous."
     :impl stratum.iforest/score}

    iforest-predict
    {:args [:function
            [:=> [:cat :stratum/SModel :stratum/SColumnMap] :stratum/SLongArray]
            [:=> [:cat :stratum/SModel :stratum/SColumnMap :stratum/SPredictOpts] :stratum/SLongArray]]
     :ret :stratum/SLongArray
     :categories [:anomaly-detection :prediction]
     :stability :stable
     :referentially-transparent? true
     :doc "Binary anomaly prediction: 1 = anomaly, 0 = normal.
Requires :contamination during training or explicit {:threshold t}."
     :examples [{:desc "With auto-threshold from contamination"
                 :code "(iforest-predict model data)"}
                {:desc "With explicit threshold"
                 :code "(iforest-predict model data {:threshold 0.65})"}]
     :impl stratum.iforest/predict}

    iforest-predict-proba
    {:args [:=> [:cat :stratum/SModel :stratum/SColumnMap] :stratum/SDoubleArray]
     :ret :stratum/SDoubleArray
     :categories [:anomaly-detection :prediction]
     :stability :stable
     :referentially-transparent? true
     :doc "Anomaly probability [0, 1] normalized from training distribution.
If model was trained with :contamination, uses min-max normalization."
     :impl stratum.iforest/predict-proba}

    iforest-predict-confidence
    {:args [:=> [:cat :stratum/SModel :stratum/SColumnMap] :stratum/SDoubleArray]
     :ret :stratum/SDoubleArray
     :categories [:anomaly-detection :prediction]
     :stability :stable
     :referentially-transparent? true
     :doc "Prediction confidence [0, 1] based on tree agreement.
Uses coefficient of variation of per-tree path lengths.
High confidence = trees agree, low confidence = uncertain."
     :impl stratum.iforest/predict-confidence}

    iforest-rotate
    {:args [:function
            [:=> [:cat :stratum/SModel :stratum/SColumnMap] :stratum/SModel]
            [:=> [:cat :stratum/SModel :stratum/SColumnMap pos-int?] :stratum/SModel]]
     :ret :stratum/SModel
     :categories [:anomaly-detection :training]
     :stability :stable
     :referentially-transparent? true
     :doc "Rotate oldest trees with new ones for online adaptation.
Returns a new model (original unchanged via CoW).
Best with clean training data — contaminated data reduces detection quality."
     :impl stratum.iforest/rotate-forest}})
