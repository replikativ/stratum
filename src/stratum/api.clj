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
     (st/parquet-dataset \"data/orders.parquet\")

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
         (let [q (:query parsed)
               result (query/q q)
               result (if-let [post (:_post-aggs q)]
                        (sql/apply-post-aggs result post)
                        result)
               result (if-let [sel (:_select-columns q)]
                        (sql/apply-select-columns result sel)
                        result)]
           result)

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
       (let [q (:query parsed)
             result (query/q q)
             result (if-let [post (:_post-aggs q)]
                      (sql/apply-post-aggs result post)
                      result)
             result (if-let [sel (:_select-columns q)]
                      (sql/apply-select-columns result sel)
                      result)]
         result)

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

(def parquet-dataset
  "Open a Parquet file as a read-only, lazy-decode StratumDataset.
   Constant-time open; row groups become chunks decoded on first
   touch. Per-row-group statistics from the parquet metadata feed
   stratum's zone-map pruning. See stratum.parquet/parquet-dataset
   for options."
  parquet/parquet-dataset)

(def close-parquet-dataset!
  "Explicitly release the file handle backing a parquet-dataset.
   The underlying mmap + reader are otherwise released by a Cleaner
   when the dataset becomes unreachable; use this when you need
   deterministic release (long-lived JVMs, tests, file rotation)."
  parquet/close-parquet-dataset!)

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

(defn window-join
  "Window join (q `wj` semantics): for each left row at time t, aggregate
   the right rows whose time falls within [t + lo, t + hi].

   This is the bridge between ASOF (one match per left row) and a regular
   range join (all matches, materialised) — it returns all left rows with
   per-row aggregates over a time window of the right side.

   Args:
     left   — column map (becomes `:from`-shaped output prefix)
     right  — column map for the source aggregated
     opts   — {:asof-on [:left-ts-col :right-ts-col]
              :window [lo hi]    ; numeric, in the column's storage unit
              :agg {:result-col {:op :sum/:count/:avg/:min/:max
                                  :col :right-col-name}}
              :temporal-unit :micros|:seconds|...   ; for [lo hi unit] form
              }

   Window-spec convenience: if `:window` is `[lo hi unit]` and the right
   ts column has `:temporal-unit :micros`, lo/hi are auto-converted.

   Equality keys (partition) are not yet supported — do a single global
   merge. Use a per-symbol partition explicitly via subsetting if needed.

   Returns a column map: original left columns + agg result columns."
  [left right {:keys [asof-on window agg temporal-unit]}]
  (let [[l-key r-key] asof-on
        [lo hi unit] (if (= 3 (count window)) window [(first window) (second window) nil])
        ;; Convert lo/hi to the right unit if (unit, temporal-unit) given
        scale (case [unit temporal-unit]
                [nil nil] 1
                [:microseconds :micros] 1
                [:milliseconds :micros] 1000
                [:seconds :micros] 1000000
                [:minutes :micros] 60000000
                [:hours :micros] 3600000000
                [:days :micros] 86400000000
                [:days :days] 1
                [:weeks :days] 7
                1)
        lo (long (* (long lo) (long scale)))
        hi (long (* (long hi) (long scale)))
        ^longs left-ts (let [d (or (get-in left [l-key :data]) (get left l-key))]
                         (if (instance? (Class/forName "[J") d) d
                             (throw (ex-info "left ts column must be long[]"
                                             {:col l-key :type (type d)}))))
        ^longs right-ts (let [d (or (get-in right [r-key :data]) (get right r-key))]
                          (if (instance? (Class/forName "[J") d) d
                              (throw (ex-info "right ts column must be long[]"
                                              {:col r-key :type (type d)}))))
        n-left (alength left-ts)
        n-right (alength right-ts)
        ;; Build per-agg accumulators using prefix sums for sum/count/avg.
        agg-results
        (into {}
              (map (fn [[as {:keys [op col]}]]
                     [as
                      (if (= op :count)
                        {:op :count}
                        (let [src
                              (let [d (or (get-in right [col :data]) (get right col))]
                                (cond (instance? (Class/forName "[D") d) d
                                      (instance? (Class/forName "[J") d)
                                      (let [da (double-array n-right)]
                                        (dotimes [i n-right] (aset da i (double (aget ^longs d i))))
                                        da)
                                      :else (throw (ex-info "agg col must be numeric"
                                                            {:col col :type (type d)}))))]
                          (case op
                            (:sum :avg)
                            (let [prefix (double-array (inc n-right))]
                              (dotimes [i n-right]
                                (aset prefix (inc i) (+ (aget prefix i) (aget ^doubles src i))))
                              {:op op :prefix prefix})
                            :min {:op :min :src src}
                            :max {:op :max :src src})))]))
              agg)
        ;; Two-pointer sweep over left (must be ascending by left-ts). For
        ;; correctness we sort left by ts here and remap result indices.
        left-order (let [arr (int-array n-left)]
                     (dotimes [i n-left] (aset arr i i))
                     ;; Sort by left-ts asc
                     (let [boxed (object-array n-left)]
                       (dotimes [i n-left] (aset boxed i (Integer/valueOf i)))
                       (java.util.Arrays/sort boxed
                                              (reify java.util.Comparator
                                                (compare [_ a b]
                                                  (Long/compare (aget left-ts (.intValue ^Integer a))
                                                                (aget left-ts (.intValue ^Integer b))))))
                       (dotimes [i n-left] (aset arr i (.intValue ^Integer (aget boxed i))))
                       arr))
        ;; Right is also sorted ascending by ts
        right-order (let [arr (int-array n-right)
                          _ (dotimes [i n-right] (aset arr i i))
                          boxed (object-array n-right)]
                      (dotimes [i n-right] (aset boxed i (Integer/valueOf i)))
                      (java.util.Arrays/sort boxed
                                             (reify java.util.Comparator
                                               (compare [_ a b]
                                                 (Long/compare (aget right-ts (.intValue ^Integer a))
                                                               (aget right-ts (.intValue ^Integer b))))))
                      (dotimes [i n-right] (aset arr i (.intValue ^Integer (aget boxed i))))
                      arr)
        ;; sorted right-ts copy for two-pointer search
        right-sorted-ts (let [a (long-array n-right)]
                          (dotimes [i n-right] (aset a i (aget right-ts (aget right-order i))))
                          a)
        ;; Build the per-agg result, in original-left-row order
        agg-out (into {}
                      (map (fn [[as _]]
                             [as (double-array n-left)]))
                      agg)]
    ;; Two-pointer sweep: for each left row in sorted order, advance lo/hi.
    (loop [i 0, lo-ptr 0, hi-ptr 0]
      (when (< i n-left)
        (let [orig-idx (aget left-order i)
              t (aget left-ts orig-idx)
              lo-target (+ t lo)
              hi-target (+ t hi)
              ;; Advance lo-ptr while right-sorted-ts < lo-target
              new-lo (loop [p (long lo-ptr)]
                       (if (and (< p n-right) (< (aget right-sorted-ts p) lo-target))
                         (recur (inc p))
                         p))
              ;; Advance hi-ptr while right-sorted-ts <= hi-target
              new-hi (loop [p (long (max hi-ptr new-lo))]
                       (if (and (< p n-right) (<= (aget right-sorted-ts p) hi-target))
                         (recur (inc p))
                         p))]
          ;; For each agg, compute over [new-lo, new-hi)
          (doseq [[as info] agg-results]
            (let [^doubles out-arr (get agg-out as)
                  n-in-window (- new-hi new-lo)
                  v (case (:op info)
                      :sum (let [^doubles pf (:prefix info)]
                             (- (aget pf new-hi) (aget pf new-lo)))
                      :avg (if (zero? n-in-window)
                             Double/NaN
                             (let [^doubles pf (:prefix info)]
                               (/ (- (aget pf new-hi) (aget pf new-lo)) (double n-in-window))))
                      :count (double n-in-window)
                      :min (if (zero? n-in-window)
                             Double/NaN
                             (let [^doubles src (:src info)]
                               (loop [p (int new-lo), m Double/POSITIVE_INFINITY]
                                 (if (< p new-hi)
                                   (let [orig (aget right-order p)
                                         x (aget src orig)]
                                     (recur (inc p) (Math/min m x)))
                                   m))))
                      :max (if (zero? n-in-window)
                             Double/NaN
                             (let [^doubles src (:src info)]
                               (loop [p (int new-lo), m Double/NEGATIVE_INFINITY]
                                 (if (< p new-hi)
                                   (let [orig (aget right-order p)
                                         x (aget src orig)]
                                     (recur (inc p) (Math/max m x)))
                                   m)))))]
              (aset out-arr orig-idx (double v))))
          (recur (inc i) new-lo new-hi))))
    ;; Return original left columns + agg columns (in original index order)
    (merge (into {} (map (fn [[k v]] [k (if (map? v) v {:type :int64 :data v})])) left)
           (into {} (map (fn [[k arr]] [k {:type :float64 :data arr}])) agg-out))))

(defn latest-on
  "Return only the latest row per partition: for each distinct combination
   of `partition-by` columns, keep the row with the maximum value in the
   `order-by` column.

   Equivalent SQL pattern:
     SELECT * FROM (
       SELECT *, ROW_NUMBER() OVER (PARTITION BY p1,p2 ORDER BY ts DESC) AS rn
       FROM t
     ) WHERE rn = 1

   Args:
     query    — base query map (anything `q` accepts)
     opts     — {:partition-by [:p1 :p2 ...] :order-by [[:ts :asc/:desc]]}

   When :order-by direction is :asc, returns the row with the *maximum* ts
   (so 'latest' in chronological terms); :desc returns the minimum.

   Returns the result of running the augmented query."
  [query {:keys [partition-by order-by]}]
  (let [order-by (or order-by [[:ts :asc]])
        ;; Flip direction so ROW_NUMBER picks the desired latest row.
        flip (fn [d] (if (= d :desc) :asc :desc))
        flipped (mapv (fn [[c d]] [c (flip d)]) order-by)]
    (q (-> query
           (update :window (fnil conj [])
                   {:op :row-number
                    :partition-by partition-by
                    :order-by flipped
                    :as :__latest_rn})
           (update :having (fnil conj [])
                   [:= :__latest_rn 1])))))

(defn generate-series
  "Generate a dense sequence of values as a `:from`-compatible column map.
   Returns {:value <column>}.

   Numeric form:
     (generate-series 1 10)            ; 1..10 step 1     → long[]
     (generate-series 0 100 5)         ; 0,5,10,…,100      → long[]
     (generate-series 0.0 1.0 0.1)     ; 0.0,0.1,…,1.0    → double[]

   Temporal form (single :step argument with :unit):
     (generate-series start-micros end-micros 5 :minutes :micros)
       ; produces {:value (column with :temporal-unit :micros)}

   `start` and `end` are inclusive. Useful as a left-side time spine for
   gap-filling joins."
  ([start end]
   (generate-series start end 1))
  ([start end step]
   (cond
     ;; Floating-point step
     (or (double? step) (and (number? start) (not (integer? start))))
     (let [s (double start)
           e (double end)
           st (double step)
           n (max 0 (long (Math/floor (/ (- e s) st))))
           arr (double-array (inc n))]
       (dotimes [i (inc n)] (aset arr i (+ s (* i st))))
       {:value {:type :float64 :data arr}})
     :else
     (let [s (long start)
           e (long end)
           st (long step)
           n (if (zero? st) 0 (max 0 (long (quot (- e s) st))))
           arr (long-array (inc n))]
       (dotimes [i (inc n)] (aset arr i (+ s (* (long i) st))))
       {:value {:type :int64 :data arr}})))
  ([start end width unit temporal-unit]
   ;; Temporal flavor: width-of-unit step, output column tagged with
   ;; :temporal-unit so DATE_TRUNC/EXTRACT/TIME_BUCKET dispatch correctly.
   (let [step-units (case [unit temporal-unit]
                      [:microseconds :micros] width
                      [:milliseconds :micros] (* width 1000)
                      [:seconds :micros]      (* width 1000000)
                      [:minutes :micros]      (* width 60000000)
                      [:hours :micros]        (* width 3600000000)
                      [:days :micros]         (* width 86400000000)
                      [:days :days]           width
                      [:weeks :days]          (* width 7)
                      [:seconds :seconds]     width
                      [:minutes :seconds]     (* width 60)
                      [:hours :seconds]       (* width 3600)
                      [:days :seconds]        (* width 86400)
                      (throw (ex-info "Unsupported (unit, temporal-unit) combination"
                                      {:unit unit :temporal-unit temporal-unit})))
         s start
         e end
         n (max 0 (long (quot (- e s) (long step-units))))
         arr (long-array (inc n))]
     (dotimes [i (inc n)] (aset arr i (+ s (* (long i) (long step-units)))))
     {:value {:type :int64 :data arr :temporal-unit temporal-unit}})))
