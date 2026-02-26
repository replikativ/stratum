(ns olap-bench
  "Comprehensive OLAP Benchmark: Stratum vs DuckDB.

   Tier 1: TPC-H Q1/Q6, SSB Q1.1 (existing B1-B6)
   Tier 2: H2O.ai db-benchmark (Q1-Q10, J1-J3)
   Tier 3: SSB Q1.2, ClickBench (~25 queries)
   Tier 4: NYC Taxi
   Tier 5: Hash Join
   Tier 6: Statistical (Median, Percentile, Approx Quantile)

   DuckDB comparison uses in-process JDBC (no CLI overhead).

   Input modes:
     (default)   — raw arrays
     idx         — PersistentColumnIndex (materialization overhead)
     sorted-idx  — sorted then indexed (zone map pruning benefit)

   Usage:
     clj -M:olap                  ;; run all tiers (arrays)
     clj -M:olap 1000000          ;; custom row count
     clj -M:olap t1 idx           ;; T1 with indices
     clj -M:olap t1 sorted-idx    ;; T1 with sorted indices (zone map pruning)
     clj -M:olap t2               ;; run only tier 2 (H2O)
     clj -M:olap t1 t2            ;; run tiers 1 and 2
     clj -M:olap 1000000 h2o cb   ;; 1M rows, H2O + ClickBench
     clj -M:olap cb q19 q43       ;; ClickBench tier, only Q19+Q43
     clj -M:olap taxi q3          ;; Taxi tier, only Q3
     clj -M:olap q19 q43          ;; auto-selects all tiers, runs only matching queries"
  (:require [stratum.query :as q]
            [stratum.index :as index]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.set])
  (:import [stratum.internal ColumnOps]
           [java.io BufferedReader InputStreamReader]
           [java.sql DriverManager Connection Statement ResultSet]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:private default-n 10000000)

;; Scale-adaptive warmup & iterations (set in -main based on N)
(def ^:private ^:dynamic *stratum-warmup* 10)
(def ^:private ^:dynamic *stratum-iters* 20)
(def ^:private ^:dynamic *duckdb-warmup* 1)
(def ^:private ^:dynamic *duckdb-iters* 20)

;; ============================================================================
;; Bench Helper
;; ============================================================================

(defn- bench
  "Run warmup + timed iterations, return {:median :min :p90 :times}.
   Always runs full warmup (critical for JIT + ForkJoinPool worker threads).
   Only reduces bench iterations for very slow queries."
  [f & {:keys [warmup iters] :or {warmup *stratum-warmup* iters *stratum-iters*}}]
  ;; Full warmup always — JIT and ForkJoinPool workers need it
  (dotimes [_ warmup] (f))
  ;; Probe post-warmup to decide bench iterations
  (let [probe-start (System/nanoTime)
        _ (f)
        probe-ms (/ (- (System/nanoTime) probe-start) 1e6)
        actual-iters (cond (> probe-ms 500) 5 (> probe-ms 200) 10 :else iters)]
    (let [times (mapv (fn [_]
                        (let [start (System/nanoTime)
                              _ (f)
                              end (System/nanoTime)]
                          (/ (- end start) 1e6)))
                      (range actual-iters))
          sorted (sort times)]
      {:median (nth sorted (quot actual-iters 2))
       :min    (first sorted)
       :p90    (nth sorted (int (* actual-iters 0.9)))
       :times  sorted})))

(defn- fmt-ms [^double ms]
  (format "%7.1fms" ms))

(defn- bench-1t
  "Bench f with Stratum forced to single-threaded execution."
  [f & opts]
  (ColumnOps/setParallelThreshold Integer/MAX_VALUE)
  (try (apply bench f opts)
    (finally (ColumnOps/setParallelThreshold 200000))))

(defn- gc!
  "Force GC + brief pause to prevent G1GC pause inflation on subsequent benchmarks."
  []
  (System/gc)
  (Thread/sleep 200))

(defn- fmt-ratio
  "Format a ratio string. Handles edge cases where times are near zero."
  [^double stratum-ms ^double duckdb-ms]
  (cond
    (and (< stratum-ms 0.1) (< duckdb-ms 0.1)) "  both <0.1ms"
    (< stratum-ms 0.01) (format "  DuckDB %.1fms" duckdb-ms)
    :else (let [ratio (/ duckdb-ms stratum-ms)]
            (if (>= ratio 1.0)
              (format "  Stratum %.1fx FASTER" ratio)
              (format "  DuckDB %.1fx faster" (/ 1.0 ratio))))))

(defn- safe-ratio ^double [^double a ^double b]
  (if (< b 0.01) 0.0 (/ a b)))

(defn- merge-best
  "Merge benchmark results, keeping the entry with the lower :stratum-1t for each key.
   When both old and new have a result for a key, the one with better (lower) 1T time wins."
  [old-results new-results]
  (merge-with (fn [old-val new-val]
                (let [old-t (get old-val :stratum-1t Double/MAX_VALUE)
                      new-t (get new-val :stratum-1t Double/MAX_VALUE)]
                  (if (<= (double old-t) (double new-t))
                    old-val
                    new-val)))
              old-results new-results))

;; ============================================================================
;; DuckDB JDBC Integration
;; ============================================================================

;; In-process JDBC connection eliminates ~1-5ms CLI process-spawn overhead.
;; One connection per tier, kept open for all queries.

(defn- duckdb-jdbc-connect
  "Open a DuckDB in-process JDBC connection. Returns ^Connection."
  ^Connection [^String db-path]
  (Class/forName "org.duckdb.DuckDBDriver")
  (DriverManager/getConnection (str "jdbc:duckdb:" db-path)))

(defn- duckdb-jdbc-exec!
  "Execute a DDL/DML statement (no result set)."
  [^Connection conn ^String sql]
  (with-open [^Statement stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn- duckdb-jdbc-query-scalar
  "Execute a query and return the first column of the first row as a string."
  ^String [^Connection conn ^String sql]
  (with-open [^Statement stmt (.createStatement conn)
              ^ResultSet rs (.executeQuery stmt sql)]
    (when (.next rs) (.getString rs 1))))

(defn- duckdb-bench
  "Benchmark a DuckDB query via JDBC. Returns {:median :min :p90} in ms.
   Uses *duckdb-warmup* and *duckdb-iters* for iteration counts.
   Optional :threads param controls DuckDB thread count (nil = default/all cores)."
  [^Connection conn sql & {:keys [warmup iters threads]
                           :or {warmup *duckdb-warmup* iters *duckdb-iters*}}]
  (when threads
    (duckdb-jdbc-exec! conn (str "SET threads=" threads)))
  ;; Warmup (DuckDB is native — only needs 1 buffer-pool warmup)
  (dotimes [_ warmup]
    (with-open [^Statement stmt (.createStatement conn)
                ^ResultSet rs (.executeQuery stmt sql)]
      (while (.next rs))))
  ;; Probe for adaptive iteration count
  (let [probe-start (System/nanoTime)
        _ (with-open [^Statement stmt (.createStatement conn)
                      ^ResultSet rs (.executeQuery stmt sql)]
            (while (.next rs)))
        probe-ms (/ (- (System/nanoTime) probe-start) 1e6)
        actual-iters (long (cond (> probe-ms 500) 5 (> probe-ms 200) 10 :else iters))
        times (mapv (fn [_]
                      (let [start (System/nanoTime)
                            _ (with-open [^Statement stmt (.createStatement conn)
                                          ^ResultSet rs (.executeQuery stmt sql)]
                                (while (.next rs)))
                            end (System/nanoTime)]
                        (/ (- end start) 1e6)))
                    (range actual-iters))
        sorted (sort times)]
    (when threads
      (duckdb-jdbc-exec! conn (str "SET threads=" (.availableProcessors (Runtime/getRuntime)))))
    {:median (nth sorted (quot actual-iters 2))
     :min    (first sorted)
     :p90    (nth sorted (int (* actual-iters 0.9)))}))

(defn- duckdb-setup-lineitem
  "Create lineitem table in a DuckDB JDBC connection. Returns ^Connection."
  ^Connection [^long n]
  (let [csv-path "data/tpch/lineitem.csv"]
    (when-not (.exists (io/file csv-path))
      (throw (ex-info "TPC-H data not found. Run: bin/download-data"
                      {:path csv-path})))
    (println "  Creating DuckDB lineitem table (JDBC)...")
    (let [conn (duckdb-jdbc-connect "")]
      (duckdb-jdbc-exec! conn (str "CREATE TABLE lineitem AS SELECT * FROM read_csv_auto('" csv-path "') LIMIT " n))
      (let [cnt (duckdb-jdbc-query-scalar conn "SELECT count(*) FROM lineitem")]
        (println (format "  DuckDB: %s rows loaded" cnt)))
      conn)))

;; ============================================================================
;; CSV Data Loading
;; ============================================================================

(defn- parse-csv-fields
  "Parse a CSV line with PostgreSQL NULL semantics.
   Unquoted empty → nil (NULL), quoted empty \"\" → empty string."
  [^String line]
  (let [fields (java.util.ArrayList.)
        sb (StringBuilder.)
        len (.length line)]
    (loop [i 0 in-quote false was-quoted false]
      (if (>= i len)
        (do (.add fields (if (and (zero? (.length sb)) (not was-quoted)) nil (.toString sb)))
            (vec fields))
        (let [c (.charAt line i)]
          (cond
            (and (= c \") (not in-quote))
            (recur (inc i) true true)

            (and (= c \") in-quote)
            (if (and (< (inc i) len) (= (.charAt line (inc i)) \"))
              (do (.append sb \") (recur (+ i 2) true was-quoted))
              (recur (inc i) false was-quoted))

            (and (= c \,) (not in-quote))
            (do (.add fields (if (and (zero? (.length sb)) (not was-quoted)) nil (.toString sb)))
                (.setLength sb 0)
                (recur (inc i) false false))

            :else
            (do (.append sb c) (recur (inc i) in-quote was-quoted))))))))

(defn- load-csv-columns
  "Load CSV file into map of keyword -> primitive array.
   col-specs: vector of {:name \"colName\" :key :col-key :type :long|:double|:string}
   Returns {:col-key array ... :n row-count} or nil if file not found.
   Streams line-by-line — never holds all CSV text in memory.
   Uses PostgreSQL NULL semantics: unquoted empty → nil, quoted empty → \"\"."
  [^String path col-specs ^long max-rows]
  (when (.exists (io/file path))
    (with-open [rdr (io/reader path)]
      (let [^java.io.BufferedReader brdr rdr
            header (vec (map #(or % "") (parse-csv-fields (.readLine brdr))))
            col-indices (mapv #(.indexOf ^java.util.List header (:name %)) col-specs)
            _ (when (some neg? col-indices)
                (let [missing (keep-indexed (fn [i idx] (when (neg? idx) (:name (nth col-specs i)))) col-indices)]
                  (println (str "  WARNING: CSV columns not found: " (str/join ", " missing)))
                  (throw (ex-info "Missing CSV columns" {:missing missing}))))
            ;; Count rows first (streaming) so we can pre-allocate arrays
            _ (println (format "  Counting rows in %s..." path))
            n (loop [cnt 0]
                (if (and (< cnt max-rows) (.readLine brdr))
                  (recur (inc cnt))
                  cnt))]
        ;; Re-open and skip header for actual parsing
        (with-open [rdr2 (io/reader path)]
          (let [^java.io.BufferedReader brdr2 rdr2
                _ (.readLine brdr2)  ;; skip header
                arrays (mapv #(case (:type %)
                                :long (long-array n)
                                :double (double-array n)
                                :string (make-array String n))
                             col-specs)]
            (dotimes [row n]
              (let [line (.readLine brdr2)
                    fields (parse-csv-fields line)]
                (dotimes [c (count col-specs)]
                  (let [idx (nth col-indices c)
                        val (nth fields idx)
                        spec (nth col-specs c)]
                    (case (:type spec)
                      :long (aset ^longs (nth arrays c) row
                                  (if (or (nil? val) (= val "")) Long/MIN_VALUE (long (Double/parseDouble val))))
                      :double (aset ^doubles (nth arrays c) row
                                    (if (or (nil? val) (= val "")) Double/NaN (Double/parseDouble val)))
                      ;; nil (unquoted empty) → null, "" (quoted empty) → ""
                      :string (aset ^"[Ljava.lang.String;" (nth arrays c) row val))))))
            (into {:n n}
                  (map-indexed (fn [i spec] [(:key spec) (nth arrays i)]) col-specs))))))))

;; ============================================================================
;; Data Generation
;; ============================================================================

(defn- generate-arrays
  "Load TPC-H lineitem data from data/tpch/lineitem.csv.
   Run bin/download-data first to generate the CSV."
  [^long n]
  (if-let [data (load-csv-columns "data/tpch/lineitem.csv"
                   [{:name "shipdate"   :key :shipdate   :type :long}
                    {:name "discount"   :key :discount   :type :double}
                    {:name "quantity"   :key :quantity    :type :long}
                    {:name "price"      :key :price      :type :double}
                    {:name "tax"        :key :tax         :type :double}
                    {:name "returnflag" :key :returnflag :type :long}
                    {:name "linestatus" :key :linestatus :type :long}]
                   n)]
    (do (println (format "  Loaded TPC-H data from CSV (%d rows)" (:n data)))
        data)
    (throw (ex-info "TPC-H data not found. Run: bin/download-data"
                    {:path "data/tpch/lineitem.csv"}))))

(defn- build-indices
  "Build Stratum indices from arrays.
   Dict-encoded columns (from q/encode-column) have their long[] codes
   converted to indices while preserving the dict metadata."
  [data]
  (println "  Building Stratum indices...")
  (let [t0 (System/nanoTime)
        result (into {}
                 (map (fn [[k v]]
                        (when (not= k :n)
                          [k (cond
                               (instance? (Class/forName "[J") v)
                               (index/index-from-seq :int64 (seq v))
                               (instance? (Class/forName "[D") v)
                               (index/index-from-seq :float64 (seq v))
                               ;; Dict-encoded map — index the codes, preserve dict
                               (and (map? v) (:dict v) (instance? (Class/forName "[J") (:data v)))
                               {:type :int64 :source :index
                                :index (index/index-from-seq :int64 (seq ^longs (:data v)))
                                :dict (:dict v) :dict-type (:dict-type v)}
                               :else v)])))
                 data)]
    (println (format "  Indices built in %.1fs" (/ (- (System/nanoTime) t0) 1e9)))
    (assoc result :n (:n data))))

(defn- argsort-long
  "Return int[] of indices that sort the long array in ascending order."
  ^ints [^longs arr ^long n]
  (let [indices (int-array n)
        idx-obj (object-array n)]
    (dotimes [i n] (aset idx-obj i (Integer/valueOf (int i))))
    (java.util.Arrays/sort idx-obj
      (reify java.util.Comparator
        (compare [_ a b]
          (Long/compare (aget arr (.intValue ^Integer a))
                        (aget arr (.intValue ^Integer b))))))
    (dotimes [i n] (aset indices i (.intValue ^Integer (aget idx-obj i))))
    indices))

(defn- argsort-double
  "Return int[] of indices that sort the double array in ascending order."
  ^ints [^doubles arr ^long n]
  (let [indices (int-array n)
        idx-obj (object-array n)]
    (dotimes [i n] (aset idx-obj i (Integer/valueOf (int i))))
    (java.util.Arrays/sort idx-obj
      (reify java.util.Comparator
        (compare [_ a b]
          (Double/compare (aget arr (.intValue ^Integer a))
                          (aget arr (.intValue ^Integer b))))))
    (dotimes [i n] (aset indices i (.intValue ^Integer (aget idx-obj i))))
    indices))

(defn- sort-data-by
  "Sort all arrays in data map by values in the key column.
   Used for sorted-idx mode to enable zone map pruning."
  [data sort-key]
  (println (format "  Sorting data by %s..." (name sort-key)))
  (let [t0 (System/nanoTime)
        n (long (:n data))
        sort-col (get data sort-key)
        _ (assert sort-col (str "Sort key " sort-key " not found in data"))
        ^ints order (cond
                      (instance? (Class/forName "[J") sort-col)
                      (argsort-long sort-col n)
                      (instance? (Class/forName "[D") sort-col)
                      (argsort-double sort-col n)
                      :else (throw (ex-info "Cannot sort by non-primitive column" {:key sort-key})))
        result (into {:n n}
                 (keep (fn [[k v]]
                         (when (not= k :n)
                           [k (cond
                                (instance? (Class/forName "[J") v)
                                (let [^longs src v
                                      dst (long-array n)]
                                  (dotimes [i n] (aset dst i (aget src (aget order i))))
                                  dst)
                                (instance? (Class/forName "[D") v)
                                (let [^doubles src v
                                      dst (double-array n)]
                                  (dotimes [i n] (aset dst i (aget src (aget order i))))
                                  dst)
                                ;; Encoded string column (map with :codes and :dict)
                                (and (map? v) (:codes v))
                                (let [^longs codes (:codes v)
                                      dst (long-array n)]
                                  (dotimes [i n] (aset dst i (aget codes (aget order i))))
                                  (assoc v :codes dst))
                                ;; Raw String arrays
                                (instance? (Class/forName "[Ljava.lang.String;") v)
                                (let [^"[Ljava.lang.String;" src v
                                      dst (make-array String n)]
                                  (dotimes [i n] (aset ^"[Ljava.lang.String;" dst i (aget src (aget order i))))
                                  dst)
                                :else v)])))
                   data)]
    (println (format "  Sorted in %.1fs" (/ (- (System/nanoTime) t0) 1e9)))
    result))

(defn- prepare-data
  "Prepare data for benchmarking based on input mode.
   :arrays - use raw arrays (default)
   :idx - convert arrays to PersistentColumnIndex
   :sorted-idx - sort by sort-key, then convert to indices"
  [data mode sort-key]
  (case mode
    :arrays data
    :idx (build-indices data)
    :sorted-idx (if sort-key
                  (build-indices (sort-data-by data sort-key))
                  (build-indices data))))

;; ============================================================================
;; Data Export: Stratum arrays → CSV → DuckDB (for shared-data validation)
;; ============================================================================

(defn- resolve-array-value
  "Get the value at index i from an array or encoded column.
   Returns a string suitable for CSV output."
  ^String [col ^long i]
  (cond
    ;; Dict-encoded string column: {:type :int64 :data long[] :dict String[]}
    (and (map? col) (:dict col))
    (let [code (aget ^longs (:data col) i)]
      (aget ^"[Ljava.lang.String;" (:dict col) (int code)))

    ;; Encoded column without dict (plain int64/float64 map)
    (and (map? col) (:data col))
    (let [arr (:data col)]
      (cond
        (instance? (Class/forName "[J") arr) (str (aget ^longs arr i))
        (instance? (Class/forName "[D") arr) (str (aget ^doubles arr i))
        :else (str (aget ^"[Ljava.lang.Object;" arr i))))

    ;; Raw long[]
    (instance? (Class/forName "[J") col)
    (str (aget ^longs col i))

    ;; Raw double[]
    (instance? (Class/forName "[D") col)
    (str (aget ^doubles col i))

    ;; Raw String[]
    (instance? (Class/forName "[Ljava.lang.String;") col)
    (aget ^"[Ljava.lang.String;" col i)

    :else (str col)))

(defn- write-arrays-csv
  "Write Stratum arrays to a CSV file. col-order is a vector of [kw col] pairs.
   Returns the path written."
  ^String [^String path col-order ^long n]
  (with-open [^java.io.Writer w (io/writer path)]
    ;; Header
    (.write w (str/join "," (map #(name (first %)) col-order)))
    (.write w "\n")
    ;; Rows
    (dotimes [i n]
      (dotimes [j (count col-order)]
        (when (pos? j) (.write w ","))
        (let [v (resolve-array-value (second (nth col-order j)) i)]
          ;; Quote strings that might contain commas
          (if (and v (or (.contains ^String v ",") (.contains ^String v "\"")))
            (.write w (str "\"" (.replace ^String v "\"" "\"\"") "\""))
            (.write w (str (or v ""))))))
      (.write w "\n")))
  path)

(defn- duckdb-load-csv
  "Load a CSV file into DuckDB as a table via JDBC. Drops existing table first."
  [^Connection conn ^String table-name ^String csv-path]
  (duckdb-jdbc-exec! conn (str "DROP TABLE IF EXISTS " table-name))
  (duckdb-jdbc-exec! conn (str "CREATE TABLE " table-name " AS SELECT * FROM read_csv_auto('" csv-path "')")))

;; ============================================================================
;; Result Validation Infrastructure
;; ============================================================================

(defn- duckdb-query-results
  "Run a SQL query on DuckDB via JDBC and parse the results.
   Returns {:columns [:col1 :col2] :rows [[v1 v2] ...]} or nil on error."
  [^Connection conn sql]
  (with-open [^Statement stmt (.createStatement conn)
              ^ResultSet rs (.executeQuery stmt sql)]
    (let [meta (.getMetaData rs)
          n-cols (.getColumnCount meta)
          header (mapv #(keyword (.toLowerCase (.getColumnLabel meta (inc %)))) (range n-cols))
          rows (loop [acc (transient [])]
                 (if (.next rs)
                   (recur (conj! acc
                            (mapv (fn [i]
                                    (let [v (.getObject rs (int (inc i)))]
                                      (cond
                                        (nil? v) nil
                                        (instance? Long v) (long v)
                                        (instance? Integer v) (long v)
                                        (instance? Short v) (long v)
                                        (instance? Double v) (double v)
                                        (instance? Float v) (double v)
                                        (instance? java.math.BigDecimal v) (.doubleValue ^java.math.BigDecimal v)
                                        (instance? java.math.BigInteger v) (.longValue ^java.math.BigInteger v)
                                        (instance? Number v) (.doubleValue ^Number v)
                                        :else (str v))))
                                  (range n-cols))))
                   (persistent! acc)))]
      (when (seq header)
        {:columns header :rows rows}))))

(defn- null-value?
  "True if v is a NULL sentinel: nil, NaN, or Long/MIN_VALUE."
  [v]
  (or (nil? v)
      (and (number? v) (Double/isNaN (double v)))))

(defn- get-flexible
  "Look up a keyword in a map, trying both underscore and dash variants."
  [m k]
  (or (get m k)
      (let [s (name k)
            alt (if (str/includes? s "_")
                  (keyword (str/replace s "_" "-"))
                  (keyword (str/replace s "-" "_")))]
        (get m alt))))

(defn- validate-results
  "Compare Stratum results (vector of maps) to DuckDB CSV results.
   Returns {:match? bool :mismatches [...] :stratum-count N :duckdb-count N}.
   Uses relative tolerance for numeric comparison.
   NULL values (nil in DuckDB, NaN in Stratum) are compared symmetrically."
  [stratum-results duckdb-results sort-keys & {:keys [tolerance] :or {tolerance 1e-6}}]
  (let [d-cols (:columns duckdb-results)
        d-raw (vec (:rows duckdb-results))
        s-sorted (if (seq sort-keys)
                   (sort-by (apply juxt (map (fn [k] #(get-flexible % k)) sort-keys)) stratum-results)
                   stratum-results)
        d-rows (if (seq sort-keys)
                 (let [key-indices (mapv #(.indexOf ^java.util.List d-cols %) sort-keys)]
                   (sort-by (fn [row] (mapv #(nth row %) key-indices)) d-raw))
                 d-raw)
        s-count (count s-sorted)
        d-count (count d-rows)]
    (if (not= s-count d-count)
      {:match? false :stratum-count s-count :duckdb-count d-count
       :mismatches [(format "Row count mismatch: Stratum=%d DuckDB=%d" s-count d-count)]}
      (let [mismatches (java.util.ArrayList.)]
        (dotimes [i (min s-count 100)]  ;; Only check first 100 rows for speed
          (let [s-row (nth s-sorted i)
                d-row (nth d-rows i)]
            (dotimes [j (count d-cols)]
              (let [col (nth d-cols j)
                    sv (get-flexible s-row col)
                    dv (nth d-row j)
                    s-null (null-value? sv)
                    d-null (null-value? dv)]
                ;; Both NULL → match. One NULL, one not → mismatch. Both non-NULL → compare.
                (cond
                  (and s-null d-null) nil ;; both NULL, OK
                  (or s-null d-null)
                  (.add mismatches
                        (format "Row %d, col %s: NULL mismatch Stratum=%s DuckDB=%s"
                                i (name col) sv dv))
                  :else
                  (let [match? (cond
                                 (and (number? sv) (number? dv))
                                 (let [s (double sv) d (double dv)]
                                   (or (== s d)
                                       (< (Math/abs (- s d))
                                          (* tolerance (Math/max 1.0 (Math/abs d))))))
                                 :else (= (str sv) (str dv)))]
                    (when-not match?
                      (.add mismatches
                            (format "Row %d, col %s: Stratum=%s DuckDB=%s"
                                    i (name col) sv dv)))))))))
        {:match? (.isEmpty mismatches)
         :stratum-count s-count
         :duckdb-count d-count
         :mismatches (vec mismatches)}))))

(defn- print-validation
  "Print validation result as PASS/FAIL."
  [label validation]
  (if (:match? validation)
    (println (format "  ✓ %s: PASS (%d rows match)" label (:stratum-count validation)))
    (do
      (println (format "  ✗ %s: FAIL (%d vs %d rows)" label
                       (:stratum-count validation) (:duckdb-count validation)))
      (doseq [m (take 5 (:mismatches validation))]
        (println (format "    → %s" m))))))

(defn- validate-query
  "Validate Stratum results against DuckDB via JDBC. Prints PASS/FAIL, never throws.
   For large result sets (>100K rows), validates a LIMIT'd subset.
   Optional :tolerance kwarg for floating-point comparison (default 1e-6)."
  [label ^Connection conn sql stratum-results sort-keys & {:keys [tolerance] :or {tolerance 1e-6}}]
  (try
    (let [n (count stratum-results)
          ;; For large results, limit DuckDB query and Stratum subset
          [duck-sql s-results]
          (if (> n 100000)
            (let [order-clause (when (seq sort-keys)
                                 (str " ORDER BY " (str/join ", " (map name sort-keys))))
                  limited-sql (str sql order-clause " LIMIT 1000")
                  s-sorted (if (seq sort-keys)
                             (sort-by (apply juxt (map (fn [k] #(get % k)) sort-keys)) stratum-results)
                             stratum-results)]
              [limited-sql (vec (take 1000 s-sorted))])
            [sql stratum-results])
          duck-r (duckdb-query-results conn duck-sql)]
      (if duck-r
        (print-validation label (validate-results s-results duck-r sort-keys :tolerance tolerance))
        (println (format "  ? %s: SKIP (DuckDB returned no results)" label))))
    (catch Exception e
      (println (format "  ? %s: ERROR (%s)" label (.getMessage e))))))

;; ============================================================================
;; B1: TPC-H Q6 — Filter + Sum-Product
;; ============================================================================

(defn- bench-b1
  "TPC-H Q6: SUM(price * discount) with 3 filter predicates."
  [data ^Connection conn]
  (println "\nB1: TPC-H Q6 — Filter + Sum-Product")
  (println "    SUM(price * discount) WHERE shipdate BETWEEN 8766..9131")
  (println "                            AND discount BETWEEN 0.05..0.07")
  (println "                            AND quantity < 24")

  (let [q {:from {:shipdate (:shipdate data) :discount (:discount data)
                  :quantity (:quantity data) :price (:price data)}
           :where [[:between :shipdate 8766 9131]
                   [:between :discount 0.05 0.07]
                   [:< :quantity 24]]
           :agg [[:sum [:* :price :discount]]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1 thread):               %s  (rev=%.0f, cnt=%d)"
                     (fmt-ms (:median r-1t))
                     (double (:sum-product (first v)))
                     (:_count (first v))))
    (println (format "  Stratum (all threads):            %s" (fmt-ms (:median r))))

    ;; DuckDB
    (let [sql "SELECT SUM(price * discount) FROM lineitem WHERE shipdate >= 8766 AND shipdate < 9131 AND discount >= 0.05 AND discount <= 0.07 AND quantity < 24"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (validate-query "B1" conn
                      "SELECT SUM(price * discount) AS \"sum-product\" FROM lineitem WHERE shipdate >= 8766 AND shipdate <= 9131 AND discount >= 0.05 AND discount <= 0.07 AND quantity < 24"
                      v [] )

      {:stratum-1t (:median r-1t)
       :stratum (:median r)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; B2: TPC-H Q1 — Group-By + Multiple Aggregates
;; ============================================================================

(defn- bench-b2
  "TPC-H Q1: Group-by + 8 aggregates with expression eval."
  [data ^Connection conn]
  (println "\nB2: TPC-H Q1 — Group-By + Multiple Aggregates")
  (println "    GROUP BY returnflag, linestatus with sum, avg, count")

  ;; Stratum scalar group-by path
  (let [q {:from {:returnflag (:returnflag data) :linestatus (:linestatus data)
                  :quantity (:quantity data) :price (:price data)
                  :discount (:discount data)}
           :where [[:< :shipdate 10471]]  ;; shipdate <= '1998-09-02'
           :group [:returnflag :linestatus]
           :agg [[:as [:sum :quantity] :sum_qty]
                 [:as [:sum :price] :sum_base_price]
                 [:as [:sum [:* :price [:- 1 :discount]]] :sum_disc_price]
                 [:as [:avg :quantity] :avg_qty]
                 [:as [:avg :price] :avg_price]
                 [:as [:avg :discount] :avg_disc]
                 [:as [:count] :count_order]]
           :order [[:returnflag :asc] [:linestatus :asc]]}
        ;; Need :shipdate in :from for the where clause
        q (assoc-in q [:from :shipdate] (:shipdate data))
        r-str (bench #(q/q q))
        r-str-1t (bench-1t #(q/q q))
        v-str (q/q q)]
    (println (format "  Stratum (1 thread):               %s  (%d groups)"
                     (fmt-ms (:median r-str-1t))
                     (count v-str)))
    (println (format "  Stratum (all threads):            %s" (fmt-ms (:median r-str))))

    ;; DuckDB
    (let [sql (str "SELECT returnflag, linestatus, "
                   "sum(quantity) AS sum_qty, sum(price) AS sum_base_price, "
                   "sum(price * (1 - discount)) AS sum_disc_price, "
                   "avg(quantity) AS avg_qty, avg(price) AS avg_price, "
                   "avg(discount) AS avg_disc, count(*) AS count_order "
                   "FROM lineitem WHERE shipdate < 10471 "
                   "GROUP BY returnflag, linestatus "
                   "ORDER BY returnflag, linestatus")
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (validate-query "B2" conn sql v-str [:returnflag :linestatus])

      {:stratum (:median r-str)
       :stratum-1t (:median r-str-1t)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; B3: SSB Q1.1 — Filter + Sum-Product (Q6 variant)
;; ============================================================================

(defn- bench-b3
  "SSB Q1.1: SUM(price * discount) with different parameters."
  [data ^Connection conn]
  (println "\nB3: SSB Q1.1 — Filter + Sum-Product (different params)")
  (println "    SUM(price * discount) WHERE shipdate BETWEEN 8766..9131")
  (println "                            AND discount BETWEEN 0.01..0.03")
  (println "                            AND quantity < 25")

  ;; Stratum on arrays (fused SIMD)
  (let [q {:from {:shipdate (:shipdate data) :discount (:discount data)
                  :quantity (:quantity data) :price (:price data)}
           :where [[:between :shipdate 8766 9131]
                   [:between :discount 0.01 0.03]
                   [:< :quantity 25]]
           :agg [[:sum [:* :price :discount]]]}
        r-str (bench #(q/q q))
        r-str-1t (bench-1t #(q/q q))
        v-str (q/q q)]
    (println (format "  Stratum (1 thread):               %s  (rev=%.0f, cnt=%d)"
                     (fmt-ms (:median r-str-1t))
                     (double (:sum-product (first v-str)))
                     (:_count (first v-str))))
    (println (format "  Stratum (all threads):            %s" (fmt-ms (:median r-str))))

    ;; DuckDB
    (let [sql "SELECT SUM(price * discount) FROM lineitem WHERE shipdate >= 8766 AND shipdate < 9131 AND discount >= 0.01 AND discount <= 0.03 AND quantity < 25"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (validate-query "B3" conn
                       "SELECT SUM(price * discount) AS \"sum-product\" FROM lineitem WHERE shipdate >= 8766 AND shipdate <= 9131 AND discount >= 0.01 AND discount <= 0.03 AND quantity < 25"
                       v-str [] )

      {:stratum (:median r-str)
       :stratum-1t (:median r-str-1t)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; B4: COUNT(*) — Pure metadata (no filter)
;; ============================================================================

(defn- bench-b4
  "COUNT(*): No filter, pure metadata query."
  [data ^Connection conn]
  (println "\nB4: COUNT(*) — No Filter (metadata)")

  (let [q {:from {:v (:price data)} :agg [[:count]]}
        r-str (bench #(q/q q))
        v (q/q q)]
    (println (format "  Stratum:                          %s  (count=%d)"
                     (fmt-ms (:median r-str))
                     (long (:count (first v)))))

    ;; DuckDB
    (let [sql "SELECT COUNT(*) FROM lineitem"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (println (str "  →" (fmt-ratio (:median r-str) (:median r-duck-1t)) " (vs 1T)"))
      (validate-query "B4" conn
                       "SELECT COUNT(*) AS count FROM lineitem"
                       v [])

      {:stratum (:median r-str)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; B5: Filtered Count — NEQ filter + count
;; ============================================================================

(defn- bench-b5
  "Filtered COUNT: COUNT(*) WHERE discount <> 0."
  [data ^Connection conn]
  (println "\nB5: Filtered Count — NEQ + COUNT")
  (println "    COUNT(*) WHERE discount <> 0")

  ;; Stratum fused SIMD count
  (let [q {:from {:discount (:discount data)}
           :where [[:!= :discount 0.0]]
           :agg [[:count]]}
        r-str (bench #(q/q q))
        r-str-1t (bench-1t #(q/q q))
        v-str (q/q q)]
    (println (format "  Stratum (1 thread):               %s  (count=%d)"
                     (fmt-ms (:median r-str-1t))
                     (long (:count (first v-str)))))
    (println (format "  Stratum (all threads):            %s" (fmt-ms (:median r-str))))

    ;; DuckDB
    (let [sql "SELECT COUNT(*) FROM lineitem WHERE discount <> 0"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (validate-query "B5" conn
                       "SELECT COUNT(*) AS count FROM lineitem WHERE discount <> 0"
                       v-str [])

      {:stratum (:median r-str)
       :stratum-1t (:median r-str-1t)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; B6: Low-Cardinality Group-By
;; ============================================================================

(defn- bench-b6
  "Low-cardinality GROUP BY: returnflag + COUNT(*) with filter."
  [data ^Connection conn]
  (println "\nB6: Low-Cardinality Group-By — Filter + Group + Count")
  (println "    GROUP BY returnflag WHERE shipdate >= 9131")

  ;; Stratum scalar group-by
  (let [q {:from {:shipdate (:shipdate data)
                  :returnflag (:returnflag data)}
           :where [[:>= :shipdate 9131]]
           :group [:returnflag]
           :agg [[:count]]
           :order [[:returnflag :asc]]}
        r-str (bench #(q/q q))
        r-str-1t (bench-1t #(q/q q))
        v-str (q/q q)]
    (println (format "  Stratum (1 thread):               %s  (%d groups)"
                     (fmt-ms (:median r-str-1t))
                     (count v-str)))
    (println (format "  Stratum (all threads):            %s" (fmt-ms (:median r-str))))

    ;; DuckDB
    (let [sql "SELECT returnflag, COUNT(*) FROM lineitem WHERE shipdate >= 9131 GROUP BY returnflag ORDER BY returnflag"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB (1 thread):                %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB (all threads):             %s" (fmt-ms (:median r-duck))))
      (validate-query "B6" conn
                       "SELECT returnflag, COUNT(*) AS count FROM lineitem WHERE shipdate >= 9131 GROUP BY returnflag ORDER BY returnflag"
                       v-str [:returnflag])

      {:stratum (:median r-str)
       :stratum-1t (:median r-str-1t)
       :duckdb-1t (:median r-duck-1t)
       :duckdb (:median r-duck)})))

;; ============================================================================
;; Tier 2: H2O.ai db-benchmark Data Generation
;; ============================================================================

(defn- make-dict-column
  "Build a dict-encoded column directly from (n-keys, format-str, rng, n-rows).
   Generates codes (long[]) and dict (String[]) without materializing n-rows String objects."
  [^long n-keys ^String fmt ^java.util.Random rng ^long n]
  (let [dict (into-array String (mapv #(format fmt (int %)) (range n-keys)))
        codes (long-array n)]
    (dotimes [i n]
      (aset codes i (long (.nextInt rng n-keys))))
    {:type :int64 :data codes :dict dict :dict-type :string}))

(defn- generate-h2o-data
  "Generate H2O db-benchmark data: id1-id6 + v1-v3.
   String columns are dict-encoded directly (no intermediate String array)
   so this scales to 1B+ rows without OOM."
  [^long n]
  (println "  Generating H2O db-benchmark data...")
  (let [rng (java.util.Random. 42)
        n-id1 100
        n-id2 100
        n-id3 (max 100 (quot n 100))
        ;; String columns — build dict + codes directly (no String[] of n elements)
        id1 (make-dict-column n-id1 "id%03d" rng n)
        id2 (make-dict-column n-id2 "id%03d" rng n)
        id3 (make-dict-column n-id3 "id%06d" rng n)
        ;; Integer columns
        id4 (long-array n)
        id5 (long-array n)
        id6 (long-array n)
        v1  (long-array n)
        v2  (long-array n)
        v3  (double-array n)]
    (dotimes [i n]
      (aset id4 i (+ 1 (.nextInt rng 100)))
      (aset id5 i (+ 1 (.nextInt rng 100)))
      (aset id6 i (+ 1 (.nextInt rng (max 100 (quot n 100)))))
      (aset v1 i (+ 1 (.nextInt rng 5)))
      (aset v2 i (+ 1 (.nextInt rng 15)))
      (aset v3 i (* 100.0 (.nextDouble rng))))
    {:id1 id1 :id2 id2 :id3 id3
     :id4 id4 :id5 id5 :id6 id6
     :v1 v1 :v2 v2 :v3 v3
     :n n}))

(defn- duckdb-setup-h2o
  "Create DuckDB h2o table via JDBC from shared CSV."
  [^Connection conn arrays ^long n]
  (let [csv-path "/tmp/olap_bench_h2o.csv"]
    (write-arrays-csv csv-path
                      [[:id1 (:id1 arrays)] [:id2 (:id2 arrays)] [:id3 (:id3 arrays)]
                       [:id4 (:id4 arrays)] [:id5 (:id5 arrays)] [:id6 (:id6 arrays)]
                       [:v1 (:v1 arrays)] [:v2 (:v2 arrays)] [:v3 (:v3 arrays)]]
                      n)
    (duckdb-load-csv conn "h2o" csv-path)))

;; ============================================================================
;; Tier 2: H2O Q1 — GROUP BY id1 (string), SUM(v1)
;; ============================================================================

(defn- bench-h2o-q1
  "H2O Q1: GROUP BY id1 (100 string groups), SUM(v1)."
  [h2o ^Connection conn]
  (println "\nH2O-Q1: GROUP BY id1 (string), SUM(v1)")
  (let [q {:from {:id1 (:id1 h2o) :v1 (:v1 h2o)}
           :group [:id1]
           :agg [[:sum :v1]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id1, SUM(v1) FROM h2o GROUP BY id1"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q1" conn
                      "SELECT id1, SUM(v1) AS sum FROM h2o GROUP BY id1" v [:id1])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q2 — GROUP BY id1, id2 (2 string cols), SUM(v1)
;; ============================================================================

(defn- bench-h2o-q2
  "H2O Q2: GROUP BY id1, id2 (100x100 string groups), SUM(v1)."
  [h2o ^Connection conn]
  (println "\nH2O-Q2: GROUP BY id1, id2 (string), SUM(v1)")
  (let [q {:from {:id1 (:id1 h2o) :id2 (:id2 h2o) :v1 (:v1 h2o)}
           :group [:id1 :id2]
           :agg [[:sum :v1]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id1, id2, SUM(v1) FROM h2o GROUP BY id1, id2"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q2" conn
                      "SELECT id1, id2, SUM(v1) AS sum FROM h2o GROUP BY id1, id2" v [:id1 :id2])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q3 — GROUP BY id3 (high-card string), SUM(v1), AVG(v3)
;; ============================================================================

(defn- bench-h2o-q3
  "H2O Q3: GROUP BY id3 (high-card string), SUM(v1), AVG(v3)."
  [h2o ^Connection conn]
  (println "\nH2O-Q3: GROUP BY id3 (high-card string), SUM(v1)+AVG(v3)")
  (let [q {:from {:id3 (:id3 h2o) :v1 (:v1 h2o) :v3 (:v3 h2o)}
           :group [:id3]
           :agg [[:sum :v1] [:avg :v3]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id3, SUM(v1), AVG(v3) FROM h2o GROUP BY id3"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q3" conn
                      "SELECT id3, SUM(v1) AS sum, AVG(v3) AS avg FROM h2o GROUP BY id3" v [:id3])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q4 — GROUP BY id4 (int, 100 keys), AVG(v1,v2,v3)
;; ============================================================================

(defn- bench-h2o-q4
  "H2O Q4: GROUP BY id4 (100 int keys), AVG(v1), AVG(v2), AVG(v3)."
  [h2o ^Connection conn]
  (println "\nH2O-Q4: GROUP BY id4 (int 100), AVG(v1)+AVG(v2)+AVG(v3)")
  (let [q {:from {:id4 (:id4 h2o) :v1 (:v1 h2o) :v2 (:v2 h2o) :v3 (:v3 h2o)}
           :group [:id4]
           :agg [[:avg :v1] [:avg :v2] [:avg :v3]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM h2o GROUP BY id4"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q4" conn
                      "SELECT id4, AVG(v1) AS avg_v1, AVG(v2) AS avg_v2, AVG(v3) AS avg_v3 FROM h2o GROUP BY id4"
                      v [:id4])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q5 — GROUP BY id6 (high-card int), SUM(v1,v2,v3)
;; ============================================================================

(defn- bench-h2o-q5
  "H2O Q5: GROUP BY id6 (high-card int), SUM(v1), SUM(v2), SUM(v3)."
  [h2o ^Connection conn]
  (println "\nH2O-Q5: GROUP BY id6 (high-card int), SUM(v1)+SUM(v2)+SUM(v3)")
  (let [q {:from {:id6 (:id6 h2o) :v1 (:v1 h2o) :v2 (:v2 h2o) :v3 (:v3 h2o)}
           :group [:id6]
           :agg [[:sum :v1] [:sum :v2] [:sum :v3]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM h2o GROUP BY id6"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q5" conn
                      "SELECT id6, SUM(v1) AS sum_v1, SUM(v2) AS sum_v2, SUM(v3) AS sum_v3 FROM h2o GROUP BY id6"
                      v [:id6])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q7 — GROUP BY id3 (string), MAX(v1)-MIN(v2)
;; ============================================================================

(defn- bench-h2o-q7
  "H2O Q7: GROUP BY id3 (high-card string), MAX(v1)-MIN(v2) per group."
  [h2o ^Connection conn]
  (println "\nH2O-Q7: GROUP BY id3 (string), MAX(v1)-MIN(v2)")
  (let [q {:from {:id3 (:id3 h2o) :v1 (:v1 h2o) :v2 (:v2 h2o)}
           :group [:id3]
           :agg [[:max :v1] [:min :v2]]}
        r (bench #(let [res (q/q q)]
                    ;; Post-agg expression: MAX(v1) - MIN(v2)
                    (mapv (fn [row] (assoc row :range_v1_v2 (- (double (:max row)) (double (:min row))))) res)))
        r-1t (bench-1t #(let [res (q/q q)]
                          (mapv (fn [row] (assoc row :range_v1_v2 (- (double (:max row)) (double (:min row))))) res)))
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM h2o GROUP BY id3"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      ;; Validate MAX and MIN separately (Stratum returns :max/:min, not combined expression)
      (validate-query "H2O-Q7" conn
                      "SELECT id3, MAX(v1) AS max, MIN(v2) AS min FROM h2o GROUP BY id3" v [:id3])
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q10 — GROUP BY id1..id6 (6 cols), SUM(v3), COUNT(*)
;; ============================================================================

(defn- bench-h2o-q10
  "H2O Q10: GROUP BY id1..id6 (6 cols), SUM(v3), COUNT(*).
   Benchmarks both row-based (Clojure maps) and columnar (arrays) output."
  [h2o ^Connection conn]
  (println "\nH2O-Q10: GROUP BY id1..id6 (6 cols), SUM(v3)+COUNT(*)")
  (let [q {:from {:id1 (:id1 h2o) :id2 (:id2 h2o) :id3 (:id3 h2o)
                  :id4 (:id4 h2o) :id5 (:id5 h2o) :id6 (:id6 h2o)
                  :v3 (:v3 h2o)}
           :group [:id1 :id2 :id3 :id4 :id5 :id6]
           :agg [[:sum :v3] [:count]]}
        qc (assoc q :result :columns)
        ;; Columnar result format: returns arrays, no per-row map creation
        rc (bench #(q/q qc) :warmup 10)
        rc-1t (bench-1t #(q/q qc) :warmup 10)
        v (q/q qc)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median rc-1t)) (:n-rows v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median rc))))
    (let [sql "SELECT id1,id2,id3,id4,id5,id6, SUM(v3), COUNT(*) FROM h2o GROUP BY id1,id2,id3,id4,id5,id6"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      ;; Q10 uses columnar result (6M groups) — validate row count only
      (try
        (let [duck-r (duckdb-query-results conn "SELECT COUNT(*) AS cnt FROM (SELECT id1,id2,id3,id4,id5,id6 FROM h2o GROUP BY id1,id2,id3,id4,id5,id6)")]
          (let [duck-cnt (first (first (:rows duck-r)))]
            (if (= (:n-rows v) duck-cnt)
              (println (format "  ✓ H2O-Q10: PASS (row count %d matches)" duck-cnt))
              (println (format "  ✗ H2O-Q10: FAIL (Stratum=%d DuckDB=%d rows)" (:n-rows v) duck-cnt)))))
        (catch Exception e (println (format "  ? H2O-Q10: ERROR (%s)" (.getMessage e)))))
      {:stratum-1t (:median rc-1t) :stratum (:median rc)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q6 — GROUP BY id4, id5, MEDIAN(v3) + STDDEV(v3)
;; ============================================================================

(defn- bench-h2o-q6
  "H2O Q6: GROUP BY id4, id5, MEDIAN(v3), STDDEV(v3).
   MEDIAN uses scalar fallback, STDDEV decomposes to SUM/COUNT."
  [h2o ^Connection conn]
  (println "\nH2O-Q6: GROUP BY id4,id5, STDDEV(v3) (compound agg)")
  ;; STDDEV only — median is too slow for large N benchmarking (scalar per-group ArrayList)
  (let [q {:from {:id4 (:id4 h2o) :id5 (:id5 h2o) :v3 (:v3 h2o)}
           :group [:id4 :id5]
           :agg [[:stddev :v3]]}
        r (bench #(q/q q) :warmup 20)
        r-1t (bench-1t #(q/q q) :warmup 20)
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id4, id5, STDDEV(v3) FROM h2o GROUP BY id4, id5"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q6" conn
                      "SELECT id4, id5, STDDEV_SAMP(v3) AS stddev FROM h2o GROUP BY id4, id5"
                      v [:id4 :id5] :tolerance 1e-4)
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q9 — GROUP BY id2, id4, CORR(v1, v2)
;; ============================================================================

(defn- bench-h2o-q9
  "H2O Q9: GROUP BY id2, id4, CORR(v1, v2), then POW(r, 2).
   CORR decomposes to 5 SUM + COUNT."
  [h2o ^Connection conn]
  (println "\nH2O-Q9: GROUP BY id2,id4, CORR(v1,v2) (compound agg)")
  (let [q {:from {:id2 (:id2 h2o) :id4 (:id4 h2o) :v1 (:v1 h2o) :v2 (:v2 h2o)}
           :group [:id2 :id4]
           :agg [[:corr :v1 :v2]]}
        r (bench #(q/q q) :warmup 20)
        r-1t (bench-1t #(q/q q) :warmup 20)
        v (q/q q)]
    (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id2, id4, CORR(v1, v2) FROM h2o GROUP BY id2, id4"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "H2O-Q9" conn
                      "SELECT id2, id4, CORR(v1, v2) AS corr FROM h2o GROUP BY id2, id4"
                      v [:id2 :id4] :tolerance 1e-4)
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Q8 — Top-2 v3 per id6 (window function)
;; ============================================================================

(defn- bench-h2o-q8
  "H2O Q8: Top-2 v3 per id6 using ROW_NUMBER window function.
   SELECT id6, v3 FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn
                         FROM x WHERE v3 IS NOT NULL) WHERE rn <= 2"
  [h2o ^Connection conn]
  (println "\nH2O-Q8: Top-2 v3 per id6 (ROW_NUMBER window)")
  (let [q {:from {:id6 (:id6 h2o) :v3 (:v3 h2o)}
           :where [[:is-not-null :v3]]
           :window [{:op :row-number :partition-by [:id6] :order-by [[:v3 :desc]] :as :rn}]
           :having [[:< :rn 3]]}
        r (bench #(q/q q) :warmup 5 :iters 10)
        r-1t (bench-1t #(q/q q) :warmup 5 :iters 10)
        v (q/q q)
        n-rows (count v)]
    (println (format "  Stratum (1T): %s (%d rows)" (fmt-ms (:median r-1t)) n-rows))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT id6, v3 FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn FROM h2o WHERE v3 IS NOT NULL) WHERE rn <= 2"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median rd-1t))))
      ;; Validate: compare row count of top-2 per partition
      (let [duck-r (duckdb-query-results conn
                     "SELECT COUNT(*) AS cnt FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn FROM h2o WHERE v3 IS NOT NULL) WHERE rn <= 2")
            duck-cnt (long (first (first (:rows duck-r))))]
        (if (= n-rows duck-cnt)
          (println (format "  \u2713 H2O-Q8: PASS (%d rows match)" n-rows))
          (println (format "  \u2717 H2O-Q8: FAIL (Stratum %d vs DuckDB %d rows)" n-rows duck-cnt))))
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 2: H2O Join Queries (J1-J3)
;; ============================================================================

(defn- generate-h2o-join-dims
  "Generate H2O db-benchmark join dimension tables.
   small: N/1M rows (keyed on id1), medium: N/1K rows (keyed on id1,id2,id4,id5)."
  [^long n]
  (println "  Generating H2O join dimension tables...")
  (let [rng (java.util.Random. 43)
        n-small (max 100 (quot n 1000000))
        n-medium (max 100 (quot n 1000))
        ;; Small: id1 (key), v2
        small-id1 (long-array n-small)
        small-v2 (double-array n-small)
        _ (dotimes [i n-small]
            (aset small-id1 i (long (inc i)))
            (aset small-v2 i (* 100.0 (.nextDouble rng))))
        ;; Medium: id1, id2, id4, id5 (keys), v2
        medium-id1 (long-array n-medium)
        medium-id2 (long-array n-medium)
        medium-id4 (long-array n-medium)
        medium-id5 (long-array n-medium)
        medium-v2 (double-array n-medium)
        _ (dotimes [i n-medium]
            (aset medium-id1 i (long (inc (mod i n-small))))
            (aset medium-id2 i (long (inc i)))
            (aset medium-id4 i (long (inc (mod i 100))))
            (aset medium-id5 i (long (inc (mod i 100))))
            (aset medium-v2 i (* 100.0 (.nextDouble rng))))]
    {:small {:id1 small-id1 :v2 small-v2 :n n-small}
     :medium {:id1 medium-id1 :id2 medium-id2 :id4 medium-id4 :id5 medium-id5
              :v2 medium-v2 :n n-medium}}))

(defn- duckdb-setup-h2o-joins
  "Create DuckDB dimension tables for H2O join queries."
  [^Connection conn dims]
  (let [small (:small dims)
        medium (:medium dims)]
    ;; Small table
    (let [csv-path "/tmp/olap_bench_h2o_small.csv"]
      (write-arrays-csv csv-path [[:id1 (:id1 small)] [:v2 (:v2 small)]] (:n small))
      (duckdb-load-csv conn "h2o_small" csv-path))
    ;; Medium table
    (let [csv-path "/tmp/olap_bench_h2o_medium.csv"]
      (write-arrays-csv csv-path [[:id1 (:id1 medium)] [:id2 (:id2 medium)]
                                  [:id4 (:id4 medium)] [:id5 (:id5 medium)]
                                  [:v2 (:v2 medium)]] (:n medium))
      (duckdb-load-csv conn "h2o_medium" csv-path))))

(defn- bench-h2o-j1
  "H2O J1: x JOIN small USING (id4) — tiny dimension join, SUM(v1), SUM(v2)."
  [h2o dims ^Connection conn]
  (println "\nH2O-J1: x JOIN small ON id4=id1, SUM(v1), SUM(v2)")
  (let [small (:small dims)
        q {:from {:id4 (:id4 h2o) :v1 (:v1 h2o)}
           :join [{:with {:id1 (:id1 small) :v2 (:v2 small)}
                   :on [:= :id4 :id1]
                   :type :inner}]
           :agg [[:sum :v1] [:sum :v2]]}
        r-1t (bench-1t #(q/q q))
        r (bench #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT SUM(x.v1), SUM(s.v2) FROM h2o x JOIN h2o_small s ON x.id4 = s.id1"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median rd-1t))))
      (validate-query "H2O-J1" conn
        "SELECT SUM(x.v1) AS sum_v1, SUM(s.v2) AS sum_v2 FROM h2o x JOIN h2o_small s ON x.id4 = s.id1"
        v [] :tolerance 1e-4)
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

(defn- bench-h2o-j2
  "H2O J2: x JOIN medium ON (id4, id5) — medium dimension, inner, SUM(v1), SUM(v2)."
  [h2o dims ^Connection conn]
  (println "\nH2O-J2: x JOIN medium ON id4=mid4, id5=mid5, SUM(v1), SUM(v2)")
  (let [medium (:medium dims)
        q {:from {:id4 (:id4 h2o) :id5 (:id5 h2o) :v1 (:v1 h2o)}
           :join [{:with {:mid4 (:id4 medium) :mid5 (:id5 medium) :v2 (:v2 medium)}
                   :on [[:= :id4 :mid4] [:= :id5 :mid5]]
                   :type :inner}]
           :agg [[:sum :v1] [:sum :v2]]}
        r-1t (bench-1t #(q/q q))
        r (bench #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT SUM(x.v1), SUM(m.v2) FROM h2o x JOIN h2o_medium m ON x.id4 = m.id4 AND x.id5 = m.id5"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median rd-1t))))
      (validate-query "H2O-J2" conn
        "SELECT SUM(x.v1) AS sum_v1, SUM(m.v2) AS sum_v2 FROM h2o x JOIN h2o_medium m ON x.id4 = m.id4 AND x.id5 = m.id5"
        v [] :tolerance 1e-4)
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

(defn- bench-h2o-j3
  "H2O J3: x LEFT JOIN medium ON (id4, id5) — medium dimension, left join."
  [h2o dims ^Connection conn]
  (println "\nH2O-J3: x LEFT JOIN medium ON id4=mid4, id5=mid5, SUM(v1), SUM(v2)")
  (let [medium (:medium dims)
        q {:from {:id4 (:id4 h2o) :id5 (:id5 h2o) :v1 (:v1 h2o)}
           :join [{:with {:mid4 (:id4 medium) :mid5 (:id5 medium) :v2 (:v2 medium)}
                   :on [[:= :id4 :mid4] [:= :id5 :mid5]]
                   :type :left}]
           :agg [[:sum :v1] [:sum :v2]]}
        r-1t (bench-1t #(q/q q))
        r (bench #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT SUM(x.v1), SUM(m.v2) FROM h2o x LEFT JOIN h2o_medium m ON x.id4 = m.id4 AND x.id5 = m.id5"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median rd-1t))))
      (validate-query "H2O-J3" conn
        "SELECT SUM(x.v1) AS sum_v1, SUM(m.v2) AS sum_v2 FROM h2o x LEFT JOIN h2o_medium m ON x.id4 = m.id4 AND x.id5 = m.id5"
        v [] :tolerance 1e-4)
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 3: SSB Q1.2 — Tighter Filter + Sum-Product
;; ============================================================================

(defn- bench-ssb-q12
  "SSB Q1.2: tighter discount and quantity ranges."
  [data ^Connection conn]
  (println "\nSSB-Q1.2: Tighter Filter + Sum-Product")
  (println "    SUM(price*discount) WHERE shipdate BETWEEN, disc 0.04..0.06, qty 26..35")
  (let [q {:from {:shipdate (:shipdate data) :discount (:discount data)
                  :quantity (:quantity data) :price (:price data)}
           :where [[:between :shipdate 8766 9131]
                   [:between :discount 0.04 0.06]
                   [:between :quantity 26 35]]
           :agg [[:sum [:* :price :discount]]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
    (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
    (let [sql "SELECT SUM(price*discount) FROM lineitem WHERE shipdate>=8766 AND shipdate<9131 AND discount>=0.04 AND discount<=0.06 AND quantity>=26 AND quantity<35"
          rd-1t (duckdb-bench conn sql :threads 1)
          rd (duckdb-bench conn sql)]
      (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
      (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
      (validate-query "SSB-Q12" conn
                       "SELECT SUM(price * discount) AS \"sum-product\" FROM lineitem WHERE shipdate >= 8766 AND shipdate <= 9131 AND discount >= 0.04 AND discount <= 0.06 AND quantity >= 26 AND quantity <= 35"
                       v [] )
      {:stratum-1t (:median r-1t) :stratum (:median r)
       :duckdb-1t (:median rd-1t) :duckdb (:median rd)})))

;; ============================================================================
;; Tier 3: ClickBench Numeric Subset
;; ============================================================================

(defn- generate-clickbench-data
  "Load ClickBench data from data/cb/hits.csv.
   Run bin/download-data first to generate the CSV."
  [^long n]
  (if-let [data (load-csv-columns "data/cb/hits.csv"
                   [{:name "AdvEngineID"     :key :adv-engine-id    :type :long}
                    {:name "ResolutionWidth" :key :resolution-width :type :long}
                    {:name "UserID"          :key :user-id          :type :long}
                    {:name "SearchEngineID"  :key :search-engine-id :type :long}
                    {:name "CounterID"       :key :counter-id       :type :long}
                    {:name "IsRefresh"       :key :is-refresh       :type :long}
                    {:name "RegionID"        :key :region-id        :type :long}
                    {:name "EventTime"       :key :event-time       :type :long}
                    {:name "URL"             :key :url              :type :string}
                    {:name "SearchPhrase"   :key :search-phrase    :type :string}
                    {:name "DontCountHits"  :key :dont-count-hits  :type :long}]
                   n)]
    (do (println (format "  Loaded ClickBench data from CSV (%d rows)" (:n data)))
        ;; Encode string columns for dictionary-encoded string operations
        (assoc data
               :url (q/encode-column ^"[Ljava.lang.String;" (:url data))
               :search-phrase (q/encode-column ^"[Ljava.lang.String;" (:search-phrase data))))
    (throw (ex-info "ClickBench data not found. Run: bin/download-data"
                    {:path "data/cb/hits.csv"}))))

(defn- duckdb-setup-clickbench
  "Create DuckDB hits table via JDBC."
  [^Connection conn ^long n]
  (let [csv-path "data/cb/hits.csv"]
    (when-not (.exists (io/file csv-path))
      (throw (ex-info "ClickBench data not found. Run: bin/download-data"
                      {:path csv-path})))
    ;; force_not_null: DuckDB read_csv_auto treats "" as NULL, but our CSV parser reads
    ;; "" as empty string. Use force_not_null to match Stratum's behavior.
    (duckdb-jdbc-exec! conn (str "CREATE TABLE hits AS SELECT * FROM read_csv('" csv-path
                                 "', header=true, max_line_size=16777216, sample_size=100000"
                                 ", force_not_null=['URL','SearchPhrase']"
                                 ") LIMIT " n))))

(defn- bench-clickbench
  "Run ClickBench numeric queries. Returns map of {keyword result-map}."
  [cb ^Connection conn run-q?]
  (println "\n--- ClickBench Numeric Subset ---")
  (let [res (atom {})]

    (when (run-q? "q0")
      ;; CB-Q0: COUNT(*)
      (println "\nCB-Q0: COUNT(*)")
      (let [q {:from {:v (:adv-engine-id cb)} :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))))

    (gc!)
    (when (run-q? "q1")
      ;; CB-Q1: COUNT(*) WHERE AdvEngineID != 0
      (println "\nCB-Q1: COUNT(*) WHERE AdvEngineID != 0")
      (let [q {:from {:ae (:adv-engine-id cb)}
               :where [[:!= :ae 0]]
               :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(*) FROM hits WHERE AdvEngineID != 0"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q1 {:stratum-1t (:median r-1t) :stratum (:median r)
                                   :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q1" conn
          "SELECT COUNT(*) AS count FROM hits WHERE AdvEngineID != 0"
          v [])))

    (gc!)
    (when (run-q? "q2")
      ;; CB-Q2: SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)
      (println "\nCB-Q2: SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)")
      (let [q {:from {:ae (:adv-engine-id cb) :rw (:resolution-width cb)}
               :agg [[:sum :ae] [:count] [:avg :rw]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q2 {:stratum-1t (:median r-1t) :stratum (:median r)
                                   :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q2" conn
          "SELECT SUM(AdvEngineID) AS sum, COUNT(*) AS count, AVG(ResolutionWidth) AS avg FROM hits"
          v [] :tolerance 1e-4)))

    (gc!)
    (when (run-q? "q7")
      ;; CB-Q7: COUNT(*) WHERE IsRefresh = 1 AND ResolutionWidth > 1000
      (println "\nCB-Q7: COUNT(*) WHERE IsRefresh=1 AND ResolutionWidth>1000")
      (let [q {:from {:ir (:is-refresh cb) :rw (:resolution-width cb)}
               :where [[:= :ir 1] [:> :rw 1000]]
               :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(*) FROM hits WHERE IsRefresh = 1 AND ResolutionWidth > 1000"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q7 {:stratum-1t (:median r-1t) :stratum (:median r)
                                   :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q7" conn
          "SELECT COUNT(*) AS count FROM hits WHERE IsRefresh = 1 AND ResolutionWidth > 1000"
          v [])))

    (gc!)
    (when (run-q? "grp-se")
      ;; CB-GRP-SE: GROUP BY SearchEngineID, COUNT(*)
      (println "\nCB-GRP-SE: GROUP BY SearchEngineID, COUNT(*)")
      (let [q {:from {:se (:search-engine-id cb)}
               :group [:se]
               :agg [[:count]]
               :order [[:count :desc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT SearchEngineID, COUNT(*) FROM hits GROUP BY SearchEngineID ORDER BY COUNT(*) DESC"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-grp-se {:stratum-1t (:median r-1t) :stratum (:median r)
                                       :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-GRP-SE" conn
          "SELECT SearchEngineID AS se, COUNT(*) AS count FROM hits GROUP BY SearchEngineID ORDER BY count DESC"
          v [:se])))

    (gc!)
    (when (run-q? "grp-reg")
      ;; CB-GRP-REG: GROUP BY RegionID, SUM+COUNT
      (println "\nCB-GRP-REG: GROUP BY RegionID, SUM(AdvEngineID)+COUNT(*)")
      (let [q {:from {:ri (:region-id cb) :ae (:adv-engine-id cb)}
               :group [:ri]
               :agg [[:sum :ae] [:count]]
               :order [[:count :desc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT RegionID, SUM(AdvEngineID), COUNT(*) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-grp-reg {:stratum-1t (:median r-1t) :stratum (:median r)
                                        :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-GRP-REG" conn
          "SELECT RegionID AS ri, SUM(AdvEngineID) AS sum, COUNT(*) AS count FROM hits GROUP BY RegionID ORDER BY count DESC"
          v [:ri])))

    (gc!)
    (when (run-q? "q15")
      ;; CB-Q15: GROUP BY UserID, COUNT(*) (high-cardinality)
      (println "\nCB-Q15: GROUP BY UserID, COUNT(*) (high-card)")
      (let [q {:from {:uid (:user-id cb)}
               :group [:uid]
               :agg [[:count]]
               :result :columns}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (:n-rows v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q15 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (try
          (let [duck-r (duckdb-query-results conn "SELECT COUNT(*) AS cnt FROM (SELECT UserID FROM hits GROUP BY UserID)")]
            (let [duck-cnt (first (first (:rows duck-r)))]
              (if (= (:n-rows v) duck-cnt)
                (println (format "  ✓ CB-Q15: PASS (row count %d matches)" duck-cnt))
                (println (format "  ✗ CB-Q15: FAIL (Stratum=%d DuckDB=%d rows)" (:n-rows v) duck-cnt)))))
          (catch Exception e (println (format "  ? CB-Q15: ERROR (%s)" (.getMessage e)))))))

    (gc!)
    (when (run-q? "q5")
      ;; CB-Q5: COUNT(DISTINCT UserID)
      (println "\nCB-Q5: COUNT(DISTINCT UserID)")
      (let [q {:from {:uid (:user-id cb)}
               :agg [[:count-distinct :uid]]}
            r-1t (bench-1t #(q/q q) :warmup 10)
            r (bench #(q/q q) :warmup 10)
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (distinct=%d)" (fmt-ms (:median r-1t))
                         (long (:count-distinct (first v)))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(DISTINCT UserID) FROM hits"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q5 {:stratum-1t (:median r-1t) :stratum (:median r)
                                   :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q5" conn
          "SELECT COUNT(DISTINCT UserID) AS \"count-distinct\" FROM hits"
          v [])))

    (gc!)
    (when (run-q? "cd-grp")
      ;; CB-CD-GRP: COUNT(DISTINCT AdvEngineID) GROUP BY RegionID
      (println "\nCB-CD-GRP: COUNT(DISTINCT AdvEngineID) GROUP BY RegionID")
      (let [q {:from {:ri (:region-id cb) :ae (:adv-engine-id cb)}
               :group [:ri]
               :agg [[:count-distinct :ae]]
               :order [[:count-distinct :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q) :warmup 10)
            r (bench #(q/q q) :warmup 10)
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (top-cd=%d, groups=%d)" (fmt-ms (:median r-1t))
                         (long (:count-distinct (first v))) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT RegionID, COUNT(DISTINCT AdvEngineID) AS cd FROM hits GROUP BY RegionID ORDER BY cd DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-cd-grp {:stratum-1t (:median r-1t) :stratum (:median r)
                                       :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        ;; Validate all groups (no LIMIT) to avoid tie-breaking differences
        (let [v-all (q/q {:from {:ri (:region-id cb) :ae (:adv-engine-id cb)}
                                :group [:ri]
                                :agg [[:count-distinct :ae]]})]
          (validate-query "CB-CD-GRP" conn
            "SELECT RegionID AS ri, COUNT(DISTINCT AdvEngineID) AS \"count-distinct\" FROM hits GROUP BY RegionID"
            v-all [:ri]))))

    (gc!)
    (when (run-q? "q19")
      ;; CB-Q19: EXTRACT minute from EventTime (expression in GROUP BY)
      (println "\nCB-Q19: GROUP BY EXTRACT(minute FROM EventTime), COUNT(*)")
      (let [q {:from {:et (:event-time cb)}
               :group [[:minute :et]]
               :agg [[:count]]
               :order [[:count :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT EXTRACT(minute FROM to_timestamp(EventTime))::INT AS m, COUNT(*) FROM hits GROUP BY m ORDER BY COUNT(*) DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q19 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        ;; Validate all 60 groups (no LIMIT) to avoid tie-breaking differences
        ;; Use // (integer division) instead of to_timestamp (no ICU extension needed)
        (let [v-all (q/q {:from {:et (:event-time cb)}
                                :group [[:minute :et]]
                                :agg [[:count]]})]
          (validate-query "CB-Q19" conn
            "SELECT (EventTime % 3600) // 60 AS minute, COUNT(*) AS count FROM hits GROUP BY minute"
            v-all [:minute]))))

    (gc!)
    (when (run-q? "like1")
      ;; CB-LIKE1: COUNT(*) WHERE URL LIKE '%example.com/page%'
      (println "\nCB-LIKE1: COUNT(*) WHERE URL LIKE '%example.com/page%'")
      (let [q {:from {:url (:url cb)}
               :where [[:like :url "%example.com/page%"]]
               :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (count=%d)" (fmt-ms (:median r-1t))
                         (long (:count (first v)))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(*) FROM hits WHERE URL LIKE '%example.com/page%'"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-like1 {:stratum-1t (:median r-1t) :stratum (:median r)
                                      :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-LIKE1" conn
          "SELECT COUNT(*) AS count FROM hits WHERE URL LIKE '%example.com/page%'"
          v [])))

    (gc!)
    (when (run-q? "like2")
      ;; CB-LIKE2: COUNT(*) WHERE URL LIKE '%search%'
      (println "\nCB-LIKE2: COUNT(*) WHERE URL LIKE '%search%'")
      (let [q {:from {:url (:url cb)}
               :where [[:like :url "%search%"]]
               :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (count=%d)" (fmt-ms (:median r-1t))
                         (long (:count (first v)))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(*) FROM hits WHERE URL LIKE '%search%'"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-like2 {:stratum-1t (:median r-1t) :stratum (:median r)
                                      :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-LIKE2" conn
          "SELECT COUNT(*) AS count FROM hits WHERE URL LIKE '%search%'"
          v [])))

    (gc!)
    (when (run-q? "like3")
      ;; CB-LIKE3: GROUP BY SearchEngineID WHERE URL LIKE '%shop%', COUNT(*)
      (println "\nCB-LIKE3: GROUP BY SearchEngineID WHERE URL LIKE '%shop%', COUNT(*)")
      (let [q {:from {:se (:search-engine-id cb) :url (:url cb)}
               :where [[:like :url "%shop%"]]
               :group [:se]
               :agg [[:count]]
               :order [[:count :desc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT SearchEngineID, COUNT(*) FROM hits WHERE URL LIKE '%shop%' GROUP BY SearchEngineID ORDER BY COUNT(*) DESC"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-like3 {:stratum-1t (:median r-1t) :stratum (:median r)
                                      :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-LIKE3" conn
          "SELECT SearchEngineID AS se, COUNT(*) AS count FROM hits WHERE URL LIKE '%shop%' GROUP BY SearchEngineID ORDER BY count DESC"
          v [:se])))

    (gc!)
    (when (run-q? "q28")
      ;; CB-Q28: AVG(LENGTH(URL))
      (println "\nCB-Q28: AVG(LENGTH(URL))")
      (let [q {:from {:url (:url cb)}
               :agg [[:avg [:length :url]]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (avg_len=%.1f)" (fmt-ms (:median r-1t))
                         (double (:avg (first v)))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT AVG(LENGTH(URL)) FROM hits"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q28 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q28" conn "SELECT AVG(LENGTH(URL)) AS avg FROM hits" v [])))

    (gc!)
    (when (run-q? "q43")
      ;; CB-Q43: GROUP BY DATE_TRUNC(minute, EventTime), COUNT(*)
      (println "\nCB-Q43: GROUP BY DATE_TRUNC(minute, EventTime), COUNT(*)")
      (let [q {:from {:et (:event-time cb)}
               :group [[:date-trunc :minute :et]]
               :agg [[:count]]
               :order [[:count :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT (EventTime - EventTime % 60) AS m, COUNT(*) FROM hits GROUP BY m ORDER BY COUNT(*) DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q43 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        ;; Validate all groups (no LIMIT) to avoid tie-breaking differences
        (let [v-all (q/q {:from {:et (:event-time cb)}
                                :group [[:date-trunc :minute :et]]
                                :agg [[:count]]})]
          (validate-query "CB-Q43" conn
            "SELECT (EventTime - EventTime % 60) AS \"__grp_0\", COUNT(*) AS count FROM hits GROUP BY \"__grp_0\""
            v-all [:__grp_0]))))

    (gc!)
    (when (run-q? "q3")
      (println "\nCB-Q3: AVG(UserID)")
      (let [q {:from {:uid (:user-id cb)} :agg [[:avg :uid]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT AVG(UserID) FROM hits"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q3 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q3" conn "SELECT AVG(UserID) AS avg FROM hits" v [] :tolerance 1e-4)))

    (gc!)
    (when (run-q? "q6")
      (println "\nCB-Q6: MIN(EventTime), MAX(EventTime)")
      (let [q {:from {:et (:event-time cb)} :agg [[:min :et] [:max :et]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s" (fmt-ms (:median r-1t))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT MIN(EventTime), MAX(EventTime) FROM hits"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q6 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q6" conn "SELECT MIN(EventTime) AS min, MAX(EventTime) AS max FROM hits" v [])))

    (gc!)
    (when (run-q? "q8")
      (println "\nCB-Q8: GROUP BY RegionID, COUNT(DISTINCT UserID) ORDER BY u DESC LIMIT 10")
      (let [q {:from {:ri (:region-id cb) :uid (:user-id cb)}
               :group [:ri]
               :agg [[:as [:count-distinct :uid] :u]]
               :order [[:u :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q8 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q8" conn
          "SELECT RegionID AS ri, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"
          v [:ri])))

    (gc!)
    (when (run-q? "q9")
      (println "\nCB-Q9: GROUP BY RegionID, SUM+COUNT+AVG+COUNT-DISTINCT")
      (let [q {:from {:ri (:region-id cb) :ae (:adv-engine-id cb) :rw (:resolution-width cb) :uid (:user-id cb)}
               :group [:ri]
               :agg [[:sum :ae] [:count] [:avg :rw] [:count-distinct :uid]]
               :order [[:count :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q9 {:stratum-1t (:median r-1t) :stratum (:median r)
                                    :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q9" conn
          "SELECT RegionID AS ri, SUM(AdvEngineID) AS sum, COUNT(*) AS count, AVG(ResolutionWidth) AS avg, COUNT(DISTINCT UserID) AS count_distinct FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC LIMIT 10"
          v [:ri] :tolerance 1e-4)))

    (gc!)
    (when (run-q? "q20")
      (println "\nCB-Q20: COUNT(*) WHERE URL LIKE '%google%'")
      (let [q {:from {:url (:url cb)}
               :where [[:like :url "%google%"]]
               :agg [[:count]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s  (count=%d)" (fmt-ms (:median r-1t)) (long (:count (first v)))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q20 {:stratum-1t (:median r-1t) :stratum (:median r)
                                     :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q20" conn "SELECT COUNT(*) AS count FROM hits WHERE URL LIKE '%google%'" v [])))

    (gc!)
    (when (run-q? "q12")
      (println "\nCB-Q12: GROUP BY SearchPhrase WHERE SearchPhrase<>'', COUNT(*) LIMIT 10")
      (let [q {:from {:sp (:search-phrase cb)}
               :where [[:!= :sp ""]]
               :group [:sp]
               :agg [[:count]]
               :order [[:count :desc]]
               :limit 10}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q12 {:stratum-1t (:median r-1t) :stratum (:median r)
                                     :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "CB-Q12" conn
          "SELECT SearchPhrase AS sp, COUNT(*) AS count FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY count DESC LIMIT 10"
          v [:sp])))

    (gc!)
    (when (run-q? "q27")
      (println "\nCB-Q27: GROUP BY CounterID, AVG(LENGTH(URL)) HAVING COUNT>100000")
      (let [q {:from {:cid (:counter-id cb) :url (:url cb)}
               :group [:cid]
               :agg [[:as [:avg [:length :url]] :l] [:count]]
               :having [[:> :count 100000]]
               :order [[:l :desc]]
               :limit 25}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT CounterID, AVG(LENGTH(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q27 {:stratum-1t (:median r-1t) :stratum (:median r)
                                     :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        ;; Validate COUNT column only (AVG(LENGTH) differs: Java String.length()=UTF-16, DuckDB=Unicode)
        (let [duck-r (duckdb-query-results conn
                       "SELECT CounterID AS cid, COUNT(*) AS count FROM hits GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY count DESC LIMIT 25")
              d-maps (mapv (fn [row] (zipmap (:columns duck-r) row)) (:rows duck-r))
              s-counts (sort-by :cid (mapv #(select-keys % [:cid :count]) v))
              d-counts (sort-by :cid (mapv #(select-keys % [:cid :count]) d-maps))]
          (if (= (count s-counts) (count d-counts))
            (let [all-match (every? true? (map (fn [s d] (and (= (long (:cid s)) (long (:cid d)))
                                                              (= (long (:count s)) (long (:count d)))))
                                               s-counts d-counts))]
              (if all-match
                (println (format "  \u2713 CB-Q27: PASS (%d rows match)" (count s-counts)))
                (println (format "  \u2717 CB-Q27: FAIL (count mismatch)"))))
            (println (format "  \u2717 CB-Q27: FAIL (Stratum %d vs DuckDB %d rows)" (count s-counts) (count d-counts)))))))

    (gc!)
    (when (run-q? "q33")
      (println "\nCB-Q33: GROUP BY URL, COUNT(*) ORDER BY c DESC LIMIT 10")
      (let [q {:from {:url (:url cb)}
               :group [:url]
               :agg [[:count]]
               :order [[:count :desc]]
               :limit 10
               :result :columns}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count (or (:url v) []))))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! res assoc :cb-q33 {:stratum-1t (:median r-1t) :stratum (:median r)
                                     :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        ;; Re-execute as row maps for validation (bench uses :result :columns)
        (let [v-rows (q/q (dissoc q :result))]
          (validate-query "CB-Q33" conn
            "SELECT URL AS url, COUNT(*) AS count FROM hits GROUP BY URL ORDER BY count DESC LIMIT 10"
            v-rows [:url]))))

    @res))

;; ============================================================================
;; Tier 4: NYC Taxi
;; ============================================================================

(defn- generate-nyc-taxi-data
  "Generate NYC Taxi-like data.
   Prefers real NYC Taxi data from data/nyc-taxi/taxi.csv if available."
  [^long n]
  (or (when-let [data (load-csv-columns "data/nyc-taxi/taxi.csv"
                        [{:name "trip_distance"   :key :trip-distance    :type :double}
                         {:name "fare_amount"     :key :fare-amount      :type :double}
                         {:name "tip_amount"      :key :tip-amount       :type :double}
                         {:name "total_amount"    :key :total-amount     :type :double}
                         {:name "passenger_count" :key :passenger-count  :type :long}
                         {:name "payment_type"    :key :payment-type     :type :long}
                         {:name "pickup_hour"     :key :pickup-hour      :type :long}
                         {:name "pickup_month"    :key :pickup-month     :type :long}
                         {:name "pickup_dow"      :key :pickup-dow       :type :long}]
                        n)]
        (println (format "  Loaded NYC Taxi data from CSV (%d rows)" (:n data)))
        data)
      (throw (ex-info "NYC Taxi data not found. Run: bin/download-data"
                      {:path "data/nyc-taxi/taxi.csv"}))))

(defn- duckdb-setup-nyc-taxi
  "Create DuckDB taxi table via JDBC."
  [^Connection conn ^long n]
  (let [csv-path "data/nyc-taxi/taxi.csv"]
    (when-not (.exists (io/file csv-path))
      (throw (ex-info "NYC Taxi data not found. Run: bin/download-data"
                      {:path csv-path})))
    (duckdb-jdbc-exec! conn (str "CREATE TABLE taxi AS SELECT * FROM read_csv_auto('" csv-path "') LIMIT " n))))

(defn- bench-nyc-taxi
  "Run NYC Taxi benchmark queries. Returns map of {:taxi-q1 {...} ...}."
  [taxi ^Connection conn run-q?]
  (println "\n--- NYC Taxi Benchmark ---")
  (let [results (atom {})]

    (when (run-q? "q1")
      ;; NYC-Q1: AVG(fare) GROUP BY payment_type
      (println "\nNYC-Q1: AVG(fare_amount) GROUP BY payment_type")
      (let [q {:from {:pt (:payment-type taxi) :fare (:fare-amount taxi)}
               :group [:pt]
               :agg [[:avg :fare]]
               :order [[:pt :asc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT payment_type, AVG(fare_amount) FROM taxi GROUP BY payment_type ORDER BY payment_type"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! results assoc :taxi-q1 {:stratum-1t (:median r-1t) :stratum (:median r)
                                          :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "NYC-Q1" conn
          "SELECT payment_type AS pt, AVG(fare_amount) AS avg FROM taxi GROUP BY payment_type ORDER BY payment_type"
          v [:pt] :tolerance 1e-4)))

    (gc!)
    (when (run-q? "q2")
      ;; NYC-Q2: AVG(tip) GROUP BY passenger_count (filter NULLs for consistent results)
      (println "\nNYC-Q2: AVG(tip_amount) GROUP BY passenger_count")
      (let [q {:from {:pc (:passenger-count taxi) :tip (:tip-amount taxi)}
               :where [[:is-not-null :pc]]
               :group [:pc]
               :agg [[:avg :tip]]
               :order [[:pc :asc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT passenger_count, AVG(tip_amount) FROM taxi WHERE passenger_count IS NOT NULL GROUP BY passenger_count ORDER BY passenger_count"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! results assoc :taxi-q2 {:stratum-1t (:median r-1t) :stratum (:median r)
                                          :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "NYC-Q2" conn
          "SELECT passenger_count AS pc, AVG(tip_amount) AS avg FROM taxi WHERE passenger_count IS NOT NULL GROUP BY passenger_count ORDER BY passenger_count"
          v [:pc] :tolerance 1e-4)))

    (gc!)
    (when (run-q? "q3")
      ;; NYC-Q3: COUNT(*) GROUP BY pickup_hour, pickup_dow
      (println "\nNYC-Q3: COUNT(*) GROUP BY pickup_hour, pickup_dow")
      (let [q {:from {:hr (:pickup-hour taxi) :dow (:pickup-dow taxi)}
               :group [:hr :dow]
               :agg [[:count]]
               :order [[:hr :asc] [:dow :asc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT pickup_hour, pickup_dow, COUNT(*) FROM taxi GROUP BY pickup_hour, pickup_dow ORDER BY pickup_hour, pickup_dow"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! results assoc :taxi-q3 {:stratum-1t (:median r-1t) :stratum (:median r)
                                          :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "NYC-Q3" conn
          "SELECT pickup_hour AS hr, pickup_dow AS dow, COUNT(*) AS count FROM taxi GROUP BY pickup_hour, pickup_dow ORDER BY pickup_hour, pickup_dow"
          v [:hr :dow])))

    (gc!)
    (when (run-q? "q4")
      ;; NYC-Q4: SUM(total) WHERE fare > 10 GROUP BY pickup_month
      (println "\nNYC-Q4: SUM(total) WHERE fare>10 GROUP BY pickup_month")
      (let [q {:from {:mo (:pickup-month taxi) :fare (:fare-amount taxi)
                      :total (:total-amount taxi)}
               :where [[:> :fare 10.0]]
               :group [:mo]
               :agg [[:sum :total]]
               :order [[:mo :asc]]}
            r-1t (bench-1t #(q/q q))
            r (bench #(q/q q))
            v (q/q q)]
        (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
        (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
        (let [sql "SELECT pickup_month, SUM(total_amount) FROM taxi WHERE fare_amount > 10 GROUP BY pickup_month ORDER BY pickup_month"
              rd-1t (duckdb-bench conn sql :threads 1)
              rd (duckdb-bench conn sql)]
          (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
          (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
          (swap! results assoc :taxi-q4 {:stratum-1t (:median r-1t) :stratum (:median r)
                                          :duckdb-1t (:median rd-1t) :duckdb (:median rd)}))
        (validate-query "NYC-Q4" conn
          "SELECT pickup_month AS mo, SUM(total_amount) AS sum FROM taxi WHERE fare_amount > 10 GROUP BY pickup_month ORDER BY pickup_month"
          v [:mo] :tolerance 1e-2)))

    @results))

;; ============================================================================
;; Summary Table
;; ============================================================================

(defn- print-full-summary
  "Print a comprehensive summary table covering all tiers."
  [all-results n mode-name]
  (println "\n================================================================")
  (println (format "  Summary — %,d rows, mode: %s" n mode-name))
  (println "================================================================")
  (println "")
  (println (format "| %-45s | %9s | %9s | %9s | %9s | %7s | %7s |"
                   "Benchmark" "Str 1T" "Str NT" "Duck 1T" "Duck NT" "1T vs" "NT vs"))
  (println (str "|" (apply str (repeat 47 "-"))
               "|" (apply str (repeat 11 "-"))
               "|" (apply str (repeat 11 "-"))
               "|" (apply str (repeat 11 "-"))
               "|" (apply str (repeat 11 "-"))
               "|" (apply str (repeat 9 "-"))
               "|" (apply str (repeat 9 "-")) "|"))

  (let [ratio-str (fn [^double s ^double d]
                    (cond
                      (and (< s 0.1) (< d 0.1)) " ~0ms"
                      (< s 0.01) "   --"
                      :else (let [r (/ d s)]
                              (if (>= r 1.0)
                                (format "%5.1fx" r)
                                (format "%5.2fx" r)))))
        row (fn [label {:keys [stratum-1t stratum duckdb-1t duckdb]}]
              (when (and stratum-1t stratum duckdb-1t duckdb)
                (println (format "| %-45s | %7.1fms | %7.1fms | %7.1fms | %7.1fms | %s | %s |"
                                 label (double stratum-1t) (double stratum) (double duckdb-1t) (double duckdb)
                                 (ratio-str (double stratum-1t) (double duckdb-1t))
                                 (ratio-str (double stratum) (double duckdb))))))
        section (fn [title]
                  (println (format "| %-45s | %9s | %9s | %9s | %9s | %7s | %7s |"
                                   (str "--- " title " ---") "" "" "" "" "" "")))
        ;; Ordered list of [key label] pairs
        bench-defs [[:section "Tier 1: TPC-H / SSB"]
                    [:b1 "B1: TPC-H Q6 (filter + sum-product)"]
                    [:b2 "B2: TPC-H Q1 (group-by + 7 aggs)"]
                    [:b3 "B3: SSB Q1.1 (filter + sum-product)"]
                    [:b5 "B5: Filtered COUNT (NEQ)"]
                    [:b6 "B6: Low-card group-by + COUNT"]
                    [:ssb-q12 "SSB-Q1.2: Tighter filter"]
                    [:section "Tier 2: H2O.ai Group-By"]
                    [:h2o-q1 "H2O-Q1: GROUP BY id1 (string 100)"]
                    [:h2o-q2 "H2O-Q2: GROUP BY id1,id2 (10K)"]
                    [:h2o-q3 "H2O-Q3: GROUP BY id3 (60K)"]
                    [:h2o-q4 "H2O-Q4: GROUP BY id4 (int 100), 3xAVG"]
                    [:h2o-q5 "H2O-Q5: GROUP BY id6 (60K), 3xSUM"]
                    [:h2o-q6 "H2O-Q6: GROUP BY id4,id5, STDDEV"]
                    [:h2o-q7 "H2O-Q7: GROUP BY id3, MAX-MIN"]
                    [:h2o-q9 "H2O-Q9: GROUP BY id2,id4, CORR"]
                    [:h2o-q10 "H2O-Q10: GROUP BY 6 cols (high-card)"]
                    [:h2o-q8 "H2O-Q8: Top-2 per id6 (window)"]
                    [:section "H2O Join"]
                    [:h2o-j1 "H2O-J1: JOIN small, SUM(v1)+SUM(v2)"]
                    [:h2o-j2 "H2O-J2: JOIN medium 2-col, SUM"]
                    [:h2o-j3 "H2O-J3: LEFT JOIN medium, SUM"]
                    [:section "Tier 3: ClickBench"]
                    [:cb-q1 "CB-Q1: COUNT WHERE AdvEngineID!=0"]
                    [:cb-q2 "CB-Q2: SUM+COUNT+AVG (multi-agg)"]
                    [:cb-q7 "CB-Q7: COUNT WHERE 2 preds"]
                    [:cb-q32 "CB-Q32: GROUP BY SearchEngineID"]
                    [:cb-q29 "CB-Q29: GROUP BY RegionID, SUM+COUNT"]
                    [:cb-q15 "CB-Q15: GROUP BY UserID (high-card)"]
                    [:cb-q5 "CB-Q5: COUNT(DISTINCT UserID)"]
                    [:cb-q5b "CB-Q5b: CD AdvEngineID GROUP BY Region"]
                    [:cb-q19 "CB-Q19: GROUP BY EXTRACT(minute)"]
                    [:cb-q21 "CB-Q21: LIKE '%example.com/page%'"]
                    [:cb-q22 "CB-Q22: LIKE '%search%'"]
                    [:cb-q23 "CB-Q23: GROUP BY + LIKE '%shop%'"]
                    [:cb-q28 "CB-Q28: AVG(LENGTH(URL))"]
                    [:cb-q43 "CB-Q43: DATE_TRUNC minute GROUP BY"]
                    [:cb-q3 "CB-Q3: AVG(UserID)"]
                    [:cb-q6 "CB-Q6: MIN+MAX(EventTime)"]
                    [:cb-q8 "CB-Q8: CD UserID GROUP BY Region TOP10"]
                    [:cb-q9 "CB-Q9: SUM+COUNT+AVG+CD GROUP BY Region"]
                    [:cb-q20 "CB-Q20: COUNT WHERE LIKE '%google%'"]
                    [:cb-q12 "CB-Q12: GROUP BY SearchPhrase, COUNT"]
                    [:cb-q27 "CB-Q27: AVG(LENGTH) HAVING COUNT>100K"]
                    [:cb-q33 "CB-Q33: GROUP BY URL (high-card)"]
                    [:section "Tier 4: NYC Taxi"]
                    [:taxi-q1 "NYC-Q1: AVG(fare) GROUP BY payment"]
                    [:taxi-q2 "NYC-Q2: AVG(tip) GROUP BY passengers"]
                    [:taxi-q3 "NYC-Q3: COUNT GROUP BY hour,dow"]
                    [:taxi-q4 "NYC-Q4: SUM WHERE fare>10 GROUP BY mo"]
                    [:section "Tier 5: Hash Join"]
                    [:join-q1 "JOIN-Q1: Fact JOIN Dim, GROUP BY, SUM"]]]
    ;; Walk bench-defs: print section headers only when following data exists
    (let [pending-section (atom nil)]
      (doseq [[k label] bench-defs]
        (if (= k :section)
          (reset! pending-section label)
          (when-let [r (get all-results k)]
            (when @pending-section
              (section @pending-section)
              (reset! pending-section nil))
            (row label r))))))

  (println "")
  (let [wins (atom 0) losses (atom 0)]
    (doseq [[k _] all-results]
      (when-let [{:keys [stratum-1t duckdb-1t]} (get all-results k)]
        (when (and (number? stratum-1t) (number? duckdb-1t) (> duckdb-1t 0.1))
          (if (< stratum-1t duckdb-1t) (swap! wins inc) (swap! losses inc)))))
    (println (format "  Stratum wins: %d, DuckDB wins: %d (1T comparison, > 0.1ms queries)"
                     @wins @losses)))
  (println (format "  Ratio > 1.0x = Stratum faster, < 1.0x = DuckDB faster"))
  (println (format "  %,d rows, %d warmup iters, %d bench iters" n *stratum-warmup* *stratum-iters*)))

;; ============================================================================
;; Tier 5: Hash Join
;; ============================================================================

(defn- generate-join-data
  "Generate star schema data for join benchmark.
   Fact table: n rows with foreign key to dimension (1K rows).
   Dimension: 1K rows with category (10 groups)."
  [^long n]
  (println "  Generating join data...")
  (let [dim-size 1000
        rng (java.util.Random. 42)
        ;; Dimension table
        dim-id (long-array dim-size)
        dim-category (long-array dim-size)
        _ (dotimes [i dim-size]
            (aset dim-id i (long i))
            (aset dim-category i (long (mod i 10))))
        ;; Fact table
        fact-id (long-array n)
        fact-fk (long-array n)
        fact-amount (double-array n)
        _ (dotimes [i n]
            (aset fact-id i (long i))
            (aset fact-fk i (long (.nextInt rng dim-size)))
            (aset fact-amount i (+ 1.0 (* 100.0 (.nextDouble rng)))))]
    {:fact-id fact-id :fact-fk fact-fk :fact-amount fact-amount
     :dim-id dim-id :dim-category dim-category
     :dim-size dim-size}))

(defn- duckdb-setup-join
  "Create DuckDB fact+dim tables via JDBC."
  [^Connection conn jd ^long n]
  (let [fact-csv "/tmp/olap_bench_fact.csv"
        dim-csv "/tmp/olap_bench_dim.csv"]
    (write-arrays-csv fact-csv
                      [[:fact_id (:fact-id jd)] [:fact_fk (:fact-fk jd)]
                       [:fact_amount (:fact-amount jd)]]
                      n)
    (write-arrays-csv dim-csv
                      [[:dim_id (:dim-id jd)] [:dim_category (:dim-category jd)]]
                      (long (:dim-size jd)))
    (duckdb-load-csv conn "fact" fact-csv)
    (duckdb-load-csv conn "dim" dim-csv)))

(defn- bench-join
  "Benchmark: fact JOIN dim ON fact_fk=dim_id, GROUP BY dim_category, SUM(amount)."
  [^long n ^Connection conn]
  (println "\n--- Hash Join Benchmark ---")
  (let [jd (generate-join-data n)
        _ (duckdb-setup-join conn jd n)
        dim-cols {:dim-id (:dim-id jd) :dim-category (:dim-category jd)}
        dim-encoded (q/encode-column (into-array String
                      (mapv #(str "cat" %) (range 10))))]

    ;; JOIN-Q1: fact JOIN dim, GROUP BY category, SUM(amount)
    (println "\nJOIN-Q1: Fact JOIN Dim ON fk=id, GROUP BY category, SUM(amount)")
    (let [q {:from {:fk (:fact-fk jd) :amount (:fact-amount jd)}
             :join [{:with dim-cols
                     :on [:= :fk :dim-id]}]
             :group [:dim-category]
             :agg [[:sum :amount]]}
          r-1t (bench-1t #(q/q q))
          r (bench #(q/q q))
          v (q/q q)]
      (println (format "  Stratum (1T): %s (%d groups)" (fmt-ms (:median r-1t)) (count v)))
      (println (format "  Stratum (NT): %s" (fmt-ms (:median r))))
      (let [sql "SELECT dim_category, SUM(fact_amount) FROM fact JOIN dim ON fact_fk=dim_id GROUP BY dim_category"
            rd-1t (duckdb-bench conn sql :threads 1)
            rd (duckdb-bench conn sql)]
        (println (format "  DuckDB  (1T): %s" (fmt-ms (:median rd-1t))))
        (println (format "  DuckDB  (NT): %s" (fmt-ms (:median rd))))
        (validate-query "JOIN-Q1" conn
                         "SELECT dim_category, SUM(fact_amount) AS sum FROM fact JOIN dim ON fact_fk=dim_id GROUP BY dim_category"
                         v [:dim_category])
        {:join-q1 {:stratum-1t (:median r-1t) :stratum (:median r)
                   :duckdb-1t (:median rd-1t) :duckdb (:median rd)}}))))

;; ============================================================================
;; Main
;; ============================================================================

;; ============================================================================
;; Tier 7: Window Function Micro-Benchmark (Step 3)
;; ============================================================================

(defn- generate-window-partition-cols
  "Generate synthetic orderkey + linenumber columns for window partitioning.
   ~4 line items per order on average."
  ^clojure.lang.IPersistentMap [^long n]
  (let [orderkey (long-array n)
        linenumber (long-array n)
        rng (java.util.Random. 42)]
    (loop [i 0 ok 1 ln 1]
      (when (< i n)
        (aset orderkey i ok)
        (aset linenumber i ln)
        (if (< (.nextInt rng 4) 1)
          (recur (inc i) (inc ok) 1)
          (recur (inc i) ok (inc ln)))))
    {:orderkey orderkey :linenumber linenumber}))

(defn- duckdb-setup-lineitem-win
  "Create DuckDB lineitem_win table via JDBC."
  [^Connection conn win-data ^long n]
  (let [csv-path "/tmp/olap_bench_lineitem_win.csv"]
    (println "  Exporting lineitem_win data to DuckDB (shared CSV)...")
    (write-arrays-csv csv-path
                      [[:orderkey (:orderkey win-data)] [:linenumber (:linenumber win-data)]
                       [:price (:price win-data)] [:shipdate (:shipdate win-data)]]
                      n)
    (duckdb-load-csv conn "lineitem_win" csv-path)))

(defn- bench-win-q1
  "WIN-Q1: ROW_NUMBER() OVER (PARTITION BY orderkey ORDER BY linenumber)"
  [data ^Connection conn]
  (println "\nWIN-Q1: ROW_NUMBER() OVER (PARTITION BY orderkey ORDER BY linenumber)")
  (let [q {:from {:orderkey (:orderkey data) :linenumber (:linenumber data) :price (:price data)}
           :window [{:op :row-number :partition-by [:orderkey] :order-by [[:linenumber :asc]] :as :rn}]}
        qc (assoc q :result :columns)
        r-1t (bench-1t #(q/q qc))
        r (bench #(q/q qc))
        cr (q/q qc)
        n (:n-rows cr)]
    (println (format "  Stratum 1T: %s  (%d rows)" (fmt-ms (:median r-1t)) n))
    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
    (let [sql "SELECT orderkey, linenumber, price, ROW_NUMBER() OVER (PARTITION BY orderkey ORDER BY linenumber) AS rn FROM lineitem_win"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median r-duck-1t))))
      (validate-query "WIN-Q1" conn
                       "SELECT orderkey, linenumber, price, ROW_NUMBER() OVER (PARTITION BY orderkey ORDER BY linenumber) AS rn FROM lineitem_win"
                       (q/q q) [:orderkey :linenumber]))))

(defn- bench-win-q2
  "WIN-Q2: LAG(price, 1) OVER (PARTITION BY orderkey ORDER BY linenumber)"
  [data ^Connection conn]
  (println "\nWIN-Q2: LAG(price, 1) OVER (PARTITION BY orderkey ORDER BY linenumber)")
  (let [q {:from {:orderkey (:orderkey data) :linenumber (:linenumber data) :price (:price data)}
           :window [{:op :lag :col :price :offset 1 :partition-by [:orderkey] :order-by [[:linenumber :asc]] :as :prev_price}]}
        qc (assoc q :result :columns)
        r-1t (bench-1t #(q/q qc))
        r (bench #(q/q qc))
        cr (q/q qc)
        n (:n-rows cr)]
    (println (format "  Stratum 1T: %s  (%d rows)" (fmt-ms (:median r-1t)) n))
    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
    (let [sql "SELECT orderkey, linenumber, price, LAG(price, 1) OVER (PARTITION BY orderkey ORDER BY linenumber) AS prev_price FROM lineitem_win"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median r-duck-1t))))
      (validate-query "WIN-Q2" conn
                       "SELECT orderkey, linenumber, price, LAG(price, 1) OVER (PARTITION BY orderkey ORDER BY linenumber) AS prev_price FROM lineitem_win"
                       (q/q q) [:orderkey :linenumber]))))

(defn- bench-win-q3
  "WIN-Q3: SUM(price) OVER (PARTITION BY orderkey ORDER BY shipdate)"
  [data ^Connection conn]
  (println "\nWIN-Q3: Running SUM(price) OVER (PARTITION BY orderkey ORDER BY shipdate)")
  (let [q {:from {:orderkey (:orderkey data) :shipdate (:shipdate data) :price (:price data)}
           :window [{:op :sum :col :price :partition-by [:orderkey] :order-by [[:shipdate :asc]] :as :running_sum}]}
        qc (assoc q :result :columns)
        r-1t (bench-1t #(q/q qc))
        r (bench #(q/q qc))
        cr (q/q qc)
        n (:n-rows cr)]
    (println (format "  Stratum 1T: %s  (%d rows)" (fmt-ms (:median r-1t)) n))
    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
    (let [sql "SELECT orderkey, shipdate, price, SUM(price) OVER (PARTITION BY orderkey ORDER BY shipdate) AS running_sum FROM lineitem_win"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median r-duck-1t))))
      (validate-query "WIN-Q3" conn
                       "SELECT orderkey, shipdate, price, SUM(price) OVER (PARTITION BY orderkey ORDER BY shipdate) AS running_sum FROM lineitem_win"
                       (q/q q) [:orderkey :shipdate]))))

;; ============================================================================
;; Tier 8: TPC-DS Benchmark (Step 4)
;; ============================================================================

(defn- duckdb-setup-tpcds
  "Generate TPC-DS sf=1 data via DuckDB JDBC. Returns ^Connection."
  ^Connection []
  (println "  Generating TPC-DS sf=1 data via DuckDB (JDBC)...")
  (let [conn (duckdb-jdbc-connect "")]
    (duckdb-jdbc-exec! conn "INSTALL tpcds")
    (duckdb-jdbc-exec! conn "LOAD tpcds")
    (duckdb-jdbc-exec! conn "CALL dsdgen(sf=1)")
    (let [cnt (duckdb-jdbc-query-scalar conn "SELECT count(*) FROM store_sales")]
      (println (format "  TPC-DS store_sales: %s rows" cnt)))
    conn))

(defn- load-tpcds-store-sales
  "Load TPC-DS store_sales from DuckDB JDBC into Stratum column arrays."
  [^Connection conn ^long max-rows]
  (println "  Loading store_sales from DuckDB (JDBC)...")
  (with-open [^Statement stmt (.createStatement conn)
              ^ResultSet rs (.executeQuery stmt
                              (str "SELECT ss_store_sk, ss_item_sk, ss_customer_sk, ss_net_profit, ss_ext_sales_price, ss_quantity FROM store_sales LIMIT " max-rows))]
    ;; First pass: collect into vectors (use getObject for NULL-safe reading)
    (let [rows (loop [acc (transient [])]
                 (if (.next rs)
                   (let [get-long (fn [^ResultSet r col]
                                    (let [v (.getObject r (int col))]
                                      (if (nil? v) Long/MIN_VALUE (long v))))
                         get-double (fn [^ResultSet r col]
                                     (let [v (.getObject r (int col))]
                                       (if (nil? v) Double/NaN (.doubleValue ^Number v))))]
                     (recur (conj! acc [(get-long rs 1) (get-long rs 2) (get-long rs 3)
                                        (get-double rs 4) (get-double rs 5) (get-long rs 6)])))
                   (persistent! acc)))
          n (count rows)
          store-sk (long-array n)
          item-sk (long-array n)
          customer-sk (long-array n)
          net-profit (double-array n)
          ext-sales (double-array n)
          quantity (long-array n)]
      (dotimes [i n]
        (let [row (nth rows i)]
          (aset store-sk i (long (nth row 0)))
          (aset item-sk i (long (nth row 1)))
          (aset customer-sk i (long (nth row 2)))
          (aset net-profit i (double (nth row 3)))
          (aset ext-sales i (double (nth row 4)))
          (aset quantity i (long (nth row 5)))))
      (println (format "    Loaded %,d rows" n))
      {:ss_store_sk store-sk :ss_item_sk item-sk :ss_customer_sk customer-sk
       :ss_net_profit net-profit :ss_ext_sales_price ext-sales :ss_quantity quantity
       :n n})))

(defn- bench-ds-q1
  "DS-Q1: Simple GROUP BY on store_sales."
  [data ^Connection conn]
  (println "\nDS-Q1: GROUP BY ss_store_sk, SUM(ss_net_profit), COUNT(*)")
  (let [q {:from (dissoc data :n)
           :group [:ss_store_sk]
           :agg [[:as [:sum :ss_net_profit] :total_profit]
                 [:as [:count] :cnt]]
           :order [[:ss_store_sk :asc]]}
        r (bench #(q/q q))
        r-1t (bench-1t #(q/q q))
        v (q/q q)]
    (println (format "  Stratum 1T: %s  (%d groups)" (fmt-ms (:median r-1t)) (count v)))
    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
    (let [sql "SELECT ss_store_sk, SUM(ss_net_profit) AS total_profit, COUNT(*) AS cnt FROM store_sales GROUP BY ss_store_sk ORDER BY ss_store_sk"
          r-duck-1t (duckdb-bench conn sql :threads 1)
          r-duck    (duckdb-bench conn sql)]
      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median r-duck-1t))))
      (validate-query "DS-Q1" conn
                       "SELECT ss_store_sk, SUM(ss_net_profit) AS total_profit, COUNT(*) AS cnt FROM store_sales WHERE ss_store_sk IS NOT NULL GROUP BY ss_store_sk ORDER BY ss_store_sk"
                       (vec (remove #(nil? (:ss_store_sk %)) v))
                       [:ss_store_sk]))))

(defn- bench-ds-q98
  "DS-Q98: ROW_NUMBER() OVER (PARTITION BY ss_store_sk ORDER BY ss_net_profit)"
  [data ^Connection conn]
  (println "\nDS-Q98: ROW_NUMBER OVER (PARTITION BY ss_store_sk ORDER BY ss_net_profit)")
  (let [q {:from (dissoc data :n)
           :window [{:op :row-number :partition-by [:ss_store_sk] :order-by [[:ss_net_profit :asc]] :as :rn}]}
        qc (assoc q :result :columns)
        r-1t (bench-1t #(q/q qc) :warmup 10 :iters 5)
        r (bench #(q/q qc) :warmup 10 :iters 5)
        cr (q/q qc)
        n (:n-rows cr)]
    (println (format "  Stratum 1T: %s  (%d rows)" (fmt-ms (:median r-1t)) n))
    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
    (let [sql "SELECT ss_store_sk, ss_item_sk, ss_net_profit, ROW_NUMBER() OVER (PARTITION BY ss_store_sk ORDER BY ss_net_profit) AS rn FROM store_sales"
          r-duck-1t (duckdb-bench conn sql :threads 1 :iters 5 :batch-size 1)
          r-duck    (duckdb-bench conn sql :iters 5 :batch-size 1)]
      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck))))
      (println (format "  Ratio 1T: %s" (fmt-ratio (:median r-1t) (:median r-duck-1t))))
      ;; Structural validation: ROW_NUMBER is non-deterministic on ties,
      ;; so we check partition counts + consecutive 1..N numbering instead of exact values
      (let [v (q/q q)
            v-filtered (vec (remove #(let [sk (:ss_store_sk %)] (or (nil? sk) (= Long/MIN_VALUE sk))) v))
            s-by-store (group-by :ss_store_sk v-filtered)
            d-counts (with-open [s2 (.createStatement conn)
                                 rs2 (.executeQuery s2 "SELECT ss_store_sk, COUNT(*) AS cnt FROM store_sales WHERE ss_store_sk IS NOT NULL GROUP BY ss_store_sk")]
                       (loop [m {}]
                         (if (.next rs2)
                           (recur (assoc m (.getLong rs2 1) (.getLong rs2 2)))
                           m)))
            issues (for [[sk rows] s-by-store
                         :let [rns (sort (map #(long (:rn %)) rows))
                               expected (range 1 (inc (count rows)))
                               duck-cnt (get d-counts (long sk) 0)]
                         :when (or (not= (count rows) duck-cnt)
                                   (not= rns (vec expected)))]
                     (format "store=%d: count=%d vs duck=%d, consecutive=%s"
                             sk (count rows) duck-cnt (= rns (vec expected))))]
        (if (empty? issues)
          (println (format "  ✓ DS-Q98: PASS (%d partitions, %d rows, consecutive rn)" (count s-by-store) (count v-filtered)))
          (do (println (format "  ✗ DS-Q98: FAIL (%d issues)" (count issues)))
              (doseq [iss (take 5 issues)] (println (format "    → %s" iss)))))))))

(defn -main [& args]
  (let [;; Parse args: numbers are row counts, strings are tier/query/mode filters
        ;; Usage: clj -M:olap [rows] [mode] [tier...] [query...]
        ;; Mode: default runs both arrays + idx. Explicit mode runs just that one.
        ;;   arrays     - run arrays only
        ;;   idx        - run indices only
        ;;   sorted-idx - run all three (arrays + idx + sorted-idx)
        ;; Examples: clj -M:olap 1000000 t2
        ;;           clj -M:olap t1               ;; run T1 with arrays + idx (default)
        ;;           clj -M:olap t1 arrays         ;; run T1 with arrays only
        ;;           clj -M:olap t1 idx            ;; run T1 with indices only
        ;;           clj -M:olap t1 sorted-idx     ;; run T1 with all three modes
        ;;           clj -M:olap 6000000 h2o cb
        ;;           clj -M:olap q19 q43           ;; individual queries (auto-selects tier)
        ;;           clj -M:olap cb q19 q43        ;; ClickBench tier, only Q19 and Q43
        ;;           clj -M:olap taxi q3           ;; Taxi tier, only Q3
        mode-names #{"idx" "sorted-idx" "arrays"}
        tier-names #{"t1" "1" "tpch" "ssb" "t2" "2" "h2o"
                     "t3" "3" "cb" "clickbench"
                     "t4" "4" "taxi" "nyc"
                     "t5" "5" "join"
                     "t6" "6" "stat" "statistical"
                     "t7" "7" "win" "window"
                     "t8" "8" "tpcds" "ds"}
        parsed (group-by #(try (Long/parseLong %) :n (catch Exception _ :tier)) args)
        n (if-let [ns (:n parsed)] (Long/parseLong (first ns)) default-n)
        ;; Scale-adaptive warmup & iterations
        _ (cond
            (> n 50000000) (do (alter-var-root #'*stratum-warmup* (constantly 2))
                               (alter-var-root #'*stratum-iters* (constantly 5))
                               (alter-var-root #'*duckdb-iters* (constantly 5)))
            (> n 1000000)  (do (alter-var-root #'*stratum-warmup* (constantly 5))
                               (alter-var-root #'*stratum-iters* (constantly 10))
                               (alter-var-root #'*duckdb-iters* (constantly 10))))
        all-args (set (map str/lower-case (or (:tier parsed) [])))
        ;; Extract mode flags — default is [:arrays :idx], explicit mode runs just that one,
        ;; sorted-idx adds all three modes
        modes (cond
                (contains? all-args "sorted-idx") [:arrays :idx :sorted-idx]
                (contains? all-args "idx")        [:idx]
                (contains? all-args "arrays")     [:arrays]
                ;; >10M rows: index-only by default (arrays waste heap/GC at scale)
                (> n 10000000)                    [:idx]
                :else                             [:arrays :idx])
        validate? (contains? all-args "validate")
        all-args (disj (clojure.set/difference all-args mode-names) "validate")
        tier-args (clojure.set/intersection all-args tier-names)
        query-args (clojure.set/difference all-args tier-names)
        ;; If only query args given (no tier), auto-select matching tiers
        run-tier? (fn [& names]
                    (let [name-set (set (map str/lower-case names))]
                      (or (and (empty? tier-args) (empty? query-args))
                          (some name-set tier-args)
                          ;; Auto-select tier when query args are present but no tier specified
                          (and (seq query-args) (empty? tier-args)))))
        ;; Query-level filter: run-q? checks if a specific query should run
        ;; When no query args, run all. Query args match by suffix (e.g., "q19" matches "q19")
        run-q? (fn [q-name]
                 (or (empty? query-args)
                     (contains? query-args (str/lower-case q-name))))]
    (let [cores (.availableProcessors (Runtime/getRuntime))
          os-name (System/getProperty "os.name")
          os-arch (System/getProperty "os.arch")
          jvm-version (System/getProperty "java.version")
          jvm-name (System/getProperty "java.vm.name")
          cpu-model (try (let [proc (.start (ProcessBuilder. ["grep" "-m1" "model name" "/proc/cpuinfo"]))
                               out (str/trim (slurp (.getInputStream proc)))]
                           (.waitFor proc)
                           (or (second (re-find #":\s*(.*)" out)) "unknown"))
                         (catch Exception _ "unknown"))
          git-sha (try (let [proc (.start (ProcessBuilder. ["git" "rev-parse" "--short" "HEAD"]))
                             out (str/trim (slurp (.getInputStream proc)))]
                         (.waitFor proc) out)
                       (catch Exception _ "unknown"))
          duckdb-version (try (with-open [vc (duckdb-jdbc-connect "")]
                                (duckdb-jdbc-query-scalar vc "SELECT version()"))
                              (catch Exception _ "unknown"))]
      (println "")
      (println "================================================================")
      (println "  Stratum vs DuckDB — OLAP Benchmark Suite")
      (println (format "  Date: %s" (java.time.LocalDateTime/now)))
      (println (format "  Git: %s" git-sha))
      (println (format "  CPU: %s (%d cores)" cpu-model cores))
      (println (format "  OS: %s/%s, JVM: %s (%s)" os-name os-arch jvm-version jvm-name))
      (println (format "  SIMD lanes: %d long, %d double" (ColumnOps/getLongLanes) (ColumnOps/getDoubleLanes)))
      (println (format "  %,d rows, input modes: %s" n (str/join ", " (map name modes))))
      (println (format "  Stratum warmup: %d, bench: %d iters" *stratum-warmup* *stratum-iters*))
      (println (format "  DuckDB warmup: %d, bench: %d iters (JDBC in-process)" *duckdb-warmup* *duckdb-iters*))
      (println (format "  DuckDB: %s" duckdb-version))
      (when (seq tier-args) (println (format "  Tiers: %s" (str/join ", " tier-args))))
      (when (seq query-args) (println (format "  Queries: %s" (str/join ", " query-args))))
      (println "================================================================"))

    ;; JIT warmup — exercise all Java hot paths so cross-tier deoptimization
    ;; doesn't distort benchmark results.
    (let [t0 (System/nanoTime)]
      (q/jit-warmup!)
      (println (format "\nJIT warmup: %.1fs" (/ (- (System/nanoTime) t0) 1e9))))

    (let [all-results (atom {})]

      ;; === Tier 1: TPC-H / SSB (existing B1-B6) ===
      ;; Sort key: :shipdate (primary filter column for range predicates)
      (when (run-tier? "t1" "1" "tpch" "ssb")
        (println "\n=== Tier 1: TPC-H / SSB ===")
        (let [arrays (generate-arrays n)
              ^Connection conn (duckdb-setup-lineitem n)]
          (try
            (doseq [m modes]
              (println (format "\n  --- Input mode: %s ---" (name m)))
              (let [data (prepare-data arrays m :shipdate)
                    tier1 (merge
                           (when (run-q? "b1") {:b1 (do (gc!) (bench-b1 data conn))})
                           (when (run-q? "b2") {:b2 (do (gc!) (bench-b2 data conn))})
                           (when (run-q? "b3") {:b3 (do (gc!) (bench-b3 data conn))})
                           (when (run-q? "b4") {:b4 (do (gc!) (bench-b4 data conn))})
                           (when (run-q? "b5") {:b5 (do (gc!) (bench-b5 data conn))})
                           (when (run-q? "b6") {:b6 (do (gc!) (bench-b6 data conn))})
                           (when (run-q? "ssb-q12") {:ssb-q12 (do (gc!) (bench-ssb-q12 data conn))}))]
                (swap! all-results merge-best tier1)))
            (finally (.close conn)))))

      ;; === Tier 2: H2O db-benchmark ===
      ;; No range predicates — no sort key benefit. idx wraps as-is.
      (when (run-tier? "t2" "2" "h2o")
        (println "\n\n=== Tier 2: H2O.ai db-benchmark ===")
        (gc!)
        (let [arrays (generate-h2o-data n)
              conn (duckdb-jdbc-connect "")]
          (try
            (println "  Exporting H2O data to DuckDB (shared CSV)...")
            (duckdb-setup-h2o conn arrays n)
            (doseq [m modes]
              (println (format "\n  --- Input mode: %s ---" (name m)))
              (let [h2o (prepare-data arrays m nil)]
                (when (run-q? "q1")  (gc!) (swap! all-results merge-best {:h2o-q1 (bench-h2o-q1 h2o conn)}))
                (when (run-q? "q2")  (gc!) (swap! all-results merge-best {:h2o-q2 (bench-h2o-q2 h2o conn)}))
                (when (run-q? "q3")  (gc!) (swap! all-results merge-best {:h2o-q3 (bench-h2o-q3 h2o conn)}))
                (when (run-q? "q4")  (gc!) (swap! all-results merge-best {:h2o-q4 (bench-h2o-q4 h2o conn)}))
                (when (run-q? "q5")  (gc!) (swap! all-results merge-best {:h2o-q5 (bench-h2o-q5 h2o conn)}))
                (when (run-q? "q7")  (gc!) (swap! all-results merge-best {:h2o-q7 (bench-h2o-q7 h2o conn)}))
                (when (run-q? "q10") (gc!) (swap! all-results merge-best {:h2o-q10 (bench-h2o-q10 h2o conn)}))
                ;; Extra GC after Q10 — creates 1M groups × 10 warmup iters = massive GC pressure
                (gc!) (gc!)
                ;; === New queries unlocked by compound aggs ===
                (when (run-q? "q6") (gc!) (swap! all-results merge-best {:h2o-q6 (bench-h2o-q6 h2o conn)}))
                (when (run-q? "q9") (gc!) (swap! all-results merge-best {:h2o-q9 (bench-h2o-q9 h2o conn)}))
                ;; === Window function query ===
                (when (run-q? "q8") (gc!) (swap! all-results merge-best {:h2o-q8 (bench-h2o-q8 h2o conn)}))))
            ;; H2O Join queries (J1-J3) — mode-independent (always arrays)
            (when (or (run-q? "j1") (run-q? "j2") (run-q? "j3"))
              (println "\n  --- H2O Join Queries ---")
              (let [dims (generate-h2o-join-dims n)]
                (duckdb-setup-h2o-joins conn dims)
                (when (run-q? "j1") (gc!) (swap! all-results merge-best {:h2o-j1 (bench-h2o-j1 arrays dims conn)}))
                (when (run-q? "j2") (gc!) (swap! all-results merge-best {:h2o-j2 (bench-h2o-j2 arrays dims conn)}))
                (when (run-q? "j3") (gc!) (swap! all-results merge-best {:h2o-j3 (bench-h2o-j3 arrays dims conn)}))))
            (finally (.close conn)))))

      ;; === Tier 3: ClickBench ===
      ;; Sort key: :event-time (used in Q19 EXTRACT, Q43 DATE_TRUNC)
      (when (run-tier? "t3" "3" "cb" "clickbench")
        (println "\n\n=== Tier 3: ClickBench ===")
        ;; Extra GC: H2O creates massive allocation pressure (3.8M result maps).
        ;; Double GC + sleep ensures G1GC fully reclaims before ClickBench.
        (gc!) (gc!)
        (let [arrays (generate-clickbench-data n)
              conn (duckdb-jdbc-connect "")]
          (try
            (println "  Creating DuckDB ClickBench table (JDBC)...")
            (duckdb-setup-clickbench conn n)
            (doseq [m modes]
              (println (format "\n  --- Input mode: %s ---" (name m)))
              (let [cb (prepare-data arrays m :event-time)]
                (gc!) (swap! all-results merge-best (bench-clickbench cb conn run-q?))))
            (finally (.close conn)))))

      ;; === Tier 4: NYC Taxi ===
      ;; Sort key: :fare-amount (Q4 WHERE fare > 10 benefits from sorted zone maps)
      (when (run-tier? "t4" "4" "taxi" "nyc")
        (println "\n\n=== Tier 4: NYC Taxi ===")
        (gc!)
        (let [arrays (generate-nyc-taxi-data n)
              conn (duckdb-jdbc-connect "")]
          (try
            (println "  Creating DuckDB NYC Taxi table (JDBC)...")
            (duckdb-setup-nyc-taxi conn n)
            (doseq [m modes]
              (println (format "\n  --- Input mode: %s ---" (name m)))
              (let [taxi (prepare-data arrays m :fare-amount)]
                (gc!) (swap! all-results merge-best (bench-nyc-taxi taxi conn run-q?))))
            (finally (.close conn)))))

      ;; === Tier 6: Statistical (Median / Percentile / Approx Quantile) ===
      (when (run-tier? "t6" "6" "stat" "statistical")
        (println "\n\n=== Tier 6: Statistical (Median / Percentile / Approx Quantile) ===")
        (gc!)
        (let [arrays (generate-arrays n)
              ^Connection conn (duckdb-setup-lineitem n)]
          (try
            (doseq [m modes]  ;; stat aggs are scalar-only but still work on idx-materialized data
              (println (format "\n  --- Input mode: %s ---" (name m)))
              (let [data (prepare-data arrays m :shipdate)]

                ;; STAT-Q1: Ungrouped MEDIAN(price)
                (when (run-q? "stat-q1")
                  (gc!)
                  (println "\nSTAT-Q1: Ungrouped MEDIAN(price)")
                  (let [q {:from {:price (:price data)} :agg [[:median :price]]}
                        r (bench #(q/q q))
                        r-1t (bench-1t #(q/q q))
                        v (q/q q)]
                    (println (format "  Stratum 1T: %s  (median=%.2f)" (fmt-ms (:median r-1t)) (double (:median (first v)))))
                    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
                    (let [sql "SELECT MEDIAN(price) FROM lineitem"
                          r-duck-1t (duckdb-bench conn sql :threads 1)
                          r-duck    (duckdb-bench conn sql)]
                      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
                      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck)))))
                    (validate-query "STAT-Q1" conn
                                    "SELECT MEDIAN(price) AS median FROM lineitem"
                                    v [] :tolerance 1e-4)))

                ;; STAT-Q2: Grouped MEDIAN by returnflag (low-card, 3 groups)
                (when (run-q? "stat-q2")
                  (gc!)
                  (println "\nSTAT-Q2: GROUP BY returnflag, MEDIAN(price)")
                  (let [q {:from {:returnflag (:returnflag data) :price (:price data)}
                           :group [:returnflag]
                           :agg [[:median :price]]
                           :order [[:returnflag :asc]]}
                        r (bench #(q/q q))
                        r-1t (bench-1t #(q/q q))]
                    (println (format "  Stratum 1T: %s" (fmt-ms (:median r-1t))))
                    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
                    (let [sql "SELECT returnflag, MEDIAN(price) FROM lineitem GROUP BY returnflag ORDER BY returnflag"
                          r-duck-1t (duckdb-bench conn sql :threads 1)
                          r-duck    (duckdb-bench conn sql)]
                      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
                      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck)))))
                    (let [v (q/q {:from {:returnflag (:returnflag data) :price (:price data)}
                                        :group [:returnflag]
                                        :agg [[:median :price]]
                                        :order [[:returnflag :asc]]})]
                      (validate-query "STAT-Q2" conn
                                      "SELECT returnflag, MEDIAN(price) AS median FROM lineitem GROUP BY returnflag ORDER BY returnflag"
                                      v [:returnflag] :tolerance 1e-4))))

                ;; STAT-Q3: Ungrouped PERCENTILE_CONT(0.95, price)
                (when (run-q? "stat-q3")
                  (gc!)
                  (println "\nSTAT-Q3: PERCENTILE_CONT(0.95, price)")
                  (let [q {:from {:price (:price data)} :agg [[:percentile :price 0.95]]}
                        r (bench #(q/q q))
                        r-1t (bench-1t #(q/q q))
                        v (q/q q)]
                    (println (format "  Stratum 1T: %s  (p95=%.2f)" (fmt-ms (:median r-1t)) (double (:percentile (first v)))))
                    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
                    (let [sql "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) FROM lineitem"
                          r-duck-1t (duckdb-bench conn sql :threads 1)
                          r-duck    (duckdb-bench conn sql)]
                      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
                      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck)))))
                    (validate-query "STAT-Q3" conn
                                    "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) AS percentile FROM lineitem"
                                    v [] :tolerance 1e-4)))

                ;; STAT-Q4: Ungrouped APPROX_QUANTILE(price, 0.95)
                (when (run-q? "stat-q4")
                  (gc!)
                  (println "\nSTAT-Q4: APPROX_QUANTILE(price, 0.95)")
                  (let [q {:from {:price (:price data)} :agg [[:approx-quantile :price 0.95]]}
                        r (bench #(q/q q))
                        r-1t (bench-1t #(q/q q))
                        v (q/q q)]
                    (println (format "  Stratum 1T: %s  (aq95=%.2f)" (fmt-ms (:median r-1t)) (double (:approx-quantile (first v)))))
                    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
                    (let [sql "SELECT APPROX_QUANTILE(price, 0.95) FROM lineitem"
                          r-duck-1t (duckdb-bench conn sql :threads 1)
                          r-duck    (duckdb-bench conn sql)]
                      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
                      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck)))))
                    ;; Approximate: t-digest vs DuckDB's algo differ, use 1% tolerance
                    (validate-query "STAT-Q4" conn
                                    "SELECT APPROX_QUANTILE(price, 0.95) AS \"approx-quantile\" FROM lineitem"
                                    v [] :tolerance 0.01)))

                ;; STAT-Q5: Multiple percentiles in one query
                (when (run-q? "stat-q5")
                  (gc!)
                  (println "\nSTAT-Q5: P25, P50, P75 of price")
                  (let [q {:from {:price (:price data)}
                           :agg [[:as [:percentile :price 0.25] :p25]
                                 [:as [:percentile :price 0.5] :p50]
                                 [:as [:percentile :price 0.75] :p75]]}
                        r (bench #(q/q q))
                        r-1t (bench-1t #(q/q q))
                        v (q/q q)]
                    (println (format "  Stratum 1T: %s  (p25=%.0f, p50=%.0f, p75=%.0f)"
                                     (fmt-ms (:median r-1t))
                                     (double (:p25 (first v)))
                                     (double (:p50 (first v)))
                                     (double (:p75 (first v)))))
                    (println (format "  Stratum NT: %s" (fmt-ms (:median r))))
                    (let [sql (str "SELECT "
                                   "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS p25, "
                                   "PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY price) AS p50, "
                                   "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS p75 "
                                   "FROM lineitem")
                          r-duck-1t (duckdb-bench conn sql :threads 1)
                          r-duck    (duckdb-bench conn sql)]
                      (println (format "  DuckDB  1T: %s" (fmt-ms (:median r-duck-1t))))
                      (println (format "  DuckDB  NT: %s" (fmt-ms (:median r-duck)))))
                    (validate-query "STAT-Q5" conn
                                    (str "SELECT "
                                         "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS p25, "
                                         "PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY price) AS p50, "
                                         "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS p75 "
                                         "FROM lineitem")
                                    v [] :tolerance 1e-4)))))
            (finally (.close conn)))))

      ;; === Tier 5: Hash Join ===
      (when (run-tier? "t5" "5" "join")
        (println "\n\n=== Tier 5: Hash Join ===")
        (gc!)
        (let [conn (duckdb-jdbc-connect "")]
          (try
            (when-let [jr (bench-join n conn)]
              (swap! all-results merge-best jr))
            (finally (.close conn)))))

      ;; === Tier 7: Window Function Micro-Benchmark ===
      (when (run-tier? "t7" "7" "win" "window")
        (println "\n\n=== Tier 7: Window Function Micro-Benchmark ===")
        (gc!)
        (let [arrays (generate-arrays n)
              actual-n (:n arrays)
              win-cols (generate-window-partition-cols actual-n)
              win-data (merge arrays win-cols)
              conn (duckdb-jdbc-connect "")]
          (try
            (duckdb-setup-lineitem-win conn win-data actual-n)
            (when (run-q? "win-q1") (gc!) (bench-win-q1 win-data conn))
            (when (run-q? "win-q2") (gc!) (bench-win-q2 win-data conn))
            (when (run-q? "win-q3") (gc!) (bench-win-q3 win-data conn))
            (finally (.close conn)))))

      ;; === Tier 8: TPC-DS ===
      (when (run-tier? "t8" "8" "tpcds" "ds")
        (println "\n\n=== Tier 8: TPC-DS (sf=1) ===")
        (gc!)
        (let [conn (duckdb-setup-tpcds)
              ss-data (load-tpcds-store-sales conn 2880404)]
          (try
            (when (run-q? "ds-q1") (gc!) (bench-ds-q1 ss-data conn))
            (when (run-q? "ds-q98") (gc!) (bench-ds-q98 ss-data conn))
            (finally (.close conn)))))

      ;; === Full Summary ===
      (let [results @all-results]
        (when (> (count results) 1)
          (print-full-summary results n (str/join "+" (map name modes))))))

    (println "\n\nBenchmark suite complete.")
    (shutdown-agents)))

(comment
  ;; Quick test with smaller data
  (-main "1000000")

  ;; Full benchmark (arrays + idx, default)
  (-main)

  ;; Run T1 with only arrays (single mode)
  (-main "t1" "arrays")

  ;; Run T1 with only indices (single mode)
  (-main "t1" "idx")

  ;; Run T1 with all three modes (arrays + idx + sorted-idx)
  (-main "t1" "sorted-idx")
  )
