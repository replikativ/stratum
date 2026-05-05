(ns stratum.parquet-test
  "Tests for Parquet import — both heap-array `from-parquet` and streaming
   `index-parquet!` paths."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.parquet :as parquet]
            [stratum.dataset :as dataset]
            [stratum.query :as q]
            [konserve.store :as kstore]
            [clojure.java.io :as io])
  (:import [org.apache.parquet.hadoop.example ExampleParquetWriter GroupWriteSupport]
           [org.apache.parquet.example.data.simple SimpleGroupFactory]
           [org.apache.parquet.schema MessageTypeParser]
           [org.apache.parquet.io LocalOutputFile]
           [org.apache.hadoop.conf Configuration]
           [java.nio.file Paths]
           [java.io File]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test fixtures: synthesize parquet files via parquet-hadoop's ExampleWriter
;; ============================================================================

(defn- ^File temp-parquet
  "Create a uniquely-named tempfile path under /tmp."
  [^String tag]
  (File. (str "/tmp/stratum-parquet-test-" tag "-" (System/nanoTime) ".parquet")))

(defn- write-parquet!
  "Write `rows` to a parquet file at `path` matching `schema-text`. Each row
   is a map keyed by column-name string. NULL values are skipped (column
   left unset for that row, which Parquet emits as definition-level 0
   provided the column is `optional`)."
  [^File path schema-text rows]
  (let [schema (MessageTypeParser/parseMessageType schema-text)
        conf (Configuration.)
        _ (GroupWriteSupport/setSchema schema conf)
        out (LocalOutputFile. (Paths/get (.getAbsolutePath path) (make-array String 0)))
        writer (-> (ExampleParquetWriter/builder out)
                   (.withConf conf)
                   ;; small row-group size forces multiple row groups for medium fixtures
                   (.withRowGroupSize 4096)
                   (.build))
        gf (SimpleGroupFactory. schema)]
    (try
      (doseq [row rows]
        (let [g (.newGroup gf)]
          (doseq [[k v] row]
            (when (some? v)
              (cond
                (string? v)  (.append g ^String k ^String v)
                (integer? v) (.append g ^String k (long v))
                (float? v)   (.append g ^String k (double v))
                (instance? Double v) (.append g ^String k (double v))
                :else (throw (ex-info "Unsupported test value type"
                                      {:key k :value v :type (type v)})))))
          (.write writer g)))
      (finally (.close writer)))
    path))

(defn- open-fresh-store
  "Open a konserve filestore in a fresh tempdir. Returns [store dir]."
  []
  (let [dir (str "/tmp/stratum-parquet-test-store-" (System/nanoTime))
        _ (.mkdirs (File. dir))
        cfg {:backend :file
             :path dir
             :id (java.util.UUID/nameUUIDFromBytes (.getBytes ^String dir "UTF-8"))}
        store (if (kstore/store-exists? cfg {:sync? true})
                (kstore/connect-store cfg {:sync? true})
                (kstore/create-store cfg {:sync? true}))]
    [store dir]))

(defn- delete-recursively! [^String path]
  (let [f (File. path)]
    (when (.exists f)
      (when (.isDirectory f)
        (doseq [child (.listFiles f)]
          (delete-recursively! (.getAbsolutePath ^File child))))
      (.delete f))))

(def ^:private trades-schema
  "message trades {
     required binary sym (STRING);
     required int64 ts;
     required double px;
     optional binary tag (STRING);
   }")

(defn- gen-trades [^long n]
  (mapv (fn [i]
          (cond-> {"sym" (case (mod i 3) 0 "AAPL" 1 "MSFT" 2 "GOOG")
                   "ts"  (long i)
                   "px"  (* 100.0 (inc i))}
            (zero? (mod i 5)) (assoc "tag" (str "t" (mod i 7)))))
        (range n)))

;; ============================================================================
;; from-parquet (heap path) — pre-allocated arrays + dict at ingest
;; ============================================================================

(deftest from-parquet-roundtrip-test
  (testing "from-parquet reads parquet into a queryable dataset"
    (let [path (temp-parquet "from-rt")]
      (try
        (write-parquet! path trades-schema (gen-trades 100))
        (let [ds (parquet/from-parquet (.getAbsolutePath path))]
          (is (= 100 (dataset/row-count ds)))
          (is (= #{:sym :ts :px :tag} (set (dataset/column-names ds))))
          ;; Dict-encoded sym: group-by returns string values
          (let [grouped (q/q {:from ds :agg [[:count]] :group [:sym]})
                by-sym (into {} (map (fn [r] [(:sym r) (long (:count r))])) grouped)]
            (is (= 34 (get by-sym "AAPL")))
            (is (= 33 (get by-sym "MSFT")))
            (is (= 33 (get by-sym "GOOG"))))
          ;; NULL handling on tag (every 5th row has a value)
          (is (= 20 (long (:count (first (q/q {:from ds :where [[:is-not-null :tag]] :agg [[:count]]}))))))
          (is (= 80 (long (:count (first (q/q {:from ds :where [[:is-null :tag]] :agg [[:count]]})))))))
        (finally (.delete path))))))

(deftest from-parquet-limit-test
  (testing "from-parquet :limit caps the row count"
    (let [path (temp-parquet "from-limit")]
      (try
        (write-parquet! path trades-schema (gen-trades 1000))
        (let [ds (parquet/from-parquet (.getAbsolutePath path) :limit 50)]
          (is (= 50 (dataset/row-count ds))))
        (finally (.delete path))))))

(deftest from-parquet-column-filter-test
  (testing "from-parquet :columns projects only requested columns"
    (let [path (temp-parquet "from-cols")]
      (try
        (write-parquet! path trades-schema (gen-trades 30))
        (let [ds (parquet/from-parquet (.getAbsolutePath path)
                                       :columns ["sym" "px"])]
          (is (= 30 (dataset/row-count ds)))
          (is (= #{:sym :px} (set (dataset/column-names ds)))))
        (finally (.delete path))))))

;; ============================================================================
;; index-parquet! (streaming path) — chunks + persistence
;; ============================================================================

(deftest index-parquet-roundtrip-test
  (testing "index-parquet! ingests a parquet directly into a chunked, persisted dataset"
    (let [path (temp-parquet "idx-rt")
          [store store-dir] (open-fresh-store)]
      (try
        (write-parquet! path trades-schema (gen-trades 100))
        (let [ds (parquet/index-parquet! (.getAbsolutePath path) store "main"
                                         :chunk-size 32 :sync-every 4)]
          (is (= 100 (dataset/row-count ds)))
          (is (= #{:sym :ts :px :tag} (set (dataset/column-names ds))))
          ;; All cols should be index-backed (streaming path)
          (doseq [c [:sym :ts :px :tag]]
            (let [col-data (get (dataset/columns ds) c)]
              (is (= :index (:source col-data)) (str c " should be index-backed"))))
          ;; Dict on string columns
          (is (some? (:dict (get (dataset/columns ds) :sym))))
          (is (some? (:dict (get (dataset/columns ds) :tag))))
          ;; Round-trip sample query
          (let [rows (q/q {:from ds :where [[:< :ts 3]] :select [:ts :sym :px :tag]})]
            (is (= 3 (count rows)))
            (is (= ["AAPL" "MSFT" "GOOG"] (mapv :sym rows)))
            (is (= [100.0 200.0 300.0] (mapv :px rows)))
            (is (= "t0" (:tag (first rows)))))
          ;; Aggregate with group-by on dict-encoded column
          (let [grouped (q/q {:from ds :agg [[:count]] :group [:sym]})
                by-sym (into {} (map (fn [r] [(:sym r) (long (:count r))])) grouped)]
            (is (= 34 (get by-sym "AAPL")))
            (is (= 33 (get by-sym "MSFT")))
            (is (= 33 (get by-sym "GOOG")))))
        (finally
          (.delete path)
          (delete-recursively! store-dir))))))

(deftest index-parquet-reload-test
  (testing "Persisted dataset can be reloaded from the store and queried"
    (let [path (temp-parquet "idx-reload")
          [store store-dir] (open-fresh-store)]
      (try
        (write-parquet! path trades-schema (gen-trades 200))
        (parquet/index-parquet! (.getAbsolutePath path) store "main"
                                :chunk-size 64 :sync-every 2)
        (let [reloaded (dataset/load store "main")]
          (is (= 200 (dataset/row-count reloaded)))
          (let [rows (q/q {:from reloaded :where [[:= :ts 50]] :select [:sym :ts :px]})]
            (is (= 1 (count rows)))
            (is (= "GOOG" (:sym (first rows))))
            (is (= 50 (long (:ts (first rows)))))
            (is (= 5100.0 (:px (first rows))))))
        (finally
          (.delete path)
          (delete-recursively! store-dir))))))

(deftest index-parquet-bounded-memory-test
  (testing "Streaming path handles a parquet larger than chunk-size × 2 with multiple syncs"
    (let [path (temp-parquet "idx-many")
          [store store-dir] (open-fresh-store)]
      (try
        (write-parquet! path trades-schema (gen-trades 500))
        (let [;; Tiny chunks + sync-every=2 forces ~250 chunks per col, ~125 syncs
              ds (parquet/index-parquet! (.getAbsolutePath path) store "main"
                                         :chunk-size 4 :sync-every 2)]
          (is (= 500 (dataset/row-count ds)))
          (let [grouped (q/q {:from ds :agg [[:count] [:sum :ts]] :group [:sym]})
                by-sym (into {} (map (fn [r] [(:sym r) [(long (:count r)) (long (:sum r))]])) grouped)]
            ;; sym=AAPL appears at i=0,3,6,…,498 → 167 rows; sum-of-ts = sum of those i's
            (is (= 167 (first (get by-sym "AAPL"))))
            (is (= 167 (first (get by-sym "MSFT"))))
            (is (= 166 (first (get by-sym "GOOG"))))))
        (finally
          (.delete path)
          (delete-recursively! store-dir))))))

(deftest index-parquet-null-column-test
  (testing "Optional column with NULLs in some chunks (including all-NULL trailing chunk)"
    (let [path (temp-parquet "idx-null")
          [store store-dir] (open-fresh-store)]
      (try
        ;; 50 rows; tag is NULL except every 7th row
        (let [rows (mapv (fn [i]
                           (cond-> {"sym" "X" "ts" (long i) "px" 1.0}
                             (zero? (mod i 7)) (assoc "tag" (str "v" i))))
                         (range 50))]
          (write-parquet! path trades-schema rows))
        ;; chunk-size 8 → 6 full chunks + 1 partial of 2; the partial is all-NULL for tag
        (let [ds (parquet/index-parquet! (.getAbsolutePath path) store "main"
                                         :chunk-size 8 :sync-every 2)]
          (is (= 50 (dataset/row-count ds)))
          (let [non-null (q/q {:from ds :where [[:is-not-null :tag]] :select [:ts :tag]})
                null     (q/q {:from ds :where [[:is-null :tag]] :agg [[:count]]})]
            ;; Non-NULL at 0,7,14,21,28,35,42,49 → 8 rows
            (is (= 8 (count non-null)))
            (is (= 42 (long (:count (first null)))))))
        (finally
          (.delete path)
          (delete-recursively! store-dir))))))

;; ============================================================================
;; Legacy on-disk fixture tests (skipped if no .parquet under data/)
;; ============================================================================

(deftest from-parquet-missing-file-test
  (testing "Reading non-existent file throws"
    (is (thrown? Exception
                 (parquet/from-parquet "/tmp/nonexistent-stratum-test.parquet")))))

(deftest from-parquet-on-disk-fixture-test
  (testing "Read parquet file if available in data directory"
    (let [test-files (filter #(.endsWith (str %) ".parquet")
                             (try (file-seq (clojure.java.io/file "data"))
                                  (catch Exception _ [])))]
      (when (seq test-files)
        (let [path (str (first test-files))
              ds (parquet/from-parquet path :limit 100)]
          (is (satisfies? dataset/IDataset ds))
          (is (pos? (count (dataset/column-names ds))))
          (is (= 100 (dataset/row-count ds)))
          (doseq [col-name (dataset/column-names ds)]
            (is (keyword? col-name))
            (is (some? (dataset/column-type ds col-name)))))))))
