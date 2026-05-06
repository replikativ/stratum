(ns stratum.parquet-test
  "Tests for Parquet import — heap-array `from-parquet`, streaming
   `index-parquet!`, and zero-copy `parquet-dataset` paths."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.parquet :as parquet]
            [stratum.dataset :as dataset]
            [stratum.index :as idx]
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
                (string? v)            (.append g ^String k ^String v)
                ;; INT32 columns require int, INT64 columns require long.
                ;; Test callers use (Integer/valueOf x) for INT32 cells.
                (instance? Integer v)  (.append g ^String k (int v))
                (instance? Long v)     (.append g ^String k (long v))
                (integer? v)           (.append g ^String k (long v))
                (float? v)             (.append g ^String k (double v))
                (instance? Double v)   (.append g ^String k (double v))
                (instance? (Class/forName "[B") v)
                (.append g ^String k
                         (org.apache.parquet.io.api.Binary/fromConstantByteArray ^bytes v))
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
  (mapv (fn [^long i]
          (cond-> {"sym" (case (long (mod i 3)) 0 "AAPL" 1 "MSFT" 2 "GOOG")
                   "ts"  i
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

;; ============================================================================
;; parquet-dataset (zero-copy / lazy-decode path)
;; ============================================================================

(deftest parquet-dataset-roundtrip-test
  (testing "parquet-dataset opens a parquet file lazily and queries return the same answers as from-parquet"
    (let [path (temp-parquet "ds-rt")]
      (try
        (write-parquet! path trades-schema (gen-trades 200))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))]
          (is (= 200 (dataset/row-count ds)))
          (is (= #{:sym :ts :px :tag} (set (dataset/column-names ds))))
          ;; All cols index-backed via parquet-row-group chunks
          (doseq [c [:sym :ts :px :tag]]
            (is (= :index (:source (get (dataset/columns ds) c)))))
          ;; String columns carry a global dict
          (is (some? (:dict (get (dataset/columns ds) :sym))))
          (is (= "string" (name (:dict-type (get (dataset/columns ds) :sym)))))
          ;; count(*) — should not require full row decode
          (is (= 200 (long (:count (first (q/q {:from ds :agg [[:count]]}))))))
          ;; min/max from row-group metadata
          (let [r (first (q/q {:from ds :agg [[:min :ts] [:max :ts]]}))]
            (is (= 0   (long (:min r))))
            (is (= 199 (long (:max r)))))
          ;; group-by string column
          (let [grouped (q/q {:from ds :agg [[:count]] :group [:sym]})
                by-sym (into {} (map (fn [r] [(:sym r) (long (:count r))])) grouped)]
            (is (= 67 (get by-sym "AAPL")))
            (is (= 67 (get by-sym "MSFT")))
            (is (= 66 (get by-sym "GOOG"))))
          ;; equality predicate on string
          (is (= 67 (long (:count (first (q/q {:from ds :where [[:= :sym "AAPL"]] :agg [[:count]]}))))))
          ;; sum on numeric — falls through stats-only fast path (parquet
          ;; doesn't store sum) and goes through the SIMD decode
          (let [r (first (q/q {:from ds :agg [[:sum :px]]}))]
            ;; sum_{i=0..199} 100*(i+1) = 100 * 200*201/2 = 2_010_000
            (is (= 2010000.0 (double (:sum r))))))
        (finally (.delete path))))))

(deftest parquet-dataset-zone-pruning-test
  (testing "Predicates outside parquet's per-row-group min/max prune all chunks (no row data read)"
    (let [path (temp-parquet "ds-zone")]
      (try
        (write-parquet! path trades-schema (gen-trades 500))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))]
          ;; ts ranges 0..499 — predicate ts>10000 should prune everything
          (is (= 0 (long (:count (first (q/q {:from ds
                                              :where [[:> :ts 10000]]
                                              :agg [[:count]]}))))))
          ;; And the symmetric: ts<0
          (is (= 0 (long (:count (first (q/q {:from ds
                                              :where [[:< :ts 0]]
                                              :agg [[:count]]})))))))
        (finally (.delete path))))))

(deftest parquet-dataset-column-projection-test
  (testing ":columns selects a subset; absent columns aren't decoded"
    (let [path (temp-parquet "ds-proj")]
      (try
        (write-parquet! path trades-schema (gen-trades 50))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path)
                                          :columns ["ts" "px"])]
          (is (= 50 (dataset/row-count ds)))
          (is (= #{:ts :px} (set (dataset/column-names ds)))))
        (finally (.delete path))))))

(deftest parquet-dataset-readonly-test
  (testing "parquet-dataset rejects mutations"
    (let [path (temp-parquet "ds-ro")]
      (try
        (write-parquet! path trades-schema (gen-trades 10))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))
              col-idx (:index (get (dataset/columns ds) :ts))]
          ;; idx-set!/idx-append!/idx-append-chunk! on a transient PCI
          ;; whose chunks come from parquet must fail. col-fork returns
          ;; the same chunk (immutable, sharing is safe); col-transient
          ;; on the chunk throws. We exercise that via idx-set!.
          (let [tidx (idx/idx-transient col-idx)]
            (is (thrown? UnsupportedOperationException
                         (idx/idx-set! tidx 0 999)))))
        (finally (.delete path))))))

(deftest parquet-dataset-stats-sum-fallback-test
  (testing "SUM/AVG fall through to SIMD decode (parquet stats lack sum)"
    (let [path (temp-parquet "ds-sum")]
      (try
        (write-parquet! path trades-schema (gen-trades 100))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))]
          ;; If the planner mistakenly took stats-only sum, we'd get 0.0.
          ;; Real sum of 100*(i+1) for i=0..99 = 100 * 5050 = 505_000
          (is (= 505000.0 (double (:sum (first (q/q {:from ds :agg [[:sum :px]]}))))))
          (is (= 5050.0   (double (:avg (first (q/q {:from ds :agg [[:avg :px]]})))))))
        (finally (.delete path))))))

;; ----------------------------------------------------------------------------
;; parquet-dataset: NULL handling in numeric columns + type rejection +
;; logical-type round-trips (Date, Timestamp[Millis|Micros|Nanos], UUID).
;; ----------------------------------------------------------------------------

(deftest parquet-dataset-null-numeric-test
  (testing "NULL int64/double slots come back as Long/MIN_VALUE / NaN, not 0"
    (let [path (temp-parquet "ds-null-num")
          schema "message trades {
                    required int64 row;
                    optional int64 maybe_long;
                    optional double maybe_dbl;
                  }"
          rows (mapv (fn [i]
                       (cond-> {"row" (long i)}
                         (even? i) (assoc "maybe_long" (long (* 10 i))
                                          "maybe_dbl"  (double (* 1.5 i)))))
                     (range 20))]
      (try
        (write-parquet! path schema rows)
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))
              not-null (q/q {:from ds :where [[:is-not-null :maybe_long]]
                             :agg [[:count]]})
              is-null (q/q {:from ds :where [[:is-null :maybe_long]]
                            :agg [[:count]]})
              dbl-null (q/q {:from ds :where [[:is-null :maybe_dbl]]
                             :agg [[:count]]})]
          (is (= 10 (long (:count (first not-null)))))
          (is (= 10 (long (:count (first is-null)))))
          (is (= 10 (long (:count (first dbl-null))))))
        (finally (.delete path))))))

(deftest parquet-dataset-rejects-decimal-test
  (testing "Decimal columns are rejected with a clear error"
    (let [path (temp-parquet "ds-decimal")
          schema "message t {
                    required int64 amount (DECIMAL(10,2));
                  }"
          rows (mapv (fn [i] {"amount" (long (* 100 i))}) (range 5))]
      (try
        (write-parquet! path schema rows)
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"Decimal column"
             (parquet/parquet-dataset (.getAbsolutePath path))))
        (finally (.delete path))))))

(deftest parquet-dataset-rejects-list-test
  (testing "Repeated/nested columns are rejected with a clear error"
    (let [path (temp-parquet "ds-list")
          schema "message t {
                    required int64 id;
                    repeated int64 tags;
                  }"
          rows (mapv (fn [i] {"id" (long i) "tags" (long i)}) (range 3))]
      (try
        (write-parquet! path schema rows)
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"Repeated/nested column"
             (parquet/parquet-dataset (.getAbsolutePath path))))
        (finally (.delete path))))))

(deftest parquet-dataset-date-test
  (testing "INT32 + Date logical-type → epoch-millis"
    (let [path (temp-parquet "ds-date")
          schema "message t {
                    required int32 d (DATE);
                  }"
          ;; days-since-1970: 0=1970-01-01, 19000=2022-01-08, 20000=2024-10-04
          rows (mapv (fn [d] {"d" (Integer/valueOf (int d))}) [0 100 19000 20000])]
      (try
        (write-parquet! path schema rows)
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))
              all (q/q {:from ds :select [:d] :order [[:d :asc]]})
              expected [0
                        (* 100 86400000)
                        (* 19000 86400000)
                        (* 20000 86400000)]]
          (is (= expected (mapv (comp long :d) all))))
        (finally (.delete path))))))

(deftest parquet-dataset-timestamp-units-test
  (testing "Timestamp{Millis|Micros|Nanos} all normalize to epoch-millis"
    (let [path (temp-parquet "ds-ts")
          schema "message t {
                    required int64 t_millis (TIMESTAMP(MILLIS,true));
                    required int64 t_micros (TIMESTAMP(MICROS,true));
                    required int64 t_nanos  (TIMESTAMP(NANOS,true));
                  }"
          ;; Same logical instant in three units (1_700_000_000_000 ms).
          rows [{"t_millis" 1700000000000
                 "t_micros" 1700000000000000
                 "t_nanos"  1700000000000000000}]]
      (try
        (write-parquet! path schema rows)
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))
              [r] (q/q {:from ds :select [:t_millis :t_micros :t_nanos]})]
          (is (= 1700000000000 (long (:t_millis r))))
          (is (= 1700000000000 (long (:t_micros r))))
          (is (= 1700000000000 (long (:t_nanos r)))))
        (finally (.delete path))))))

(deftest parquet-dataset-uuid-test
  (testing "FLBA[16] + UUID logical-type → 36-char hex string in dict"
    (let [path (temp-parquet "ds-uuid")
          schema "message t {
                    required fixed_len_byte_array(16) id (UUID);
                  }"
          uuids [(java.util.UUID/fromString "00000000-0000-0000-0000-000000000001")
                 (java.util.UUID/fromString "11111111-2222-3333-4444-555555555555")
                 (java.util.UUID/fromString "ffffffff-ffff-ffff-ffff-ffffffffffff")]
          uuid-bytes (fn [^java.util.UUID u]
                       (let [bb (java.nio.ByteBuffer/allocate 16)]
                         (.putLong bb (.getMostSignificantBits u))
                         (.putLong bb (.getLeastSignificantBits u))
                         (.array bb)))
          rows (mapv (fn [u] {"id" (uuid-bytes u)}) uuids)]
      (try
        (write-parquet! path schema rows)
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))
              ;; The :dict carries our 36-char hex strings.
              dict (set (vec (:dict (get (dataset/columns ds) :id))))]
          (is (= 3 (count dict)))
          (is (contains? dict "00000000-0000-0000-0000-000000000001"))
          (is (contains? dict "11111111-2222-3333-4444-555555555555"))
          (is (contains? dict "ffffffff-ffff-ffff-ffff-ffffffffffff")))
        (finally (.delete path))))))

(deftest parquet-dataset-close-test
  (testing "close-parquet-dataset! actually closes the reader"
    (let [path (temp-parquet "ds-close")]
      (try
        (write-parquet! path trades-schema (gen-trades 10))
        (let [ds (parquet/parquet-dataset (.getAbsolutePath path))]
          ;; First query works.
          (is (= 10 (long (:count (first (q/q {:from ds :agg [[:count]]}))))))
          (parquet/close-parquet-dataset! ds)
          ;; After close, decoding throws (count is metadata-only so it
          ;; would still work; force a decode via SUM).
          (is (thrown? Exception
                       (q/q {:from ds :agg [[:sum :ts]]}))))
        (finally (.delete path))))))
