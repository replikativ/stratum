(ns stratum.server-restart-test
  "End-to-end restart-cycle tests for stratum.server with a :store
   configured: CREATE TABLE + INSERT survive process boundaries; new
   INSERTs after restart correctly append."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as kstore]
            [stratum.dataset]
            [stratum.server :as srv])
  (:import [java.io File]
           [java.util UUID]
           [stratum.internal PgWireServer$QueryResult]))

(defn- temp-dir ^String []
  (let [d (File/createTempFile "stratum-restart-" "")]
    (.delete d)
    (.mkdirs d)
    (.getAbsolutePath d)))

(defn- delete-dir [^String path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [^File child (reverse (file-seq f))]
        (.delete child)))))

(defn- file-store-at [^String path]
  (let [cfg {:backend :file
             :path    path
             :id      (UUID/nameUUIDFromBytes (.getBytes path "UTF-8"))}]
    (if (kstore/store-exists? cfg {:sync? true})
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store  cfg {:sync? true}))))

;; Counter to give each test a distinct port — avoids "Address already
;; in use" if tests are re-run before the previous process bound the
;; socket got reclaimed.
(def ^:private port-counter (atom 5560))

(defn- next-port [] (swap! port-counter inc))

(defn- run-sql [server sql]
  (let [^PgWireServer$QueryResult qr
        (@(requiring-resolve 'stratum.server/execute-sql)
         sql (:registry server) (:data-dir server) (:store server))]
    {:tag   (.commandTag qr)
     :error (.error qr)
     :rows  (when (.rows qr) (mapv vec (vec (.rows qr))))}))

(defmacro ^:private with-server [[binding-name opts] & body]
  `(let [~binding-name (srv/start ~opts)]
     (try ~@body
          (finally (srv/stop ~binding-name)))))

;; ============================================================================
;; CREATE TABLE persistence

(deftest create-table-persists-across-restart
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (let [r (run-sql s "CREATE TABLE foo (id INT, price DOUBLE)")]
            (is (nil? (:error r)))
            (is (= "CREATE TABLE" (:tag r))))))

      ;; Reopen the store, verify table is hydrated
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (contains? @(:registry s) "foo")
              "Table 'foo' must be in registry after restart")
          (let [r (run-sql s "SELECT id, price FROM foo")]
            (is (nil? (:error r)))
            (is (empty? (:rows r)) "Empty table after restart"))))
      (finally (delete-dir path)))))

;; ============================================================================
;; INSERT + restart round-trip

(deftest insert-survives-restart
  (let [path (temp-dir)]
    (try
      ;; Session 1: create + insert
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (nil? (:error (run-sql s "CREATE TABLE nums (id INT, price DOUBLE)"))))
          (is (nil? (:error (run-sql s "INSERT INTO nums VALUES (1, 9.5)"))))
          (is (nil? (:error (run-sql s "INSERT INTO nums VALUES (2, 19.5)"))))
          ;; Sanity within same session
          (is (= [["1" "9.5"] ["2" "19.5"]]
                 (:rows (run-sql s "SELECT id, price FROM nums ORDER BY id"))))))

      ;; Session 2: confirm rows survive
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= [["1" "9.5"] ["2" "19.5"]]
                 (:rows (run-sql s "SELECT id, price FROM nums ORDER BY id")))
              "Inserted rows must survive restart")
          ;; Append after restart still works
          (is (nil? (:error (run-sql s "INSERT INTO nums VALUES (3, 29.5)"))))
          (is (= [["1" "9.5"] ["2" "19.5"] ["3" "29.5"]]
                 (:rows (run-sql s "SELECT id, price FROM nums ORDER BY id")))
              "Post-restart INSERTs append to existing rows")))

      ;; Session 3: verify session-2's INSERT also survives
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= [["1" "9.5"] ["2" "19.5"] ["3" "29.5"]]
                 (:rows (run-sql s "SELECT id, price FROM nums ORDER BY id")))
              "Rows accumulated across multiple restarts must all survive")))
      (finally (delete-dir path)))))

;; ============================================================================
;; INSERT without explicit column list — relies on column-order metadata

(deftest insert-without-column-list-uses-declared-order
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TABLE t (a INT, b DOUBLE)")
          ;; INSERT INTO t VALUES (1, 2.0) — no column list. Must
          ;; bind positionally to declaration order (a, b).
          (is (nil? (:error (run-sql s "INSERT INTO t VALUES (1, 2.0)"))))
          (is (= [["1" "2.0"]]
                 (:rows (run-sql s "SELECT a, b FROM t"))))))

      ;; Same after restart — the column-order metadata is rebuilt
      ;; from the durable record
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (nil? (:error (run-sql s "INSERT INTO t VALUES (5, 6.0)"))))
          (is (= [["1" "2.0"] ["5" "6.0"]]
                 (:rows (run-sql s "SELECT a, b FROM t ORDER BY a"))))))
      (finally (delete-dir path)))))

;; ============================================================================
;; Multi-table

(deftest multiple-tables-roundtrip
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TABLE a (x INT)")
          (run-sql s "CREATE TABLE b (y DOUBLE)")
          (run-sql s "INSERT INTO a VALUES (1)")
          (run-sql s "INSERT INTO a VALUES (2)")
          (run-sql s "INSERT INTO b VALUES (10.5)")))

      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= #{"a" "b"} (set (keys @(:registry s)))))
          (is (= [["1"] ["2"]] (:rows (run-sql s "SELECT x FROM a ORDER BY x"))))
          (is (= [["10.5"]] (:rows (run-sql s "SELECT y FROM b"))))))
      (finally (delete-dir path)))))

;; ============================================================================
;; Backwards compatibility: no :store still works (with warning)

(deftest no-store-still-works
  ;; The warning is logged but the server must keep functioning —
  ;; existing programmatic users have not opted into :store.
  (with-server [s {:port (next-port)}]
    (is (nil? (:store s)))
    (is (nil? (:error (run-sql s "CREATE TABLE t (id INT)"))))
    (is (nil? (:error (run-sql s "INSERT INTO t VALUES (42)"))))
    (is (= [["42"]] (:rows (run-sql s "SELECT id FROM t"))))))

;; ============================================================================
;; R3: trained models survive restart

(deftest create-model-persists-across-restart
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          ;; Build training data
          (run-sql s "CREATE TABLE sensors (temp DOUBLE, hum DOUBLE)")
          (doseq [vals [[20.0 50.0] [21.0 52.0] [19.5 48.0] [22.0 55.0] [20.5 51.0]
                        [100.0 5.0]  ; outlier
                        [21.5 53.0] [19.0 47.0] [20.8 50.5] [21.2 52.5]]]
            (run-sql s (format "INSERT INTO sensors VALUES (%s, %s)" (first vals) (second vals))))
          (is (nil? (:error (run-sql s "CREATE MODEL m TYPE ISOLATION_FOREST OPTIONS (n_trees = 50, sample_size = 8, seed = 42, contamination = 0.1) AS SELECT temp, hum FROM sensors"))))
          (is (contains? (get @(:registry s) "__models__") "m"))))

      ;; Reopen — model must be hydrated back
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (contains? (get @(:registry s) "__models__") "m")
              "Trained model 'm' must be in registry after restart")
          ;; ANOMALY_SCORE works against the rehydrated model
          (let [r (run-sql s "SELECT ANOMALY_SCORE('m', 100.0, 5.0) AS score")]
            (is (nil? (:error r)))
            (is (= 1 (count (:rows r)))))))
      (finally (delete-dir path)))))

(deftest drop-model-clears-durable-record
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TABLE sensors (temp DOUBLE, hum DOUBLE)")
          (doseq [vals [[1.0 2.0] [3.0 4.0] [5.0 6.0] [7.0 8.0] [9.0 10.0]
                        [11.0 12.0] [13.0 14.0] [15.0 16.0]]]
            (run-sql s (format "INSERT INTO sensors VALUES (%s, %s)" (first vals) (second vals))))
          (run-sql s "CREATE MODEL m TYPE ISOLATION_FOREST OPTIONS (n_trees = 10, sample_size = 4, seed = 1) AS SELECT temp, hum FROM sensors")
          (is (contains? (get @(:registry s) "__models__") "m"))
          (run-sql s "DROP MODEL m")
          (is (not (contains? (get @(:registry s) "__models__") "m"))
              "Model dropped from in-memory registry")))
      ;; After restart the model must not reappear
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (not (contains? (get @(:registry s) "__models__") "m"))
              "Dropped model must not be rehydrated")))
      (finally (delete-dir path)))))

;; ============================================================================
;; R4: live-table bindings survive restart

(deftest live-table-binding-persists
  (let [path (temp-dir)]
    (try
      ;; Session 1: create a dataset via the Clojure API + register-live-table!
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (let [ds (-> (stratum.dataset/make-dataset
                        {:k (long-array [1 2 3]) :v (double-array [10.0 20.0 30.0])}
                        {:name "live"})
                       stratum.dataset/ensure-indexed
                       (stratum.dataset/sync! store "live-branch"))]
            (srv/register-live-table! s "lt" store "live-branch")
            (is (= [["1" "10.0"] ["2" "20.0"] ["3" "30.0"]]
                   (:rows (run-sql s "SELECT k, v FROM lt ORDER BY k")))))))
      ;; Session 2: binding must come back with no user action
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (contains? @(:registry s) "lt")
              "Live-table binding 'lt' must be hydrated after restart")
          (is (= [["1" "10.0"] ["2" "20.0"] ["3" "30.0"]]
                 (:rows (run-sql s "SELECT k, v FROM lt ORDER BY k"))))))
      (finally (delete-dir path)))))

(deftest unregister-table-clears-durable-binding
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (-> (stratum.dataset/make-dataset {:x (long-array [42])} {:name "x"})
              stratum.dataset/ensure-indexed
              (stratum.dataset/sync! store "x-branch"))
          (srv/register-live-table! s "lt" store "x-branch")
          (is (contains? @(:registry s) "lt"))
          (srv/unregister-table! s "lt")
          (is (not (contains? @(:registry s) "lt")))))
      ;; After restart, the live-table must NOT reappear
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (not (contains? @(:registry s) "lt"))
              "Unregistered live-table must not be rehydrated")))
      (finally (delete-dir path)))))

;; ============================================================================
;; Defensive: registry-only and live-table from a foreign store

;; ============================================================================
;; R2.5: dict-string append — TEXT columns now survive restart

(deftest text-column-survives-restart
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (nil? (:error (run-sql s "CREATE TABLE pet (name TEXT, weight DOUBLE)"))))
          (is (nil? (:error (run-sql s "INSERT INTO pet (name, weight) VALUES ('rex', 10.0)"))))
          (is (nil? (:error (run-sql s "INSERT INTO pet (name, weight) VALUES ('mio', 5.5)"))))))
      ;; Reopen — string column data must survive
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= [["mio" "5.5"] ["rex" "10.0"]]
                 (:rows (run-sql s "SELECT name, weight FROM pet ORDER BY name")))
              "TEXT column data must round-trip across restart")
          ;; Append after restart — dict should extend cleanly
          (is (nil? (:error (run-sql s "INSERT INTO pet (name, weight) VALUES ('rex', 11.0)"))))
          (is (= [["mio" "5.5"] ["rex" "10.0"] ["rex" "11.0"]]
                 (:rows (run-sql s "SELECT name, weight FROM pet ORDER BY name, weight"))))))
      (finally (delete-dir path)))))

(deftest enum-column-data-durable-after-r2-5
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')")
          (run-sql s "CREATE TABLE pet (name TEXT, m mood)")
          (is (nil? (:error (run-sql s "INSERT INTO pet (name, m) VALUES ('rex','happy')"))))
          (is (nil? (:error (run-sql s "INSERT INTO pet (name, m) VALUES ('mio','sad')"))))))
      ;; Reopen — enum column data must survive (full ENUM durability)
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= [["mio" "sad"] ["rex" "happy"]]
                 (:rows (run-sql s "SELECT name, m FROM pet ORDER BY name")))
              "ENUM column data persists end-to-end after R2.5")
          ;; INSERT validation still enforced on the rehydrated table
          (let [r (run-sql s "INSERT INTO pet (name, m) VALUES ('bad','grumpy')")]
            (is (re-find #"invalid input value for enum mood" (or (:error r) ""))))))
      (finally (delete-dir path)))))

(deftest dict-extend-reuses-existing-codes
  ;; Inserting an already-seen string must not duplicate it in the
  ;; dict — the forward map should hit and reuse the existing code.
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TABLE t (name TEXT)")
          (dotimes [_ 5] (run-sql s "INSERT INTO t (name) VALUES ('alice')"))
          (run-sql s "INSERT INTO t (name) VALUES ('bob')")
          (let [dict (-> @(:registry s) (get "t") :name :dict)]
            ;; In transient mode mid-INSERT the dict is an ArrayList;
            ;; resync-durable-table! flips it back to String[]. Either
            ;; way the unique-label count is what we assert.
            (is (= 2 (count (vec dict)))
                "Dict must contain exactly 2 unique labels"))))
      ;; Round-trip through restart for good measure
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (is (= [["alice"] ["alice"] ["alice"] ["alice"] ["alice"] ["bob"]]
                 (:rows (run-sql s "SELECT name FROM t ORDER BY name"))))))
      (finally (delete-dir path)))))

;; ============================================================================
;; Defensive: registry-only and live-table from a foreign store

(deftest foreign-store-live-table-not-persisted
  (let [path-server (temp-dir)
        path-foreign (temp-dir)]
    (try
      (let [server-store (file-store-at path-server)
            foreign-store (file-store-at path-foreign)]
        (with-server [s {:port (next-port) :store server-store}]
          (-> (stratum.dataset/make-dataset {:n (long-array [1 2 3])} {:name "n"})
              stratum.dataset/ensure-indexed
              (stratum.dataset/sync! foreign-store "main"))
          (srv/register-live-table! s "remote" foreign-store "main")
          (is (contains? @(:registry s) "remote") "in this session: yes")))
      ;; After restart with just server-store: foreign-store-backed
      ;; live-table can't be hydrated automatically
      (let [server-store (file-store-at path-server)]
        (with-server [s {:port (next-port) :store server-store}]
          (is (not (contains? @(:registry s) "remote"))
              "Foreign-store live-table is NOT persisted (documented v1 limit)")))
      (finally
        (delete-dir path-server)
        (delete-dir path-foreign)))))
