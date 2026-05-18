(ns stratum.server-restart-test
  "End-to-end restart-cycle tests for stratum.server with a :store
   configured: CREATE TABLE + INSERT survive process boundaries; new
   INSERTs after restart correctly append."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as kstore]
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
