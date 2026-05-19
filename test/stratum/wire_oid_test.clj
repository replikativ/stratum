(ns stratum.wire-oid-test
  "Step 4a: column-meta-aware result formatting. Confirms that declared
   per-column types surface as the right PG OID on the wire instead of
   OID_INT8 / OID_TEXT."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.server :as srv])
  (:import [stratum.internal PgWireServer$QueryResult]))

(def ^:private port-counter (atom 5900))
(defn- next-port [] (swap! port-counter inc))

(defn- run-sql [server sql]
  (let [^PgWireServer$QueryResult qr
        (@(requiring-resolve 'stratum.server/execute-sql)
         sql (:registry server) (:data-dir server) (:store server))]
    {:tag   (.commandTag qr)
     :error (.error qr)
     :oids  (when (.columnOids qr) (vec (.columnOids qr)))
     :rows  (when (.rows qr) (mapv vec (vec (.rows qr))))}))

(defmacro ^:private with-server [[binding-name] & body]
  `(let [~binding-name (srv/start {:port (next-port)})]
     (try ~@body (finally (srv/stop ~binding-name)))))

;; ============================================================================
;; PG OID constants — sanity check the test against expected values

(def OID_INT8      20)
(def OID_FLOAT8    701)
(def OID_TEXT      25)
(def OID_DATE      1082)
(def OID_TIMESTAMP 1114)

;; ============================================================================
;; DATE column → OID_DATE

(deftest date-column-emits-oid-date
  (with-server [s]
    (run-sql s "CREATE TABLE t (d DATE)")
    (run-sql s "INSERT INTO t (d) VALUES (20467)")     ; epoch-day for 2026-01-15
    (let [r (run-sql s "SELECT d FROM t")]
      (is (= 1 (count (:rows r))))
      (is (= [OID_DATE] (:oids r))
          "DATE column must emit OID_DATE (1082), not OID_INT8"))))

;; ============================================================================
;; TIMESTAMP precision variants → OID_TIMESTAMP

(deftest timestamp-column-emits-oid-timestamp
  (with-server [s]
    (run-sql s "CREATE TABLE t (ts TIMESTAMP)")
    (run-sql s "INSERT INTO t (ts) VALUES (1700000000000000)")
    (let [r (run-sql s "SELECT ts FROM t")]
      (is (= 1 (count (:rows r))))
      (is (= [OID_TIMESTAMP] (:oids r))
          "TIMESTAMP column must emit OID_TIMESTAMP (1114), not OID_INT8"))))

(deftest timestamp-ms-column-emits-oid-timestamp
  (with-server [s]
    (run-sql s "CREATE TABLE t (ts TIMESTAMP_MS)")
    (run-sql s "INSERT INTO t (ts) VALUES (1700000000000)")
    (let [r (run-sql s "SELECT ts FROM t")]
      (is (= 1 (count (:rows r))))
      (is (= [OID_TIMESTAMP] (:oids r))
          "TIMESTAMP_MS column also maps to OID_TIMESTAMP — PG has only one timestamp OID"))))

(deftest timestamp-s-column-emits-oid-timestamp
  (with-server [s]
    (run-sql s "CREATE TABLE t (ts TIMESTAMP_S)")
    (run-sql s "INSERT INTO t (ts) VALUES (1700000000)")
    (let [r (run-sql s "SELECT ts FROM t")]
      (is (= 1 (count (:rows r))))
      (is (= [OID_TIMESTAMP] (:oids r))))))

;; ============================================================================
;; ENUM column → enum's allocated OID

(deftest enum-column-emits-enum-oid
  (with-server [s]
    (run-sql s "CREATE TYPE mood AS ENUM ('sad','ok','happy')")
    (run-sql s "CREATE TABLE pet (m mood)")
    (run-sql s "INSERT INTO pet (m) VALUES ('happy')")
    (let [enum-oid (-> @(:registry s) (get "__enums__") (get "mood") :oid)
          r (run-sql s "SELECT m FROM pet")]
      (is (pos? enum-oid))
      (is (= [enum-oid] (:oids r))
          "ENUM column must emit the allocated user-type OID, not OID_TEXT"))))

;; ============================================================================
;; Mixed: column-ref + computed expression — meta-driven for the column,
;; value-inference for the expression

(deftest mixed-column-and-expression
  (with-server [s]
    (run-sql s "CREATE TABLE t (id INT, d DATE)")
    (run-sql s "INSERT INTO t (id, d) VALUES (1, 20467)")
    (let [r (run-sql s "SELECT id, d, id + 1 AS computed FROM t")]
      (is (= 1 (count (:rows r))))
      ;; id → meta says int8, d → meta says date, computed → no meta
      ;; (alias not in registry) so falls back to value-inference. The
      ;; engine promotes `id + 1` to double in projection, so the
      ;; fallback emits OID_FLOAT8 — that's the existing behaviour the
      ;; test is asserting preserved.
      (is (= [OID_INT8 OID_DATE OID_FLOAT8] (:oids r))))))

;; ============================================================================
;; Backwards compat: tables registered programmatically without column-schema
;; metadata still inherit value-based inference.

(deftest programmatic-register-table-keeps-value-inference
  (with-server [s]
    (srv/register-table! s "raw"
                         {:x (long-array [1 2 3])
                          :y (double-array [1.5 2.5 3.5])})
    (let [r (run-sql s "SELECT x, y FROM raw ORDER BY x")]
      (is (= [OID_INT8 OID_FLOAT8] (:oids r))
          "register-table! columns have no schema metadata → value-based inference"))))
