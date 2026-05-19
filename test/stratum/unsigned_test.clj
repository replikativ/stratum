(ns stratum.unsigned-test
  "Step 6: DuckDB-style unsigned integer columns — UTINYINT, USMALLINT,
   UINTEGER.

   Stratum stores all integers as int64, so unsigned columns share the
   same backing representation but carry a `:unsigned-width` tag
   (8/16/32) so the INSERT path can range-check and the wire layer
   picks a PG-compatible OID (no native unsigned types in PG — u8/u16
   ride INT2 and u32 rides INT8)."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.server :as srv]
            [stratum.sql :as sql])
  (:import [stratum.internal PgWireServer$QueryResult]))

(defn- run-sql [server sql-string]
  (let [^PgWireServer$QueryResult qr
        (@(requiring-resolve 'stratum.server/execute-sql)
         sql-string (:registry server) (:data-dir server) (:store server))]
    {:tag   (.commandTag qr)
     :error (.error qr)
     :oids  (when (.columnOids qr) (vec (.columnOids qr)))
     :rows  (when (.rows qr) (mapv vec (vec (.rows qr))))}))

(defmacro ^:private with-server [[binding-name opts] & body]
  `(let [~binding-name (srv/start ~opts)]
     (try ~@body (finally (srv/stop ~binding-name)))))

(def OID_INT2 21)
(def OID_INT8 20)

;; ---------------------------------------------------------------------------
;; DDL recognition

(deftest ddl-utinyint-creates-int2-on-wire
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UTINYINT)")
    (run-sql s "INSERT INTO t VALUES (255)")
    (let [{:keys [oids rows]} (run-sql s "SELECT id FROM t")]
      (is (= [OID_INT2] oids) "UTINYINT renders as PG INT2 on the wire")
      (is (= [["255"]] rows)))))

(deftest ddl-usmallint-renders-as-int2
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id USMALLINT)")
    (run-sql s "INSERT INTO t VALUES (65535)")
    (let [{:keys [oids rows]} (run-sql s "SELECT id FROM t")]
      (is (= [OID_INT2] oids))
      (is (= [["65535"]] rows)))))

(deftest ddl-uinteger-renders-as-int8
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UINTEGER)")
    (run-sql s "INSERT INTO t VALUES (4294967295)")
    (let [{:keys [oids rows]} (run-sql s "SELECT id FROM t")]
      (is (= [OID_INT8] oids) "UINTEGER renders as PG INT8 (u32 max > INT4 range)")
      (is (= [["4294967295"]] rows)))))

;; ---------------------------------------------------------------------------
;; Range-checking on INSERT — out-of-range values must throw 22003

(deftest insert-utinyint-negative-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UTINYINT)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (-1)")]
      (is (some? error))
      (is (re-find #"out of range" error)
          "negative value into UTINYINT must surface as a range error"))))

(deftest insert-utinyint-overflow-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UTINYINT)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (256)")]
      (is (some? error))
      (is (re-find #"out of range" error)
          "256 overflows UTINYINT (max=255)"))))

(deftest insert-usmallint-overflow-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id USMALLINT)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (65536)")]
      (is (some? error))
      (is (re-find #"out of range" error)))))

(deftest insert-uinteger-overflow-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UINTEGER)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (4294967296)")]
      (is (some? error))
      (is (re-find #"out of range" error)))))

(deftest insert-multiple-rows-some-out-of-range
  (testing "If any row fails range, the INSERT fails atomically — no partial state"
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (id UTINYINT)")
      (let [{:keys [error]}
            (run-sql s "INSERT INTO t VALUES (10), (20), (300), (40)")]
        (is (some? error))
        (is (re-find #"out of range" error)))
      (let [{:keys [rows]} (run-sql s "SELECT COUNT(*) FROM t")]
        (is (= [["0"]] rows)
            "no partial rows must land when any row fails the range check")))))

;; ---------------------------------------------------------------------------
;; Happy-path arithmetic — unsigned columns ride the standard int64 kernels

(deftest unsigned-aggregate-works
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UTINYINT, qty USMALLINT)")
    (run-sql s "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
    ;; SUM over int64-backed columns returns FLOAT8 in Stratum (same as
    ;; for plain INT8 columns) — the unsigned-width tag doesn't change
    ;; the aggregate's output type.
    (let [{:keys [rows]} (run-sql s "SELECT SUM(qty) FROM t")]
      (is (= [["600.0"]] rows)))))

(deftest unsigned-where-filter
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UINTEGER, val UINTEGER)")
    (run-sql s "INSERT INTO t VALUES (1, 1000000), (2, 2000000), (3, 4000000000)")
    (let [{:keys [rows]} (run-sql s "SELECT id FROM t WHERE val > 1500000 ORDER BY id")]
      (is (= [["2"] ["3"]] rows)))))

;; ---------------------------------------------------------------------------
;; NULL handling — NULL is always allowed regardless of the range constraint

(deftest insert-null-into-unsigned-allowed
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UTINYINT)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (NULL)")]
      (is (nil? error)
          "NULL must always be accepted, even on unsigned columns"))))
