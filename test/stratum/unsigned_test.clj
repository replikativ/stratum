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
(def OID_NUMERIC 1700)

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

;; ---------------------------------------------------------------------------
;; Step 8a — UBIGINT (unsigned 64-bit)

(deftest ddl-ubigint-renders-as-numeric
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UBIGINT)")
    (run-sql s "INSERT INTO t VALUES (42)")
    (let [{:keys [oids rows]} (run-sql s "SELECT id FROM t")]
      (is (= [OID_NUMERIC] oids)
          "UBIGINT renders as PG NUMERIC since u64 exceeds INT8 signed range")
      (is (= [["42"]] rows)))))

(deftest ubigint-stores-max-u64
  (testing "UBIGINT accepts 2^64-1 — the maximum unsigned value.
            Note: JSqlParser can't lex integer literals > Long.MAX_VALUE,
            so use string-literal form. Stratum's INSERT path accepts a
            string into a numeric column via the unsigned range check."
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (id UBIGINT)")
      (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES ('18446744073709551615')")]
        (is (nil? error)
            (str "UBIGINT max should INSERT: " error)))
      (let [{:keys [rows]} (run-sql s "SELECT id FROM t")]
        (is (= [["18446744073709551615"]] rows)
            "UBIGINT max round-trips through unsigned rendering")))))

(deftest ubigint-stores-above-int64-max
  (testing "UBIGINT accepts values > Long/MAX_VALUE via string literal.
            Skips 2^63 exactly: its int64 bit pattern is Long/MIN_VALUE,
            which Stratum currently uses as the long-column NULL sentinel.
            See ubigint-cannot-store-exact-2-63 below."
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (id UBIGINT)")
      ;; 2^63 + 1 = 9223372036854775809 — one past the sentinel collision
      (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES ('9223372036854775809')")]
        (is (nil? error)
            (str "9223372036854775809 should INSERT: " error)))
      (let [{:keys [rows]} (run-sql s "SELECT id FROM t")]
        (is (= [["9223372036854775809"]] rows))))))

(deftest ubigint-cannot-store-exact-2-63
  (testing "Documented limitation: 2^63 (= 9223372036854775808) has int64 bit pattern
            Long.MIN_VALUE, which Stratum reserves as the NULL sentinel for long
            columns. UBIGINT values at exactly 2^63 round-trip as NULL.
            Proper fix requires explicit validity bitmap independent of the
            stored bit pattern — significant existing-system refactor."
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (id UBIGINT)")
      (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES ('9223372036854775808')")]
        (is (nil? error) "2^63 is in range — INSERT must succeed"))
      (let [{:keys [rows]} (run-sql s "SELECT id FROM t")]
        ;; Documented as a known limitation: surfaces as NULL because the
        ;; bit pattern collides with the sentinel.
        (is (= [[nil]] rows)
            "Round-trip surfaces as NULL — see docstring")))))

(deftest ubigint-negative-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UBIGINT)")
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES (-1)")]
      (is (some? error))
      (is (re-find #"out of range" error)
          "Negative value into UBIGINT must surface as a range error"))))

(deftest ubigint-overflow-rejected
  (with-server [s {:port 0}]
    (run-sql s "CREATE TABLE t (id UBIGINT)")
    ;; 2^64 = 18446744073709551616 (one past UBIGINT max)
    (let [{:keys [error]} (run-sql s "INSERT INTO t VALUES ('18446744073709551616')")]
      (is (some? error))
      (is (re-find #"out of range" error)))))

(deftest ubigint-mixed-with-other-unsigned
  (testing "A table can mix UBIGINT and smaller unsigned types"
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (tiny UTINYINT, big UBIGINT)")
      (run-sql s "INSERT INTO t VALUES (255, '18446744073709551615'), (0, '0')")
      (let [{:keys [oids rows]} (run-sql s "SELECT tiny, big FROM t")]
        (is (= [OID_INT2 OID_NUMERIC] oids))
        (is (= [["255" "18446744073709551615"]
                ["0" "0"]]
               rows))))))

(deftest ubigint-accepts-small-numeric-literal
  (testing "Values that fit in Long can still use bare integer literals"
    (with-server [s {:port 0}]
      (run-sql s "CREATE TABLE t (id UBIGINT)")
      (run-sql s "INSERT INTO t VALUES (42), (1000000)")
      (let [{:keys [rows]} (run-sql s "SELECT id FROM t")]
        (is (= [["42"] ["1000000"]] rows))))))
