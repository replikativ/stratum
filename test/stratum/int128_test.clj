(ns stratum.int128-test
  "Steps 8b / 8c / UUID — passthrough column support for typed reference
   arrays whose elements don't fit Stratum's primitive (long/double/string)
   storage model.

   Backed by the Step 7 INTERVAL infrastructure: register-table! accepts a
   BigInteger[] / BigDecimal[] / UUID[]; encode-column tags it; the engine's
   SELECT passthrough preserves the array; format-results infers the wire
   OID from the element type and renders via toString (or toPlainString
   for BigDecimal).

   Out of scope for this first cut:
     - WHERE / GROUP BY / aggregates over HUGEINT / DECIMAL128 / UUID
     - SQL CREATE TABLE … HUGEINT / DECIMAL128 / UUID DDL + INSERT parsing
     - Durable (Konserve-backed) storage of these types
     - Binary wire format for HUGEINT (PG has no int128 OID)"
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.server :as srv])
  (:import [java.math BigDecimal BigInteger]
           [java.util UUID]
           [stratum.internal PgWireServer$QueryResult]))

(def OID_NUMERIC 1700)
(def OID_UUID    2950)

(defn- run-select [server sql]
  (@(requiring-resolve 'stratum.server/execute-sql)
   sql (:registry server) (:data-dir server) (:store server)))

(defmacro ^:private with-server [[binding-name opts] & body]
  `(let [~binding-name (srv/start ~opts)]
     (try ~@body (finally (srv/stop ~binding-name)))))

;; ---------------------------------------------------------------------------
;; HUGEINT via BigInteger[] passthrough

(deftest hugeint-select-passthrough
  (with-server [s {:port 0}]
    (srv/register-table!
     s "ids"
     {:id (into-array BigInteger
                      [(BigInteger. "1")
                       (BigInteger. "170141183460469231731687303715884105727")
                       (BigInteger. "-170141183460469231731687303715884105728")
                       (BigInteger. "99999999999999999999999999999999")])})
    (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM ids")]
      (is (nil? (.error qr)) (str "HUGEINT SELECT errored: " (.error qr)))
      (is (= [OID_NUMERIC] (vec (.columnOids qr))))
      (let [rows (mapv vec (vec (.rows qr)))]
        (is (= 4 (count rows)))
        (is (= ["1"] (first rows)))
        (is (= ["170141183460469231731687303715884105727"] (nth rows 1)))
        (is (= ["-170141183460469231731687303715884105728"] (nth rows 2)))
        (is (= ["99999999999999999999999999999999"] (nth rows 3)))))))

(deftest hugeint-with-null-passthrough
  (with-server [s {:port 0}]
    (let [arr (make-array BigInteger 3)]
      (aset arr 0 (BigInteger. "100"))
      (aset arr 1 nil)
      (aset arr 2 (BigInteger. "200"))
      (srv/register-table! s "t" {:id arr})
      (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM t")
            rows (mapv vec (vec (.rows qr)))]
        (is (= [["100"] [nil] ["200"]] rows))))))

;; ---------------------------------------------------------------------------
;; DECIMAL128 via BigDecimal[] passthrough

(deftest decimal128-select-passthrough
  (with-server [s {:port 0}]
    (srv/register-table!
     s "amounts"
     {:amt (into-array BigDecimal
                       [(BigDecimal. "1.00")
                        (BigDecimal. "1234567890123456789.123456789012345678")
                        (BigDecimal. "-99999999999999999999.99")
                        (BigDecimal. "0.000000000000000001")])})
    (let [^PgWireServer$QueryResult qr (run-select s "SELECT amt FROM amounts")]
      (is (nil? (.error qr)) (str "DECIMAL128 SELECT errored: " (.error qr)))
      (is (= [OID_NUMERIC] (vec (.columnOids qr))))
      (let [rows (mapv vec (vec (.rows qr)))]
        (is (= 4 (count rows)))
        (is (= ["1.00"] (first rows)))
        (is (= ["1234567890123456789.123456789012345678"] (nth rows 1)))
        (is (= ["-99999999999999999999.99"] (nth rows 2)))
        (is (= ["0.000000000000000001"] (nth rows 3)))))))

(deftest decimal128-no-scientific-notation
  (with-server [s {:port 0}]
    (srv/register-table!
     s "t"
     {:v (into-array BigDecimal
                     [(BigDecimal. "0.0000001")
                      (BigDecimal. "10000000000000000000")])})
    (let [^PgWireServer$QueryResult qr (run-select s "SELECT v FROM t")
          rows (mapv vec (vec (.rows qr)))]
      (is (not-any? #(re-find #"E" (first %)) rows))
      (is (= [["0.0000001"] ["10000000000000000000"]] rows)))))

;; ---------------------------------------------------------------------------
;; UUID via java.util.UUID[] passthrough

(deftest uuid-select-passthrough
  (with-server [s {:port 0}]
    (let [u1 (UUID/fromString "deadbeef-cafe-1234-5678-90abcdef0001")
          u2 (UUID/fromString "00000000-0000-0000-0000-000000000000")
          u3 (UUID/fromString "ffffffff-ffff-ffff-ffff-ffffffffffff")]
      (srv/register-table! s "users" {:id (into-array UUID [u1 u2 u3])})
      (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM users")]
        (is (nil? (.error qr)) (str "UUID SELECT errored: " (.error qr)))
        (is (= [OID_UUID] (vec (.columnOids qr))))
        (is (= [["deadbeef-cafe-1234-5678-90abcdef0001"]
                ["00000000-0000-0000-0000-000000000000"]
                ["ffffffff-ffff-ffff-ffff-ffffffffffff"]]
               (mapv vec (vec (.rows qr)))))))))

(deftest uuid-with-null-passthrough
  (with-server [s {:port 0}]
    (let [arr (make-array UUID 3)]
      (aset arr 0 (UUID/randomUUID))
      (aset arr 1 nil)
      (aset arr 2 (UUID/randomUUID))
      (srv/register-table! s "t" {:id arr})
      (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM t")
            rows (mapv vec (vec (.rows qr)))]
        (is (= 36 (count (ffirst rows))))
        (is (nil? (first (nth rows 1))))
        (is (= 36 (count (first (nth rows 2)))))))))

;; ---------------------------------------------------------------------------
;; Object[] forms are also accepted

(deftest hugeint-via-object-array
  (with-server [s {:port 0}]
    (srv/register-table! s "t"
                         {:id (object-array [(BigInteger. "42")
                                             (BigInteger. "10000000000000000000")])})
    (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM t")]
      (is (nil? (.error qr)))
      (is (= [OID_NUMERIC] (vec (.columnOids qr))))
      (is (= [["42"] ["10000000000000000000"]]
             (mapv vec (vec (.rows qr))))))))

(deftest uuid-via-object-array
  (with-server [s {:port 0}]
    (let [u (UUID/fromString "00112233-4455-6677-8899-aabbccddeeff")]
      (srv/register-table! s "t" {:id (object-array [u])})
      (let [^PgWireServer$QueryResult qr (run-select s "SELECT id FROM t")]
        (is (= [OID_UUID] (vec (.columnOids qr))))
        (is (= [["00112233-4455-6677-8899-aabbccddeeff"]]
               (mapv vec (vec (.rows qr)))))))))
