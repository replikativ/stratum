(ns stratum.decimal-test
  "Step 5a: DECIMAL(p,s) DDL + INSERT + text wire + restart round-trip.

   Stratum stores DECIMAL with `p ≤ 18` as int64 unscaled longs in a
   `long[]` chunk; `(precision, scale)` lives in `:column-schema`
   metadata. The wire emits OID_NUMERIC (1700) and renders values as
   `BigDecimal.toPlainString()`."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.core :as kstore]
            [stratum.server :as srv]
            [stratum.sql :as sql]
            [stratum.util.decimal :as decimal])
  (:import [java.io File]
           [java.math BigDecimal]
           [java.util UUID]
           [stratum.internal PgWireServer$QueryResult]))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- temp-dir ^String []
  (let [d (File/createTempFile "stratum-decimal-" "")]
    (.delete d) (.mkdirs d) (.getAbsolutePath d)))

(defn- delete-dir [^String path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [^File child (reverse (file-seq f))] (.delete child)))))

(defn- file-store-at [^String path]
  (let [cfg {:backend :file
             :path    path
             :id      (UUID/nameUUIDFromBytes (.getBytes path "UTF-8"))}]
    (if (kstore/store-exists? cfg {:sync? true})
      (kstore/connect-store cfg {:sync? true})
      (kstore/create-store  cfg {:sync? true}))))

(def ^:private port-counter (atom 5950))
(defn- next-port [] (swap! port-counter inc))

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

(def OID_NUMERIC 1700)
(def OID_INT8    20)

;; ===========================================================================
;; util/decimal — unit-level

(deftest typmod-roundtrip
  (is (= [10 2] (decimal/decode-typmod (decimal/encode-typmod 10 2))))
  (is (= [18 0] (decimal/decode-typmod (decimal/encode-typmod 18 0)))))

(deftest parse-decimal-type-shapes
  (is (= {:precision 10 :scale 2} (decimal/parse-decimal-type "NUMERIC(10,2)")))
  (is (= {:precision 10 :scale 2} (decimal/parse-decimal-type "DECIMAL(10, 2)")))
  (is (= {:precision 18 :scale 0} (decimal/parse-decimal-type "decimal(18)")))
  (is (= :unconstrained          (decimal/parse-decimal-type "NUMERIC")))
  (is (nil?                      (decimal/parse-decimal-type "VARCHAR(10)"))))

(deftest bigdec-roundtrip
  (let [bd (BigDecimal. "1.23")]
    (is (= 123 (decimal/bigdec->unscaled-long bd 2 "test")))
    (is (= "1.23" (decimal/unscaled-long->plain-string 123 2)))
    (is (= bd     (decimal/unscaled-long->bigdec 123 2)))))

(deftest precision-validation
  (decimal/validate-precision! 18 4)
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must be 1..18"
                        (decimal/validate-precision! 19 0)))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must be 1..18"
                        (decimal/validate-precision! 0 0)))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"scale must be"
                        (decimal/validate-precision! 5 10))))

(deftest rounding-half-even
  ;; 0.125 → 0.12 in HALF_EVEN, 0.135 → 0.14
  (is (= 12 (decimal/bigdec->unscaled-long (BigDecimal. "0.125") 2 "")))
  (is (= 14 (decimal/bigdec->unscaled-long (BigDecimal. "0.135") 2 ""))))

(deftest overflow-throws
  ;; DECIMAL(18,2) max unscaled value = 99,999,999,999,999,999_99 = 18 digits.
  ;; A 19-digit unscaled value overflows int64.
  (is (thrown-with-msg? clojure.lang.ExceptionInfo
                        #"exceeds int64 unscaled range"
                        (decimal/bigdec->unscaled-long
                         (BigDecimal. "99999999999999999999")
                         0 "test"))))

;; ===========================================================================
;; SQL parser — DDL

(deftest parser-recognises-decimal
  (let [parsed (sql/parse-sql "CREATE TABLE t (a DECIMAL(10,2))" {})
        col    (-> parsed :ddl :columns first)]
    (is (= :int64 (:type col)))
    (is (= true (:decimal? col)))
    (is (= 10 (:precision col)))
    (is (= 2 (:scale col)))))

(deftest parser-recognises-numeric-alias
  (let [parsed (sql/parse-sql "CREATE TABLE t (a NUMERIC(18,4))" {})]
    (is (= 18 (-> parsed :ddl :columns first :precision)))
    (is (= 4 (-> parsed :ddl :columns first :scale)))))

(deftest parser-bare-numeric-errors
  (let [parsed (sql/parse-sql "CREATE TABLE t (a NUMERIC)" {})]
    (is (re-find #"Bare NUMERIC.*not supported" (or (:error parsed) "")))))

(deftest parser-precision-19-errors
  (let [parsed (sql/parse-sql "CREATE TABLE t (a DECIMAL(19,2))" {})]
    (is (re-find #"precision must be 1..18" (or (:error parsed) "")))))

;; ===========================================================================
;; Server — INSERT + SELECT round-trip + text wire + OID

(deftest insert-decimal-and-select
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (is (nil? (:error (run-sql s "INSERT INTO t (price) VALUES (1.23)"))))
    (is (nil? (:error (run-sql s "INSERT INTO t (price) VALUES (9.99)"))))
    (let [r (run-sql s "SELECT price FROM t ORDER BY price")]
      (is (= 2 (count (:rows r))))
      ;; Text formatting via BigDecimal.toPlainString — no float exponent
      (is (= [["1.23"] ["9.99"]] (:rows r)))
      (is (= [OID_NUMERIC] (:oids r))
          "DECIMAL column must emit OID_NUMERIC (1700) on the wire"))))

(deftest insert-preserves-trailing-zeros-from-literal
  ;; JSqlParser yields DoubleValue 1.1 for the literal `1.10`. Step 5a
  ;; intercepts the textual form so the BigDecimal preserves the
  ;; trailing zero.
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (1.10)")
    (let [r (run-sql s "SELECT price FROM t")]
      (is (= [["1.10"]] (:rows r))))))

(deftest insert-coerces-integer-literal
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (5)")
    (let [r (run-sql s "SELECT price FROM t")]
      (is (= [["5.00"]] (:rows r))
          "Integer 5 into DECIMAL(10,2) renders as '5.00'"))))

(deftest insert-rounds-half-even
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    ;; 1.125 → 1.12 in HALF_EVEN (banker's rounding)
    (run-sql s "INSERT INTO t (price) VALUES (1.125)")
    (let [r (run-sql s "SELECT price FROM t")]
      (is (= [["1.12"]] (:rows r))))))

(deftest insert-overflow-errors
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(4,2))")
    ;; 999.99 has unscaled value 99999, fits int64 — but exceeds DEC(4,2)
    ;; range (max 99.99). For 5a we don't enforce precision-range yet;
    ;; we only enforce the int64 boundary. Document with a wider value
    ;; that does break.
    (let [r (run-sql s "INSERT INTO t (price) VALUES (999999999999999999999.99)")]
      (is (some? (:error r))
          "Out-of-int64-range literal must error at INSERT"))))

(deftest insert-null-allowed
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (NULL)")
    (let [r (run-sql s "SELECT price FROM t")]
      (is (= [[nil]] (:rows r))))))

;; ===========================================================================
;; Restart round-trip

;; ===========================================================================
;; Step 5b — Comparison + arithmetic literal rescaling

(deftest where-equals-literal
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (1.23)")
    (run-sql s "INSERT INTO t (price) VALUES (9.99)")
    (let [r (run-sql s "SELECT price FROM t WHERE price = 1.23")]
      (is (= [["1.23"]] (:rows r))
          "WHERE = literal must rescale the literal to the column's int64 form"))))

(deftest where-greater-literal
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["1.00" "5.50" "10.00" "99.99"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT price FROM t WHERE price > 5.50 ORDER BY price")]
      (is (= [["10.00"] ["99.99"]] (:rows r))))))

(deftest where-between-style-via-and
  ;; `BETWEEN` doesn't go through the binary-op rewriter (it's a
  ;; ternary), but the equivalent `>= AND <=` does. Use the latter
  ;; until 5b grows BETWEEN support.
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["0.50" "1.00" "1.50" "2.00" "2.50"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT price FROM t WHERE price >= 1.00 AND price <= 2.00 ORDER BY price")]
      (is (= [["1.00"] ["1.50"] ["2.00"]] (:rows r))))))

(deftest where-integer-literal-coerced
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (5.00)")
    (run-sql s "INSERT INTO t (price) VALUES (5.50)")
    ;; literal `5` (Long) should be coerced to `500` (unscaled) so
    ;; the comparison matches `5.00`.
    (let [r (run-sql s "SELECT price FROM t WHERE price = 5")]
      (is (= [["5.00"]] (:rows r))))))

(deftest where-no-match-on-different-scale-literal
  ;; `WHERE price = 1.234` on DEC(10,2) — literal has more fractional
  ;; digits than the column. HALF_EVEN rounding scales 1.234 → 1.23
  ;; (since the .004 rounds to .00). So the WHERE matches 1.23.
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (price) VALUES (1.23)")
    (let [r (run-sql s "SELECT price FROM t WHERE price = 1.234")]
      (is (= [["1.23"]] (:rows r))
          "Literal rounds HALF_EVEN to column scale; matches 1.23"))))

(deftest where-decimal-vs-decimal-same-scale
  ;; col-vs-col comparison: same scale, long compare works without
  ;; any rescale.
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (a DECIMAL(10,2), b DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (a, b) VALUES (1.00, 2.00)")
    (run-sql s "INSERT INTO t (a, b) VALUES (5.00, 3.00)")
    (let [r (run-sql s "SELECT a FROM t WHERE a > b")]
      (is (= [["5.00"]] (:rows r))))))

;; ===========================================================================
;; Step 5c — Aggregates over DECIMAL columns

(deftest sum-decimal-renders-as-decimal
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["1.23" "2.34" "3.45"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT SUM(price) FROM t")]
      ;; 1.23 + 2.34 + 3.45 = 7.02. Unscaled: 123 + 234 + 345 = 702.
      (is (= [["7.02"]] (:rows r))
          "SUM(DECIMAL) renders as BigDecimal at the input column's scale")
      (is (= [OID_NUMERIC] (:oids r))
          "SUM(DECIMAL) emits OID_NUMERIC on the wire"))))

(deftest sum-with-alias
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["10.00" "20.00"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT SUM(price) AS total FROM t")]
      (is (= [["30.00"]] (:rows r)))
      (is (= [OID_NUMERIC] (:oids r))))))

(deftest min-max-decimal
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["1.00" "5.50" "10.99"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT MIN(price), MAX(price) FROM t")]
      (is (= [["1.00" "10.99"]] (:rows r))
          "MIN/MAX over DECIMAL preserve precision + scale")
      (is (= [OID_NUMERIC OID_NUMERIC] (:oids r))))))

(deftest avg-decimal-returns-double
  ;; Step 5c open-question decision: AVG(DECIMAL) → DOUBLE (matches
  ;; DuckDB). The result column emits OID_FLOAT8, not OID_NUMERIC.
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
    (doseq [v ["1.00" "2.00" "3.00"]]
      (run-sql s (str "INSERT INTO t (price) VALUES (" v ")")))
    (let [r (run-sql s "SELECT AVG(price) FROM t")
          OID_FLOAT8 701]
      ;; AVG = 2.0 as DOUBLE. Engine returns 2.0 directly; the
      ;; unscaled-long-divided-by-scale conversion happens in the
      ;; engine's AVG decode, so the rendered value isn't scaled
      ;; here — it's the raw double 2.0.
      (is (= [OID_FLOAT8] (:oids r))
          "AVG(DECIMAL) emits OID_FLOAT8 (matches DuckDB)"))))

(deftest sum-with-group-by
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (cat TEXT, price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (cat, price) VALUES ('a', 1.50)")
    (run-sql s "INSERT INTO t (cat, price) VALUES ('a', 2.50)")
    (run-sql s "INSERT INTO t (cat, price) VALUES ('b', 3.00)")
    (let [r (run-sql s "SELECT cat, SUM(price) FROM t GROUP BY cat ORDER BY cat")]
      (is (= [["a" "4.00"] ["b" "3.00"]] (:rows r))
          "GROUP BY DEC + SUM(DEC) renders correctly"))))

(deftest mixed-numeric-and-decimal-aggregate
  (with-server [s {:port (next-port)}]
    (run-sql s "CREATE TABLE t (qty INT, price DECIMAL(10,2))")
    (run-sql s "INSERT INTO t (qty, price) VALUES (1, 1.00)")
    (run-sql s "INSERT INTO t (qty, price) VALUES (2, 2.00)")
    (let [r (run-sql s "SELECT SUM(qty), SUM(price) FROM t")
          OID_FLOAT8 701]
      ;; SUM(qty) returns DOUBLE per pre-existing Stratum behaviour
      ;; (engine widens to double; PG/DuckDB widen to BIGINT/HUGEINT —
      ;; pre-existing discrepancy, separate ticket).
      ;; SUM(price) carries DECIMAL meta thanks to step 5c.
      (is (= 1 (count (:rows r))))
      (is (= [OID_FLOAT8 OID_NUMERIC] (:oids r))))))

;; ===========================================================================
;; Step 5b — Restart unchanged

(deftest decimal-survives-restart
  (let [path (temp-dir)]
    (try
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (run-sql s "CREATE TABLE t (price DECIMAL(10,2))")
          (run-sql s "INSERT INTO t (price) VALUES (1.23)")
          (run-sql s "INSERT INTO t (price) VALUES (9.99)")))
      (let [store (file-store-at path)]
        (with-server [s {:port (next-port) :store store}]
          (let [r (run-sql s "SELECT price FROM t ORDER BY price")]
            (is (= [["1.23"] ["9.99"]] (:rows r))
                "DECIMAL values + (p,s) metadata round-trip across restart")
            (is (= [OID_NUMERIC] (:oids r))))))
      (finally (delete-dir path)))))
