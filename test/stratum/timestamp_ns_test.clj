(ns stratum.timestamp-ns-test
  "Step 4c: TIMESTAMP_NS DDL + `:nanos` temporal-unit query ops via the
   ÷1000/×1000 dance against the existing `:micros` kernels. Sub-second
   extracts (NANOSECOND, MICROSECOND) read directly from the source for
   full-precision results; calendar-aware ops preserve the sub-micro
   remainder."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]
            [stratum.sql :as sql]))

(defn- en
  "Epoch nanoseconds for the given UTC datetime + nanos."
  ([y mo d h mi s]     (en y mo d h mi s 0))
  ([y mo d h mi s ns]
   (let [t (.atOffset (java.time.LocalDateTime/of y mo d h mi s 0)
                      java.time.ZoneOffset/UTC)
         secs (.toEpochSecond t)]
     (+ (* 1000000000 secs) ns))))

(defn- ns-col [& vs]
  {:type :int64 :data (long-array (vec vs)) :temporal-unit :nanos})

(defn- run-1 [reg sql-string]
  (let [{:keys [query]} (sql/parse-sql sql-string reg)]
    (first (q/q query))))

;; ===========================================================================
;; DDL

(deftest ddl-recognized
  (let [parsed (sql/parse-sql "CREATE TABLE t (ts TIMESTAMP_NS)" {})]
    (is (= :nanos
           (-> parsed :ddl :columns first :temporal-unit)))))

;; ===========================================================================
;; EXTRACT NANOSECOND — the cheapest path

(deftest extract-nanosecond
  (let [reg {"t" {:ts (ns-col (en 2026 1 15 10 30 45 123456789))}}
        r (run-1 reg "SELECT EXTRACT(NANOSECOND FROM ts) AS r FROM t")]
    (is (== 123456789.0 (:r r))
        "NANOSECOND must return the sub-second nano part at full precision")))

(deftest extract-microsecond-on-nanos
  ;; MICROSECOND from :nanos: floor-div by 1000, mod by 1_000_000 →
  ;; returns the sub-second micros part, dropping sub-micro nanos.
  (let [reg {"t" {:ts (ns-col (en 2026 1 15 10 30 45 123456789))}}
        r (run-1 reg "SELECT EXTRACT(MICROSECOND FROM ts) AS r FROM t")]
    (is (== 123456.0 (:r r)))))

;; ===========================================================================
;; DATE_TRUNC — sub-micro precision survives via the remainder pattern

(deftest trunc-second-zeros-subsecond
  (let [reg {"t" {:ts (ns-col (en 2026 1 15 10 30 45 123456789))}}
        r (run-1 reg "SELECT DATE_TRUNC('second', ts) AS r FROM t")]
    (is (== (double (en 2026 1 15 10 30 45 0)) (:r r))
        "DATE_TRUNC('second') zeros sub-second on :nanos column")))

(deftest trunc-day-on-nanos
  (let [reg {"t" {:ts (ns-col (en 2026 1 15 10 30 45 123456789))}}
        r (run-1 reg "SELECT DATE_TRUNC('day', ts) AS r FROM t")]
    (is (== (double (en 2026 1 15 0 0 0 0)) (:r r)))))

(deftest trunc-month-on-nanos
  (let [reg {"t" {:ts (ns-col (en 2026 5 15 0 0 0 123456789))}}
        r (run-1 reg "SELECT DATE_TRUNC('month', ts) AS r FROM t")]
    (is (== (double (en 2026 5 1 0 0 0 0)) (:r r)))))

;; ===========================================================================
;; DATE_ADD — sub-second precision preserved via the remainder pattern

(deftest add-hour-preserves-nanos
  (let [reg {"t" {:ts (ns-col (en 2026 1 15 10 0 0 999999999))}}
        r (run-1 reg "SELECT ts + INTERVAL '5' HOUR AS r FROM t")]
    (is (== (double (en 2026 1 15 15 0 0 999999999)) (:r r))
        "Adding 5 HOUR preserves the sub-second nanosecond remainder")))

(deftest add-month-preserves-subsecond
  (let [reg {"t" {:ts (ns-col (en 2026 1 31 12 0 0 555555555))}}
        r (run-1 reg "SELECT ts + INTERVAL '1' MONTH AS r FROM t")]
    ;; Jan 31 + 1 MONTH → clamps to Feb 28 (non-leap); sub-second preserved.
    (is (== (double (en 2026 2 28 12 0 0 555555555)) (:r r)))))

;; ===========================================================================
;; EXTRACT YEAR / MONTH / DAY all go through micros without precision loss

(deftest extract-y-m-d-on-nanos
  (let [reg {"t" {:ts (ns-col (en 2026 7 4 12 0 0))}}
        r (run-1 reg "SELECT EXTRACT(YEAR FROM ts) AS y,
                             EXTRACT(MONTH FROM ts) AS m,
                             EXTRACT(DAY FROM ts) AS d FROM t")]
    (is (= {:y 2026.0 :m 7.0 :d 4.0} r))))
