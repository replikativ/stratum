(ns stratum.timestamp-precision-test
  "Step 3: TIMESTAMP_S / TIMESTAMP_MS DDL recognition + `:millis`
   temporal-unit dispatch through DATE_TRUNC, DATE_ADD, EXTRACT,
   TIME_BUCKET, and parquet ingest precision preservation.

   Pre-step-3, only `:micros` and `:seconds` were dispatched; `:millis`
   was a documented-but-unwired unit. Parquet TIMESTAMP(MICROS) /
   TIMESTAMP(NANOS) sources were destructively normalised to
   millis (covered by the rewritten `parquet-dataset-timestamp-units-test`)."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]
            [stratum.sql :as sql]))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- em
  "Epoch milliseconds for the given UTC date/time."
  ([year month day]               (em year month day 0 0 0 0))
  ([year month day h mi s]        (em year month day h mi s 0))
  ([year month day h mi s ms]
   (let [t (.atOffset (java.time.LocalDateTime/of year month day h mi s
                                                  (* ms 1000000))
                      java.time.ZoneOffset/UTC)
         secs (.toEpochSecond t)]
     (+ (* 1000 secs) ms))))

(defn- run-1 [reg sql]
  (let [{:keys [query]} (sql/parse-sql sql reg)]
    (first (q/q query))))

(defn- ms-col [& vs]
  {:type :int64 :data (long-array (vec vs)) :temporal-unit :millis})

(defn- sec-col [& vs]
  {:type :int64 :data (long-array (vec vs)) :temporal-unit :seconds})

;; ===========================================================================
;; DDL recognition

(deftest timestamp-s-ddl-creates-seconds-column
  (let [parsed (sql/parse-sql "CREATE TABLE t (ts TIMESTAMP_S)" {})]
    (is (= :seconds
           (-> parsed :ddl :columns first :temporal-unit)))))

(deftest timestamp-ms-ddl-creates-millis-column
  (let [parsed (sql/parse-sql "CREATE TABLE t (ts TIMESTAMP_MS)" {})]
    (is (= :millis
           (-> parsed :ddl :columns first :temporal-unit)))))

(deftest timestamp-sec-alias-recognized
  (let [parsed (sql/parse-sql "CREATE TABLE t (ts TIMESTAMP_SEC)" {})]
    (is (= :seconds
           (-> parsed :ddl :columns first :temporal-unit)))))

(deftest timestamp-ns-ddl-now-supported
  ;; Step 4c lifted the prior "not yet supported" stub.
  (let [parsed (sql/parse-sql "CREATE TABLE t (ts TIMESTAMP_NS)" {})]
    (is (= :nanos
           (-> parsed :ddl :columns first :temporal-unit)))))

;; ===========================================================================
;; DATE_ADD on :millis column — every unit, scaled through the micros kernel

(deftest date-add-hours-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 0 0))}}]
    (is (= {:r (double (em 2026 1 15 15 0 0))}
           (run-1 reg "SELECT ts + INTERVAL '5' HOUR AS r FROM t")))))

(deftest date-add-seconds-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 0 0))}}]
    (is (= {:r (double (em 2026 1 15 10 1 30))}
           (run-1 reg "SELECT ts + INTERVAL '90' SECOND AS r FROM t")))))

(deftest date-add-month-clamps-on-millis
  ;; Jan 31 → Feb 28 (non-leap) — same calendar correctness as step 2
  (let [reg {"t" {:ts (ms-col (em 2026 1 31))}}]
    (is (= {:r (double (em 2026 2 28))}
           (run-1 reg "SELECT ts + INTERVAL '1' MONTH AS r FROM t")))))

;; ===========================================================================
;; DATE_TRUNC on :millis column

(deftest date-trunc-hour-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 30 45 123))}}]
    (is (= {:r (double (em 2026 1 15 10 0 0))}
           (run-1 reg "SELECT DATE_TRUNC('hour', ts) AS r FROM t")))))

(deftest date-trunc-day-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 30 45 123))}}]
    (is (= {:r (double (em 2026 1 15))}
           (run-1 reg "SELECT DATE_TRUNC('day', ts) AS r FROM t")))))

(deftest date-trunc-second-on-millis-zeros-subsecond
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 30 45 678))}}]
    (is (= {:r (double (em 2026 1 15 10 30 45))}
           (run-1 reg "SELECT DATE_TRUNC('second', ts) AS r FROM t")))))

;; ===========================================================================
;; EXTRACT on :millis column

(deftest extract-year-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 6 15 10 30 45))}}]
    (is (= {:r 2026.0}
           (run-1 reg "SELECT EXTRACT(YEAR FROM ts) AS r FROM t")))))

(deftest extract-month-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 6 15 10 30 45))}}]
    (is (= {:r 6.0}
           (run-1 reg "SELECT EXTRACT(MONTH FROM ts) AS r FROM t")))))

(deftest extract-hour-on-millis
  (let [reg {"t" {:ts (ms-col (em 2026 6 15 10 30 45))}}]
    (is (= {:r 10.0}
           (run-1 reg "SELECT EXTRACT(HOUR FROM ts) AS r FROM t")))))

(deftest extract-millisecond-on-millis-returns-subsecond
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 30 45 678))}}]
    (is (= {:r 678.0}
           (run-1 reg "SELECT EXTRACT(MILLISECOND FROM ts) AS r FROM t")))))

(deftest extract-microsecond-on-millis-errors
  (let [reg {"t" {:ts (ms-col (em 2026 1 15 10 30 45))}}
        {:keys [query]} (sql/parse-sql
                         "SELECT EXTRACT(MICROSECOND FROM ts) AS r FROM t"
                         reg)]
    (is (thrown-with-msg? Exception
                          #"MICROSECOND requires :temporal-unit :micros"
                          (q/q query)))))

;; ===========================================================================
;; Existing :seconds path still works (regression guard)

(deftest seconds-temporal-unit-unchanged
  (let [reg {"t" {:ts (sec-col 1700000000)}}]
    (is (= {:r 2023.0}
           (run-1 reg "SELECT EXTRACT(YEAR FROM ts) AS r FROM t")))))
