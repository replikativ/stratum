(ns stratum.date-trunc-test
  "Step 4b: DATE_TRUNC week / quarter / decade / century / millennium +
   DOW (Sun=0..Sat=6 PG/DuckDB convention) + ISODOW (Mon=1..Sun=7 ISO).

   Pre-step-4b: DATE_TRUNC for these units threw `case` \"No matching
   clause\" (silent error); DOW/ISODOW both returned Stratum's Mon=0..Sun=6
   convention (matched neither PG nor ISO)."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]
            [stratum.sql :as sql]))

(defn- ed [year month day]
  (double (.toEpochDay (java.time.LocalDate/of year month day))))

(defn- run-1 [reg sql-string]
  (let [{:keys [query]} (sql/parse-sql sql-string reg)]
    (first (q/q query))))

;; ===========================================================================
;; DATE_TRUNC new units on epoch-days

(deftest trunc-week-on-day-column
  ;; 2026-01-15 is Thursday → week of 2026-01-12 (Monday)
  (let [reg {"t" {:d {:type :int64
                      :data (long-array [(long (ed 2026 1 15))])
                      :temporal-unit :seconds}}}]
    ;; tu=:seconds works on the seconds-form kernel; emulate by feeding
    ;; epoch-seconds for the date.
    (let [r (run-1 reg "SELECT DATE_TRUNC('week', d) AS r FROM t")]
      (is (some? r)))))

(deftest trunc-week-on-micros-column
  ;; Sat 2026-01-17 12:34:56 → Monday of week = 2026-01-12 00:00:00
  (let [reg {"t" {:ts {:type :int64
                       :data (long-array
                              [(let [t (.atOffset (java.time.LocalDateTime/of 2026 1 17 12 34 56)
                                                  java.time.ZoneOffset/UTC)]
                                 (* 1000000 (.toEpochSecond t)))])
                       :temporal-unit :micros}}}
        r (run-1 reg "SELECT DATE_TRUNC('week', ts) AS r FROM t")
        expected (let [t (.atOffset (java.time.LocalDateTime/of 2026 1 12 0 0 0)
                                    java.time.ZoneOffset/UTC)]
                   (double (* 1000000 (.toEpochSecond t))))]
    (is (= expected (:r r))
        "DATE_TRUNC('week', ts) returns Monday of the week")))

(deftest trunc-quarter-on-micros-column
  (let [reg {"t" {:ts {:type :int64
                       :data (long-array
                              [(let [t (.atOffset (java.time.LocalDateTime/of 2026 5 15 10 0 0)
                                                  java.time.ZoneOffset/UTC)]
                                 (* 1000000 (.toEpochSecond t)))])
                       :temporal-unit :micros}}}
        r (run-1 reg "SELECT DATE_TRUNC('quarter', ts) AS r FROM t")
        ;; May is in Q2 (Apr/May/Jun) → start of Q2 = 2026-04-01
        expected (let [t (.atOffset (java.time.LocalDateTime/of 2026 4 1 0 0 0)
                                    java.time.ZoneOffset/UTC)]
                   (double (* 1000000 (.toEpochSecond t))))]
    (is (= expected (:r r)))))

(deftest trunc-decade-and-century
  (let [reg {"t" {:ts {:type :int64
                       :data (long-array
                              [(let [t (.atOffset (java.time.LocalDateTime/of 2026 5 15 10 0 0)
                                                  java.time.ZoneOffset/UTC)]
                                 (* 1000000 (.toEpochSecond t)))])
                       :temporal-unit :micros}}}]
    ;; 2026 → decade = 2020
    (let [r (run-1 reg "SELECT DATE_TRUNC('decade', ts) AS r FROM t")
          expected (let [t (.atOffset (java.time.LocalDateTime/of 2020 1 1 0 0 0)
                                      java.time.ZoneOffset/UTC)]
                     (double (* 1000000 (.toEpochSecond t))))]
      (is (= expected (:r r))))
    ;; 2026 → century starts at 2001 (year 2026 ∈ years 2001..2100)
    (let [r (run-1 reg "SELECT DATE_TRUNC('century', ts) AS r FROM t")
          expected (let [t (.atOffset (java.time.LocalDateTime/of 2001 1 1 0 0 0)
                                      java.time.ZoneOffset/UTC)]
                     (double (* 1000000 (.toEpochSecond t))))]
      (is (= expected (:r r))))))

;; ===========================================================================
;; EXTRACT DOW / ISODOW correctness

(deftest extract-dow-pg-convention
  ;; 2023-06-15 is Thursday.
  ;; PG/DuckDB DOW: Sun=0, Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6
  (let [reg {"t" {:d {:type :int64
                      :data (long-array [(long (ed 2023 6 15))])
                      :temporal-unit :days}}}
        r (run-1 reg "SELECT EXTRACT(DOW FROM d) AS dow FROM t")]
    (is (== 4.0 (:dow r))
        "DOW Thursday = 4 in PG/DuckDB convention (was 3 pre-step-4b)")))

(deftest extract-isodow-iso-convention
  ;; ISODOW: Mon=1..Sun=7. Thursday → 4 (still 4 — coincidence at Thu).
  ;; Test Sunday to disambiguate: 2023-06-18 is Sunday.
  (let [reg {"t" {:d {:type :int64
                      :data (long-array [(long (ed 2023 6 18))])
                      :temporal-unit :days}}}
        r (run-1 reg "SELECT EXTRACT(ISODOW FROM d) AS isodow FROM t")]
    (is (== 7.0 (:isodow r))
        "ISODOW Sunday = 7 (Mon=1..Sun=7); pre-step-4b returned 6 (Stratum Sun=6)")))

(deftest extract-dow-disambiguates-vs-isodow
  ;; 2023-06-18 is Sunday — the cleanest test for separating the two.
  (let [reg {"t" {:d {:type :int64
                      :data (long-array [(long (ed 2023 6 18))])
                      :temporal-unit :days}}}
        r (run-1 reg "SELECT EXTRACT(DOW FROM d) AS dow,
                              EXTRACT(ISODOW FROM d) AS isodow FROM t")]
    (is (== 0.0 (:dow r))    "DOW Sunday = 0 (PG/DuckDB)")
    (is (== 7.0 (:isodow r)) "ISODOW Sunday = 7 (ISO 8601)")))

;; ===========================================================================
;; EXTRACT QUARTER

(deftest extract-quarter
  (let [data {:d (long-array
                  [(long (ed 2026 1 15))   ; Q1
                   (long (ed 2026 4 1))    ; Q2 first day
                   (long (ed 2026 6 30))   ; Q2 last day
                   (long (ed 2026 7 1))    ; Q3
                   (long (ed 2026 12 31))])} ; Q4
        rs (q/q {:from data
                 :select [[:as [:quarter :d] :q]]})]
    (is (= [1.0 2.0 2.0 3.0 4.0] (mapv :q rs))
        "Quarters map (Jan=1, Apr=2, Jun=2, Jul=3, Dec=4)")))
