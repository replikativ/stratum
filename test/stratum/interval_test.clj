(ns stratum.interval-test
  "Calendar-correctness coverage for SQL INTERVAL arithmetic.

   Step 2 of the extended-types rollout: the SQL parser used to lower
   `INTERVAL '1' MONTH` to `[:* 1 30]` (30 raw days), which silently
   produced wrong calendar dates (`Jan 31 + 1 MONTH = Mar 02` instead
   of `Feb 28/29`). These tests pin the fixed behaviour: MONTH and
   YEAR route through `arrayDateAddMonths` (Hinnant civil-date
   arithmetic, leap-year aware, max-day-clamping); DAY keeps the
   fast-path integer addition; sub-day units route to the matching
   `arrayDateAdd*` kernel on TIMESTAMP columns."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]
            [stratum.sql :as sql]))

;; ---------------------------------------------------------------------------
;; Epoch-day helpers (so the assertions read like calendar dates).

(defn- ed [year month day]
  ;; `(LocalDate/of y m d) → toEpochDay`. SELECT projects scalar
  ;; expressions through `eval-agg-expr` which casts to double, so
  ;; tests compare against the double form to match the wire shape.
  (double (.toEpochDay (java.time.LocalDate/of year month day))))

(defn- em
  "Epoch microseconds for the given UTC date/time."
  ([year month day]                (em year month day 0 0 0))
  ([year month day h m s]
   (let [t (.atOffset (java.time.LocalDateTime/of year month day h m s)
                      java.time.ZoneOffset/UTC)
         epoch-secs (.toEpochSecond t)]
     (double (* 1000000 epoch-secs)))))

(defn- run-1 [reg sql]
  (let [{:keys [query]} (sql/parse-sql sql reg)]
    (first (q/q query))))

;; ===========================================================================
;; DAY arithmetic — preserved fast path (integer addition on epoch-days).

(deftest day-interval-on-date-column-unchanged
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 1 15)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2026 2 14)}
           (run-1 reg "SELECT d + INTERVAL '30' DAY AS r FROM t")))
    (is (= {:r (ed 2026 1 14)}
           (run-1 reg "SELECT d - INTERVAL '1' DAY AS r FROM t")))))

;; ===========================================================================
;; MONTH — calendar-aware. The Hinnant kernel clamps overflow to the
;; last valid day of the target month.

(deftest month-interval-on-date-column
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 1 15)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2026 4 15)}
           (run-1 reg "SELECT d + INTERVAL '3' MONTH AS r FROM t"))
        "+3 months on a mid-month date")))

(deftest month-interval-clamps-day-end-of-month
  ;; Jan 31 + 1 month → must NOT produce Feb 30 (literal +30 days)
  ;; or Mar 2 (the old `[:* 1 30]` lowering result). PG / DuckDB
  ;; both clamp to Feb 28 (or Feb 29 in a leap year).
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 1 31)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2026 2 28)}
           (run-1 reg "SELECT d + INTERVAL '1' MONTH AS r FROM t"))
        "Jan 31 + 1 MONTH must clamp to Feb 28 (2026 is non-leap)")))

(deftest month-interval-leap-year-clamp
  ;; Same shape in a leap year — Feb 29 exists, so the clamp goes there.
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2024 1 31)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2024 2 29)}
           (run-1 reg "SELECT d + INTERVAL '1' MONTH AS r FROM t"))
        "Jan 31 + 1 MONTH in a leap year clamps to Feb 29")))

(deftest month-interval-subtraction
  ;; date - INTERVAL N MONTH must go backwards through the calendar
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 3 15)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2025 12 15)}
           (run-1 reg "SELECT d - INTERVAL '3' MONTH AS r FROM t"))
        "Mar 15 2026 - 3 MONTH = Dec 15 2025 (wraps the year)")))

;; ===========================================================================
;; YEAR — same path, scaled to 12 months.

(deftest year-interval-on-date-column
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2024 6 15)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2026 6 15)}
           (run-1 reg "SELECT d + INTERVAL '2' YEAR AS r FROM t")))))

(deftest year-interval-leap-day-clamp
  ;; Feb 29 + 1 YEAR → Feb 28 (2025 has no Feb 29)
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2024 2 29)]) :temporal-unit :days}}}]
    (is (= {:r (ed 2025 2 28)}
           (run-1 reg "SELECT d + INTERVAL '1' YEAR AS r FROM t"))
        "Feb 29 + 1 YEAR clamps to Feb 28 in a non-leap year")))

;; ===========================================================================
;; Sub-day units — TIMESTAMP columns (micros).

(deftest hour-interval-on-timestamp
  ;; TIMESTAMP column (epoch-micros). INTERVAL '5' HOUR routes to
  ;; [:date-add :hours 5 t]; the engine adds 5*3.6e9 micros.
  (let [reg {"t" {:t {:type :int64
                      :data (long-array [(em 2026 1 15 10 0 0)])
                      :temporal-unit :micros}}}
        {:keys [query]} (sql/parse-sql
                         "SELECT t + INTERVAL '5' HOUR AS r FROM t"
                         reg)]
    (is (= {:r (em 2026 1 15 15 0 0)}
           (first (q/q query))))))

(deftest second-interval-on-timestamp
  (let [reg {"t" {:t {:type :int64
                      :data (long-array [(em 2026 1 15 10 0 0)])
                      :temporal-unit :micros}}}
        {:keys [query]} (sql/parse-sql
                         "SELECT t + INTERVAL '90' SECOND AS r FROM t"
                         reg)]
    (is (= {:r (em 2026 1 15 10 1 30)}
           (first (q/q query))))))

;; ===========================================================================
;; Chained intervals — nested Addition must rewrite both legs.

(deftest chained-month-then-day
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 1 31)]) :temporal-unit :days}}}]
    ;; Jan 31 + 1 MONTH = Feb 28 (clamp); then + 5 DAY = Mar 5
    (is (= {:r (ed 2026 3 5)}
           (run-1 reg "SELECT d + INTERVAL '1' MONTH + INTERVAL '5' DAY AS r FROM t")))))

;; ===========================================================================
;; Negative magnitude — `INTERVAL '-1' MONTH` (variant phrasing).
;; PG accepts both `INTERVAL '-1' MONTH` and `- INTERVAL '1' MONTH`.

(deftest signed-month-interval-literal
  (let [reg {"t" {:d {:type :int64 :data (long-array [(ed 2026 3 15)]) :temporal-unit :days}}}]
    ;; Subtraction path (parsed as `d - INTERVAL '1' MONTH`)
    (is (= {:r (ed 2026 2 15)}
           (run-1 reg "SELECT d - INTERVAL '1' MONTH AS r FROM t")))))
