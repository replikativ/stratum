(ns stratum.temporal-micros-test
  "Tests for microsecond-precision TIMESTAMP columns (:temporal-unit :micros).

   A TIMESTAMP column is stored as a long[] of microseconds since the Unix
   epoch (1970-01-01T00:00:00 UTC). Functions like DATE_TRUNC, EXTRACT,
   DATE_ADD, DATE_DIFF and TIME_BUCKET dispatch on the column's
   :temporal-unit metadata so they treat the long values at the right
   scale. Columns without :temporal-unit metadata fall back to the legacy
   :seconds-based behavior."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]))

;; Reference instant: 2024-01-15T10:30:45.123456 UTC
(def ^:private ts-micros 1705314645123456)
;; Reference floor-to-day:    2024-01-15T00:00:00 UTC
(def ^:private day-micros  1705276800000000)
;; Reference floor-to-hour:   2024-01-15T10:00:00 UTC
(def ^:private hour-micros 1705312800000000)
;; Reference floor-to-minute: 2024-01-15T10:30:00 UTC
(def ^:private min-micros  1705314600000000)
;; Reference floor-to-second: 2024-01-15T10:30:45 UTC
(def ^:private sec-micros  1705314645000000)
;; Reference floor-to-ms:     2024-01-15T10:30:45.123 UTC
(def ^:private ms-micros   1705314645123000)
;; Reference floor-to-month:  2024-01-01T00:00:00 UTC
(def ^:private mo-micros   1704067200000000)
;; Reference floor-to-year:   2024-01-01T00:00:00 UTC
(def ^:private yr-micros   1704067200000000)

(defn- micros-col [vs]
  {:type :int64 :data (long-array vs) :temporal-unit :micros})

;; ============================================================================
;; DATE_TRUNC
;; ============================================================================

(deftest date-trunc-micros-test
  (testing "DATE_TRUNC on microsecond-precision TIMESTAMP"
    (let [data {:ts (micros-col [ts-micros])}
          r (q/q {:from data
                  :select [[:as [:date-trunc :microsecond :ts] :us]
                           [:as [:date-trunc :millisecond :ts] :ms]
                           [:as [:date-trunc :second :ts] :s]
                           [:as [:date-trunc :minute :ts] :m]
                           [:as [:date-trunc :hour :ts] :h]
                           [:as [:date-trunc :day :ts] :d]
                           [:as [:date-trunc :month :ts] :mo]
                           [:as [:date-trunc :year :ts] :y]]})]
      (is (== ts-micros (:us (first r))))
      (is (== ms-micros (:ms (first r))))
      (is (== sec-micros (:s (first r))))
      (is (== min-micros (:m (first r))))
      (is (== hour-micros (:h (first r))))
      (is (== day-micros (:d (first r))))
      (is (== mo-micros (:mo (first r))))
      (is (== yr-micros (:y (first r)))))))

;; ============================================================================
;; EXTRACT
;; ============================================================================

(deftest extract-micros-test
  (testing "EXTRACT from microsecond-precision TIMESTAMP"
    (let [data {:ts (micros-col [ts-micros])}
          r (q/q {:from data
                  :select [[:as [:year :ts] :y]
                           [:as [:month :ts] :mo]
                           [:as [:day :ts] :d]
                           [:as [:hour :ts] :h]
                           [:as [:minute :ts] :mi]
                           [:as [:second :ts] :s]
                           [:as [:millisecond :ts] :ms]
                           [:as [:microsecond :ts] :us]]})]
      (is (== 2024.0 (:y (first r))))
      (is (== 1.0 (:mo (first r))))
      (is (== 15.0 (:d (first r))))
      (is (== 10.0 (:h (first r))))
      (is (== 30.0 (:mi (first r))))
      (is (== 45.0 (:s (first r))))
      (is (== 123.0 (:ms (first r))))
      (is (== 123456.0 (:us (first r)))))))

;; ============================================================================
;; DATE_ADD
;; ============================================================================

(deftest date-add-micros-test
  (testing "DATE_ADD on microsecond-precision TIMESTAMP"
    (let [data {:ts (micros-col [ts-micros])}
          r (q/q {:from data
                  :select [[:as [:date-add :microseconds 100 :ts] :u]
                           [:as [:date-add :milliseconds 500 :ts] :ms]
                           [:as [:date-add :seconds 30 :ts] :s]
                           [:as [:date-add :minutes 5 :ts] :m]
                           [:as [:date-add :hours 2 :ts] :h]
                           [:as [:date-add :days 7 :ts] :d]]})]
      (is (== (+ ts-micros 100) (:u (first r))))
      (is (== (+ ts-micros 500000) (:ms (first r))))
      (is (== (+ ts-micros (* 30 1000000)) (:s (first r))))
      (is (== (+ ts-micros (* 5 60 1000000)) (:m (first r))))
      (is (== (+ ts-micros (* 2 3600 1000000)) (:h (first r))))
      (is (== (+ ts-micros (* 7 86400 1000000)) (:d (first r)))))))

;; ============================================================================
;; DATE_DIFF
;; ============================================================================

(deftest date-diff-micros-test
  (testing "DATE_DIFF on microsecond-precision TIMESTAMP columns"
    (let [t0 (micros-col [ts-micros])
          t1 (micros-col [(+ ts-micros 1000000)]) ;; +1 second
          data {:a t1 :b t0}
          r (q/q {:from data
                  :select [[:as [:date-diff :microseconds :a :b] :u]
                           [:as [:date-diff :milliseconds :a :b] :ms]
                           [:as [:date-diff :seconds :a :b] :s]]})]
      (is (== 1000000.0 (:u (first r))))
      (is (== 1000.0 (:ms (first r))))
      (is (== 1.0 (:s (first r)))))))

;; ============================================================================
;; TIME_BUCKET
;; ============================================================================

(deftest time-bucket-micros-test
  (testing "TIME_BUCKET groups micros-precision rows into fixed-width windows"
    (let [;; 4 timestamps inside two 5-minute windows
          ts (micros-col [1705314600000000     ;; 10:30:00.000
                          1705314720000000     ;; 10:32:00
                          1705314900000000     ;; 10:35:00 (next bucket)
                          1705315145123456])   ;; 10:39:05.123
          data {:ts ts}
          r (q/q {:from data
                  :select [[:as [:time-bucket 5 :minutes :ts] :b]]})]
      (is (== 1705314600000000 (:b (nth r 0))))
      (is (== 1705314600000000 (:b (nth r 1))))
      (is (== 1705314900000000 (:b (nth r 2))))
      (is (== 1705314900000000 (:b (nth r 3))))))

  (testing "TIME_BUCKET in GROUP BY correctly aggregates per-window"
    (let [ts (micros-col [1705314600000000  ;; 10:30
                          1705314720000000  ;; 10:32
                          1705314900000000  ;; 10:35
                          1705315145123456]) ;; 10:39
          v  (double-array [1.0 2.0 4.0 8.0])
          r (q/q {:from {:ts ts :v v}
                  :group [[:time-bucket 5 :minutes :ts]]
                  :agg [[:sum :v]]})
          ;; Sort by bucket value (ORDER BY doesn't always re-sort group keys)
          sorted (sort-by :__gk_expr_0 r)]
      (is (= 2 (count sorted)))
      (is (== 1705314600000000 (:__gk_expr_0 (first sorted))))
      (is (== 3.0 (:sum (first sorted))))
      (is (== 1705314900000000 (:__gk_expr_0 (second sorted))))
      (is (== 12.0 (:sum (second sorted))))))

  (testing "TIME_BUCKET on hours"
    (let [;; 3 rows: hours 10, 10, 12
          ts (micros-col [1705312800000000   ;; 10:00
                          1705314600000000   ;; 10:30
                          1705320000000000]) ;; 12:00
          data {:ts ts}
          r (q/q {:from data
                  :select [[:as [:time-bucket 1 :hours :ts] :b]]})]
      (is (== 1705312800000000 (:b (nth r 0))))
      (is (== 1705312800000000 (:b (nth r 1))))
      (is (== 1705320000000000 (:b (nth r 2))))))

  (testing "TIME_BUCKET on day-precision DATE column"
    (let [;; 4 epoch-day rows in two 7-day buckets
          d {:type :int64
             :data (long-array [10000 10001 10006 10007])
             :temporal-unit :days}
          r (q/q {:from {:d d}
                  :select [[:as [:time-bucket 7 :days :d] :b]]})]
      ;; epoch-day → floor(d/7)*7
      ;; 10000 → 9996; 10001 → 9996; 10006 → 10003; 10007 → 10003
      (is (== 9996 (:b (nth r 0))))
      (is (== 9996 (:b (nth r 1))))
      (is (== 10003 (:b (nth r 2))))
      (is (== 10003 (:b (nth r 3)))))))

;; ============================================================================
;; Backward compatibility with seconds-based columns
;; ============================================================================

(deftest seconds-backward-compat-test
  (testing "Untagged long[] columns continue to behave as epoch-seconds"
    (let [;; same epoch instant, but as seconds long[] (no temporal-unit)
          ts-secs (long-array [1705314645])
          data {:ts ts-secs}
          r (q/q {:from data
                  :select [[:as [:date-trunc :hour :ts] :h]
                           [:as [:hour :ts] :hh]
                           [:as [:date-add :hours 2 :ts] :a]]})]
      (is (== 1705312800.0 (:h (first r))))
      (is (== 10.0 (:hh (first r))))
      (is (== (+ 1705314645 (* 2 3600)) (:a (first r)))))))
