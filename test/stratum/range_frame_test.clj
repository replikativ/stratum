(ns stratum.range-frame-test
  "Tests for RANGE BETWEEN INTERVAL frame semantics.

   In ROWS mode, the frame is a count of rows; in RANGE mode, the frame is
   a value distance on the ORDER BY column. RANGE is the correct mode for
   irregular time series — a 7-day rolling average over irregularly-spaced
   trades should cover 7 wall-clock days regardless of how many rows fall
   within."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]))

(def ^:private day-us 86400000000)
(def ^:private sec-us 1000000)

(defn- ts-col [vs]
  {:type :int64 :data (long-array vs) :temporal-unit :micros})

(deftest range-7-day-rolling-sum-test
  (testing "RANGE BETWEEN 7 DAYS PRECEDING AND CURRENT ROW"
    (let [;; Trades at days 1, 5, 8, 11 (irregular spacing)
          ts (ts-col [(* 1 day-us) (* 5 day-us) (* 8 day-us) (* 11 day-us)])
          v  (double-array [10.0 20.0 30.0 40.0])
          r (q/q {:from {:ts ts :v v}
                  :window [{:op :sum :col :v
                            :order-by [[:ts :asc]]
                            :frame {:type :range
                                    :start [(* 7 day-us) :preceding]
                                    :end :current-row}
                            :as :rolling-sum}]
                  :order [[:ts :asc]]})]
      ;; day 1:  window=[day1]                 → 10
      ;; day 5:  window=[day1, day5]           → 30
      ;; day 8:  window=[day1, day5, day8]     (day1 is exactly 7 days back, inclusive) → 60
      ;; day 11: window=[day5, day8, day11]    (day1 excluded — 11-1=10 > 7) → 90
      (is (== 10.0 (:rolling-sum (nth r 0))))
      (is (== 30.0 (:rolling-sum (nth r 1))))
      (is (== 60.0 (:rolling-sum (nth r 2))))
      (is (== 90.0 (:rolling-sum (nth r 3)))))))

(deftest range-rows-vs-range-test
  (testing "ROWS and RANGE give different results on irregular time series"
    (let [;; rows at 1, 2, 3, 100, 101 seconds
          ts (ts-col [(* 1 sec-us) (* 2 sec-us) (* 3 sec-us)
                      (* 100 sec-us) (* 101 sec-us)])
          v  (double-array [1.0 2.0 3.0 4.0 5.0])
          ;; ROWS: 2-PRECEDING window = last 3 rows
          rows-r (q/q {:from {:ts ts :v v}
                       :window [{:op :sum :col :v
                                 :order-by [[:ts :asc]]
                                 :frame {:type :rows :start [2 :preceding] :end :current-row}
                                 :as :s}]
                       :order [[:ts :asc]]})
          ;; RANGE: 2-second-PRECEDING window covers physical 2 seconds
          range-r (q/q {:from {:ts ts :v v}
                        :window [{:op :sum :col :v
                                  :order-by [[:ts :asc]]
                                  :frame {:type :range
                                          :start [(* 2 sec-us) :preceding]
                                          :end :current-row}
                                  :as :s}]
                        :order [[:ts :asc]]})]
      ;; Row at 100s: ROWS includes 3, 100  but only 100 in window of 2s — wait,
      ;; 3-row sliding window is [98s, 100s, 100s] = rows 1,2,3? No:
      ;; ROWS 2 PRECEDING includes the 2 rows before plus current = 3 rows total.
      ;; So row at 100s gets {3, 100, 101? no — 101 is current. Actually let me recompute}
      ;; rows in order: [1s, 2s, 3s, 100s, 101s] indices [0..4]
      ;; idx 3 (100s): rows-window=[1,2,3] → sum= 2+3+4=9
      (is (== 9.0 (:s (nth rows-r 3))))
      ;; RANGE 2s PRECEDING at row 100s: window=[98s, 100s] → only 100s → 4
      (is (== 4.0 (:s (nth range-r 3)))))))

(deftest range-centered-window-test
  (testing "RANGE BETWEEN N SECONDS PRECEDING AND N SECONDS FOLLOWING"
    (let [;; 1-second-spaced rows
          ts (ts-col [(* 1 sec-us) (* 2 sec-us) (* 3 sec-us) (* 4 sec-us) (* 5 sec-us)])
          v  (double-array [1.0 2.0 3.0 4.0 5.0])
          r (q/q {:from {:ts ts :v v}
                  :window [{:op :sum :col :v
                            :order-by [[:ts :asc]]
                            :frame {:type :range
                                    :start [(* 1 sec-us) :preceding]
                                    :end   [(* 1 sec-us) :following]}
                            :as :centered-sum}]
                  :order [[:ts :asc]]})]
      ;; t=1s: [0s, 2s] → rows at 1s, 2s    → 1+2=3
      ;; t=2s: [1s, 3s] → 1+2+3 = 6
      ;; t=3s: [2s, 4s] → 2+3+4 = 9
      ;; t=4s: [3s, 5s] → 3+4+5 = 12
      ;; t=5s: [4s, 6s] → 4+5 = 9
      (is (== 3.0  (:centered-sum (nth r 0))))
      (is (== 6.0  (:centered-sum (nth r 1))))
      (is (== 9.0  (:centered-sum (nth r 2))))
      (is (== 12.0 (:centered-sum (nth r 3))))
      (is (== 9.0  (:centered-sum (nth r 4)))))))

(deftest range-with-partition-test
  (testing "RANGE frames respect PARTITION BY"
    (let [;; Two symbols, each with 3 rows at days 1, 2, 5
          sym (long-array [0 0 0 1 1 1])
          ts  (ts-col [(* 1 day-us) (* 2 day-us) (* 5 day-us)
                       (* 1 day-us) (* 2 day-us) (* 5 day-us)])
          v   (double-array [10.0 20.0 30.0 100.0 200.0 300.0])
          r (q/q {:from {:sym sym :ts ts :v v}
                  :window [{:op :sum :col :v
                            :partition-by [:sym]
                            :order-by [[:ts :asc]]
                            :frame {:type :range
                                    :start [(* 3 day-us) :preceding]
                                    :end :current-row}
                            :as :rolling}]
                  :order [[:sym :asc] [:ts :asc]]})]
      ;; sym 0:
      ;;   day 1: [day1]            → 10
      ;;   day 2: [day1, day2]      → 30
      ;;   day 5: [day2, day5]      (5-3=2, day1 excluded) → 50
      ;; sym 1:
      ;;   day 1: [day1]            → 100
      ;;   day 2: [day1, day2]      → 300
      ;;   day 5: [day2, day5]      → 500
      (is (== 10.0  (:rolling (nth r 0))))
      (is (== 30.0  (:rolling (nth r 1))))
      (is (== 50.0  (:rolling (nth r 2))))
      (is (== 100.0 (:rolling (nth r 3))))
      (is (== 300.0 (:rolling (nth r 4))))
      (is (== 500.0 (:rolling (nth r 5)))))))

(deftest range-avg-test
  (testing "RANGE-frame AVG = SUM / COUNT over the value-window"
    (let [ts (ts-col [(* 1 sec-us) (* 2 sec-us) (* 3 sec-us)])
          v  (double-array [10.0 20.0 30.0])
          r (q/q {:from {:ts ts :v v}
                  :window [{:op :avg :col :v
                            :order-by [[:ts :asc]]
                            :frame {:type :range
                                    :start [(* 1 sec-us) :preceding]
                                    :end :current-row}
                            :as :ma}]
                  :order [[:ts :asc]]})]
      ;; t=1: only self → 10
      ;; t=2: [1,2] → (10+20)/2 = 15
      ;; t=3: [2,3] → (20+30)/2 = 25
      (is (== 10.0 (:ma (nth r 0))))
      (is (== 15.0 (:ma (nth r 1))))
      (is (== 25.0 (:ma (nth r 2)))))))
