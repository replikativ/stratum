(ns stratum.window-value-test
  "Tests for FIRST_VALUE / LAST_VALUE / NTH_VALUE window functions.

   Common time-series pattern: OHLC bar generation per symbol/period.
   - open  = FIRST_VALUE(price) within partition
   - close = LAST_VALUE(price)  within partition
   - low   = MIN(price)         within partition (already supported)
   - high  = MAX(price)         within partition (already supported)"
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]))

(deftest first-value-basic-test
  (testing "FIRST_VALUE returns the first row's value per partition"
    (let [data {:sym (long-array [0 0 0 1 1])
                :ts  (long-array [10 20 30 100 200])
                :price (double-array [10.0 11.0 9.5 50.0 51.0])}
          r (q/q {:from data
                  :window [{:op :first-value :col :price
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :open}]
                  :select [:sym :ts :open]
                  :order [[:sym :asc] [:ts :asc]]})]
      (is (== 10.0 (:open (nth r 0))))   ;; sym 0, all rows
      (is (== 10.0 (:open (nth r 1))))
      (is (== 10.0 (:open (nth r 2))))
      (is (== 50.0 (:open (nth r 3))))   ;; sym 1
      (is (== 50.0 (:open (nth r 4)))))))

(deftest last-value-basic-test
  (testing "LAST_VALUE returns the last row's value per partition (full-partition frame)"
    (let [data {:sym (long-array [0 0 0 1 1])
                :ts  (long-array [10 20 30 100 200])
                :price (double-array [10.0 11.0 9.5 50.0 51.0])}
          r (q/q {:from data
                  :window [{:op :last-value :col :price
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :close}]
                  :select [:sym :ts :close]
                  :order [[:sym :asc] [:ts :asc]]})]
      (is (== 9.5 (:close (nth r 0))))    ;; sym 0 last price
      (is (== 9.5 (:close (nth r 1))))
      (is (== 9.5 (:close (nth r 2))))
      (is (== 51.0 (:close (nth r 3))))   ;; sym 1 last price
      (is (== 51.0 (:close (nth r 4)))))))

(deftest nth-value-basic-test
  (testing "NTH_VALUE returns the n-th row's value (1-based, NaN for short partitions)"
    (let [data {:sym (long-array [0 0 0 1])
                :ts  (long-array [10 20 30 100])
                :price (double-array [10.0 11.0 9.5 50.0])}
          r (q/q {:from data
                  :window [{:op :nth-value :col :price
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :offset 2 :as :v2}]
                  :select [:sym :ts :v2]
                  :order [[:sym :asc] [:ts :asc]]})]
      ;; Sym 0: 2nd value is 11.0 (across all rows in partition)
      (is (== 11.0 (:v2 (nth r 0))))
      (is (== 11.0 (:v2 (nth r 1))))
      (is (== 11.0 (:v2 (nth r 2))))
      ;; Sym 1: only 1 row, NTH_VALUE(2) → SQL NULL (nil in row maps)
      (is (nil? (:v2 (nth r 3)))))))

(deftest ohlc-pattern-test
  (testing "OHLC bar generation: open = FIRST_VALUE, close = LAST_VALUE, hi/lo via window agg"
    (let [data {:sym (long-array [0 0 0 0])
                :ts  (long-array [10 20 30 40])
                :price (double-array [10.0 12.0 8.0 11.0])}
          r (q/q {:from data
                  :window [{:op :first-value :col :price :partition-by [:sym] :order-by [[:ts :asc]] :as :open}
                           {:op :last-value  :col :price :partition-by [:sym] :order-by [[:ts :asc]] :as :close}
                           {:op :max :col :price :partition-by [:sym] :order-by [[:ts :asc]]
                            :frame {:type :rows :start :unbounded-preceding :end :unbounded-following}
                            :as :hi}
                           {:op :min :col :price :partition-by [:sym] :order-by [[:ts :asc]]
                            :frame {:type :rows :start :unbounded-preceding :end :unbounded-following}
                            :as :lo}]
                  :order [[:ts :asc]]})
          first-row (first r)]
      (is (== 10.0 (:open first-row)))
      (is (== 11.0 (:close first-row)))
      (is (== 12.0 (:hi first-row)))
      (is (== 8.0  (:lo first-row))))))
