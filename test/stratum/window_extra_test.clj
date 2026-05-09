(ns stratum.window-extra-test
  "Tests for time-series-specific window functions: FILLS (LOCF), EMA, RLEID."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]))

(deftest fills-locf-basic-test
  (testing "FILLS forward-fills NaN values within partition"
    (let [data {:sym (long-array [0 0 0 0 1 1])
                :ts  (long-array [10 20 30 40 100 200])
                :v   (double-array [1.0 Double/NaN Double/NaN 4.0 50.0 Double/NaN])}
          r (q/q {:from data
                  :window [{:op :fills :col :v
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :filled}]
                  :order [[:sym :asc] [:ts :asc]]})]
      (is (== 1.0  (:filled (nth r 0))))
      (is (== 1.0  (:filled (nth r 1))))
      (is (== 1.0  (:filled (nth r 2))))
      (is (== 4.0  (:filled (nth r 3))))
      (is (== 50.0 (:filled (nth r 4))))
      (is (== 50.0 (:filled (nth r 5)))))))

(deftest fills-leading-nan-test
  (testing "Leading NaNs in a partition stay NaN (no prior value to carry forward)"
    (let [data {:sym (long-array [0 0 0])
                :ts  (long-array [1 2 3])
                :v   (double-array [Double/NaN Double/NaN 5.0])}
          r (q/q {:from data
                  :window [{:op :fills :col :v
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :filled}]
                  :order [[:ts :asc]]})]
      ;; nil indicates SQL NULL in row-map output
      (is (nil? (:filled (nth r 0))))
      (is (nil? (:filled (nth r 1))))
      (is (== 5.0 (:filled (nth r 2)))))))

(deftest ema-with-alpha-test
  (testing "EMA with alpha < 1 uses it directly as smoothing factor"
    (let [data {:ts (long-array [1 2 3 4])
                :v  (double-array [10.0 20.0 30.0 40.0])}
          r (q/q {:from data
                  :window [{:op :ema :col :v
                            :order-by [[:ts :asc]]
                            :offset 0.5 :as :ema}]
                  :order [[:ts :asc]]})]
      ;; α=0.5: ema[0]=10; ema[1]=10+0.5*(20-10)=15; ema[2]=15+0.5*(30-15)=22.5;
      ;; ema[3]=22.5+0.5*(40-22.5)=31.25
      (is (== 10.0   (:ema (nth r 0))))
      (is (== 15.0   (:ema (nth r 1))))
      (is (== 22.5   (:ema (nth r 2))))
      (is (== 31.25  (:ema (nth r 3)))))))

(deftest ema-with-period-test
  (testing "EMA with offset >= 1 treats it as a period N (alpha = 2/(N+1))"
    (let [data {:ts (long-array [1 2 3 4])
                :v  (double-array [10.0 20.0 30.0 40.0])}
          r (q/q {:from data
                  :window [{:op :ema :col :v
                            :order-by [[:ts :asc]]
                            :offset 3 :as :ema}]
                  :order [[:ts :asc]]})
          ;; α = 2/(3+1) = 0.5 (same numbers as above)
          ]
      (is (== 10.0   (:ema (nth r 0))))
      (is (== 15.0   (:ema (nth r 1))))
      (is (== 22.5   (:ema (nth r 2))))
      (is (== 31.25  (:ema (nth r 3)))))))

(deftest rleid-basic-test
  (testing "RLEID assigns a new ID each time the value changes (in sorted order)"
    (let [data {:sym (long-array [0 0 0 0 0 0])
                :ts  (long-array [1 2 3 4 5 6])
                :v   (long-array [1 1 2 2 1 1])}
          r (q/q {:from data
                  :window [{:op :rleid :col :v
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :run}]
                  :order [[:ts :asc]]})]
      (is (== 1.0 (:run (nth r 0))))
      (is (== 1.0 (:run (nth r 1))))
      (is (== 2.0 (:run (nth r 2))))
      (is (== 2.0 (:run (nth r 3))))
      (is (== 3.0 (:run (nth r 4))))
      (is (== 3.0 (:run (nth r 5)))))))

(deftest rleid-restarts-per-partition-test
  (testing "RLEID restarts at 1 for each new partition"
    (let [data {:sym (long-array [0 0 1 1 1])
                :ts  (long-array [1 2 1 2 3])
                :v   (long-array [1 2 9 9 8])}
          r (q/q {:from data
                  :window [{:op :rleid :col :v
                            :partition-by [:sym] :order-by [[:ts :asc]]
                            :as :run}]
                  :order [[:sym :asc] [:ts :asc]]})]
      ;; sym 0: [1, 2] → [1, 2]
      ;; sym 1: [9, 9, 8] → [1, 1, 2]
      (is (== 1.0 (:run (nth r 0))))
      (is (== 2.0 (:run (nth r 1))))
      (is (== 1.0 (:run (nth r 2))))
      (is (== 1.0 (:run (nth r 3))))
      (is (== 2.0 (:run (nth r 4)))))))
