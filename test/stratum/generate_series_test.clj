(ns stratum.generate-series-test
  "Tests for stratum.api/generate-series — produces a dense column suitable
   as a `:from` value for gap-filling time spines, integer ranges, etc."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.api :as st]
            [stratum.query :as q]))

(deftest int-range-test
  (testing "generate-series for integer range"
    (let [r (st/generate-series 1 5)]
      (is (= [1 2 3 4 5] (vec (:data (:value r)))))
      (is (= :int64 (:type (:value r)))))))

(deftest int-range-with-step-test
  (testing "generate-series with explicit step"
    (let [r (st/generate-series 0 100 25)]
      (is (= [0 25 50 75 100] (vec (:data (:value r))))))))

(deftest float-range-test
  (testing "generate-series with double step produces double[]"
    (let [r (st/generate-series 0.0 1.0 0.25)]
      (is (= [0.0 0.25 0.5 0.75 1.0] (vec (:data (:value r)))))
      (is (= :float64 (:type (:value r)))))))

(deftest temporal-spine-test
  (testing "generate-series temporal form produces :temporal-unit-tagged column"
    (let [day-us 86400000000
          r (st/generate-series 0 (* 5 day-us) 1 :days :micros)
          arr (:data (:value r))]
      (is (= :micros (:temporal-unit (:value r))))
      (is (= 6 (alength arr)))
      (is (= 0 (aget arr 0)))
      (is (= (* 5 day-us) (aget arr 5))))))

(deftest gap-fill-via-asof-left-test
  (testing "Gap-fill: dense spine LEFT ASOF JOIN sparse measurements + LOCF semantics"
    (let [day-us 86400000000
          spine (st/generate-series 0 (* 5 day-us) 1 :days :micros)
          data {:t {:type :int64 :data (long-array [(* 1 day-us) (* 3 day-us)])
                    :temporal-unit :micros}
                :v (double-array [10.0 30.0])}
          r (q/q {:from {:ts (:value spine)}
                  :join [{:with data
                          :type :asof-left
                          :on [:>= :ts :t]}]
                  :select [:ts :v]})]
      (is (= 6 (count r)))
      ;; day 0: before first sample → nil
      (is (nil? (:v (nth r 0))))
      ;; days 1..2 → carry value 10.0 from day 1
      (is (== 10.0 (:v (nth r 1))))
      (is (== 10.0 (:v (nth r 2))))
      ;; days 3..5 → carry 30.0 from day 3
      (is (== 30.0 (:v (nth r 3))))
      (is (== 30.0 (:v (nth r 4))))
      (is (== 30.0 (:v (nth r 5)))))))
