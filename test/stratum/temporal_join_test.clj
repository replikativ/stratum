(ns stratum.temporal-join-test
  "Tests for the time-series-specific join helpers in stratum.api:
     - window-join : q `wj`-style range-join + per-row aggregation
     - latest-on   : DISTINCT-ON-style most-recent-row-per-partition"
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.api :as st]))

;; ============================================================================
;; window-join — for each left row, aggregate all right rows in [t+lo, t+hi]
;; ============================================================================

(def ^:private min-us 60000000)

(deftest window-join-basic-test
  (testing "5-minute lookback window aggregates: avg, count, max"
    (let [trades {:ts {:type :int64
                       :data (long-array [(* 5 min-us) (* 10 min-us) (* 20 min-us)])
                       :temporal-unit :micros}}
          quotes {:t {:type :int64
                      :data (long-array [(* 1 min-us) (* 2 min-us) (* 3 min-us) (* 4 min-us)
                                         (* 6 min-us) (* 8 min-us) (* 9 min-us)
                                         (* 18 min-us) (* 19 min-us)])
                      :temporal-unit :micros}
                  :bid (double-array [10.0 11.0 12.0 13.0 15.0 16.0 17.0 25.0 26.0])}
          r (st/window-join trades quotes
                            {:asof-on [:ts :t]
                             :window [-5 0 :minutes]
                             :temporal-unit :micros
                             :agg {:avg-bid {:op :avg :col :bid}
                                   :n {:op :count}
                                   :max-bid {:op :max :col :bid}
                                   :min-bid {:op :min :col :bid}
                                   :sum-bid {:op :sum :col :bid}}})]
      ;; Trade at min 5: window [0,5] → quotes at minutes 1,2,3,4
      ;;   n=4, avg=11.5, sum=46, min=10, max=13
      (is (== 4.0  (aget ^doubles (:data (:n r)) 0)))
      (is (== 11.5 (aget ^doubles (:data (:avg-bid r)) 0)))
      (is (== 46.0 (aget ^doubles (:data (:sum-bid r)) 0)))
      (is (== 10.0 (aget ^doubles (:data (:min-bid r)) 0)))
      (is (== 13.0 (aget ^doubles (:data (:max-bid r)) 0)))
      ;; Trade at min 10: window [5,10] → quotes at minutes 6,8,9
      ;;   n=3, avg=16, sum=48, min=15, max=17
      (is (== 3.0  (aget ^doubles (:data (:n r)) 1)))
      (is (== 16.0 (aget ^doubles (:data (:avg-bid r)) 1)))
      (is (== 48.0 (aget ^doubles (:data (:sum-bid r)) 1)))
      (is (== 15.0 (aget ^doubles (:data (:min-bid r)) 1)))
      (is (== 17.0 (aget ^doubles (:data (:max-bid r)) 1)))
      ;; Trade at min 20: window [15,20] → quotes at minutes 18,19
      ;;   n=2, avg=25.5, sum=51, min=25, max=26
      (is (== 2.0  (aget ^doubles (:data (:n r)) 2)))
      (is (== 25.5 (aget ^doubles (:data (:avg-bid r)) 2)))
      (is (== 51.0 (aget ^doubles (:data (:sum-bid r)) 2)))
      (is (== 25.0 (aget ^doubles (:data (:min-bid r)) 2)))
      (is (== 26.0 (aget ^doubles (:data (:max-bid r)) 2))))))

(deftest window-join-empty-window-test
  (testing "When no right rows fall in the window, COUNT=0 and AVG/MIN/MAX = NaN"
    (let [trades {:ts {:type :int64 :data (long-array [(* 100 min-us)])
                       :temporal-unit :micros}}
          quotes {:t {:type :int64 :data (long-array [(* 1 min-us) (* 2 min-us)])
                      :temporal-unit :micros}
                  :v (double-array [1.0 2.0])}
          r (st/window-join trades quotes
                            {:asof-on [:ts :t]
                             :window [-1 0 :minutes]
                             :temporal-unit :micros
                             :agg {:n {:op :count}
                                   :avg {:op :avg :col :v}
                                   :sum {:op :sum :col :v}
                                   :max {:op :max :col :v}}})]
      (is (== 0.0 (aget ^doubles (:data (:n r)) 0)))
      (is (Double/isNaN (aget ^doubles (:data (:avg r)) 0)))
      (is (== 0.0 (aget ^doubles (:data (:sum r)) 0)))
      (is (Double/isNaN (aget ^doubles (:data (:max r)) 0))))))

;; ============================================================================
;; latest-on — most recent row per partition (DISTINCT ON / LATEST ON)
;; ============================================================================

(deftest latest-on-basic-test
  (testing "latest-on returns the most recent row per partition"
    (let [data {:sensor (long-array [1 2 1 2 1])
                :ts (long-array [10 20 30 40 50])
                :reading (double-array [100.0 200.0 110.0 210.0 120.0])}
          r (sort-by :sensor
                     (st/latest-on {:from data}
                                   {:partition-by [:sensor]
                                    :order-by [[:ts :asc]]}))]
      ;; sensor 1: latest ts=50, reading=120
      ;; sensor 2: latest ts=40, reading=210
      (is (= 2 (count r)))
      (is (= 1 (:sensor (first r))))
      (is (= 50 (:ts (first r))))
      (is (== 120.0 (:reading (first r))))
      (is (= 2 (:sensor (second r))))
      (is (= 40 (:ts (second r))))
      (is (== 210.0 (:reading (second r)))))))

(deftest latest-on-multi-partition-key-test
  (testing "latest-on with multi-key partition"
    (let [data {:venue (into-array String ["NYSE" "NASDAQ" "NYSE" "NASDAQ"])
                :sym (long-array [1 1 1 1])
                :ts (long-array [10 20 30 40])
                :px (double-array [100.0 200.0 110.0 210.0])}
          r (st/latest-on {:from data}
                          {:partition-by [:venue :sym]
                           :order-by [[:ts :asc]]})]
      ;; Each (venue, sym) should appear once, with latest ts
      (is (= 2 (count r))))))
