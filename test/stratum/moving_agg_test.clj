(ns stratum.moving-agg-test
  "Tests for q-style named moving aggregates (MAVG, MSUM, MMIN, MMAX,
   MCOUNT, MDEV) — sugar over `op OVER (ROWS BETWEEN N-1 PRECEDING AND
   CURRENT ROW)` with the width N riding on :offset."
  (:require [clojure.test :refer [deftest is testing]]
            [stratum.query :as q]))

(deftest mavg-msum-test
  (testing "MAVG and MSUM with width 3 — expanding for the first 2 rows"
    (let [data {:ts (long-array [1 2 3 4 5])
                :v  (double-array [1.0 2.0 4.0 8.0 16.0])}
          r (q/q {:from data
                  :window [{:op :mavg :col :v :order-by [[:ts :asc]] :offset 3 :as :ma}
                           {:op :msum :col :v :order-by [[:ts :asc]] :offset 3 :as :ms}]
                  :order [[:ts :asc]]})]
      ;; Row 1: window=[1] → avg=1, sum=1
      ;; Row 2: window=[1,2] → avg=1.5, sum=3
      ;; Row 3: window=[1,2,4] → avg=7/3, sum=7
      ;; Row 4: window=[2,4,8] → avg=14/3, sum=14
      ;; Row 5: window=[4,8,16] → avg=28/3, sum=28
      (is (== 1.0  (:ma (nth r 0))))
      (is (== 1.5  (:ma (nth r 1))))
      (is (< (Math/abs (- (/ 7.0 3.0) (:ma (nth r 2)))) 1e-9))
      (is (< (Math/abs (- (/ 14.0 3.0) (:ma (nth r 3)))) 1e-9))
      (is (< (Math/abs (- (/ 28.0 3.0) (:ma (nth r 4)))) 1e-9))
      (is (== 1.0  (:ms (nth r 0))))
      (is (== 28.0 (:ms (nth r 4)))))))

(deftest mmin-mmax-mcount-test
  (testing "MMIN, MMAX, MCOUNT respect the moving frame"
    (let [data {:ts (long-array [1 2 3 4 5])
                :v  (double-array [3.0 1.0 4.0 1.0 5.0])}
          r (q/q {:from data
                  :window [{:op :mmin :col :v :order-by [[:ts :asc]] :offset 3 :as :mn}
                           {:op :mmax :col :v :order-by [[:ts :asc]] :offset 3 :as :mx}
                           {:op :mcount :col :v :order-by [[:ts :asc]] :offset 3 :as :mc}]
                  :order [[:ts :asc]]})]
      ;; window of 3:
      ;;   row 1: [3]                → min=3, max=3, count=1
      ;;   row 2: [3,1]              → min=1, max=3, count=2
      ;;   row 3: [3,1,4]            → min=1, max=4, count=3
      ;;   row 4: [1,4,1]            → min=1, max=4, count=3
      ;;   row 5: [4,1,5]            → min=1, max=5, count=3
      (is (== 3.0 (:mn (nth r 0)))) (is (== 3.0 (:mx (nth r 0)))) (is (== 1.0 (:mc (nth r 0))))
      (is (== 1.0 (:mn (nth r 1)))) (is (== 3.0 (:mx (nth r 1)))) (is (== 2.0 (:mc (nth r 1))))
      (is (== 1.0 (:mn (nth r 2)))) (is (== 4.0 (:mx (nth r 2)))) (is (== 3.0 (:mc (nth r 2))))
      (is (== 1.0 (:mn (nth r 3)))) (is (== 4.0 (:mx (nth r 3)))) (is (== 3.0 (:mc (nth r 3))))
      (is (== 1.0 (:mn (nth r 4)))) (is (== 5.0 (:mx (nth r 4)))) (is (== 3.0 (:mc (nth r 4)))))))

(deftest mdev-test
  (testing "MDEV (moving population stddev) over evenly spaced values"
    (let [data {:ts (long-array [1 2 3 4 5])
                :v  (double-array [10.0 20.0 30.0 40.0 50.0])}
          r (q/q {:from data
                  :window [{:op :mdev :col :v :order-by [[:ts :asc]] :offset 3 :as :sd}]
                  :order [[:ts :asc]]})]
      ;; Row 1 (n=1):    stddev = 0
      ;; Row 2 (n=2):    mean=15, var=((10-15)^2+(20-15)^2)/2 = 25, sd = 5
      ;; Row 3 (n=3):    [10,20,30] mean=20, var=200/3, sd ≈ 8.165
      ;; Rows 4 and 5 likewise: 200/3 → sd ≈ 8.165
      (is (== 0.0 (:sd (nth r 0))))
      (is (== 5.0 (:sd (nth r 1))))
      (is (< (Math/abs (- (Math/sqrt (/ 200.0 3.0)) (:sd (nth r 2)))) 1e-9))
      (is (< (Math/abs (- (Math/sqrt (/ 200.0 3.0)) (:sd (nth r 3)))) 1e-9))
      (is (< (Math/abs (- (Math/sqrt (/ 200.0 3.0)) (:sd (nth r 4)))) 1e-9)))))
