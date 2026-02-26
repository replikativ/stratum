(ns stratum.api-test
  "Tests for the Stratum top-level API."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.api :as st]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test Data
;; ============================================================================

(def ^:private prices (double-array [10.0 20.0 30.0 40.0 50.0]))
(def ^:private quantities (long-array [1 2 3 4 5]))
(def ^:private categories (into-array String ["A" "B" "A" "B" "A"]))

;; ============================================================================
;; Query Map Tests
;; ============================================================================

(deftest q-with-map-test
  (testing "Simple aggregate with query map"
    (let [result (st/q {:from {:price prices}
                        :agg [[:sum :price]]})]
      (is (= 1 (count result)))
      (is (= 150.0 (:sum (first result))))))

  (testing "Filter + aggregate"
    (let [result (st/q {:from {:price prices :qty quantities}
                        :where [[:> :price 20.0]]
                        :agg [[:count]]})]
      (is (= 3 (:_count (first result))))))

  (testing "Group-by"
    (let [result (st/q {:from {:cat categories :price prices}
                        :group [:cat]
                        :agg [[:sum :price]]})]
      (is (= 2 (count result)))
      ;; A: 10+30+50=90, B: 20+40=60
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (= 90.0 (get by-cat "A")))
        (is (= 60.0 (get by-cat "B")))))))

;; ============================================================================
;; SQL Tests
;; ============================================================================

(deftest q-with-sql-test
  (testing "Simple SQL aggregate"
    (let [result (st/q "SELECT SUM(price) FROM orders"
                       {"orders" {:price prices}})]
      (is (= 1 (count result)))
      (is (= 150.0 (:sum (first result))))))

  (testing "SQL with WHERE"
    (let [result (st/q "SELECT COUNT(*) FROM orders WHERE price > 25"
                       {"orders" {:price prices}})]
      (is (= 3 (:_count (first result))))))

  (testing "SQL requires table map"
    (is (thrown? clojure.lang.ExceptionInfo
                 (st/q "SELECT * FROM orders")))))

;; ============================================================================
;; Explain Tests
;; ============================================================================

(deftest explain-test
  (testing "Explain returns strategy info"
    (let [plan (st/explain {:from {:price prices :qty quantities}
                            :where [[:< :qty 3]]
                            :agg [[:sum :price]]})]
      (is (keyword? (:strategy plan)))
      (is (= 5 (:n-rows plan)))
      (is (pos? (:columns plan)))))

  (testing "Explain with SQL"
    (let [plan (st/explain "SELECT SUM(price) FROM orders WHERE price > 10"
                           {"orders" {:price prices}})]
      (is (keyword? (:strategy plan)))
      (is (= 5 (:n-rows plan))))))

;; ============================================================================
;; from-maps Tests
;; ============================================================================

(deftest from-maps-test
  (testing "Convert maps to column format"
    (let [table (st/from-maps [{:name "Alice" :age 30}
                               {:name "Bob" :age 25}
                               {:name "Carol" :age 35}])
          result (st/q {:from table :agg [[:count]]})]
      (is (= 3 (:_count (first result))))))

  (testing "Query over from-maps data"
    (let [table (st/from-maps [{:x 10 :y 1}
                               {:x 20 :y 2}
                               {:x 30 :y 3}])
          result (st/q {:from table
                        :agg [[:sum :x]]})]
      (is (= 60.0 (:sum (first result)))))))

;; ============================================================================
;; Re-export Tests
;; ============================================================================

(deftest re-exports-test
  (testing "encode-column is available"
    (let [encoded (st/encode-column (double-array [1.0 2.0 3.0]))]
      (is (= :float64 (:type encoded)))
      (is (some? (:data encoded))))))
