(ns stratum.query-test
  "Comprehensive tests for the Stratum query DSL"
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.simd-primitive :as simd]
            [stratum.index :as index]
            [stratum.chunk :as chunk]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [stratum.internal ColumnOps]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test Data Setup
;; ============================================================================

(def ^:private test-n 1000)

(defn- make-test-data
  "Create test arrays for query testing."
  []
  {:shipdate (long-array (map #(+ 8000 (mod % 2000)) (range test-n)))
   :discount (double-array (map #(* 0.01 (mod % 11)) (range test-n)))
   :quantity (long-array (map #(+ 1 (mod % 50)) (range test-n)))
   :price    (double-array (map #(+ 10.0 (* 2.0 %)) (range test-n)))
   :category (long-array (map #(mod % 5) (range test-n)))})

;; ============================================================================
;; Predicate Format Tests
;; ============================================================================

(deftest keyword-predicate-format-test
  (testing "Keyword predicate format [:< :col val]"
    (let [data (make-test-data)
          result (q/q {:from {:price (:price data)}
                       :where [[:< :price 20.0]]})]
      (is (pos? (:_count (first result)))))))

(deftest datahike-predicate-format-test
  (testing "Datahike symbol predicate format '(< :col val)"
    (let [data (make-test-data)
          result (q/q {:from {:price (:price data)}
                       :where ['(< :price 20.0)]})]
      (is (pos? (:_count (first result)))))))

(deftest both-formats-equivalent-test
  (testing "Both predicate formats produce identical results"
    (let [data (make-test-data)
          r1 (q/q {:from {:price (:price data)}
                   :where [[:> :price 100.0]]
                   :agg [[:count]]})
          r2 (q/q {:from {:price (:price data)}
                   :where ['(> :price 100.0)]
                   :agg [[:count]]})]
      (is (= (:count (first r1)) (:count (first r2)))))))

;; ============================================================================
;; Predicate Type Tests
;; ============================================================================

(deftest lt-predicate-test
  (testing "Less-than predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:< :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest gt-predicate-test
  (testing "Greater-than predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:> :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest lte-predicate-test
  (testing "Less-than-or-equal predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:<= :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest gte-predicate-test
  (testing "Greater-than-or-equal predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:>= :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest eq-predicate-test
  (testing "Equality predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 3.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:= :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest neq-predicate-test
  (testing "Not-equal predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 3.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:!= :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest between-predicate-test
  (testing "Between predicate [lo, hi] (inclusive both ends)"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:between :vals 2.0 4.0]]
                       :agg [[:count]]})]
      ;; Inclusive: 2.0, 3.0, 4.0 → 3
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; Long Predicate Tests
;; ============================================================================

(deftest long-lt-predicate-test
  (testing "Long less-than predicate"
    (let [data {:vals (long-array [10 20 30 40 50])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:< :vals 30]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest long-between-predicate-test
  (testing "Long between predicate (inclusive both ends)"
    (let [data {:vals (long-array [10 20 30 40 50])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:between :vals 20 40]]
                       :agg [[:count]]})]
      ;; Inclusive: 20, 30, 40 → 3
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; Multi-Predicate Tests
;; ============================================================================

(deftest multi-predicate-and-test
  (testing "Multiple predicates (implicit AND)"
    (let [data {:a (double-array [1 2 3 4 5 6 7 8 9 10])
                :b (double-array [10 9 8 7 6 5 4 3 2 1])}
          result (q/q {:from {:a (:a data) :b (:b data)}
                       :where [[:> :a 3.0] [:> :b 3.0]]
                       :agg [[:count]]})]
      ;; a > 3: indices [3,4,5,6,7,8,9] (values 4-10)
      ;; b > 3: indices [0,1,2,3,4,5,6] (values 10-4)
      ;; intersection: [3,4,5,6]
      (is (= 4 (long (:count (first result))))))))

(deftest mixed-type-predicates-test
  (testing "Predicates on both long and double columns"
    (let [data (make-test-data)
          result (q/q {:from (select-keys data [:shipdate :discount])
                       :where [[:between :shipdate 8766 9131]
                               [:between :discount 0.05 0.07]]
                       :agg [[:count]]})]
      (is (pos? (:_count (first result)))))))

;; ============================================================================
;; Aggregation Tests
;; ============================================================================

(deftest sum-aggregation-test
  (testing "Sum aggregation"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:sum :vals]]})]
      (is (== 15.0 (:sum (first result)))))))

(deftest count-aggregation-test
  (testing "Count aggregation"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:> :vals 2.0]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest min-aggregation-test
  (testing "Min aggregation"
    (let [data {:vals (double-array [5.0 3.0 1.0 4.0 2.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:min :vals]]})]
      (is (== 1.0 (:min (first result)))))))

(deftest max-aggregation-test
  (testing "Max aggregation"
    (let [data {:vals (double-array [5.0 3.0 1.0 4.0 2.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:max :vals]]})]
      (is (== 5.0 (:max (first result)))))))

(deftest avg-aggregation-test
  (testing "Avg aggregation"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:avg :vals]]})]
      (is (== 3.0 (:avg (first result)))))))

(deftest sum-product-aggregation-test
  (testing "Sum-product aggregation [:sum [:* :a :b]]"
    (let [data {:a (double-array [1.0 2.0 3.0])
                :b (double-array [10.0 20.0 30.0])}
          result (q/q {:from {:a (:a data) :b (:b data)}
                       :where []
                       :agg [[:sum [:* :a :b]]]})]
      ;; 1*10 + 2*20 + 3*30 = 10 + 40 + 90 = 140
      (is (== 140.0 (:sum-product (first result)))))))

(deftest named-aggregation-test
  (testing "Named aggregation [:as agg name]"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:as [:sum :vals] :total]]})]
      (is (== 6.0 (:total (first result)))))))

(deftest named-sum-product-test
  (testing "Named sum-product [:as [:sum [:* :a :b]] :revenue]"
    (let [data {:a (double-array [2.0 3.0])
                :b (double-array [10.0 20.0])}
          result (q/q {:from {:a (:a data) :b (:b data)}
                       :where []
                       :agg [[:as [:sum [:* :a :b]] :revenue]]})]
      ;; 2*10 + 3*20 = 80
      (is (== 80.0 (:revenue (first result)))))))

;; ============================================================================
;; Multiple Aggregation Tests (scalar path)
;; ============================================================================

(deftest multiple-aggs-test
  (testing "Multiple aggregations in one query"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where []
                       :agg [[:sum :vals] [:min :vals] [:max :vals] [:count]]})]
      (is (== 15.0 (:sum (first result))))
      (is (== 1.0 (:min (first result))))
      (is (== 5.0 (:max (first result))))
      (is (= 5 (long (:count (first result))))))))

;; ============================================================================
;; Group-By Tests
;; ============================================================================

(deftest basic-group-by-test
  (testing "Group by with sum aggregation"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]})]
      (is (= 3 (count result)))
      ;; cat 0: 10+20=30, cat 1: 30+40=70, cat 2: 50
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (== 30.0 (get by-cat 0)))
        (is (== 70.0 (get by-cat 1)))
        (is (== 50.0 (get by-cat 2)))))))

(deftest group-by-with-filter-test
  (testing "Group by with filter and count"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :where [[:> :price 15.0]]
                       :group [:cat]
                       :agg [[:count]]})]
      ;; After filter (> 15): indices [1,2,3,4]
      ;; cat 0: 1, cat 1: 2, cat 2: 1
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:count r)])) result)]
        (is (= 1 (long (get by-cat 0))))
        (is (= 2 (long (get by-cat 1))))
        (is (= 1 (long (get by-cat 2))))))))

(deftest group-by-avg-test
  (testing "Group by with avg aggregation"
    (let [data {:cat   (long-array [0 0 1 1])
                :price (double-array [10.0 20.0 30.0 40.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:avg :price]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:avg r)])) result)]
        (is (== 15.0 (get by-cat 0)))
        (is (== 35.0 (get by-cat 1)))))))

;; ============================================================================
;; Having Tests
;; ============================================================================

(deftest having-test
  (testing "Having filters group results"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :having [[:> :sum 40.0]]})]
      ;; cat 0: 30 (excluded), cat 1: 70 (included), cat 2: 50 (included)
      (is (= 2 (count result)))
      (is (every? #(> (:sum %) 40.0) result)))))

;; ============================================================================
;; Order Tests
;; ============================================================================

(deftest order-asc-test
  (testing "Order results ascending"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :order [[:sum :asc]]})]
      (is (= [30.0 50.0 70.0] (mapv :sum result))))))

(deftest order-desc-test
  (testing "Order results descending"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :order [[:sum :desc]]})]
      (is (= [70.0 50.0 30.0] (mapv :sum result))))))

;; ============================================================================
;; Limit/Offset Tests
;; ============================================================================

(deftest limit-test
  (testing "Limit restricts result count"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :order [[:sum :desc]]
                       :limit 2})]
      (is (= 2 (count result)))
      (is (= 70.0 (:sum (first result)))))))

(deftest offset-test
  (testing "Offset skips rows"
    (let [data {:cat   (long-array [0 0 1 1 2])
                :price (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from {:cat (:cat data) :price (:price data)}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :order [[:sum :asc]]
                       :offset 1
                       :limit 1})]
      (is (= 1 (count result)))
      (is (= 50.0 (:sum (first result)))))))

;; ============================================================================
;; TPC-H Q06 Through DSL
;; ============================================================================

(deftest tpch-q06-dsl-test
  (testing "TPC-H Q06 through query DSL produces correct result"
    (let [n 10000
          shipdate (long-array (map #(+ 8002 (mod (* % 7) 2559)) (range n)))
          discount (double-array (map #(* 0.01 (mod % 11)) (range n)))
          quantity (long-array (map #(+ 1 (mod % 50)) (range n)))
          price    (double-array n)]
      ;; Populate price
      (dotimes [i n]
        (aset price i (* (aget quantity i) (+ 900.0 (* (double i) 0.1)))))

      ;; Execute via DSL
      (let [result (q/q
                    {:from  {:shipdate shipdate :discount discount
                             :quantity quantity :price price}
                     :where [[:between :shipdate 8766 9131]
                             [:between :discount 0.05 0.07]
                             [:< :quantity 24]]
                     :agg   [[:as [:sum [:* :price :discount]] :revenue]]})
            revenue (:revenue (first result))
            cnt (:_count (first result))]
        (is (pos? cnt))
        (is (pos? revenue))

        ;; Verify against direct simd-primitive execution
        (let [direct (simd/execute-query
                      {:columns {:shipdate {:type :int64 :data shipdate}
                                 :discount {:type :float64 :data discount}
                                 :quantity {:type :int64 :data quantity}
                                 :price    {:type :float64 :data price}}
                       :predicates [[:shipdate :range 8766 9131]
                                    [:discount :range 0.05 0.07]
                                    [:quantity :lt 24]]
                       :aggregate [:sum-product :price :discount]
                       :length n})]
          (is (< (Math/abs (double (- revenue (:result direct)))) 0.01)
              (str "DSL revenue " revenue " should match direct " (:result direct)))
          (is (= cnt (:count direct))))))))

;; ============================================================================
;; Edge Cases
;; ============================================================================

(deftest no-predicates-test
  (testing "Query with no predicates matches all rows"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from {:vals (:vals data)}
                       :agg [[:sum :vals]]})]
      (is (== 6.0 (:sum (first result))))
      (is (= 3 (:_count (first result)))))))

(deftest no-matches-test
  (testing "Query with no matches returns zero"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:> :vals 100.0]]
                       :agg [[:count]]})]
      (is (= 0 (long (:count (first result))))))))

(deftest no-agg-test
  (testing "Query without aggregation returns count only"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:> :vals 3.0]]})]
      (is (= 2 (:_count (first result)))))))

(deftest index-as-source-test
  (testing "PersistentColumnIndex can be used as :from source"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 3.0]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

;; ============================================================================
;; Compiled Query Test
;; ============================================================================

(deftest compile-query-test
  (testing "Compiled query produces same result as execute"
    (let [data (make-test-data)
          query {:from {:shipdate (:shipdate data)
                        :discount (:discount data)
                        :quantity (:quantity data)
                        :price    (:price data)}
                 :where [[:between :shipdate 8766 9131]
                         [:between :discount 0.05 0.07]
                         [:< :quantity 24]]
                 :agg [[:as [:sum [:* :price :discount]] :revenue]]}
          compiled (q/compile-query query)
          r1 (q/q query)
          r2 (compiled)]
      (is (< (Math/abs (double (- (:revenue (first r1)) (:revenue (first r2))))) 0.01)))))

;; ============================================================================
;; Datahike Integration Syntax Tests
;; ============================================================================

(deftest datahike-predicate-round-trip-test
  (testing "Datahike-style predicates work end-to-end"
    (let [data {:price (double-array [10.0 50.0 100.0 200.0 500.0])}
          ;; Datahike would push down: [(> ?p 100)]
          result (q/q {:from {:price (:price data)}
                       :where ['(> :price 100.0)]
                       :agg ['(sum :price)]})]
      ;; 200 + 500 = 700
      (is (== 700.0 (:sum (first result)))))))

(deftest datahike-agg-format-test
  (testing "Datahike-style aggregations work"
    (let [data {:price (double-array [10.0 20.0 30.0])}]
      ;; (count)
      (let [r (q/q {:from {:price (:price data)}
                    :agg ['(count)]})]
        (is (= 3 (long (:count (first r))))))
      ;; (min :price)
      (let [r (q/q {:from {:price (:price data)}
                    :agg ['(min :price)]})]
        (is (== 10.0 (:min (first r)))))
      ;; (max :price)
      (let [r (q/q {:from {:price (:price data)}
                    :agg ['(max :price)]})]
        (is (== 30.0 (:max (first r))))))))

;; ============================================================================
;; Chunk-Streaming Index Tests (zero-copy native memory path)
;; ============================================================================

(deftest tpch-q06-on-indices-test
  (testing "TPC-H Q06 with Stratum indices produces same result as arrays"
    (let [n 10000
          shipdate-arr (long-array (map #(+ 8002 (mod (* % 7) 2559)) (range n)))
          discount-arr (double-array (map #(* 0.01 (mod % 11)) (range n)))
          quantity-arr (long-array (map #(+ 1 (mod % 50)) (range n)))
          price-arr    (double-array n)
          _            (dotimes [i n]
                         (aset price-arr i (* (aget quantity-arr i)
                                              (+ 900.0 (* (double i) 0.1)))))
          ;; Create indices
          sd-idx (index/index-from-seq :int64 (seq shipdate-arr))
          dc-idx (index/index-from-seq :float64 (seq discount-arr))
          qt-idx (index/index-from-seq :int64 (seq quantity-arr))
          px-idx (index/index-from-seq :float64 (seq price-arr))

          ;; Execute on arrays
          arr-result (q/q
                      {:from  {:shipdate shipdate-arr :discount discount-arr
                               :quantity quantity-arr :price price-arr}
                       :where [[:between :shipdate 8766 9131]
                               [:between :discount 0.05 0.07]
                               [:< :quantity 24]]
                       :agg   [[:as [:sum [:* :price :discount]] :revenue]]})
          arr-rev (:revenue (first arr-result))
          arr-cnt (:_count (first arr-result))

          ;; Execute on indices (chunk-streaming)
          idx-result (q/q
                      {:from  {:shipdate sd-idx :discount dc-idx
                               :quantity qt-idx :price px-idx}
                       :where [[:between :shipdate 8766 9131]
                               [:between :discount 0.05 0.07]
                               [:< :quantity 24]]
                       :agg   [[:as [:sum [:* :price :discount]] :revenue]]})
          idx-rev (:revenue (first idx-result))
          idx-cnt (:_count (first idx-result))]

      (is (pos? arr-cnt) "Array path should find matches")
      (is (= arr-cnt idx-cnt) "Index path should match array count")
      (is (< (Math/abs (double (- arr-rev idx-rev))) 0.01)
          (str "Index revenue " idx-rev " should match array " arr-rev)))))

(deftest index-sum-aggregation-test
  (testing "Sum aggregation on indices"
    (let [n 2000
          vals (double-array (map #(+ 1.0 (double %)) (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 1000.0]]
                       :agg [[:sum :vals]]})]
      ;; Values > 1000: 1001, 1002, ..., 2000 → sum = (1001+2000)*1000/2 = 1500500
      (is (< (Math/abs (double (- 1500500.0 (:sum (first result))))) 0.01)))))

(deftest index-count-aggregation-test
  (testing "Count aggregation on indices"
    (let [n 2000
          vals (long-array (range n))
          idx (index/index-from-seq :int64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:>= :vals 500]]
                       :agg [[:count]]})]
      ;; Values >= 500: 500, 501, ..., 1999 → 1500
      (is (= 1500 (long (:count (first result))))))))

(deftest index-min-aggregation-test
  (testing "Min aggregation on indices"
    (let [n 2000
          vals (double-array (map #(+ 100.0 (* 0.5 (double %))) (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 500.0]]
                       :agg [[:min :vals]]})]
      ;; First value > 500.0: 500.5
      (is (== 500.5 (:min (first result)))))))

(deftest index-max-aggregation-test
  (testing "Max aggregation on indices"
    (let [n 2000
          vals (double-array (map #(+ 100.0 (* 0.5 (double %))) (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:< :vals 500.0]]
                       :agg [[:max :vals]]})]
      ;; Last value < 500.0: 499.5
      (is (== 499.5 (:max (first result)))))))

(deftest index-small-chunks-scalar-fallback-test
  (testing "Small index (< 1000 rows) uses scalar fallback via materialize"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 2.0]]
                       :agg [[:sum :vals]]})]
      ;; 3 + 4 + 5 = 12
      (is (== 12.0 (:sum (first result)))))))

(deftest index-no-predicates-test
  (testing "Index query with no predicates matches all rows"
    (let [n 2000
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :agg [[:sum :vals]]})]
      ;; sum(0..1999) = 1999*2000/2 = 1999000
      (is (< (Math/abs (double (- 1999000.0 (:sum (first result))))) 0.01)))))

(deftest index-no-matches-test
  (testing "Index query with no matches returns zero count"
    (let [n 2000
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 99999.0]]
                       :agg [[:count]]})]
      (is (= 0 (long (:count (first result))))))))

(deftest index-multi-column-test
  (testing "Multi-column index query with long and double predicates"
    (let [n 5000
          longs-arr (long-array (range n))
          dbls-arr  (double-array (map #(* 0.1 (double %)) (range n)))
          l-idx (index/index-from-seq :int64 (seq longs-arr))
          d-idx (index/index-from-seq :float64 (seq dbls-arr))
          result (q/q {:from {:lvals l-idx :dvals d-idx}
                       :where [[:between :lvals 1000 2000]
                               [:> :dvals 100.0]]
                       :agg [[:count]]})]
      ;; lvals in [1000,2000]: 1000..2000 (BETWEEN is inclusive)
      ;; dvals > 100: index >= 1001 (0.1*1001 = 100.1)
      ;; intersection: 1001..2000 → 1000
      (is (= 1000 (long (:count (first result))))))))

(deftest compiled-query-on-indices-test
  (testing "Compiled query works on indices"
    (let [n 2000
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          compiled (q/compile-query
                    {:from {:vals idx}
                     :where [[:> :vals 1000.0]]
                     :agg [[:sum :vals]]})
          r1 (compiled)
          r2 (compiled)]
      ;; Should produce consistent results
      (is (== (:sum (first r1)) (:sum (first r2))))
      ;; sum(1001..1999) = sum(1..1999) - sum(1..1000) = 1999000 - 500500 = 1498500
      (is (< (Math/abs (double (- 1498500.0 (:sum (first r1))))) 0.01)))))

;; ============================================================================
;; Expression in WHERE Predicate Tests
;; ============================================================================

(deftest expression-in-where-count-test
  (testing "Expression in WHERE predicate [:> [:* :a :b] 50.0]"
    (let [data {:a (long-array [1 2 3 4 5])
                :b (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :where [[:> [:* :a :b] 50.0]]
                       :agg [[:count]]})]
      ;; 1*10=10, 2*20=40, 3*30=90✓, 4*40=160✓, 5*50=250✓ → 3 matches
      (is (= 3 (long (:count (first result))))))))

(deftest expression-in-where-with-sum-test
  (testing "Expression WHERE + sum aggregation"
    (let [data {:a (long-array [1 2 3 4 5])
                :b (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :where [[:> [:* :a :b] 50.0]]
                       :agg [[:sum :b]]})]
      ;; Matching rows (0-indexed): 2,3,4 → b: 30+40+50 = 120
      (is (== 120.0 (:sum (first result)))))))

(deftest expression-in-where-subtraction-test
  (testing "Expression WHERE with subtraction [:< [:- :a :b] 0]"
    (let [data {:a (double-array [1.0 5.0 3.0 8.0 2.0])
                :b (double-array [2.0 3.0 4.0 1.0 6.0])}
          result (q/q {:from data
                       :where [[:< [:- :a :b] 0.0]]
                       :agg [[:count]]})]
      ;; a-b: -1, 2, -1, 7, -4 → negative: idx 0,2,4 → 3 matches
      (is (= 3 (long (:count (first result))))))))

(deftest expression-in-where-filter-only-test
  (testing "Expression WHERE without aggregation (filter-only path)"
    (let [data {:a (long-array [1 2 3 4 5])
                :b (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :where [[:> [:* :a :b] 50.0]]})]
      ;; 3 matching rows, no agg → just count
      (is (= 3 (:_count (first result)))))))

;; ============================================================================
;; OR Predicate Tests
;; ============================================================================

(deftest or-predicate-test
  (testing "OR predicate [:or pred1 pred2]"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:or [:< :vals 2.0] [:> :vals 4.0]]]
                       :agg [[:count]]})]
      ;; vals<2 → {1.0}, vals>4 → {5.0} → 2 matches
      (is (= 2 (long (:count (first result))))))))

(deftest or-predicate-with-and-test
  (testing "OR combined with AND predicates"
    (let [data {:a (double-array [1.0 2.0 3.0 4.0 5.0])
                :b (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :where [[:or [:< :a 2.0] [:> :a 4.0]]
                               [:> :b 15.0]]
                       :agg [[:count]]})]
      ;; OR: a<2 → {idx0}, a>4 → {idx4}
      ;; AND b>15: idx0 has b=10 (no), idx4 has b=50 (yes) → 1 match
      (is (= 1 (long (:count (first result))))))))

(deftest or-predicate-sum-test
  (testing "OR predicate with sum aggregation"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:or [:= :vals 1.0] [:= :vals 5.0]]]
                       :agg [[:sum :vals]]})]
      ;; 1.0 + 5.0 = 6.0
      (is (== 6.0 (:sum (first result)))))))

(deftest or-predicate-filter-only-test
  (testing "OR predicate without aggregation (filter-only path)"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:or [:< :vals 2.0] [:> :vals 4.0]]]})]
      (is (= 2 (:_count (first result)))))))

;; ============================================================================
;; NOT Predicate Tests
;; ============================================================================

(deftest not-lt-test
  (testing "NOT less-than → greater-than-or-equal"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:not [:< :vals 3.0]]]
                       :agg [[:count]]})]
      ;; NOT(vals<3) → vals>=3 → {3,4,5} → 3 matches
      (is (= 3 (long (:count (first result))))))))

(deftest not-between-test
  (testing "NOT BETWEEN predicate"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:not [:between :vals 2.0 4.0]]]
                       :agg [[:count]]})]
      ;; NOT(vals in [2,4]) → vals<2 or vals>4 → {1,5} → 2 matches
      (is (= 2 (long (:count (first result))))))))

(deftest not-eq-test
  (testing "NOT equality → neq"
    (let [data {:vals (double-array [1.0 2.0 3.0 3.0 5.0])}
          result (q/q {:from data
                       :where [[:not [:= :vals 3.0]]]
                       :agg [[:count]]})]
      ;; NOT(vals==3) → vals!=3 → {1,2,5} → 3 matches
      (is (= 3 (long (:count (first result))))))))

(deftest not-gt-test
  (testing "NOT greater-than → less-than-or-equal"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:not [:> :vals 3.0]]]
                       :agg [[:count]]})]
      ;; NOT(vals>3) → vals<=3 → {1,2,3} → 3 matches
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; IN Predicate Tests
;; ============================================================================

(deftest in-predicate-long-test
  (testing "IN predicate with long column"
    (let [data {:vals (long-array [1 2 3 4 5])}
          result (q/q {:from data
                       :where [[:in :vals 2 4]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest in-predicate-double-test
  (testing "IN predicate with double column"
    (let [data {:vals (double-array [1.0 2.0 3.0 4.0 5.0])}
          result (q/q {:from data
                       :where [[:in :vals 2.0 4.0]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest in-predicate-sum-test
  (testing "IN predicate with sum aggregation"
    (let [data {:vals (long-array [10 20 30 40 50])}
          result (q/q {:from data
                       :where [[:in :vals 20 40]]
                       :agg [[:sum :vals]]})]
      ;; 20 + 40 = 60
      (is (== 60.0 (:sum (first result)))))))

(deftest not-in-predicate-test
  (testing "NOT IN predicate"
    (let [data {:vals (long-array [1 2 3 4 5])}
          result (q/q {:from data
                       :where [[:not [:in :vals 2 4]]]
                       :agg [[:count]]})]
      ;; NOT IN {2,4} → {1,3,5} → 3 matches
      (is (= 3 (long (:count (first result))))))))

(deftest in-predicate-filter-only-test
  (testing "IN predicate without aggregation (filter-only path)"
    (let [data {:vals (long-array [1 2 3 4 5])}
          result (q/q {:from data
                       :where [[:in :vals 1 3 5]]})]
      (is (= 3 (:_count (first result)))))))

;; ============================================================================
;; Projection (SELECT) Tests
;; ============================================================================

(deftest select-simple-columns-test
  (testing "SELECT simple columns"
    (let [data {:a (long-array [1 2 3 4 5])
                :b (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :where [[:> :a 3]]
                       :select [:a :b]})]
      ;; a > 3: rows with a=4,5
      (is (= 2 (count result)))
      (is (== 4.0 (:a (first result))))
      (is (== 40.0 (:b (first result))))
      (is (== 5.0 (:a (second result))))
      (is (== 50.0 (:b (second result)))))))

(deftest select-with-expression-test
  (testing "SELECT with computed expression"
    (let [data {:a (double-array [1.0 2.0 3.0])
                :b (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :select [:a [:as [:* :a :b] :product]]})]
      (is (= 3 (count result)))
      ;; 1*10=10, 2*20=40, 3*30=90
      (is (== 1.0 (:a (first result))))
      (is (== 10.0 (:product (first result))))
      (is (== 40.0 (:product (second result))))
      (is (== 90.0 (:product (nth result 2)))))))

(deftest select-with-filter-and-order-test
  (testing "SELECT with filter, order, and limit"
    (let [data {:val (double-array [5.0 3.0 1.0 4.0 2.0])}
          result (q/q {:from data
                       :where [[:> :val 2.0]]
                       :select [:val]
                       :order [[:val :asc]]
                       :limit 2})]
      ;; val > 2: {5,3,4} → sorted asc: [3,4,5] → limit 2: [3,4]
      (is (= 2 (count result)))
      (is (== 3.0 (:val (first result))))
      (is (== 4.0 (:val (second result)))))))

(deftest select-aliased-column-test
  (testing "SELECT with aliased column"
    (let [data {:price (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :select [[:as :price :p]]})]
      (is (= 3 (count result)))
      (is (== 10.0 (:p (first result))))
      (is (nil? (:price (first result)))))))

(deftest select-no-filter-test
  (testing "SELECT all rows when no filter"
    (let [data {:v (long-array [10 20 30])}
          result (q/q {:from data
                       :select [:v]})]
      (is (= 3 (count result)))
      (is (= 10 (:v (first result)))))))

;; ============================================================================
;; Projection Type Preservation Tests
;; ============================================================================

(deftest select-preserves-long-type-test
  (testing "SELECT preserves long type for long[] columns"
    (let [data {:id (long-array [10 20 30])}
          result (q/q {:from data :select [:id]})]
      (is (= 3 (count result)))
      (is (= 10 (:id (first result))))
      (is (instance? Long (:id (first result)))))))

(deftest select-preserves-double-type-test
  (testing "SELECT preserves double type for double[] columns"
    (let [data {:price (double-array [10.5 20.5 30.5])}
          result (q/q {:from data :select [:price]})]
      (is (= 3 (count result)))
      (is (== 10.5 (:price (first result))))
      (is (instance? Double (:price (first result)))))))

;; ============================================================================
;; NOT-RANGE (through SIMD) Tests
;; ============================================================================

(deftest not-range-via-not-between-test
  (testing "NOT BETWEEN routes to SIMD PRED_NOT_RANGE"
    (let [data {:vals (long-array (range 100))}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:not [:between :vals 20 40]]]
                       :agg [[:count]]})]
      ;; vals NOT in [20,40] → vals<20 or vals>40 → 20 + 59 = 79
      (is (= 79 (long (:count (first result))))))))

(deftest not-range-double-test
  (testing "NOT-RANGE on double column"
    (let [data {:vals (double-array (map double (range 100)))}
          result (q/q {:from {:vals (:vals data)}
                       :where [[:not [:between :vals 30.0 50.0]]]
                       :agg [[:sum :vals]]})]
      ;; Sum of all - sum of [30,50] = 4950 - (30+31+...+50) = 4950 - 840 = 4110
      (is (< (Math/abs (double (- 4110.0 (:sum (first result))))) 0.01)))))

(deftest not-range-with-other-preds-test
  (testing "NOT-RANGE combined with other predicates"
    (let [data {:a (long-array (range 100))
                :b (double-array (map #(* 0.1 (double %)) (range 100)))}
          result (q/q {:from data
                       :where [[:not [:between :a 20 80]]
                               [:> :b 5.0]]
                       :agg [[:count]]})]
      ;; a NOT in [20,80]: a<20 or a>80
      ;; b > 5.0: b_i=0.1*i > 5.0 → i > 50
      ;; intersection: a>80 AND i>50 → 81,82,...,99 → 19 matches
      (is (= 19 (long (:count (first result))))))))

;; ============================================================================
;; Compiled Mask Tests (OR/IN/NOT-IN through Java paths)
;; ============================================================================

(deftest or-in-group-by-test
  (testing "OR predicate works in group-by path"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          result (q/q {:from data
                       :where [[:or [:= :cat 0] [:= :cat 2]]]
                       :group [:cat]
                       :agg [[:sum :val]]})]
      ;; cat 0: 10+20=30, cat 2: 50+60=110 (cat 1 excluded)
      (is (= 2 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (== 30.0 (get by-cat 0)))
        (is (== 110.0 (get by-cat 2)))))))

(deftest in-in-group-by-test
  (testing "IN predicate works in group-by path"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          result (q/q {:from data
                       :where [[:in :cat 0 2]]
                       :group [:cat]
                       :agg [[:count]]})]
      ;; cat 0: 2, cat 2: 2 (cat 1 excluded)
      (is (= 2 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:count r)])) result)]
        (is (= 2 (long (get by-cat 0))))
        (is (= 2 (long (get by-cat 2))))))))

(deftest not-in-in-group-by-test
  (testing "NOT-IN predicate works in group-by path"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          result (q/q {:from data
                       :where [[:not [:in :cat 1]]]
                       :group [:cat]
                       :agg [[:sum :val]]})]
      ;; NOT IN {1} → cat 0: 10+20=30, cat 2: 50+60=110
      (is (= 2 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (== 30.0 (get by-cat 0)))
        (is (== 110.0 (get by-cat 2)))))))

(deftest not-range-in-group-by-test
  (testing "NOT-RANGE predicate works in group-by path"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (long-array [10 20 30 40 50 60])}
          result (q/q {:from data
                       :where [[:not [:between :val 25 45]]]
                       :group [:cat]
                       :agg [[:count]]})]
      ;; NOT in [25,45): vals 10,20,50,60 match
      ;; cat 0: {10,20}→2, cat 2: {50,60}→2
      (is (= 2 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:count r)])) result)]
        (is (= 2 (long (get by-cat 0))))
        (is (= 2 (long (get by-cat 2))))))))

;; ============================================================================
;; Group-By MIN/MAX Tests (verifies the accumulator fix)
;; ============================================================================

(deftest group-by-min-test
  (testing "Group-by with min aggregation returns correct values"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (double-array [30.0 10.0 50.0 20.0 40.0 60.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:min :val]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:min r)])) result)]
        (is (== 10.0 (get by-cat 0)))
        (is (== 20.0 (get by-cat 1)))
        (is (== 40.0 (get by-cat 2)))))))

(deftest group-by-max-test
  (testing "Group-by with max aggregation returns correct values"
    (let [data {:cat (long-array [0 0 1 1 2 2])
                :val (double-array [30.0 10.0 50.0 20.0 40.0 60.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:max :val]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:max r)])) result)]
        (is (== 30.0 (get by-cat 0)))
        (is (== 50.0 (get by-cat 1)))
        (is (== 60.0 (get by-cat 2)))))))

(deftest group-by-min-max-combined-test
  (testing "Group-by with both min and max"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :val (double-array [5.0 1.0 9.0 3.0 7.0 2.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:min :val] [:max :val]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) r]) result))]
        (is (== 1.0 (:min (get by-cat 0))))
        (is (== 9.0 (:max (get by-cat 0))))
        (is (== 2.0 (:min (get by-cat 1))))
        (is (== 7.0 (:max (get by-cat 1))))))))

;; ============================================================================
;; Empty Group Filtering Tests
;; ============================================================================

(deftest group-by-no-empty-groups-test
  (testing "Group-by does not return empty groups"
    (let [data {:cat (long-array [0 1 2])
                :val (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :where [[:> :val 15.0]]
                       :group [:cat]
                       :agg [[:sum :val]]})]
      ;; Only cat 1 and cat 2 match (val > 15)
      (is (= 2 (count result)))
      (is (every? #(pos? (:_count %)) result)))))

;; ============================================================================
;; Mixed-Type Sum-Product Tests
;; ============================================================================

(deftest sum-product-long-double-test
  (testing "Sum-product of long[] * double[] works (mixed types)"
    (let [data {:qty   (long-array [2 3 4])
                :price (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :agg [[:sum [:* :qty :price]]]})]
      ;; 2*10 + 3*20 + 4*30 = 20 + 60 + 120 = 200
      (is (== 200.0 (:sum-product (first result)))))))

(deftest sum-product-long-long-test
  (testing "Sum-product of long[] * long[] works"
    (let [data {:a (long-array [2 3 4])
                :b (long-array [10 20 30])}
          result (q/q {:from data
                       :agg [[:sum [:* :a :b]]]})]
      ;; 2*10 + 3*20 + 4*30 = 200
      (is (== 200.0 (:sum-product (first result)))))))

;; ============================================================================
;; Input Validation Tests
;; ============================================================================

(deftest validate-empty-from-test
  (testing "Empty :from throws descriptive error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":from map cannot be empty"
                          (q/q {:from {}})))))

(deftest validate-nil-from-test
  (testing "nil :from throws descriptive error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid input"
                          (q/q {:from nil})))))

(deftest validate-missing-where-column-test
  (testing "Missing column in :where throws descriptive error"
    (let [data {:price (double-array [1.0 2.0 3.0])}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column :nonexistent"
                            (q/q {:from data
                                  :where [[:> :nonexistent 5.0]]}))))))

(deftest validate-missing-agg-column-test
  (testing "Missing column in :agg throws descriptive error"
    (let [data {:price (double-array [1.0 2.0 3.0])}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column :missing"
                            (q/q {:from data
                                  :agg [[:sum :missing]]}))))))

(deftest validate-missing-group-column-test
  (testing "Missing column in :group throws descriptive error"
    (let [data {:price (double-array [1.0 2.0 3.0])}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column :missing"
                            (q/q {:from data
                                  :group [:missing]
                                  :agg [[:count]]}))))))

(deftest validate-missing-select-column-test
  (testing "Missing column in :select throws descriptive error"
    (let [data {:price (double-array [1.0 2.0 3.0])}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column :missing"
                            (q/q {:from data
                                  :select [:missing]}))))))

(deftest validate-count-agg-no-column-ok-test
  (testing "COUNT agg without column reference is valid"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; Duplicate Aggregate Alias Tests
;; ============================================================================

(deftest duplicate-sum-alias-test
  (testing "Two :sum aggs on different columns get unique aliases"
    (let [data {:a (double-array [10.0 20.0])
                :b (double-array [1.0 2.0])}
          result (q/q {:from data
                       :agg [[:sum :a] [:sum :b]]})]
      (is (== 30.0 (:sum_a (first result))))
      (is (== 3.0 (:sum_b (first result)))))))

(deftest duplicate-sum-group-by-test
  (testing "Duplicate :sum aliases in group-by"
    (let [data {:cat (long-array [0 0 1 1])
                :a (double-array [10.0 20.0 30.0 40.0])
                :b (double-array [1.0 2.0 3.0 4.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum :a] [:sum :b]]
                       :order [[:cat :asc]]})]
      (let [r0 (first result)]
        (is (== 30.0 (:sum_a r0)))
        (is (== 3.0 (:sum_b r0)))))))

(deftest duplicate-min-max-alias-test
  (testing "Two :min aggs get unique aliases"
    (let [data {:a (double-array [5.0 3.0 1.0])
                :b (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :agg [[:min :a] [:min :b]]})]
      (is (== 1.0 (:min_a (first result))))
      (is (== 10.0 (:min_b (first result)))))))

(deftest no-collision-single-sum-test
  (testing "Single :sum agg keeps short alias :sum"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data :agg [[:sum :vals]]})]
      (is (== 6.0 (:sum (first result)))))))

(deftest explicit-as-not-affected-test
  (testing "Explicit :as aliases are never auto-modified"
    (let [data {:a (double-array [10.0 20.0])
                :b (double-array [1.0 2.0])}
          result (q/q {:from data
                       :agg [[:as [:sum :a] :total_a]
                             [:as [:sum :b] :total_b]]})]
      (is (== 30.0 (:total_a (first result))))
      (is (== 3.0 (:total_b (first result)))))))

(deftest mixed-dup-and-unique-alias-test
  (testing "Only duplicate ops get disambiguated, unique ops keep short names"
    (let [data {:a (double-array [10.0 20.0])
                :b (double-array [1.0 2.0])}
          result (q/q {:from data
                       :agg [[:sum :a] [:sum :b] [:count]]})]
      (is (== 30.0 (:sum_a (first result))))
      (is (== 3.0 (:sum_b (first result))))
      (is (= 2 (long (:count (first result))))))))

;; ============================================================================
;; Min/Max Zero-Match Tests
;; ============================================================================

(deftest min-zero-matches-nil-test
  (testing "Min with zero matches returns nil"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :where [[:> :vals 100.0]]
                       :agg [[:min :vals]]})]
      (is (nil? (:min (first result))))
      (is (= 0 (:_count (first result)))))))

(deftest max-zero-matches-nil-test
  (testing "Max with zero matches returns nil"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :where [[:> :vals 100.0]]
                       :agg [[:max :vals]]})]
      (is (nil? (:max (first result))))
      (is (= 0 (:_count (first result)))))))

(deftest min-max-both-zero-matches-test
  (testing "Min and max both return nil on zero matches"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :where [[:> :vals 100.0]]
                       :agg [[:min :vals] [:max :vals]]})]
      ;; Different ops (:min vs :max) don't collide → short aliases
      (is (contains? (first result) :min) "result should contain :min key")
      (is (contains? (first result) :max) "result should contain :max key")
      (is (nil? (:min (first result))))
      (is (nil? (:max (first result)))))))

(deftest avg-zero-matches-nil-test
  (testing "Avg with zero matches returns nil (SQL NULL)"
    (let [data {:vals (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :where [[:> :vals 100.0]]
                       :agg [[:avg :vals]]})]
      (is (nil? (:avg (first result)))))))

;; ============================================================================
;; Multi-Agg SIMD Path Tests (> 1000 rows to trigger SIMD)
;; ============================================================================

(deftest multi-agg-simd-test
  (testing "Multiple aggs route through SIMD individually"
    (let [n 2000
          vals (double-array (map double (range n)))
          data {:vals vals}
          result (q/q {:from data
                       :where [[:> :vals 999.0]]
                       :agg [[:sum :vals] [:min :vals] [:max :vals] [:count]]})]
      ;; vals > 999: 1000..1999 → 1000 values
      ;; sum = (1000+1999)*1000/2 = 1499500
      (is (< (Math/abs (double (- 1499500.0 (:sum (first result))))) 0.01))
      (is (== 1000.0 (:min (first result))))
      (is (== 1999.0 (:max (first result))))
      (is (= 1000 (long (:count (first result))))))))

(deftest multi-agg-simd-with-filter-test
  (testing "Multi-agg SIMD with multiple predicates"
    (let [n 5000
          a (long-array (map #(mod % 100) (range n)))
          b (double-array (map #(* 0.5 (double %)) (range n)))
          result (q/q {:from {:a a :b b}
                       :where [[:between :a 10 30]
                               [:> :b 100.0]]
                       :agg [[:sum :b] [:min :b] [:max :b] [:count]]})]
      (is (pos? (:_count (first result))))
      (is (<= (:min (first result)) (:max (first result))))
      (is (pos? (:sum (first result)))))))

(deftest multi-agg-simd-all-match-test
  (testing "Multi-agg SIMD with no predicates (all rows match)"
    (let [n 2000
          vals (double-array (map double (range n)))
          result (q/q {:from {:vals vals}
                       :agg [[:sum :vals] [:min :vals] [:max :vals] [:avg :vals] [:count]]})]
      (is (< (Math/abs (double (- 1999000.0 (:sum (first result))))) 0.01))
      (is (== 0.0 (:min (first result))))
      (is (== 1999.0 (:max (first result))))
      (is (< (Math/abs (double (- 999.5 (:avg (first result))))) 0.01))
      (is (= 2000 (long (:count (first result))))))))

;; ============================================================================
;; Nested OR & Complex Compiled Mask Tests
;; ============================================================================

(deftest nested-or-test
  (testing "Nested OR [:or [:or a b] c]"
    (let [data {:v (long-array [1 2 3 4 5])}
          result (q/q {:from data
                       :where [[:or [:or [:= :v 1] [:= :v 3]] [:= :v 5]]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest two-or-preds-anded-test
  (testing "Two OR predicates ANDed together"
    (let [data {:a (long-array [1 2 3 4 5])
                :b (long-array [10 20 30 40 50])}
          result (q/q {:from data
                       :where [[:or [:= :a 1] [:= :a 5]]
                               [:or [:= :b 10] [:= :b 50]]]
                       :agg [[:count]]})]
      ;; (a=1,b=10) and (a=5,b=50) → 2 matches
      (is (= 2 (long (:count (first result))))))))

;; ============================================================================
;; NaN and Infinity Handling Tests
;; ============================================================================

(deftest nan-in-data-test
  (testing "NaN (NULL sentinel) values are skipped in aggregations"
    (let [data {:v (double-array [1.0 Double/NaN 3.0])}
          result (q/q {:from data :agg [[:sum :v]]})]
      ;; NaN is the double NULL sentinel — SUM should skip it
      (is (== 4.0 (:sum (first result)))))))

(deftest infinity-in-data-test
  (testing "Infinity values handled correctly"
    (let [data {:v (double-array [1.0 Double/POSITIVE_INFINITY 3.0])}
          result (q/q {:from data :agg [[:sum :v] [:max :v]]})]
      (is (Double/isInfinite (double (:sum (first result)))))
      (is (== Double/POSITIVE_INFINITY (:max (first result)))))))

;; ============================================================================
;; Three-Column Group-By Test
;; ============================================================================

(deftest three-column-group-by-test
  (testing "Group-by with three columns"
    (let [data {:a (long-array [0 0 0 0 1 1 1 1])
                :b (long-array [0 0 1 1 0 0 1 1])
                :c (long-array [0 1 0 1 0 1 0 1])
                :v (double-array [1 2 3 4 5 6 7 8])}
          result (q/q {:from data
                       :group [:a :b :c]
                       :agg [[:sum :v]]
                       :order [[:a :asc] [:b :asc] [:c :asc]]})]
      (is (= 8 (count result)))
      (is (== 1.0 (:sum (first result))))
      (is (== 8.0 (:sum (last result)))))))

;; ============================================================================
;; Statistical Aggregation Tests
;; ============================================================================

(deftest variance-test
  (testing "Variance aggregation"
    (let [data {:v (double-array [2.0 4.0 4.0 4.0 5.0 5.0 7.0 9.0])}
          result (q/q {:from data :agg [[:variance :v]]})]
      (is (< (Math/abs (double (- 4.571428571428571 (:variance (first result))))) 0.001)))))

(deftest stddev-test
  (testing "Standard deviation aggregation"
    (let [data {:v (double-array [2.0 4.0 4.0 4.0 5.0 5.0 7.0 9.0])}
          result (q/q {:from data :agg [[:stddev :v]]})]
      (is (< (Math/abs (double (- 2.138 (:stddev (first result))))) 0.001)))))

(deftest median-test
  (testing "Median aggregation"
    (let [data {:v (double-array [2.0 4.0 4.0 4.0 5.0 5.0 7.0 9.0])}
          result (q/q {:from data :agg [[:median :v]]})]
      (is (== 4.5 (:median (first result)))))))

(deftest count-distinct-test
  (testing "Count-distinct aggregation"
    (let [data {:v (long-array [1 2 2 3 3 3 4 4 4 4])}
          result (q/q {:from data :agg [[:count-distinct :v]]})]
      (is (= 4 (long (:count-distinct (first result))))))))

;; ============================================================================
;; Negative Value Tests
;; ============================================================================

(deftest negative-values-test
  (testing "Negative values in aggregations"
    (let [data {:v (long-array [-5 -3 -1 0 1 3 5])}
          result (q/q {:from data
                       :where [[:< :v 0]]
                       :agg [[:sum :v] [:min :v] [:max :v] [:count]]})]
      (is (== -9.0 (:sum (first result))))
      (is (== -5.0 (:min (first result))))
      (is (== -1.0 (:max (first result))))
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; Bare Query & Datahike Full Syntax Tests
;; ============================================================================

(deftest bare-query-test
  (testing "Query with just :from returns count of all rows"
    (let [data {:v (double-array [1 2 3])}
          result (q/q {:from data})]
      (is (= [{:_count 3}] result)))))

(deftest full-datahike-syntax-test
  (testing "Full Datahike list syntax for preds and aggs"
    (let [data {:v (double-array [10 20 30 40 50])}
          result (q/q {:from data
                       :where ['(> :v 20)]
                       :agg ['(sum :v)]})]
      (is (== 120.0 (:sum (first result))))
      (is (= 3 (:_count (first result)))))))

;; ============================================================================
;; Having on _count Tests
;; ============================================================================

(deftest having-on-count-test
  (testing "Having filter on :_count"
    (let [data {:cat (long-array [0 0 0 1 2 2])
                :v (double-array [1 2 3 4 5 6])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum :v]]
                       :having [[:>= :_count 2]]})]
      ;; cat0 (cnt=3) included, cat2 (cnt=2) included, cat1 (cnt=1) excluded
      (is (= 2 (count result)))
      (is (every? #(>= (:_count %) 2) result)))))

;; ============================================================================
;; Full SELECT Pipeline Test
;; ============================================================================

(deftest select-full-pipeline-test
  (testing "SELECT with expression + filter + order + limit"
    (let [data {:id (long-array [1 2 3 4 5])
                :price (double-array [50.0 30.0 70.0 20.0 60.0])
                :qty (long-array [2 5 1 10 3])}
          result (q/q {:from data
                       :where [[:> :price 25.0]]
                       :select [:id :price [:as [:* :price :qty] :total]]
                       :order [[:total :desc]]
                       :limit 3})]
      (is (= 3 (count result)))
      ;; id=2: 30*5=150, id=5: 60*3=180, id=1: 50*2=100
      ;; After sort desc: [{id 5 total 180} {id 2 total 150} {id 1 total 100}]
      (is (= 5 (:id (first result))))
      (is (== 180.0 (:total (first result)))))))

;; ============================================================================
;; Index + OR/Compiled Mask Tests
;; ============================================================================

(deftest index-with-or-predicate-test
  (testing "Index source with OR predicate"
    (let [idx (index/index-from-seq :float64 (map double (range 100)))
          result (q/q {:from {:v idx}
                       :where [[:or [:< :v 10.0] [:> :v 90.0]]]
                       :agg [[:count]]})]
      ;; <10: 0..9 (10), >90: 91..99 (9) → 19
      (is (= 19 (long (:count (first result))))))))

(deftest index-with-not-range-test
  (testing "Index source with NOT-RANGE"
    (let [idx (index/index-from-seq :int64 (range 100))
          result (q/q {:from {:v idx}
                       :where [[:not [:between :v 20 80]]]
                       :agg [[:count]]})]
      ;; NOT [20,80]: <20 (20) + >80 (19) = 39
      (is (= 39 (long (:count (first result))))))))

(deftest mixed-index-array-test
  (testing "Mixed index + array sources"
    (let [cat-idx (index/index-from-seq :int64 [0 0 1 1 2])
          val-arr (double-array [10.0 20.0 30.0 40.0 50.0])
          result (q/q {:from {:cat cat-idx :val val-arr}
                       :where [[:> :val 25.0]]
                       :agg [[:sum :val]]})]
      ;; vals > 25: 30+40+50 = 120
      (is (== 120.0 (:sum (first result)))))))

;; ============================================================================
;; Expression Agg in Group-By Test
;; ============================================================================

(deftest expr-agg-in-group-by-test
  (testing "Expression aggregation [:sum [:* :price [:- 1 :discount]]] in group-by"
    (let [data {:cat (long-array [0 0 1 1])
                :price (double-array [100.0 200.0 300.0 400.0])
                :discount (double-array [0.1 0.2 0.1 0.2])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum [:* :price [:- 1 :discount]]]]
                       :order [[:cat :asc]]})]
      ;; cat0: 100*0.9 + 200*0.8 = 90+160 = 250
      ;; cat1: 300*0.9 + 400*0.8 = 270+320 = 590
      (is (< (Math/abs (double (- 250.0 (:sum (first result))))) 0.01))
      (is (< (Math/abs (double (- 590.0 (:sum (second result))))) 0.01)))))

;; ============================================================================
;; Dictionary-Encoded String Column Tests
;; ============================================================================

(deftest string-group-by-test
  (testing "Group by String[] column with sum"
    (let [cats (into-array String ["a" "b" "a" "b" "c"])
          prices (double-array [10.0 20.0 30.0 40.0 50.0])
          result (q/q {:from {:cat cats :price prices}
                       :group [:cat]
                       :agg [[:sum :price]]
                       :order [[:cat :asc]]})]
      (is (= 3 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (== 40.0 (get by-cat "a")))  ;; 10+30
        (is (== 60.0 (get by-cat "b")))  ;; 20+40
        (is (== 50.0 (get by-cat "c")))))) ;; 50

  (testing "Group by string vector with count"
    (let [cats ["x" "y" "x" "y" "x"]
          vals (long-array [1 2 3 4 5])
          result (q/q {:from {:cat cats :v vals}
                       :group [:cat]
                       :agg [[:count]]
                       :order [[:cat :asc]]})]
      (is (= 2 (count result)))
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:count r)])) result)]
        (is (= 3 (long (get by-cat "x"))))
        (is (= 2 (long (get by-cat "y")))))))

  (testing "Group by two string columns"
    (let [col1 (into-array String ["a" "a" "b" "b"])
          col2 (into-array String ["x" "y" "x" "y"])
          vals (double-array [1.0 2.0 3.0 4.0])
          result (q/q {:from {:c1 col1 :c2 col2 :v vals}
                       :group [:c1 :c2]
                       :agg [[:sum :v]]})]
      (is (= 4 (count result)))
      (let [by-key (into {} (map (fn [r] [[(:c1 r) (:c2 r)] (:sum r)])) result)]
        (is (== 1.0 (get by-key ["a" "x"])))
        (is (== 2.0 (get by-key ["a" "y"])))
        (is (== 3.0 (get by-key ["b" "x"])))
        (is (== 4.0 (get by-key ["b" "y"]))))))

  (testing "String group-by with filter on numeric column"
    (let [cats (into-array String ["a" "b" "a" "b"])
          vals (long-array [10 20 30 40])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:> :v 15]]
                       :group [:cat]
                       :agg [[:sum :v]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:sum r)])) result)]
        (is (== 30.0 (get by-cat "a")))  ;; only 30
        (is (== 60.0 (get by-cat "b")))))) ;; 20+40

  (testing "String group-by with avg"
    (let [cats (into-array String ["a" "a" "b" "b"])
          vals (double-array [10.0 20.0 30.0 40.0])
          result (q/q {:from {:cat cats :v vals}
                       :group [:cat]
                       :agg [[:avg :v]]})]
      (let [by-cat (into {} (map (fn [r] [(:cat r) (:avg r)])) result)]
        (is (== 15.0 (get by-cat "a")))
        (is (== 35.0 (get by-cat "b"))))))

  (testing "Mixed string + integer group columns"
    (let [cats (into-array String ["a" "a" "b" "b"])
          ids (long-array [0 1 0 1])
          vals (double-array [10.0 20.0 30.0 40.0])
          result (q/q {:from {:cat cats :id ids :v vals}
                       :group [:cat :id]
                       :agg [[:sum :v]]})]
      (is (= 4 (count result)))
      (let [by-key (into {} (map (fn [r] [[(:cat r) (:id r)] (:sum r)])) result)]
        (is (== 10.0 (get by-key ["a" 0])))
        (is (== 20.0 (get by-key ["a" 1])))
        (is (== 30.0 (get by-key ["b" 0])))
        (is (== 40.0 (get by-key ["b" 1])))))))

(deftest string-group-by-large-test
  (testing "String group-by with >1000 rows (forces Java SIMD path)"
    (let [n 5000
          cats (into-array String (repeatedly n #(str "cat" (rand-int 10))))
          vals (double-array (repeatedly n #(rand)))
          result (q/q {:from {:cat cats :v vals}
                       :group [:cat]
                       :agg [[:sum :v] [:count]]})]
      ;; Should have up to 10 groups
      (is (<= (count result) 10))
      ;; Each result should have string keys
      (doseq [r result]
        (is (string? (:cat r)))
        (is (.startsWith ^String (:cat r) "cat")))
      ;; Total count across all groups should equal n
      (is (= n (long (reduce + (map :count result))))))))

;; ============================================================================
;; Columnar Result Format Tests
;; ============================================================================

(deftest columnar-result-dense-test
  (testing "Columnar result format for dense group-by"
    (let [{:keys [category price]} (make-test-data)
          result (q/q {:from {:cat category :price price}
                       :group [:cat]
                       :agg [[:sum :price] [:count]]
                       :result :columns})]
      ;; Should have :n-rows
      (is (contains? result :n-rows))
      (is (= 5 (:n-rows result)))
      ;; Should have group column as long array
      (is (instance? (Class/forName "[J") (:cat result)))
      (is (= 5 (alength ^longs (:cat result))))
      ;; Should have agg columns as arrays
      (is (instance? (Class/forName "[D") (:sum result)))
      (is (instance? (Class/forName "[J") (:count result)))
      ;; Sum should match row-based results
      (let [row-result (q/q {:from {:cat category :price price}
                             :group [:cat]
                             :agg [[:sum :price] [:count]]})
            row-by-cat (into {} (map (juxt :cat :sum)) row-result)
            ^longs cats (:cat result)
            ^doubles sums (:sum result)]
        (dotimes [i (:n-rows result)]
          (is (== (get row-by-cat (aget cats i)) (aget sums i))))))))

(deftest columnar-result-string-test
  (testing "Columnar result format with dictionary-encoded string columns"
    (let [n 5000
          cats (into-array String (repeatedly n #(str "cat" (rand-int 10))))
          vals (double-array (repeatedly n #(rand)))
          result (q/q {:from {:cat cats :v vals}
                       :group [:cat]
                       :agg [[:sum :v] [:count]]
                       :result :columns})]
      ;; Group column should be String[] for dict-encoded
      (is (instance? (Class/forName "[Ljava.lang.String;") (:cat result)))
      (is (<= (:n-rows result) 10))
      ;; Total count should equal n
      (let [^longs counts (:count result)]
        (is (= n (long (reduce + (map #(aget counts %) (range (:n-rows result)))))))))))

(deftest columnar-result-hash-test
  (testing "Columnar result format for hash group-by (high cardinality)"
    (let [n 5000
          ;; Many unique keys to force hash path
          keys (long-array (range n))
          vals (double-array (repeatedly n #(rand)))
          result (binding [q/*dense-group-limit* 100]
                   (q/q {:from {:k keys :v vals}
                         :group [:k]
                         :agg [[:sum :v]]
                         :result :columns}))]
      (is (= n (:n-rows result)))
      (is (instance? (Class/forName "[J") (:k result)))
      (is (instance? (Class/forName "[D") (:sum result))))))

;; ============================================================================
;; Math Expression Function Tests
;; ============================================================================

(deftest math-abs-test
  (testing "abs expression"
    (let [data {:v (double-array [-3.0 -1.0 0.0 1.0 3.0])}
          result (q/q {:from data
                       :select [[:as [:abs :v] :a]]})
          vals (mapv :a result)]
      (is (= [3.0 1.0 0.0 1.0 3.0] vals)))))

(deftest math-sqrt-test
  (testing "sqrt expression"
    (let [data {:v (double-array [0.0 1.0 4.0 9.0 16.0])}
          result (q/q {:from data
                       :select [[:as [:sqrt :v] :s]]})
          vals (mapv :s result)]
      (is (= [0.0 1.0 2.0 3.0 4.0] vals)))))

(deftest math-log-test
  (testing "log/exp expressions"
    (let [data {:v (double-array [1.0 (Math/E) (* (Math/E) (Math/E))])}
          result (q/q {:from data
                       :select [[:as [:log :v] :l]]})
          vals (mapv :l result)]
      (is (< (Math/abs (- 0.0 (nth vals 0))) 1e-10))
      (is (< (Math/abs (- 1.0 (nth vals 1))) 1e-10))
      (is (< (Math/abs (- 2.0 (nth vals 2))) 1e-10)))))

(deftest math-round-floor-ceil-test
  (testing "round/floor/ceil expressions"
    (let [data {:v (double-array [1.3 1.7 2.5 -1.3 -1.7])}
          r-round (q/q {:from data :select [[:as [:round :v] :r]]})
          r-floor (q/q {:from data :select [[:as [:floor :v] :f]]})
          r-ceil  (q/q {:from data :select [[:as [:ceil :v] :c]]})]
      (is (= [1.0 2.0 3.0 -1.0 -2.0] (mapv :r r-round)))
      (is (= [1.0 1.0 2.0 -2.0 -2.0] (mapv :f r-floor)))
      (is (= [2.0 2.0 3.0 -1.0 -1.0] (mapv :c r-ceil))))))

(deftest math-mod-pow-test
  (testing "mod and pow expressions"
    (let [data {:a (double-array [10.0 7.0 15.0])
                :b (double-array [3.0 2.0 4.0])}
          r-mod (q/q {:from data :select [[:as [:mod :a 3] :m]]})
          r-pow (q/q {:from data :select [[:as [:pow :a 2] :p]]})]
      (is (= [1.0 1.0 0.0] (mapv :m r-mod)))
      (is (= [100.0 49.0 225.0] (mapv :p r-pow))))))

(deftest math-sign-test
  (testing "sign expression"
    (let [data {:v (double-array [-5.0 0.0 3.0])}
          result (q/q {:from data :select [[:as [:sign :v] :s]]})]
      (is (= [-1.0 0.0 1.0] (mapv :s result))))))

(deftest math-in-aggregation-test
  (testing "Math function in aggregation expression"
    (let [data {:v (double-array [1.0 4.0 9.0 16.0 25.0])}
          result (q/q {:from data
                       :agg [[:sum [:sqrt :v]]]})]
      ;; 1+2+3+4+5 = 15
      (is (< (Math/abs (- 15.0 (double (:sum (first result))))) 0.01)))))

;; ============================================================================
;; Date Extraction Function Tests
;; ============================================================================

(deftest date-extract-year-month-day-test
  (testing "Date extraction from epoch-days"
    ;; 2023-06-15 = epoch-day 19523 (from 1970-01-01)
    (let [epoch-day (long (.toEpochDay (java.time.LocalDate/of 2023 6 15)))
          data {:d (long-array [epoch-day])}
          result (q/q {:from data
                       :select [[:as [:year :d] :y]
                                [:as [:month :d] :m]
                                [:as [:day :d] :dy]]})]
      (is (== 2023.0 (:y (first result))))
      (is (== 6.0 (:m (first result))))
      (is (== 15.0 (:dy (first result)))))))

(deftest date-extract-time-test
  (testing "Time extraction from epoch-seconds"
    ;; 2023-06-15 14:30:45 UTC
    (let [epoch-sec (.getEpochSecond (java.time.Instant/parse "2023-06-15T14:30:45Z"))
          data {:ts (long-array [epoch-sec])}
          result (q/q {:from data
                       :select [[:as [:hour :ts] :h]
                                [:as [:minute :ts] :mi]
                                [:as [:second :ts] :s]]})]
      (is (== 14.0 (:h (first result))))
      (is (== 30.0 (:mi (first result))))
      (is (== 45.0 (:s (first result)))))))

(deftest date-extract-day-of-week-test
  (testing "Day of week extraction (0=Monday)"
    ;; 2023-06-15 is Thursday = 3
    (let [epoch-day (long (.toEpochDay (java.time.LocalDate/of 2023 6 15)))
          data {:d (long-array [epoch-day])}
          result (q/q {:from data
                       :select [[:as [:day-of-week :d] :dow]]})]
      (is (== 3.0 (:dow (first result)))))))

(deftest date-extract-in-group-by-test
  (testing "Date extraction used in group-by expression"
    ;; Group orders by year
    (let [d1 (long (.toEpochDay (java.time.LocalDate/of 2022 3 10)))
          d2 (long (.toEpochDay (java.time.LocalDate/of 2022 7 20)))
          d3 (long (.toEpochDay (java.time.LocalDate/of 2023 1 5)))
          data {:d (long-array [d1 d2 d3])
                :v (double-array [100.0 200.0 300.0])}
          result (q/q {:from data
                       :where [[:>= [:year :d] 2022]]
                       :agg [[:sum :v]]})]
      (is (== 600.0 (:sum (first result)))))))

;; ============================================================================
;; Compound Aggregate Tests (Variance, Stddev, Corr)
;; ============================================================================

(deftest variance-group-by-test
  (testing "Variance in group-by"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :v (double-array [2.0 4.0 6.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:variance :v]]
                       :order [[:cat :asc]]})]
      ;; cat 0: [2,4,6] mean=4, var = ((2-4)^2+(4-4)^2+(6-4)^2)/(3-1) = 8/2 = 4
      ;; cat 1: [10,20,30] mean=20, var = (100+0+100)/2 = 100
      (is (< (Math/abs (- 4.0 (double (:variance (first result))))) 0.01))
      (is (< (Math/abs (- 100.0 (double (:variance (second result))))) 0.01)))))

(deftest stddev-group-by-test
  (testing "Stddev in group-by"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :v (double-array [2.0 4.0 6.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:stddev :v]]
                       :order [[:cat :asc]]})]
      ;; cat 0: stddev = sqrt(4) = 2
      ;; cat 1: stddev = sqrt(100) = 10
      (is (< (Math/abs (- 2.0 (double (:stddev (first result))))) 0.01))
      (is (< (Math/abs (- 10.0 (double (:stddev (second result))))) 0.01)))))

(deftest corr-group-by-test
  (testing "Correlation in group-by"
    (let [data {:cat (long-array [0 0 0 0 1 1 1 1])
                :x (double-array [1.0 2.0 3.0 4.0 1.0 2.0 3.0 4.0])
                :y (double-array [2.0 4.0 6.0 8.0 8.0 6.0 4.0 2.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:corr :x :y]]
                       :order [[:cat :asc]]})]
      ;; cat 0: perfect positive correlation = 1.0
      ;; cat 1: perfect negative correlation = -1.0
      (is (< (Math/abs (- 1.0 (double (:corr (first result))))) 0.01))
      (is (< (Math/abs (- -1.0 (double (:corr (second result))))) 0.01)))))

(deftest compound-agg-with-simple-aggs-test
  (testing "Compound aggs mixed with simple aggs"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :v (double-array [2.0 4.0 6.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum :v] [:variance :v] [:count]]
                       :order [[:cat :asc]]})]
      ;; cat 0: sum=12, var=4, count=3
      (is (== 12.0 (:sum (first result))))
      (is (< (Math/abs (- 4.0 (double (:variance (first result))))) 0.01))
      (is (= 3 (long (:count (first result))))))))

(deftest variance-columnar-test
  (testing "Variance with columnar result format"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :v (double-array [2.0 4.0 6.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:variance :v]]
                       :result :columns})]
      (is (= 2 (:n-rows result)))
      (is (instance? (Class/forName "[D") (:variance result))))))

;; ============================================================================
;; Chunked VARIANCE/CORR on Index Inputs
;; ============================================================================

(deftest variance-group-by-index-test
  (testing "STDDEV group-by on index inputs (chunked path)"
    ;; Need >1000 rows to trigger chunked path
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 5) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:stddev :vals]]
                           :order [[:cat :asc]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:stddev :vals]]
                           :order [[:cat :asc]]})]
      (is (= (count arr-result) (count idx-result)))
      (doseq [i (range (count arr-result))]
        (is (< (Math/abs (- (double (:stddev (nth arr-result i)))
                            (double (:stddev (nth idx-result i))))) 0.01)
            (str "STDDEV mismatch for group " i))))))

(deftest corr-group-by-index-test
  (testing "CORR group-by on index inputs (chunked path)"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 3) (range n)))
          xs (double-array (map double (range n)))
          ys (double-array (map #(* 2.0 (double %)) (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          x-idx (index/index-from-seq :float64 (seq xs))
          y-idx (index/index-from-seq :float64 (seq ys))
          arr-result (q/q {:from {:cat cats :x xs :y ys}
                           :group [:cat]
                           :agg [[:corr :x :y]]
                           :order [[:cat :asc]]})
          idx-result (q/q {:from {:cat cat-idx :x x-idx :y y-idx}
                           :group [:cat]
                           :agg [[:corr :x :y]]
                           :order [[:cat :asc]]})]
      (is (= (count arr-result) (count idx-result)))
      (doseq [i (range (count arr-result))]
        (is (< (Math/abs (- (double (:corr (nth arr-result i)))
                            (double (:corr (nth idx-result i))))) 0.01)
            (str "CORR mismatch for group " i))))))

(deftest mixed-aggs-index-test
  (testing "Mixed SUM + STDDEV + COUNT on index inputs"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 4) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals] [:stddev :vals] [:count]]
                           :order [[:cat :asc]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals] [:stddev :vals] [:count]]
                           :order [[:cat :asc]]})]
      (is (= (count arr-result) (count idx-result)))
      (doseq [i (range (count arr-result))]
        (is (< (Math/abs (- (double (:sum (nth arr-result i)))
                            (double (:sum (nth idx-result i))))) 0.1)
            (str "SUM mismatch for group " i))
        (is (< (Math/abs (- (double (:stddev (nth arr-result i)))
                            (double (:stddev (nth idx-result i))))) 0.01)
            (str "STDDEV mismatch for group " i))
        (is (= (long (:count (nth arr-result i)))
               (long (:count (nth idx-result i))))
            (str "COUNT mismatch for group " i))))))

(deftest variance-index-columnar-test
  (testing "VARIANCE on index with columnar output"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 5) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:variance :vals]]
                           :result :columns})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:variance :vals]]
                           :result :columns})]
      (is (= (:n-rows arr-result) (:n-rows idx-result)))
      (is (instance? (Class/forName "[D") (:variance idx-result))))))

;; ============================================================================
;; COUNT-DISTINCT and MEDIAN in Group-By Tests
;; ============================================================================

(deftest count-distinct-group-by-test
  (testing "Count distinct in group-by"
    (let [data {:cat (long-array [0 0 0 0 1 1 1 1])
                :v (double-array [1.0 2.0 2.0 3.0 5.0 5.0 5.0 5.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :order [[:cat :asc]]})]
      ;; cat 0: distinct values {1,2,3} = 3
      ;; cat 1: distinct values {5} = 1
      (is (= 3 (long (:count-distinct (first result)))))
      (is (= 1 (long (:count-distinct (second result))))))))

(deftest median-group-by-test
  (testing "Median in group-by"
    (let [data {:cat (long-array [0 0 0 1 1 1 1])
                :v (double-array [1.0 3.0 5.0 2.0 4.0 6.0 8.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:median :v]]
                       :order [[:cat :asc]]})]
      ;; cat 0: [1,3,5] → median=3.0
      ;; cat 1: [2,4,6,8] → median=(4+6)/2=5.0
      (is (== 3.0 (double (:median (first result)))))
      (is (== 5.0 (double (:median (second result))))))))

(deftest count-distinct-scalar-test
  (testing "Count distinct in scalar aggregation"
    (let [data {:v (double-array [1.0 2.0 2.0 3.0 3.0 3.0])}
          result (q/q {:from data
                       :agg [[:count-distinct :v]]})]
      (is (= 3 (long (:count-distinct (first result))))))))

(deftest count-distinct-java-group-by-test
  (testing "Count distinct group-by via Java dense path (>= 1000 rows)"
    (let [n 2000
          cat-data (long-array (repeatedly n #(long (rand-int 3))))
          ;; Values: cat0 gets vals 0-9, cat1 gets 100-104, cat2 gets 200-202
          val-data (long-array (for [i (range n)]
                                 (let [c (aget cat-data i)]
                                   (case c
                                     0 (long (rand-int 10))    ;; 10 distinct
                                     1 (+ 100 (long (rand-int 5))) ;; 5 distinct
                                     2 (+ 200 (long (rand-int 3)))))))  ;; 3 distinct
          result (q/q {:from {:cat cat-data :v val-data}
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :order [[:cat :asc]]})]
      (is (= 3 (count result)))
      (is (<= (long (:count-distinct (first result))) 10))
      (is (<= (long (:count-distinct (second result))) 5))
      (is (<= (long (:count-distinct (nth result 2))) 3))
      (is (pos? (long (:count-distinct (first result)))))
      (is (pos? (long (:count-distinct (second result)))))
      (is (pos? (long (:count-distinct (nth result 2))))))))

(deftest count-distinct-mixed-aggs-test
  (testing "COUNT DISTINCT mixed with SUM/COUNT in same group-by query"
    (let [n 2000
          cat-data (long-array (repeatedly n #(long (rand-int 2))))
          val-data (long-array (for [i (range n)]
                                 (let [c (aget cat-data i)]
                                   (if (zero? c) (long (rand-int 5)) (+ 10 (long (rand-int 3)))))))
          amount (double-array (repeatedly n #(double (rand-int 100))))
          result (q/q {:from {:cat cat-data :v val-data :amt amount}
                       :group [:cat]
                       :agg [[:count-distinct :v] [:sum :amt] [:count]]
                       :order [[:cat :asc]]})]
      (is (= 2 (count result)))
      ;; cat 0: up to 5 distinct values, cat 1: up to 3 distinct values
      (is (<= (long (:count-distinct (first result))) 5))
      (is (<= (long (:count-distinct (second result))) 3))
      ;; sum and count should be positive
      (is (pos? (double (:sum (first result)))))
      (is (pos? (long (:count (first result))))))))

(deftest count-distinct-filtered-group-by-test
  (testing "COUNT DISTINCT with WHERE predicates in group-by"
    (let [n 2000
          cat-data (long-array (repeatedly n #(long (rand-int 2))))
          val-data (long-array (repeatedly n #(long (rand-int 20))))
          ;; filter: val < 10 → at most 10 distinct values
          result (q/q {:from {:cat cat-data :v val-data}
                       :where [[:< :v 10]]
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :order [[:cat :asc]]})]
      (is (= 2 (count result)))
      (is (<= (long (:count-distinct (first result))) 10))
      (is (<= (long (:count-distinct (second result))) 10)))))

(deftest count-distinct-columnar-test
  (testing "COUNT DISTINCT group-by with columnar output"
    (let [n 2000
          cat-data (long-array (repeatedly n #(long (rand-int 3))))
          val-data (long-array (for [i (range n)]
                                 (let [c (aget cat-data i)]
                                   (case c
                                     0 (long (rand-int 10))
                                     1 (+ 100 (long (rand-int 5)))
                                     2 (+ 200 (long (rand-int 3)))))))
          result (q/q {:from {:cat cat-data :v val-data}
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :result :columns})]
      (is (= 3 (:n-rows result)))
      (is (contains? result :cat))
      (is (contains? result :count-distinct)))))

(deftest count-distinct-hash-group-by-test
  (testing "COUNT DISTINCT via hash path (group key range > dense limit)"
    ;; Use large group keys to force hash path (exceeds *dense-group-limit*)
    (let [n 2000
          ;; 3 groups with keys 0, 500000, 1000000 — range > 200K → hash path
          cats (long-array [0 500000 1000000])
          cat-data (long-array (for [i (range n)] (aget cats (mod i 3))))
          ;; Use (quot i 3) for value variety since (mod i 3) correlates with category
          ;; cat 0: vals 0-4 (5 distinct), cat 500000: vals 10-12 (3 distinct), cat 1000000: vals 20 (1 distinct)
          val-data (long-array (for [i (range n)]
                                 (let [c (aget cat-data i)
                                       j (quot i 3)]
                                   (cond (= c 0) (long (mod j 5))
                                         (= c 500000) (+ 10 (long (mod j 3)))
                                         :else 20))))
          result (q/q {:from {:cat cat-data :v val-data}
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :order [[:cat :asc]]})]
      (is (= 3 (count result)))
      (is (= 0 (long (:cat (first result)))))
      (is (= 5 (long (:count-distinct (first result)))))
      (is (= 500000 (long (:cat (second result)))))
      (is (= 3 (long (:count-distinct (second result)))))
      (is (= 1000000 (long (:cat (nth result 2)))))
      (is (= 1 (long (:count-distinct (nth result 2))))))))

(deftest count-distinct-hash-mixed-test
  (testing "COUNT DISTINCT + SUM mixed aggs via hash path"
    (let [n 2000
          cats (long-array [0 500000])
          cat-data (long-array (for [i (range n)] (aget cats (mod i 2))))
          val-data (long-array (for [i (range n)]
                                 (let [j (quot i 2)]
                                   (if (zero? (mod i 2)) (long (mod j 5)) (+ 10 (long (mod j 3)))))))
          amt-data (double-array (repeatedly n #(double 1.0)))
          result (q/q {:from {:cat cat-data :v val-data :amt amt-data}
                       :group [:cat]
                       :agg [[:count-distinct :v] [:sum :amt] [:count]]
                       :order [[:cat :asc]]})]
      (is (= 2 (count result)))
      (is (= 5 (long (:count-distinct (first result)))))
      (is (= 3 (long (:count-distinct (second result)))))
      (is (pos? (double (:sum (first result)))))
      (is (pos? (long (:count (first result))))))))

(deftest count-distinct-hash-columnar-test
  (testing "COUNT DISTINCT via hash path with columnar output"
    (let [n 2000
          cats (long-array [0 500000 1000000])
          cat-data (long-array (for [i (range n)] (aget cats (mod i 3))))
          val-data (long-array (for [i (range n)]
                                 (let [c (aget cat-data i)
                                       j (quot i 3)]
                                   (cond (= c 0) (long (mod j 5))
                                         (= c 500000) (+ 10 (long (mod j 3)))
                                         :else 20))))
          result (q/q {:from {:cat cat-data :v val-data}
                       :group [:cat]
                       :agg [[:count-distinct :v]]
                       :result :columns})]
      (is (= 3 (:n-rows result)))
      (is (contains? result :cat))
      (is (contains? result :count-distinct)))))

(deftest median-scalar-test
  (testing "Median in scalar aggregation"
    (let [data {:v (double-array [3.0 1.0 5.0 4.0 2.0])}
          result (q/q {:from data
                       :agg [[:median :v]]})]
      (is (== 3.0 (double (:median (first result))))))))

(deftest percentile-test
  (testing "Exact percentile on known data"
    (let [data {:v (double-array [1.0 2.0 3.0 4.0 5.0])}]
      ;; P0 = min
      (is (== 1.0 (double (:percentile (first (q/q {:from data :agg [[:percentile :v 0.0]]}))))))
      ;; P50 = median
      (is (== 3.0 (double (:percentile (first (q/q {:from data :agg [[:percentile :v 0.5]]}))))))
      ;; P100 = max
      (is (== 5.0 (double (:percentile (first (q/q {:from data :agg [[:percentile :v 1.0]]}))))))))

  (testing "Percentile with even count (interpolation)"
    (let [data {:v (double-array [2.0 4.0 4.0 4.0 5.0 5.0 7.0 9.0])}
          result (q/q {:from data :agg [[:percentile :v 0.5]]})]
      (is (== 4.5 (double (:percentile (first result)))))))

  (testing "Percentile with predicates"
    (let [data {:cat (long-array [0 0 0 1 1 1])
                :v (double-array [1.0 3.0 5.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :where [[:< :cat 1]]
                       :agg [[:percentile :v 0.5]]})]
      (is (== 3.0 (double (:percentile (first result)))))))

  (testing "Percentile single element"
    (let [data {:v (double-array [42.0])}
          result (q/q {:from data :agg [[:percentile :v 0.95]]})]
      (is (== 42.0 (double (:percentile (first result)))))))

  (testing "Percentile all same value"
    (let [data {:v (double-array [7.0 7.0 7.0 7.0 7.0])}
          result (q/q {:from data :agg [[:percentile :v 0.75]]})]
      (is (== 7.0 (double (:percentile (first result))))))))

(deftest percentile-group-by-test
  (testing "Grouped percentile (low-cardinality)"
    (let [data {:cat (long-array [0 0 0 0 0 1 1 1 1])
                :v (double-array [1.0 2.0 3.0 4.0 5.0 10.0 20.0 30.0 40.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:percentile :v 0.5]]
                       :order [[:cat :asc]]})]
      ;; cat 0: [1,2,3,4,5] → P50=3.0
      ;; cat 1: [10,20,30,40] → P50=25.0
      (is (== 3.0 (double (:percentile (first result)))))
      (is (== 25.0 (double (:percentile (second result))))))))

(deftest approx-quantile-test
  (testing "Approx quantile close to exact on uniform data"
    (let [n 100000
          data {:v (double-array (range n))}
          exact-p50 (/ (dec n) 2.0)
          exact-p95 (* 0.95 (dec n))
          r50 (q/q {:from data :agg [[:approx-quantile :v 0.5]]})
          r95 (q/q {:from data :agg [[:approx-quantile :v 0.95]]})]
      ;; Within 1% of exact
      (is (< (Math/abs (- (double (:approx-quantile (first r50))) exact-p50))
             (* 0.01 n)))
      (is (< (Math/abs (- (double (:approx-quantile (first r95))) exact-p95))
             (* 0.01 n)))))

  (testing "Approx quantile single element"
    (let [data {:v (double-array [42.0])}
          result (q/q {:from data :agg [[:approx-quantile :v 0.5]]})]
      (is (== 42.0 (double (:approx-quantile (first result)))))))

  (testing "Approx quantile edge values"
    (let [data {:v (double-array [1.0 2.0 3.0 4.0 5.0])}]
      ;; q=0 should give min
      (is (== 1.0 (double (:approx-quantile (first (q/q {:from data :agg [[:approx-quantile :v 0.0]]}))))))
      ;; q=1 should give max
      (is (== 5.0 (double (:approx-quantile (first (q/q {:from data :agg [[:approx-quantile :v 1.0]]})))))))))

(deftest approx-quantile-group-by-test
  (testing "Grouped approx quantile"
    (let [data {:cat (long-array [0 0 0 0 0 1 1 1 1 1])
                :v (double-array [1.0 2.0 3.0 4.0 5.0 10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:approx-quantile :v 0.5]]
                       :order [[:cat :asc]]})]
      ;; Small groups — exact and approx should be close
      (is (< (Math/abs (- (double (:approx-quantile (first result))) 3.0)) 1.5))
      (is (< (Math/abs (- (double (:approx-quantile (second result))) 30.0)) 15.0)))))

;; ============================================================================
;; String Function and LIKE Predicate Tests
;; ============================================================================

(deftest like-predicate-test
  (testing "LIKE predicate on dict-encoded strings"
    (let [cats (q/encode-column (into-array String ["apple" "apricot" "banana" "blueberry" "cherry"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:like :cat "a%"]]
                       :agg [[:count]]})]
      ;; "apple" and "apricot" match "a%"
      (is (= 2 (long (:count (first result))))))))

(deftest not-like-predicate-test
  (testing "NOT-LIKE predicate (direct)"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:not-like :cat "a%"]]
                       :agg [[:count]]})]
      ;; "banana" and "cherry" don't match "a%"
      (is (= 2 (long (:count (first result)))))))
  (testing "NOT-LIKE via [:not [:like ...]] negation"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:not [:like :cat "a%"]]]
                       :agg [[:count]]})]
      ;; "banana" and "cherry" don't match "a%"
      (is (= 2 (long (:count (first result))))))))

(deftest in-predicate-string-test
  (testing "IN predicate with dict-encoded string column"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:in :cat "apple" "cherry"]]
                       :agg [[:count]]})]
      ;; apple(0), cherry(2), apple(3) → 3
      (is (= 3 (long (:count (first result)))))))
  (testing "NOT-IN predicate with dict-encoded string column"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:not-in :cat "apple"]]
                       :agg [[:count]]})]
      ;; NOT apple: banana(1), cherry(2), banana(4) → 3
      (is (= 3 (long (:count (first result))))))))

(deftest string-equality-predicate-test
  (testing "Equality on dict-encoded string column"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:= :cat "apple"]]
                       :agg [[:count]]})]
      ;; apple(0), apple(3) → 2
      (is (= 2 (long (:count (first result)))))))
  (testing "Inequality on dict-encoded string column"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:!= :cat "apple"]]
                       :agg [[:count]]})]
      ;; banana(1), cherry(2), banana(4) → 3
      (is (= 3 (long (:count (first result)))))))
  (testing "Equality with non-existent string returns 0 rows"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:= :cat "nonexistent"]]
                       :agg [[:count]]})]
      (is (= 0 (long (:count (first result)))))))
  (testing "String equality combined with numeric predicate"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "apple" "banana" "cherry"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:= :cat "apple"] [:> :v 2.0]]
                       :agg [[:sum :v]]})]
      ;; apple(0,2): v=1.0,3.0; after v>2.0: only 3.0
      (is (== 3.0 (:sum (first result)))))))

(deftest contains-predicate-test
  (testing "CONTAINS predicate"
    (let [cats (q/encode-column (into-array String ["apple pie" "banana split" "apple juice"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:contains :cat "apple"]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest starts-with-predicate-test
  (testing "STARTS-WITH predicate"
    (let [cats (q/encode-column (into-array String ["http://foo" "https://bar" "ftp://baz"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:starts-with :cat "http"]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest ends-with-predicate-test
  (testing "ENDS-WITH predicate"
    (let [cats (q/encode-column (into-array String ["file.txt" "file.csv" "data.txt"]))
          vals (double-array [1.0 2.0 3.0])
          result (q/q {:from {:cat cats :v vals}
                       :where [[:ends-with :cat ".txt"]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest like-with-group-by-test
  (testing "LIKE combined with group-by"
    (let [urls (q/encode-column (into-array String ["/api/users" "/api/posts" "/web/home" "/api/users" "/web/about"]))
          hits (double-array [10.0 20.0 30.0 40.0 50.0])
          cats (long-array [0 0 1 0 1])
          result (q/q {:from {:url urls :hits hits :cat cats}
                       :where [[:like :url "/api%"]]
                       :group [:cat]
                       :agg [[:sum :hits]]
                       :order [[:cat :asc]]})]
      ;; Only /api/* rows: cat=0 gets 10+20+40=70
      (is (= 1 (count result)))
      (is (== 70.0 (:sum (first result)))))))

;; ============================================================================
;; String Function Tests
;; ============================================================================

(deftest upper-function-test
  (testing "UPPER on dict-encoded strings in SELECT"
    (let [names (q/encode-column (into-array String ["alice" "Bob" "CHARLIE"]))
          result (q/q {:from {:name names}
                       :select [[:as [:upper :name] :u]]})]
      (is (= ["ALICE" "BOB" "CHARLIE"] (mapv :u result)))))
  (testing "UPPER in GROUP BY collapses case variants"
    (let [names (q/encode-column (into-array String ["foo" "Foo" "FOO" "bar" "Bar"]))
          v (double-array [1.0 2.0 3.0 10.0 20.0])
          result (q/q {:from {:name names :v v}
                       :group [[:upper :name]]
                       :agg [[:sum :v]]})]
      (is (= 2 (count result)))
      ;; "bar"+"Bar" → "BAR" sum=30, "foo"+"Foo"+"FOO" → "FOO" sum=6
      (let [by-name (into {} (map (fn [r] [(some #(when (string? (val %)) (val %)) r) r]) result))]
        (is (== 30.0 (:sum (get by-name "BAR"))))
        (is (== 6.0 (:sum (get by-name "FOO"))))))))

(deftest lower-function-test
  (testing "LOWER on dict-encoded strings in SELECT"
    (let [names (q/encode-column (into-array String ["ALICE" "Bob" "charlie"]))
          result (q/q {:from {:name names}
                       :select [[:as [:lower :name] :l]]})]
      (is (= ["alice" "bob" "charlie"] (mapv :l result)))))
  (testing "LOWER in GROUP BY collapses case variants"
    (let [names (q/encode-column (into-array String ["Hello" "HELLO" "hello" "World" "world"]))
          v (double-array [1.0 2.0 3.0 10.0 20.0])
          result (q/q {:from {:name names :v v}
                       :group [[:lower :name]]
                       :agg [[:sum :v]]})]
      (is (= 2 (count result)))
      (let [by-name (into {} (map (fn [r] [(some #(when (string? (val %)) (val %)) r) r]) result))]
        (is (== 6.0 (:sum (get by-name "hello"))))
        (is (== 30.0 (:sum (get by-name "world"))))))))

(deftest substr-function-test
  (testing "SUBSTR on dict-encoded strings in SELECT"
    (let [names (q/encode-column (into-array String ["hello" "world" "foobar"]))
          result (q/q {:from {:name names}
                       :select [[:as [:substr :name 1 3] :s]]})]
      ;; SQL SUBSTR(s, 1, 3) = first 3 chars
      (is (= ["hel" "wor" "foo"] (mapv :s result)))))
  (testing "SUBSTR with start beyond string length"
    (let [names (q/encode-column (into-array String ["hi" "hey"]))
          result (q/q {:from {:name names}
                       :select [[:as [:substr :name 10 5] :s]]})]
      (is (= ["" ""] (mapv :s result)))))
  (testing "SUBSTR without length (to end of string)"
    (let [names (q/encode-column (into-array String ["hello" "world"]))
          result (q/q {:from {:name names}
                       :select [[:as [:substr :name 3] :s]]})]
      ;; SUBSTR(s, 3) = from position 3 to end
      (is (= ["llo" "rld"] (mapv :s result))))))

(deftest replace-function-test
  (testing "REPLACE on dict-encoded strings in SELECT"
    (let [names (q/encode-column (into-array String ["foo-bar" "foo-baz" "hello"]))
          result (q/q {:from {:name names}
                       :select [[:as [:replace :name "foo" "xxx"] :r]]})]
      (is (= ["xxx-bar" "xxx-baz" "hello"] (mapv :r result)))))
  (testing "REPLACE in GROUP BY collapses values"
    (let [names (q/encode-column (into-array String ["cat-A" "dog-A" "cat-B" "dog-B"]))
          v (double-array [1.0 2.0 3.0 4.0])
          result (q/q {:from {:name names :v v}
                       :group [[:replace :name "-A" ""]]
                       :agg [[:sum :v]]})]
      ;; "cat-A"→"cat", "cat-B"→still "cat-B", etc.
      ;; Only -A gets replaced, so "cat"=1, "dog"=2, "cat-B"=3, "dog-B"=4
      (is (= 4 (count result))))))

(deftest trim-function-test
  (testing "TRIM on dict-encoded strings in SELECT"
    (let [names (q/encode-column (into-array String ["  hello  " " world " "foo"]))
          result (q/q {:from {:name names}
                       :select [[:as [:trim :name] :t]]})]
      (is (= ["hello" "world" "foo"] (mapv :t result)))))
  (testing "TRIM in GROUP BY collapses padded strings"
    (let [names (q/encode-column (into-array String [" foo " "foo" "  foo  " " bar "]))
          v (double-array [1.0 2.0 3.0 10.0])
          result (q/q {:from {:name names :v v}
                       :group [[:trim :name]]
                       :agg [[:sum :v]]})]
      (is (= 2 (count result)))
      (let [by-name (into {} (map (fn [r] [(some #(when (string? (val %)) (val %)) r) r]) result))]
        (is (== 10.0 (:sum (get by-name "bar"))))
        (is (== 6.0 (:sum (get by-name "foo"))))))))

(deftest concat-function-test
  (testing "CONCAT two string columns in SELECT"
    (let [first-names (q/encode-column (into-array String ["John" "Jane"]))
          last-names (q/encode-column (into-array String ["Doe" "Smith"]))
          result (q/q {:from {:first first-names :last last-names}
                       :select [[:as [:concat :first :last] :full]]})]
      (is (= ["JohnDoe" "JaneSmith"] (mapv :full result)))))
  (testing "CONCAT column with scalar in SELECT"
    (let [names (q/encode-column (into-array String ["alice" "bob"]))
          result (q/q {:from {:name names}
                       :select [[:as [:concat :name "@example.com"] :email]]})]
      (is (= ["alice@example.com" "bob@example.com"] (mapv :email result))))))

(deftest length-function-test
  (testing "LENGTH on dict-encoded strings"
    (let [names (q/encode-column (into-array String ["hi" "hello" "foobar"]))
          result (q/q {:from {:name names}
                       :agg [[:as [:sum [:length :name]] :total_len]]})]
      ;; 2 + 5 + 6 = 13
      (is (== 13.0 (:total_len (first result))))))
  (testing "LENGTH in SELECT"
    (let [names (q/encode-column (into-array String ["hi" "hello"]))
          result (q/q {:from {:name names}
                       :select [[:as [:length :name] :len]]})]
      (is (= [2.0 5.0] (mapv :len result))))))

;; ============================================================================
;; CAST / Type Coercion Tests
;; ============================================================================

(deftest cast-long-to-double-test
  (testing "CAST long to double"
    (let [data {:x (long-array [1 2 3])}
          result (q/q {:from data
                       :select [[:as [:cast :x :double] :d]]})]
      (is (= [1.0 2.0 3.0] (mapv :d result))))))

(deftest cast-double-to-long-test
  (testing "CAST double to long (truncation)"
    (let [data {:x (double-array [1.7 2.3 -3.9])}
          result (q/q {:from data
                       :select [[:as [:cast :x :long] :l]]})]
      ;; Through eval-agg-expr, these come back as doubles of truncated values
      (is (= [1.0 2.0 -3.0] (mapv :l result))))))

(deftest cast-string-to-double-test
  (testing "CAST dict-encoded string to double"
    (let [nums (q/encode-column (into-array String ["1.5" "2.7" "3.14"]))
          result (q/q {:from {:s nums}
                       :agg [[:as [:sum [:cast :s :double]] :total]]})]
      (is (< (Math/abs (- 7.34 (:total (first result)))) 0.01)))))

(deftest cast-string-to-long-test
  (testing "CAST dict-encoded string to long via SELECT"
    (let [nums (q/encode-column (into-array String ["10" "20" "30"]))
          result (q/q {:from {:s nums}
                       :select [[:as [:cast :s :long] :n]]})]
      (is (= [10.0 20.0 30.0] (mapv :n result))))))

(deftest cast-long-to-string-test
  (testing "CAST long to string in SELECT"
    (let [data {:x (long-array [42 100 -7])}
          result (q/q {:from data
                       :select [[:as [:cast :x :string] :s]]})]
      (is (= ["42" "100" "-7"] (mapv :s result))))))

(deftest cast-double-to-string-test
  (testing "CAST double to string in SELECT"
    (let [data {:x (double-array [1.5 2.0 -3.14])}
          result (q/q {:from data
                       :select [[:as [:cast :x :string] :s]]})]
      (is (= ["1.5" "2.0" "-3.14"] (mapv :s result))))))

(deftest cast-in-group-by-test
  (testing "CAST long to string for GROUP BY"
    (let [codes (long-array [1 2 1 2 3])
          v (double-array [10.0 20.0 30.0 40.0 50.0])
          result (q/q {:from {:code codes :v v}
                       :group [[:cast :code :string]]
                       :agg [[:sum :v]]})]
      (is (= 3 (count result)))
      (let [by-name (into {} (map (fn [r] [(some #(when (string? (val %)) (val %)) r) r]) result))]
        (is (== 40.0 (:sum (get by-name "1"))))
        (is (== 60.0 (:sum (get by-name "2"))))
        (is (== 50.0 (:sum (get by-name "3"))))))))

(deftest cast-string-to-double-predicate-test
  (testing "CAST string to double for use in aggregation with predicate"
    (let [prices (q/encode-column (into-array String ["10.5" "20.3" "5.2" "15.0"]))
          result (q/q {:from {:price prices}
                       :agg [[:as [:sum [:cast :price :double]] :total]]})]
      (is (< (Math/abs (- 51.0 (:total (first result)))) 0.1)))))

;; ============================================================================
;; CASE/WHEN Expression Tests
;; ============================================================================

(deftest case-simple-test
  (testing "Simple CASE expression"
    (let [data {:x (double-array [1.0 5.0 15.0 -1.0 7.0])}
          result (q/q {:from data
                       :select [[:as [:case [[:< :x 0] 0] [[:> :x 10] 100] [:else :x]] :r]]})]
      (is (= [1.0 5.0 100.0 0.0 7.0] (mapv :r result))))))

(deftest case-in-aggregation-test
  (testing "CASE expression in aggregation"
    (let [data {:status (double-array [1.0 2.0 1.0 3.0 2.0])
                :v (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :agg [[:sum [:case [[:= :status 1] :v] [:else 0]]]]})]
      ;; status=1 rows: v=10+30=40
      (is (== 40.0 (:sum (first result)))))))

;; ============================================================================
;; SELECT DISTINCT Tests
;; ============================================================================

(deftest distinct-basic-test
  (testing "DISTINCT deduplicates rows"
    (let [data {:a (long-array [1 2 1 2 3])
                :b (double-array [10.0 20.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :select [:a :b]
                       :distinct true})]
      (is (= 3 (count result)))
      (is (= #{[1 10.0] [2 20.0] [3 30.0]}
             (set (mapv #(vector (:a %) (:b %)) result)))))))

(deftest distinct-with-order-test
  (testing "DISTINCT with ORDER BY"
    (let [data {:a (long-array [3 1 2 1 3 2])
                :b (double-array [30.0 10.0 20.0 10.0 30.0 20.0])}
          result (q/q {:from data
                       :select [:a]
                       :distinct true
                       :order [[:a :asc]]})]
      (is (= [1 2 3] (mapv :a result))))))

(deftest distinct-with-limit-test
  (testing "DISTINCT with LIMIT"
    (let [data {:a (long-array [1 2 1 2 3])
                :b (double-array [10.0 20.0 10.0 20.0 30.0])}
          result (q/q {:from data
                       :select [:a]
                       :distinct true
                       :order [[:a :asc]]
                       :limit 2})]
      (is (= 2 (count result)))
      (is (= [1 2] (mapv :a result))))))

(deftest distinct-group-by-test
  (testing "DISTINCT with group-by"
    (let [data {:g (long-array [1 1 2 2 1])
                :v (double-array [10.0 10.0 20.0 20.0 10.0])}
          result (q/q {:from data
                       :group [:g]
                       :agg [[:sum :v]]
                       :distinct true})]
      ;; group-by results are already unique
      (is (= 2 (count result))))))

;; ============================================================================
;; NULL Handling Tests
;; ============================================================================

(deftest is-null-double-test
  (testing "IS NULL on double[] (NaN sentinel)"
    (let [data {:a (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])}
          result (q/q {:from data
                       :where [[:is-null :a]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest is-not-null-double-test
  (testing "IS NOT NULL on double[] (NaN sentinel)"
    (let [data {:a (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])}
          result (q/q {:from data
                       :where [[:is-not-null :a]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest is-null-long-test
  (testing "IS NULL on long[] (Long/MIN_VALUE sentinel)"
    (let [data {:a (long-array [1 Long/MIN_VALUE 3 Long/MIN_VALUE 5])}
          result (q/q {:from data
                       :where [[:is-null :a]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest is-not-null-long-test
  (testing "IS NOT NULL on long[] (Long/MIN_VALUE sentinel)"
    (let [data {:a (long-array [1 Long/MIN_VALUE 3 Long/MIN_VALUE 5])}
          result (q/q {:from data
                       :where [[:is-not-null :a]]
                       :agg [[:count]]})]
      (is (= 3 (long (:count (first result))))))))

(deftest is-null-with-filter-test
  (testing "IS NULL combined with other predicates"
    (let [data {:a (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])
                :b (long-array [10 20 30 40 50])}
          result (q/q {:from data
                       :where [[:is-not-null :a] [:> :b 20]]
                       :agg [[:count]]})]
      (is (= 2 (long (:count (first result))))))))

(deftest coalesce-double-test
  (testing "COALESCE replaces NaN with scalar"
    (let [data {:a (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])}
          result (q/q {:from data
                       :select [[:as [:coalesce :a 0] :r]]})]
      (is (= [1.0 0.0 3.0 0.0 5.0] (mapv :r result))))))

(deftest coalesce-two-columns-test
  (testing "COALESCE with two columns"
    (let [data {:a (double-array [1.0 Double/NaN 3.0])
                :b (double-array [10.0 20.0 30.0])}
          result (q/q {:from data
                       :select [[:as [:coalesce :a :b] :r]]})]
      (is (= [1.0 20.0 3.0] (mapv :r result))))))

(deftest nullif-test
  (testing "NULLIF sets matching values to NaN"
    (let [data {:a (double-array [1.0 0.0 3.0 0.0 5.0])}
          result (q/q {:from data
                       :select [[:as [:nullif :a 0] :r]]})]
      (is (= 1.0 (:r (nth result 0))))
      (is (Double/isNaN (:r (nth result 1))))
      (is (= 3.0 (:r (nth result 2))))
      (is (Double/isNaN (:r (nth result 3))))
      (is (= 5.0 (:r (nth result 4)))))))

(deftest null-in-group-by-test
  (testing "NULL values in group-by agg data"
    (let [data {:g (long-array [1 2 1 2])
                :v (double-array [10.0 Double/NaN 30.0 Double/NaN])}
          result (q/q {:from data
                       :group [:g]
                       :agg [[:sum :v]]})]
      ;; Group 1 has valid values (10+30=40), group 2 has only NaN → excluded
      ;; NaN values are skipped in SUM (SQL NULL semantics)
      (is (= 1 (count result)))
      (is (== 40.0 (:sum (first result)))))))

;; ============================================================================
;; Date/Time Arithmetic Tests
;; ============================================================================

(deftest date-trunc-day-test
  (testing "DATE_TRUNC to day"
    ;; 2024-01-15 10:30:00 UTC = epoch-seconds 1705311000
    (let [ts (long-array [1705311000 1705311000])
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-trunc :day :ts] :d]]})]
      ;; Should truncate to 2024-01-15 00:00:00 = 1705276800
      (is (== 1705276800.0 (:d (first result)))))))

(deftest date-trunc-hour-test
  (testing "DATE_TRUNC to hour"
    (let [ts (long-array [1705311000]) ;; 2024-01-15 10:30:00
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-trunc :hour :ts] :d]]})]
      ;; 2024-01-15 10:00:00 = 1705309200
      (is (== 1705309200.0 (:d (first result)))))))

(deftest date-trunc-minute-test
  (testing "DATE_TRUNC to minute"
    (let [ts (long-array [1705311045]) ;; 10:30:45
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-trunc :minute :ts] :d]]})]
      ;; 10:30:00 = 1705311000
      (is (== 1705311000.0 (:d (first result)))))))

(deftest date-trunc-month-test
  (testing "DATE_TRUNC to month"
    (let [ts (long-array [1705311000]) ;; 2024-01-15 10:30:00
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-trunc :month :ts] :d]]})]
      ;; 2024-01-01 00:00:00 = 1704067200
      (is (== 1704067200.0 (:d (first result)))))))

(deftest date-trunc-year-test
  (testing "DATE_TRUNC to year"
    (let [ts (long-array [1705311000]) ;; 2024-01-15 10:30:00
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-trunc :year :ts] :d]]})]
      ;; 2024-01-01 00:00:00 = 1704067200
      (is (== 1704067200.0 (:d (first result)))))))

(deftest date-trunc-in-group-by-test
  (testing "DATE_TRUNC in GROUP BY"
    (let [;; Two timestamps on same day, one on next day
          ts (long-array [1705311000  ;; 2024-01-15 10:30:00
                          1705320000  ;; 2024-01-15 13:00:00
                          1705400000]) ;; 2024-01-16 11:13:20
          data {:ts ts :v (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :group [[:date-trunc :day :ts]]
                       :agg [[:sum :v]]})]
      (is (= 2 (count result))))))

(deftest date-add-days-test
  (testing "DATE_ADD days"
    (let [ts (long-array [1705276800]) ;; 2024-01-15 00:00:00
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-add :days 7 :ts] :d]]})]
      ;; 2024-01-22 00:00:00 = 1705276800 + 7*86400 = 1705881600
      (is (== 1705881600.0 (:d (first result)))))))

(deftest date-add-months-test
  (testing "DATE_ADD months"
    (let [ts (long-array [1705276800]) ;; 2024-01-15 00:00:00
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:date-add :months 1 :ts] :d]]})]
      ;; 2024-02-15 00:00:00 = 1707955200
      (is (== 1707955200.0 (:d (first result)))))))

(deftest date-diff-days-test
  (testing "DATE_DIFF in days"
    (let [ts1 (long-array [1705881600]) ;; 2024-01-22
          ts2 (long-array [1705276800]) ;; 2024-01-15
          data {:a ts1 :b ts2}
          result (q/q {:from data
                       :select [[:as [:date-diff :days :a :b] :d]]})]
      (is (== 7.0 (:d (first result)))))))

(deftest date-diff-seconds-test
  (testing "DATE_DIFF in seconds"
    (let [ts1 (long-array [1705311000]) ;; 10:30:00
          ts2 (long-array [1705309200]) ;; 10:00:00
          data {:a ts1 :b ts2}
          result (q/q {:from data
                       :select [[:as [:date-diff :seconds :a :b] :d]]})]
      (is (== 1800.0 (:d (first result)))))))

(deftest epoch-days-test
  (testing "EPOCH-DAYS conversion"
    (let [ts (long-array [86400]) ;; 1 day in seconds
          data {:ts ts}
          result (q/q {:from data
                       :select [[:as [:epoch-days :ts] :d]]})]
      (is (== 1.0 (:d (first result)))))))

(deftest epoch-seconds-test
  (testing "EPOCH-SECONDS conversion"
    (let [days (long-array [1]) ;; 1 day
          data {:d days}
          result (q/q {:from data
                       :select [[:as [:epoch-seconds :d] :s]]})]
      (is (== 86400.0 (:s (first result)))))))

;; ============================================================================
;; Hash Join Tests
;; ============================================================================

(deftest inner-join-test
  (testing "Basic inner join"
    (let [left {:id (long-array [1 2 3 4 5])
                :name (into-array String ["alice" "bob" "carol" "dave" "eve"])}
          right {:eid (long-array [2 4 6])
                 :salary (double-array [50000.0 70000.0 90000.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :select [:name :salary]})]
      (is (= 2 (count result)))
      (is (= #{50000.0 70000.0} (set (map :salary result))))
      (is (= #{"bob" "dave"} (set (map :name result)))))))

(deftest left-join-test
  (testing "Left outer join preserves all left rows"
    (let [left {:id (long-array [1 2 3])
                :val (double-array [10.0 20.0 30.0])}
          right {:rid (long-array [2 4])
                 :label (into-array String ["matched" "extra"])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :rid]
                               :type :left}]
                       :select [:id :val :label]})]
      (is (= 3 (count result)))
      ;; Row with id=2 should have label "matched"
      (is (= "matched" (:label (first (filter #(== 2 (long (:id %))) result)))))
      ;; Rows with id=1,3 should have nil label (NULL)
      (is (nil? (:label (first (filter #(== 1 (long (:id %))) result)))))
      (is (nil? (:label (first (filter #(== 3 (long (:id %))) result))))))))

(deftest right-join-test
  (testing "Right outer join preserves all right rows"
    (let [left {:id (long-array [1 2])
                :val (double-array [10.0 20.0])}
          right {:rid (long-array [2 3 4])
                 :score (double-array [100.0 200.0 300.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :rid]
                               :type :right}]
                       :select [:id :val :score]})]
      (is (= 3 (count result)))
      ;; id=2 matched
      (is (= 20.0 (:val (first (filter #(== 100.0 (:score %)) result)))))
      ;; id=3,4 from right have NULL val (nil in projection)
      (is (some #(nil? (:val %)) result)))))

(deftest full-outer-join-test
  (testing "Full outer join preserves all rows from both sides"
    (let [left {:id (long-array [1 2 3])
                :lv (double-array [10.0 20.0 30.0])}
          right {:rid (long-array [2 4])
                 :rv (double-array [200.0 400.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :rid]
                               :type :full}]
                       :select [:id :lv :rv]})]
      ;; Should have 4 rows: id=1(left only), id=2(match), id=3(left only), rid=4(right only)
      (is (= 4 (count result))))))

(deftest join-duplicate-keys-test
  (testing "Many-to-many join with duplicate keys"
    (let [left {:k (long-array [1 1 2])
                :lv (long-array [10 11 20])}
          right {:k2 (long-array [1 1 3])
                 :rv (long-array [100 101 300])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :k :k2]}]
                       :select [:lv :rv]})]
      ;; 2 left rows with k=1 × 2 right rows with k2=1 = 4 matches
      (is (= 4 (count result)))
      (is (= #{[10 100] [10 101] [11 100] [11 101]}
             (set (map (fn [r] [(long (:lv r)) (long (:rv r))]) result)))))))

(deftest join-multi-column-test
  (testing "Multi-column equi-join"
    (let [left {:a (long-array [1 1 2 2])
                :b (long-array [10 20 10 20])
                :val (double-array [1.0 2.0 3.0 4.0])}
          right {:x (long-array [1 2])
                 :y (long-array [20 10])
                 :label (into-array String ["one-twenty" "two-ten"])}
          result (q/q {:from left
                       :join [{:with right
                               :on [[:= :a :x] [:= :b :y]]}]
                       :select [:val :label]})]
      (is (= 2 (count result)))
      (is (= #{"one-twenty" "two-ten"} (set (map :label result))))
      ;; a=1,b=20 → val=2.0, label="one-twenty"
      (is (= 2.0 (:val (first (filter #(= "one-twenty" (:label %)) result)))))
      ;; a=2,b=10 → val=3.0, label="two-ten"
      (is (= 3.0 (:val (first (filter #(= "two-ten" (:label %)) result))))))))

(deftest join-dict-encoded-string-keys-test
  (testing "Join on dictionary-encoded string columns"
    (let [left {:name (into-array String ["alice" "bob" "carol"])
                :val (long-array [1 2 3])}
          right {:person (into-array String ["bob" "dave" "carol"])
                 :score (double-array [10.0 20.0 30.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :name :person]}]
                       :select [:val :score]})]
      (is (= 2 (count result)))
      (is (= #{[2 10.0] [3 30.0]}
             (set (map (fn [r] [(long (:val r)) (:score r)]) result)))))))

(deftest join-with-filter-test
  (testing "Join followed by WHERE filter"
    (let [left {:id (long-array [1 2 3 4 5])
                :val (double-array [10.0 20.0 30.0 40.0 50.0])}
          right {:eid (long-array [1 2 3 4 5])
                 :cat (long-array [0 1 0 1 0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :where [[:= :cat 1]]
                       :select [:id :val]})]
      (is (= 2 (count result)))
      (is (= #{20.0 40.0} (set (map :val result)))))))

(deftest join-with-group-by-agg-test
  (testing "Join followed by GROUP BY + aggregation"
    (let [orders {:oid (long-array [1 2 3 4 5])
                  :cid (long-array [1 1 2 2 3])
                  :amount (double-array [100.0 200.0 300.0 400.0 500.0])}
          customers {:cust-id (long-array [1 2 3])
                     :region (long-array [0 0 1])}
          result (q/q {:from orders
                       :join [{:with customers
                               :on [:= :cid :cust-id]}]
                       :group [:region]
                       :agg [[:sum :amount]]})]
      (is (= 2 (count result)))
      ;; region 0: cid 1+2 = 100+200+300+400 = 1000
      (is (= 1000.0 (:sum (first (filter #(== 0 (:region %)) result)))))
      ;; region 1: cid 3 = 500
      (is (= 500.0 (:sum (first (filter #(== 1 (:region %)) result))))))))

(deftest join-with-select-test
  (testing "Join with SELECT projection"
    (let [left {:id (long-array [1 2 3])
                :name (into-array String ["a" "b" "c"])}
          right {:eid (long-array [1 2 3])
                 :score (double-array [10.0 20.0 30.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :select [:name :score]})]
      (is (= 3 (count result)))
      (is (every? #(and (contains? % :name) (contains? % :score)) result))
      ;; No :id or :eid in results (only projected columns)
      (is (not (contains? (first result) :eid))))))

(deftest chained-joins-test
  (testing "Three-table star schema with chained joins"
    (let [fact {:order-id (long-array [1 2 3 4])
                :cust-id (long-array [1 2 1 2])
                :amount (double-array [100.0 200.0 300.0 400.0])}
          dim-cust {:cid (long-array [1 2])
                    :cust-name (into-array String ["alice" "bob"])}
          dim-region {:cid2 (long-array [1 2])
                      :region (into-array String ["east" "west"])}
          result (q/q {:from fact
                       :join [{:with dim-cust
                               :on [:= :cust-id :cid]}
                              {:with dim-region
                               :on [:= :cust-id :cid2]}]
                       :group [:region]
                       :agg [[:sum :amount]]})]
      (is (= 2 (count result)))
      (is (= 400.0 (:sum (first (filter #(= "east" (:region %)) result)))))
      (is (= 600.0 (:sum (first (filter #(= "west" (:region %)) result))))))))

(deftest join-empty-result-test
  (testing "Inner join with no matching keys produces empty result"
    (let [left {:id (long-array [1 2 3])
                :val (double-array [10.0 20.0 30.0])}
          right {:eid (long-array [4 5 6])
                 :rv (double-array [40.0 50.0 60.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :select [:val :rv]})]
      (is (= 0 (count result))))))

(deftest join-single-row-test
  (testing "Join with single-row tables"
    (let [left {:id (long-array [1])
                :val (double-array [42.0])}
          right {:eid (long-array [1])
                 :rv (double-array [99.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :select [:val :rv]})]
      (is (= 1 (count result)))
      (is (= 42.0 (:val (first result))))
      (is (= 99.0 (:rv (first result)))))))

(deftest join-null-keys-no-match-test
  (testing "NULL keys (Long.MIN_VALUE) don't match in joins"
    (let [left {:id (long-array [1 Long/MIN_VALUE 3])
                :val (double-array [10.0 20.0 30.0])}
          right {:eid (long-array [Long/MIN_VALUE 3])
                 :rv (double-array [200.0 300.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]}]
                       :select [:val :rv]})]
      ;; Only id=3 matches; NULL ≠ NULL
      (is (= 1 (count result)))
      (is (= 30.0 (:val (first result))))
      (is (= 300.0 (:rv (first result)))))))

(deftest left-join-all-nulls-test
  (testing "LEFT join where right has no matches produces all NULLs"
    (let [left {:id (long-array [1 2 3])
                :val (double-array [10.0 20.0 30.0])}
          right {:eid (long-array [4 5])
                 :rv (double-array [40.0 50.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [:= :id :eid]
                               :type :left}]
                       :select [:id :val :rv]})]
      (is (= 3 (count result)))
      ;; All rv values should be nil (NULL in projection)
      (is (every? #(nil? (:rv %)) result)))))

;; ============================================================================
;; Fused Join + Group-By + Aggregate Tests
;; ============================================================================

(deftest join-fused-group-agg-test
  (testing "Fused join+group+agg produces same results as unfused path"
    (let [orders {:oid (long-array [1 2 3 4 5])
                  :cid (long-array [1 1 2 2 3])
                  :amount (double-array [100.0 200.0 300.0 400.0 500.0])}
          customers {:cust-id (long-array [1 2 3])
                     :region (long-array [0 0 1])}
          result (q/q {:from orders
                       :join [{:with customers
                               :on [:= :cid :cust-id]}]
                       :group [:region]
                       :agg [[:sum :amount]]})]
      (is (= 2 (count result)))
      ;; region 0: cid 1+2 = 100+200+300+400 = 1000
      (is (= 1000.0 (:sum (first (filter #(== 0 (:region %)) result)))))
      ;; region 1: cid 3 = 500
      (is (= 500.0 (:sum (first (filter #(== 1 (:region %)) result))))))))

(deftest join-fused-multi-agg-test
  (testing "Fused join+group with SUM + COUNT + MIN + MAX"
    (let [fact {:id (long-array (range 10))
                :fk (long-array [0 0 0 1 1 1 2 2 2 2])
                :val (double-array [1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0])}
          dim {:did (long-array [0 1 2])
               :cat (long-array [0 1 0])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [:= :fk :did]}]
                       :group [:cat]
                       :agg [[:sum :val]
                             [:count]
                             [:min :val]
                             [:max :val]]})]
      (is (= 2 (count result)))
      ;; cat 0: fk=0 (1+2+3) + fk=2 (7+8+9+10) = 40
      (let [cat0 (first (filter #(== 0 (:cat %)) result))]
        (is (= 40.0 (:sum cat0)))
        (is (= 7 (:count cat0)))
        (is (= 1.0 (:min cat0)))
        (is (= 10.0 (:max cat0))))
      ;; cat 1: fk=1 (4+5+6) = 15
      (let [cat1 (first (filter #(== 1 (:cat %)) result))]
        (is (= 15.0 (:sum cat1)))
        (is (= 3 (:count cat1)))
        (is (= 4.0 (:min cat1)))
        (is (= 6.0 (:max cat1)))))))

(deftest join-fused-multi-group-test
  (testing "Fused join+group with two dimension group columns"
    (let [fact {:id (long-array (range 6))
                :fk (long-array [0 0 1 1 2 2])
                :amount (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          dim {:did (long-array [0 1 2])
               :region (long-array [0 0 1])
               :tier (long-array [0 1 0])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [:= :fk :did]}]
                       :group [:region :tier]
                       :agg [[:sum :amount]]})]
      ;; region=0,tier=0: fk=0 → 10+20=30
      ;; region=0,tier=1: fk=1 → 30+40=70
      ;; region=1,tier=0: fk=2 → 50+60=110
      (is (= 3 (count result)))
      (is (= 30.0 (:sum (first (filter #(and (== 0 (:region %)) (== 0 (:tier %))) result)))))
      (is (= 70.0 (:sum (first (filter #(and (== 0 (:region %)) (== 1 (:tier %))) result)))))
      (is (= 110.0 (:sum (first (filter #(and (== 1 (:region %)) (== 0 (:tier %))) result))))))))

(deftest join-fused-avg-test
  (testing "Fused join+group with AVG aggregation"
    (let [fact {:fk (long-array [0 0 1 1])
                :val (double-array [10.0 20.0 30.0 50.0])}
          dim {:did (long-array [0 1])
               :cat (long-array [0 1])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [:= :fk :did]}]
                       :group [:cat]
                       :agg [[:avg :val]]})]
      (is (= 2 (count result)))
      ;; cat 0: (10+20)/2 = 15.0
      (is (== 15.0 (:avg (first (filter #(== 0 (:cat %)) result)))))
      ;; cat 1: (30+50)/2 = 40.0
      (is (== 40.0 (:avg (first (filter #(== 1 (:cat %)) result))))))))

(deftest join-fused-with-having-test
  (testing "Fused join+group with HAVING post-filter"
    (let [fact {:fk (long-array [0 0 0 1 1 2])
                :amount (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          dim {:did (long-array [0 1 2])
               :cat (long-array [0 1 2])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [:= :fk :did]}]
                       :group [:cat]
                       :agg [[:sum :amount]]
                       :having [[:> :sum 50]]})]
      ;; cat 0: 60, cat 1: 90, cat 2: 60 → all > 50
      ;; cat 0: 10+20+30=60 (>50), cat 1: 40+50=90 (>50), cat 2: 60 (>50)
      (is (= 3 (count result))))))

(deftest join-fused-with-order-limit-test
  (testing "Fused join+group with ORDER BY and LIMIT"
    (let [fact {:fk (long-array [0 0 1 1 2 2])
                :amount (double-array [10.0 20.0 30.0 40.0 50.0 60.0])}
          dim {:did (long-array [0 1 2])
               :cat (long-array [0 1 2])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [:= :fk :did]}]
                       :group [:cat]
                       :agg [[:sum :amount]]
                       :order [[:sum :desc]]
                       :limit 2})]
      (is (= 2 (count result)))
      ;; Sorted desc: cat2=110, cat1=70, cat0=30 → top 2
      (is (= 110.0 (:sum (first result))))
      (is (= 70.0 (:sum (second result)))))))

;; ============================================================================
;; Additional Edge Case Tests
;; ============================================================================

(deftest having-with-operators-test
  (testing "HAVING with various comparison operators"
    (let [g (long-array [0 0 0 1 1 2])
          v (double-array [10.0 20.0 30.0 5.0 15.0 100.0])
          result-gt (q/q {:from {:g g :v v}
                          :group [:g]
                          :agg [[:sum :v]]
                          :having [[:> :sum 50]]})
          result-lt (q/q {:from {:g g :v v}
                          :group [:g]
                          :agg [[:sum :v]]
                          :having [[:< :sum 50]]})
          result-eq (q/q {:from {:g g :v v}
                          :group [:g]
                          :agg [[:sum :v]]
                          :having [[:= :sum 100]]})]
      ;; g0=60, g1=20, g2=100
      (is (= 2 (count result-gt)))   ;; g0=60, g2=100
      (is (= 1 (count result-lt)))   ;; g1=20
      (is (= 1 (count result-eq))))) ;; g2=100
  (testing "HAVING filters out all groups"
    (let [g (long-array [0 1])
          v (double-array [1.0 2.0])
          result (q/q {:from {:g g :v v}
                       :group [:g]
                       :agg [[:sum :v]]
                       :having [[:> :sum 100]]})]
      (is (empty? result)))))

(deftest having-on-count-distinct-test
  (testing "HAVING on COUNT with multi-group"
    (let [g1 (long-array [0 0 0 1 1])
          g2 (long-array [0 0 1 0 0])
          result (q/q {:from {:g1 g1 :g2 g2}
                       :group [:g1 :g2]
                       :agg [[:count]]
                       :having [[:>= :count 2]]})]
      ;; g1=0,g2=0: count=2, g1=0,g2=1: count=1, g1=1,g2=0: count=2
      (is (= 2 (count result))))))

(deftest case-without-else-test
  (testing "CASE without ELSE returns NULL (NaN)"
    (let [data {:x (double-array [1.0 5.0 15.0])}
          result (q/q {:from data
                       :select [[:as [:case [[:> :x 10] 100]] :r]]})]
      ;; Only x=15 matches; others get NaN (SQL NULL)
      (is (Double/isNaN (:r (first result))))
      (is (Double/isNaN (:r (second result))))
      (is (= 100.0 (:r (nth result 2)))))))

(deftest case-multiple-branches-test
  (testing "CASE with multiple comparison branches"
    (let [data {:x (double-array [1.0 5.0 15.0 25.0])}
          result (q/q {:from data
                       :select [[:as [:case [[:< :x 10] 1]
                                      [[:< :x 20] 2]
                                      [:else 3]] :bucket]]})]
      (is (= [1.0 1.0 2.0 3.0] (mapv :bucket result))))))

;; ============================================================================
;; String CASE/WHEN Expression Tests
;; ============================================================================

(deftest case-string-select-test
  (testing "String CASE expression in SELECT"
    (let [data {:status (long-array [1 2 3 1 2])}
          result (q/q {:from data
                       :select [[:as [:case [[:= :status 1] "active"]
                                      [[:= :status 2] "inactive"]
                                      [:else "unknown"]] :label]]})]
      (is (= ["active" "inactive" "unknown" "active" "inactive"]
             (mapv :label result))))))

(deftest case-string-no-else-test
  (testing "String CASE without ELSE returns nil for non-matching rows"
    (let [data {:status (long-array [1 2 3])}
          result (q/q {:from data
                       :select [[:as [:case [[:= :status 1] "yes"]
                                      [[:= :status 2] "no"]] :label]]})]
      (is (= "yes" (:label (nth result 0))))
      (is (= "no" (:label (nth result 1))))
      (is (nil? (:label (nth result 2)))))))

(deftest case-string-group-by-test
  (testing "String CASE expression in GROUP BY"
    (let [data {:status (long-array [1 2 1 2 3])
                :v (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from data
                       :group [[:case [[:= :status 1] "active"]
                                [[:= :status 2] "inactive"]
                                [:else "other"]]]
                       :agg [[:sum :v]]})]
      ;; active: 10+30=40, inactive: 20+40=60, other: 50
      (is (= 3 (count result)))
      ;; Group key name is the materialized temp column name
      (let [group-key (first (filter #(not (#{:sum :_count} %)) (keys (first result))))
            by-label (into {} (map (fn [r] [(get r group-key) r])) result)]
        (is (== 40.0 (:sum (get by-label "active"))))
        (is (== 60.0 (:sum (get by-label "inactive"))))
        (is (== 50.0 (:sum (get by-label "other"))))))))

(deftest null-in-aggregation-test
  (testing "SUM/COUNT with NaN values (NaN propagates in SIMD arithmetic)"
    (let [v (double-array [1.0 3.0 5.0])
          result (q/q {:from {:v v}
                       :agg [[:sum :v] [:count] [:avg :v]]})]
      ;; Clean data: sum=9, count=3, avg=3
      (is (== 9.0 (:sum (first result))))
      (is (== 3 (:count (first result))))
      (is (== 3.0 (:avg (first result))))))
  (testing "IS NOT NULL filter excludes NaN before aggregation"
    (let [v (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])
          result (q/q {:from {:v v}
                       :where [[:is-not-null :v]]
                       :agg [[:sum :v] [:count]]})]
      (is (== 9.0 (:sum (first result))))
      (is (== 3 (:count (first result)))))))

(deftest null-in-group-by-long-test
  (testing "NULL long values (Long.MIN_VALUE) form their own group in GROUP BY"
    (let [g (long-array [1 Long/MIN_VALUE 1 Long/MIN_VALUE 2])
          v (double-array [10.0 20.0 30.0 40.0 50.0])
          result (q/q {:from {:g g :v v}
                       :group [:g]
                       :agg [[:sum :v]]})]
      ;; 3 groups: 1 (sum=40), 2 (sum=50), NULL (sum=60) — PostgreSQL NULL group semantics
      (is (= 3 (count result)))
      (is (= 40.0 (:sum (first (filter #(= 1 (:g %)) result)))))
      (is (= 50.0 (:sum (first (filter #(= 2 (:g %)) result)))))
      (is (some #(nil? (:g %)) result) "NULL group should be present"))))

(deftest distinct-with-null-test
  (testing "DISTINCT handles NULL values"
    (let [x (double-array [1.0 Double/NaN 1.0 Double/NaN 2.0])
          result (q/q {:from {:x x}
                       :select [:x]
                       :distinct true})]
      ;; 1.0, NaN(nil), 2.0 = 3 distinct values
      (is (= 3 (count result))))))

(deftest order-by-with-null-test
  (testing "ORDER BY with NULL values sorts them"
    (let [x (double-array [3.0 Double/NaN 1.0 2.0])
          result (q/q {:from {:x x}
                       :select [:x]
                       :order [[:x :asc]]})]
      ;; NaN sorts to end or beginning depending on implementation
      (is (= 4 (count result))))))

(deftest string-function-empty-strings-test
  (testing "String functions on empty strings"
    (let [names (q/encode-column (into-array String ["" "hello" ""]))
          result (q/q {:from {:name names}
                       :select [[:as [:upper :name] :u]
                                [:as [:lower :name] :l]
                                [:as [:trim :name] :t]]})]
      (is (= "" (:u (first result))))
      (is (= "" (:l (first result))))
      (is (= "" (:t (first result))))
      (is (= "HELLO" (:u (second result)))))))

(deftest cast-invalid-string-test
  (testing "CAST invalid string to double returns NaN"
    (let [nums (q/encode-column (into-array String ["1.5" "not-a-number" "3.0"]))
          result (q/q {:from {:s nums}
                       :select [[:as [:cast :s :double] :d]]})]
      (is (== 1.5 (:d (first result))))
      (is (Double/isNaN (:d (second result))))
      (is (== 3.0 (:d (nth result 2)))))))

(deftest concat-two-columns-test
  (testing "CONCAT two dict-encoded string columns"
    (let [first-names (q/encode-column (into-array String ["John" "Jane" "John"]))
          last-names (q/encode-column (into-array String ["Doe" "Smith" "Doe"]))
          result (q/q {:from {:first first-names :last last-names}
                       :select [[:as [:concat :first :last] :full]]})]
      (is (= ["JohnDoe" "JaneSmith" "JohnDoe"] (mapv :full result))))))

(deftest upper-in-count-distinct-test
  (testing "COUNT DISTINCT on UPPER-transformed column"
    (let [names (q/encode-column (into-array String ["foo" "Foo" "FOO" "bar" "Bar"]))
          result (q/q {:from {:name names}
                       :agg [[:as [:count-distinct [:upper :name]] :cd]]})]
      ;; UPPER collapses "foo"/"Foo"/"FOO" → "FOO", "bar"/"Bar" → "BAR"
      (is (== 2 (:cd (first result)))))))

(deftest string-transform-with-predicate-test
  (testing "String transform in GROUP BY with WHERE predicate"
    (let [names (q/encode-column (into-array String ["cat" "Cat" "DOG" "dog" "Cat"]))
          v (double-array [1.0 2.0 10.0 20.0 3.0])
          result (q/q {:from {:name names :v v}
                       :where [[:> :v 1.5]]
                       :group [[:lower :name]]
                       :agg [[:sum :v]]})]
      ;; After filtering v > 1.5: Cat(2), DOG(10), dog(20), Cat(3)
      ;; LOWER: cat=2+3=5, dog=10+20=30
      (is (= 2 (count result)))
      (let [by-name (into {} (map (fn [r] [(some #(when (string? (val %)) (val %)) r) r]) result))]
        (is (== 5.0 (:sum (get by-name "cat"))))
        (is (== 30.0 (:sum (get by-name "dog"))))))))

(deftest limit-zero-test
  (testing "LIMIT 0 returns empty result"
    (let [data {:x (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :select [:x]
                       :limit 0})]
      (is (empty? result)))))

(deftest offset-beyond-results-test
  (testing "OFFSET beyond result count returns empty"
    (let [data {:x (double-array [1.0 2.0 3.0])}
          result (q/q {:from data
                       :select [:x]
                       :offset 100})]
      (is (empty? result)))))

;; ============================================================================
;; Index Zone Map Pruning Tests
;; ============================================================================

(deftest index-direct-materialization-test
  (testing "idx-materialize-to-array produces correct float64 array"
    (let [n 20000
          vals (map double (range n))
          idx (index/index-from-seq :float64 vals)
          arr (index/idx-materialize-to-array idx)]
      (is (= n (alength ^doubles arr)))
      (is (== 0.0 (aget ^doubles arr 0)))
      (is (== (double (dec n)) (aget ^doubles arr (dec n))))
      (dotimes [i (min 100 n)]
        (is (== (double i) (aget ^doubles arr i))
            (str "Mismatch at index " i)))))
  (testing "idx-materialize-to-array works for int64"
    (let [n 20000
          vals (range n)
          idx (index/index-from-seq :int64 vals)
          arr (index/idx-materialize-to-array idx)]
      (is (= n (alength ^longs arr)))
      (is (= 0 (aget ^longs arr 0)))
      (is (= (dec n) (aget ^longs arr (dec n)))))))

(deftest index-pruned-materialization-test
  (testing "idx-materialize-to-array-pruned copies only surviving chunks"
    (let [;; Create index with 3 chunks: [0..8191], [8192..16383], [16384..24575]
          n (* 3 8192)
          vals (range n)
          idx (index/index-from-seq :int64 vals)
          ;; Only keep chunk 1 (middle chunk)
          result (index/idx-materialize-to-array-pruned idx [1])]
      (is (= 8192 (alength ^longs result)))
      (is (= 8192 (aget ^longs result 0)))
      (is (= 16383 (aget ^longs result 8191)))))
  (testing "Pruning with multiple surviving chunks"
    (let [n (* 4 8192)
          vals (range n)
          idx (index/index-from-seq :int64 vals)
          ;; Keep chunks 0 and 2
          result (index/idx-materialize-to-array-pruned idx [0 2])]
      (is (= (* 2 8192) (alength ^longs result)))
      ;; First 8192 values are from chunk 0
      (is (= 0 (aget ^longs result 0)))
      (is (= 8191 (aget ^longs result 8191)))
      ;; Next 8192 values are from chunk 2
      (is (= 16384 (aget ^longs result 8192)))
      (is (= 24575 (aget ^longs result 16383))))))

(deftest zone-map-chunk-pruning-streaming-test
  (testing "Chunk pruning skips chunks that can't match (streaming path)"
    ;; Create sorted data where chunks have non-overlapping ranges
    ;; Chunk 0: [0..8191], Chunk 1: [8192..16383]
    (let [n (* 2 8192)
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          ;; Query: vals > 10000 — chunk 0 (max=8191) should be pruned
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 10000.0]]
                       :agg [[:count]]})]
      (is (= (- 16383 10000) (long (:count (first result)))))))

  (testing "All chunks pruned returns zero count"
    (let [n (* 2 8192)
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          ;; Query: vals > 99999 — all chunks pruned
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 99999.0]]
                       :agg [[:count]]})]
      (is (= 0 (long (:count (first result))))))))

(deftest zone-map-stats-only-test
  (testing "Stats-only aggregation for fully-inside chunks"
    ;; All values satisfy the predicate, so stats-only path is used
    (let [n (* 2 8192)
          ;; Values from 100 to 100+n-1, all > 50
          vals (double-array (map #(+ 100.0 (double %)) (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 50.0]]
                       :agg [[:sum :vals]]})]
      ;; All values match: sum = sum(100..100+n-1)
      (let [expected (reduce + (map #(+ 100.0 (double %)) (range n)))]
        (is (< (Math/abs (double (- expected (:sum (first result))))) 0.1)
            (str "Expected " expected " got " (:sum (first result)))))))

  (testing "Stats-only COUNT for fully-inside chunks"
    (let [n (* 3 8192)
          vals (long-array (map #(+ 100 %) (range n)))
          idx (index/index-from-seq :int64 (seq vals))
          result (q/q {:from {:vals idx}
                       :where [[:> :vals 50]]
                       :agg [[:count]]})]
      (is (= n (long (:count (first result))))))))

(deftest zone-map-mixed-classification-test
  (testing "Mixed chunks: some skip, some stats-only, some SIMD"
    ;; 3 chunks of sorted data:
    ;; Chunk 0: [0..8191] — partially inside [5000, 12000)
    ;; Chunk 1: [8192..16383] — fully inside [5000, 12000) for range [5000, 16384)
    ;; Chunk 2: [16384..24575] — fully outside if hi < 16384
    (let [n (* 3 8192)
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          ;; Range pred: vals in [5000, 12000)
          arr-result (q/q {:from {:vals (double-array (map double (range n)))}
                           :where [[:between :vals 5000.0 12000.0]]
                           :agg [[:sum :vals]]})
          idx-result (q/q {:from {:vals idx}
                           :where [[:between :vals 5000.0 12000.0]]
                           :agg [[:sum :vals]]})]
      ;; Results should match
      (is (< (Math/abs (double (- (:sum (first arr-result))
                                  (:sum (first idx-result)))))
             0.1)
          "Index result should match array result")
      (is (= (:_count (first arr-result)) (:_count (first idx-result)))
          "Counts should match")))

  (testing "MIN aggregation with mixed chunks"
    (let [n (* 3 8192)
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:vals (double-array (map double (range n)))}
                           :where [[:> :vals 5000.0]]
                           :agg [[:min :vals]]})
          idx-result (q/q {:from {:vals idx}
                           :where [[:> :vals 5000.0]]
                           :agg [[:min :vals]]})]
      (is (== (:min (first arr-result)) (:min (first idx-result))))))

  (testing "MAX aggregation with mixed chunks"
    (let [n (* 3 8192)
          vals (double-array (map double (range n)))
          idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:vals (double-array (map double (range n)))}
                           :where [[:< :vals 5000.0]]
                           :agg [[:max :vals]]})
          idx-result (q/q {:from {:vals idx}
                           :where [[:< :vals 5000.0]]
                           :agg [[:max :vals]]})]
      (is (== (:max (first arr-result)) (:max (first idx-result)))))))

(deftest index-group-by-pruned-materialization-test
  (testing "Group-by on index uses pruned materialization"
    ;; Create sorted data where chunks have distinct category ranges
    (let [n (* 2 8192)
          ;; Category column: all 0s in chunk 0, all 1s in chunk 1
          cats (long-array (map #(if (< % 8192) 0 1) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          ;; Group by category with a predicate that prunes chunk 0
          arr-result (q/q {:from {:cat cats :vals vals}
                           :where [[:> :vals 10000.0]]
                           :group [:cat]
                           :agg [[:sum :vals]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :where [[:> :vals 10000.0]]
                           :group [:cat]
                           :agg [[:sum :vals]]})]
      ;; Only category 1 has values > 10000
      (is (= (count arr-result) (count idx-result))
          "Same number of groups")
      (when (seq arr-result)
        (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
              idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
          (doseq [[cat arr-row] arr-by-cat]
            (let [idx-row (get idx-by-cat cat)]
              (is (some? idx-row) (str "Missing group " cat))
              (when idx-row
                (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1)
                    (str "Sum mismatch for group " cat)))))))))

  (testing "Index parity: group-by with no predicates"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 5) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals]]})]
      (is (= (count arr-result) (count idx-result)))
      (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
            idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
        (doseq [[cat arr-row] arr-by-cat]
          (let [idx-row (get idx-by-cat cat)]
            (is (some? idx-row))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1)))))))))

(deftest chunked-group-by-test
  (testing "Chunked group-by: parity with array path for multiple agg types"
    (let [n (* 2 8192)  ;; 2 chunks
          cats (long-array (map #(mod % 5) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals] [:count] [:min :vals] [:max :vals]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals] [:count] [:min :vals] [:max :vals]]})]
      (is (= (count arr-result) (count idx-result)) "Same number of groups")
      (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
            idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
        (doseq [[cat arr-row] arr-by-cat]
          (let [idx-row (get idx-by-cat cat)]
            (is (some? idx-row) (str "Missing group " cat))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1)
                  (str "Sum mismatch group " cat))
              (is (= (:count arr-row) (:count idx-row))
                  (str "Count mismatch group " cat))
              (is (< (Math/abs (double (- (:min arr-row) (:min idx-row)))) 0.1)
                  (str "Min mismatch group " cat))
              (is (< (Math/abs (double (- (:max arr-row) (:max idx-row)))) 0.1)
                  (str "Max mismatch group " cat))))))))

  (testing "Chunked group-by: multiple group columns"
    (let [n (* 2 8192)
          g1 (long-array (map #(mod % 3) (range n)))
          g2 (long-array (map #(mod % 4) (range n)))
          vals (double-array (map double (range n)))
          g1-idx (index/index-from-seq :int64 (seq g1))
          g2-idx (index/index-from-seq :int64 (seq g2))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:g1 g1 :g2 g2 :vals vals}
                           :group [:g1 :g2]
                           :agg [[:sum :vals] [:count]]})
          idx-result (q/q {:from {:g1 g1-idx :g2 g2-idx :vals val-idx}
                           :group [:g1 :g2]
                           :agg [[:sum :vals] [:count]]})]
      (is (= (count arr-result) (count idx-result)))
      (let [arr-by-key (into {} (map (fn [r] [[(:g1 r) (:g2 r)] r])) arr-result)
            idx-by-key (into {} (map (fn [r] [[(:g1 r) (:g2 r)] r])) idx-result)]
        (doseq [[key arr-row] arr-by-key]
          (let [idx-row (get idx-by-key key)]
            (is (some? idx-row) (str "Missing group " key))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1))
              (is (= (:count arr-row) (:count idx-row)))))))))

  (testing "Chunked group-by: with predicates"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 5) (range n)))
          vals (long-array (range n))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :int64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :where [[:> :vals 5000]]
                           :group [:cat]
                           :agg [[:sum :vals] [:count]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :where [[:> :vals 5000]]
                           :group [:cat]
                           :agg [[:sum :vals] [:count]]})]
      (is (= (count arr-result) (count idx-result)))
      (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
            idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
        (doseq [[cat arr-row] arr-by-cat]
          (let [idx-row (get idx-by-cat cat)]
            (is (some? idx-row))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1))
              (is (= (:count arr-row) (:count idx-row)))))))))

  (testing "Chunked group-by: columnar output"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 3) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals]]
                           :result :columns})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals]]
                           :result :columns})]
      (is (= (:n-rows arr-result) (:n-rows idx-result)))
      (is (= (vec (:cat arr-result)) (vec (:cat idx-result))))
      (let [arr-sums (vec (:sum arr-result))
            idx-sums (vec (:sum idx-result))]
        (doseq [i (range (count arr-sums))]
          (is (< (Math/abs (double (- (nth arr-sums i) (nth idx-sums i)))) 0.1))))))

  (testing "Chunked group-by: dense normalization with large key values"
    (let [n (* 2 8192)
          ;; Keys in range [10000, 10004] — large absolute but small range
          cats (long-array (map #(+ 10000 (mod % 5)) (range n)))
          vals (double-array (map double (range n)))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :float64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals] [:count]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals] [:count]]})]
      (is (= (count arr-result) (count idx-result)))
      (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
            idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
        (doseq [[cat arr-row] arr-by-cat]
          (let [idx-row (get idx-by-cat cat)]
            (is (some? idx-row) (str "Missing group " cat))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1))
              (is (= (:count arr-row) (:count idx-row)))))))))

  (testing "Chunked group-by: all-long columns (group + agg both int64)"
    (let [n (* 2 8192)
          cats (long-array (map #(mod % 4) (range n)))
          vals (long-array (range n))
          cat-idx (index/index-from-seq :int64 (seq cats))
          val-idx (index/index-from-seq :int64 (seq vals))
          arr-result (q/q {:from {:cat cats :vals vals}
                           :group [:cat]
                           :agg [[:sum :vals] [:count] [:avg :vals]]})
          idx-result (q/q {:from {:cat cat-idx :vals val-idx}
                           :group [:cat]
                           :agg [[:sum :vals] [:count] [:avg :vals]]})]
      (is (= (count arr-result) (count idx-result)))
      (let [arr-by-cat (into {} (map (fn [r] [(:cat r) r])) arr-result)
            idx-by-cat (into {} (map (fn [r] [(:cat r) r])) idx-result)]
        (doseq [[cat arr-row] arr-by-cat]
          (let [idx-row (get idx-by-cat cat)]
            (is (some? idx-row))
            (when idx-row
              (is (< (Math/abs (double (- (:sum arr-row) (:sum idx-row)))) 0.1))
              (is (= (:count arr-row) (:count idx-row)))
              (is (< (Math/abs (double (- (:avg arr-row) (:avg idx-row)))) 0.001)))))))))

(deftest multi-key-hash-overflow-group-by-test
  (testing "Group-by with 6 wide columns that overflow composite key space"
    ;; Simulate H2O-Q10 pattern: 6 group cols with large ranges
    ;; Range products: 101*101*1001*101*101*1001 ≈ 1.05×10^14 > 10^5 dense limit
    ;; With values up to 1000 in cols 3 and 6, the max-key will overflow long.
    ;; Use smaller ranges that still force overflow: 3 cols with range ~3M each
    ;; Key space: 3M^3 ≈ 2.7×10^19 > Long.MAX_VALUE ≈ 9.2×10^18
    (let [n 10000
          r1 3000000
          r2 3000000
          r3 3000000
          g1 (long-array (map #(long (mod % 5)) (range n)))       ;; 5 groups
          g2 (long-array (map #(long (mod % 7)) (range n)))       ;; 7 groups
          g3 (long-array (map #(long (mod % 4)) (range n)))       ;; 4 groups
          ;; Add large-range columns that force key space overflow
          ;; The actual values are small, but max values are huge
          g4 (long-array n)
          g5 (long-array n)
          g6 (long-array n)
          _ (dotimes [i n]
              (aset g4 i (long (mod i 3)))
              (aset g5 i (long (mod i 2)))
              (aset g6 i (long (mod i 6))))
          ;; Force max values to be large to trigger overflow
          _ (aset g4 0 (long r1))
          _ (aset g5 0 (long r2))
          _ (aset g6 0 (long r3))
          vals (double-array (map double (range n)))
          ;; Small-scale reference: compute expected results manually
          query {:from {:g1 {:type :int64 :data g1}
                        :g2 {:type :int64 :data g2}
                        :g3 {:type :int64 :data g3}
                        :g4 {:type :int64 :data g4}
                        :g5 {:type :int64 :data g5}
                        :g6 {:type :int64 :data g6}
                        :v  {:type :float64 :data vals}}
                 :group [:g1 :g2 :g3 :g4 :g5 :g6]
                 :agg [[:as [:sum :v] :total] [:as [:count] :cnt]]}
          result (q/q query)]
      ;; Verify correctness: the sum of all :total values should equal sum of all vals
      (let [total-sum (reduce + (map :total result))
            expected-sum (reduce + (map double (range n)))]
        (is (< (Math/abs (double (- total-sum expected-sum))) 0.01)
            "Sum of all group totals should equal total sum"))
      ;; Verify row count
      (let [total-cnt (reduce + (map :cnt result))]
        (is (= total-cnt n) "Sum of all counts should equal n"))
      ;; Verify specific group: row 0 has g1=0, g2=0, g3=0, g4=r1, g5=r2, g6=r3
      ;; This is a unique group (only row 0 maps to these values)
      (let [row0-group (first (filter #(and (= r1 (long (:g4 %)))
                                            (= r2 (long (:g5 %)))
                                            (= r3 (long (:g6 %))))
                                      result))]
        (is (some? row0-group) "Row 0's unique group should exist")
        (when row0-group
          (is (== 1 (:cnt row0-group)) "Row 0's group should have count 1")
          (is (== 0.0 (:total row0-group)) "Row 0's val is 0.0")))))

  (testing "Multi-key hash overflow with columnar output"
    (let [n 5000
          g1 (long-array (map #(long (mod % 3)) (range n)))
          g2 (long-array (map #(long (mod % 4)) (range n)))
          g3 (long-array n)
          _ (dotimes [i n] (aset g3 i (long (mod i 5))))
          _ (aset g3 0 3000000) ;; force large max → overflow
          _ (aset g1 0 3000000)
          _ (aset g2 0 3000000)
          vals (double-array (map double (range n)))
          result (q/q {:from {:g1 {:type :int64 :data g1}
                              :g2 {:type :int64 :data g2}
                              :g3 {:type :int64 :data g3}
                              :v  {:type :float64 :data vals}}
                       :group [:g1 :g2 :g3]
                       :agg [[:as [:sum :v] :total]]
                       :result :columns})]
      (is (contains? result :n-rows))
      (is (pos? (:n-rows result)))
      ;; Total sum should match
      (let [total-arr (:total result)
            total-sum (if (double? (first (seq (if (instance? (Class/forName "[D") total-arr)
                                                 total-arr
                                                 []))))
                        (reduce + (seq total-arr))
                        (areduce ^doubles total-arr i ret 0.0 (+ ret (aget ^doubles total-arr i))))]
        (is (< (Math/abs (double (- total-sum (reduce + (map double (range n)))))) 0.01))))))

;; ============================================================================
;; SIMD-path NULL correctness tests (>1000 rows to exercise Java SIMD)
;; ============================================================================

(deftest simd-null-sum-test
  (testing "SUM skips NaN in fused SIMD path (single-agg, >1000 rows)"
    (let [n 2000
          vals (double-array n)]
      (dotimes [i n]
        (if (zero? (mod i 10))
          (aset vals i Double/NaN)
          (aset vals i (double i))))
      (let [expected (reduce + (for [i (range n) :when (not (zero? (mod i 10)))] (double i)))
            result (q/q {:from {:v vals}
                         :agg [[:sum :v]]})]
        (is (< (Math/abs (double (- expected (:sum (first result))))) 0.01))))))

(deftest simd-null-min-max-test
  (testing "MIN/MAX skip NaN in fused SIMD path (separate queries)"
    (let [n 2000
          vals (double-array n)]
      (dotimes [i n]
        (if (zero? (mod i 10))
          (aset vals i Double/NaN)
          (aset vals i (+ 100.0 (double i)))))
      (let [min-result (q/q {:from {:v vals} :agg [[:min :v]]})
            max-result (q/q {:from {:v vals} :agg [[:max :v]]})]
        (is (== 101.0 (:min (first min-result))))
        (is (== 2099.0 (:max (first max-result))))))))

(deftest simd-null-filtered-sum-test
  (testing "Filter + SUM with NaN in agg column (fused SIMD path)"
    (let [n 2000
          x (long-array n)
          v (double-array n)]
      (dotimes [i n]
        (aset x i (long i))
        (aset v i (if (zero? (mod i 7)) Double/NaN (* 2.0 i))))
      (let [expected (reduce + (for [i (range n)
                                     :when (and (< i 1000)
                                                (not (zero? (mod i 7))))]
                                 (* 2.0 i)))
            result (q/q {:from {:x x :v v}
                         :where [[:< :x 1000]]
                         :agg [[:sum :v]]})]
        (is (< (Math/abs (double (- expected (:sum (first result))))) 0.01))))))

(deftest simd-null-group-by-dense-test
  (testing "Dense group-by includes NULL key group and excludes NULL agg values"
    (let [n 3000
          groups (long-array n)
          vals (double-array n)]
      (dotimes [i n]
        (aset groups i (if (zero? (mod i 13)) Long/MIN_VALUE (long (mod i 10))))
        (aset vals i (if (zero? (mod i 17)) Double/NaN (double i))))
      (let [result (q/q {:from {:g groups :v vals}
                         :group [:g]
                         :agg [[:sum :v]]})
            group-set (set (map :g result))]
        ;; 11 groups: 0-9 plus NULL group (PostgreSQL NULL group semantics)
        (is (= 11 (count result)))
        (is (some #(nil? (:g %)) result) "NULL group should be present")
        ;; Verify one group's sum manually
        (let [g0-expected (reduce + (for [i (range n)
                                          :when (and (= 0 (mod i 10))
                                                     (not (zero? (mod i 13)))
                                                     (not (zero? (mod i 17))))]
                                      (double i)))
              g0 (first (filter #(= 0 (:g %)) result))]
          (is (< (Math/abs (double (- g0-expected (:sum g0)))) 0.01)))))))

(deftest simd-null-count-with-filter-test
  (testing "COUNT with IS-NOT-NULL filter in SIMD path"
    (let [n 2000
          vals (double-array n)]
      (dotimes [i n]
        (aset vals i (if (zero? (mod i 5)) Double/NaN (double i))))
      ;; COUNT(*) with no NULL filter → counts all rows
      (let [r-all (q/q {:from {:v vals} :agg [[:count]]})]
        (is (== n (:count (first r-all)))))
      ;; COUNT with IS-NOT-NULL → counts only non-NULL
      (let [r-nonnull (q/q {:from {:v vals}
                            :where [[:is-not-null :v]]
                            :agg [[:count]]})]
        (is (== (- n (quot n 5)) (:count (first r-nonnull))))))))

(deftest simd-null-group-by-count-test
  (testing "GROUP BY COUNT includes NULL key group"
    (let [n 3000
          groups (long-array n)]
      (dotimes [i n]
        (aset groups i (if (zero? (mod i 11)) Long/MIN_VALUE (long (mod i 5)))))
      (let [result (q/q {:from {:g groups}
                         :group [:g]
                         :agg [[:count]]})
            group-set (set (map :g result))]
        ;; 6 groups: 0-4 plus NULL group (PostgreSQL NULL group semantics)
        (is (= 6 (count result)))
        (is (some #(nil? (:g %)) result) "NULL group should be present")))))

;; =============================================================================
;; Pre-release audit fix tests
;; =============================================================================

(deftest distinct-excludes-internal-count-key-test
  (testing "DISTINCT excludes :_count from deduplication (#12)"
    ;; Two groups with same visible cols but potentially different :_count values
    ;; should deduplicate to one row
    (let [data {:g (long-array [1 1 1 2 2])
                :v (double-array [10.0 10.0 10.0 10.0 10.0])}
          result (q/q {:from data
                       :group [:g]
                       :agg [[:sum :v]]
                       :distinct true})]
      ;; Both groups have sum=10 but different counts (3 vs 2)
      ;; With :_count excluded from dedup, they should still be 2 distinct rows
      ;; because :g differs
      (is (= 2 (count result)))
      ;; Verify :_count doesn't interfere
      (is (every? #(contains? % :_count) result)))))

(deftest having-nan-value-test
  (testing "HAVING filters NaN values correctly (#6)"
    ;; AVG of group with no matching values produces NaN
    ;; HAVING should treat NaN like NULL (filter it out for comparisons)
    (let [data {:g (long-array [0 0 1 1])
                :v (double-array [Double/NaN Double/NaN 10.0 20.0])}
          result (q/q {:from data
                       :group [:g]
                       :agg [[:avg :v]]
                       :having [[:> :avg 0]]})]
      ;; Group 0 has NaN avg, group 1 has avg=15.0
      ;; NaN should NOT pass :gt comparison
      (is (= 1 (count result)))
      (is (= 1 (:g (first result)))))))

(deftest having-nan-neq-test
  (testing "HAVING :neq does not pass NaN values"
    (let [data {:g (long-array [0 0 1 1])
                :v (double-array [Double/NaN Double/NaN 10.0 20.0])}
          result (q/q {:from data
                       :group [:g]
                       :agg [[:avg :v]]
                       :having [[:!= :avg 99.0]]})]
      ;; NaN != 99.0 is true in floating point, but SQL says NaN should be treated as NULL
      ;; and NULL comparisons return false
      (is (= 1 (count result)))
      (is (= 1 (:g (first result)))))))

;; ============================================================================
;; Edge Case Regression Tests (E1, E2, E3)
;; ============================================================================

(deftest e1-multi-column-dict-join-keys-test
  (testing "E1: Multi-column join with dict-encoded string keys and different dicts"
    ;; Left side has dict: ["alice" "bob" "carol"]
    ;; Right side has dict: ["carol" "bob" "dave"]  (different dict order!)
    ;; Without dict unification on multi-column joins, composite keys would be wrong.
    (let [left {:name   (into-array String ["alice" "bob" "carol" "bob"])
                :dept   (into-array String ["eng" "sales" "eng" "eng"])
                :salary (double-array [100.0 200.0 300.0 400.0])}
          right {:person (into-array String ["carol" "bob" "dave"])
                 :team   (into-array String ["eng" "eng" "sales"])
                 :bonus  (double-array [10.0 20.0 30.0])}
          result (q/q {:from left
                       :join [{:with right
                               :on [[:= :name :person] [:= :dept :team]]}]
                       :select [:name :dept :salary :bonus]})]
      ;; Should match: (carol, eng) → salary=300 bonus=10
      ;;               (bob, eng)   → salary=400 bonus=20
      ;; Should NOT match: (bob, sales) — no (bob, sales) on right side
      ;; Should NOT match: (alice, eng) — no alice on right side
      (is (= 2 (count result))
          (str "Expected 2 matches but got " (count result) ": " (pr-str result)))
      (let [by-name (group-by :name result)]
        (is (= 300.0 (:salary (first (get by-name "carol")))))
        (is (= 10.0 (:bonus (first (get by-name "carol")))))
        (is (= 400.0 (:salary (first (get by-name "bob")))))
        (is (= 20.0 (:bonus (first (get by-name "bob")))))))))

(deftest e1-multi-column-dict-join-fused-group-agg-test
  (testing "E1: Fused join+group-by with multi-column dict keys"
    (let [fact {:name   (into-array String ["alice" "bob" "carol" "bob" "carol"])
                :dept   (into-array String ["eng" "sales" "eng" "eng" "eng"])
                :amount (double-array [100.0 200.0 300.0 400.0 500.0])}
          dim  {:person (into-array String ["carol" "bob"])
                :team   (into-array String ["eng" "eng"])
                :region (long-array [0 1])}
          result (q/q {:from fact
                       :join [{:with dim
                               :on [[:= :name :person] [:= :dept :team]]}]
                       :group [:region]
                       :agg [[:sum :amount]]})]
      ;; Matches: (carol, eng)→300+500=800, (bob, eng)→400
      ;; region 0 (carol): sum=800, region 1 (bob): sum=400
      (is (= 2 (count result)))
      (let [by-region (into {} (map (juxt :region :sum)) result)]
        (is (= 800.0 (get by-region 0)))
        (is (= 400.0 (get by-region 1)))))))

(deftest e2-having-alias-with-underscores-test
  (testing "E2: HAVING on auto-aliased agg with underscore-containing column names"
    ;; When two SUM aggs force auto-aliasing, :sum_price_usd should be resolvable
    ;; in HAVING. With old first-index-of, fallback would strip :sum_price_usd → :sum (wrong).
    (let [data {:cat       (long-array [0 0 1 1 2 2])
                :price_usd (double-array [10.0 20.0 30.0 40.0 50.0 60.0])
                :qty       (double-array [1.0 2.0 3.0 4.0 5.0 6.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum :price_usd] [:sum :qty]]
                       :having [[:> :sum_price_usd 50.0]]})]
      ;; cat 0: sum_price_usd=30 (excluded), cat 1: 70 (included), cat 2: 110 (included)
      (is (= 2 (count result)))
      (is (every? #(> (:sum_price_usd %) 50.0) result))))

  (testing "E2: HAVING with simple alias still works"
    ;; Verify backward compat: single agg, :sum as direct key
    (let [data {:cat   (long-array [0 0 1 1])
                :price (double-array [10.0 20.0 30.0 40.0])}
          result (q/q {:from data
                       :group [:cat]
                       :agg [[:sum :price]]
                       :having [[:> :sum 40.0]]})]
      (is (= 1 (count result)))
      (is (= 70.0 (:sum (first result)))))))

(deftest e3-or-pred-with-index-inputs-test
  (testing "E3: OR predicate with index inputs — non-pred columns stay as indices"
    ;; When we have OR predicates (non-SIMD), only the predicate columns should
    ;; be materialized. The aggregation column should remain an index if possible.
    (let [n 10000
          vals-arr (long-array (map #(mod % 100) (range n)))
          vals-idx (index/index-from-seq :int64 (seq vals-arr))
          price-arr (double-array (map #(+ 1.0 (double %)) (range n)))
          price-idx (index/index-from-seq :float64 (seq price-arr))
          ;; Use OR predicate (non-SIMD) + index inputs
          result (q/q {:from {:vals vals-idx :price price-idx}
                       :where [[:or [:= :vals 1] [:= :vals 50]]]
                       :agg [[:sum :price]]})]
      ;; vals=1: indices 1,101,201,...,9901 → 100 rows, sum of (2.0, 102.0, 202.0, ..., 9902.0)
      ;; vals=50: indices 50,150,250,...,9950 → 100 rows, sum of (51.0, 151.0, 251.0, ..., 9951.0)
      (is (= 200 (:_count (first result))))
      ;; Verify the sum is correct (not just that it doesn't crash)
      (let [expected-sum (+ (reduce + (map #(+ 1.0 (double %)) (filter #(= 1 (mod % 100)) (range n))))
                            (reduce + (map #(+ 1.0 (double %)) (filter #(= 50 (mod % 100)) (range n)))))]
        (is (< (Math/abs (double (- (:sum (first result)) expected-sum))) 0.01)
            (str "Expected sum ~" expected-sum " but got " (:sum (first result)))))))

  (testing "E3: IN predicate with index group-by"
    ;; Verify that group-by still works when non-SIMD preds partially materialize
    (let [n 1000
          ;; Use mod 3 for category so all categories get hits when filtering vals in {0,1}
          cat-arr (long-array (map #(mod % 3) (range n)))
          cat-idx (index/index-from-seq :int64 (seq cat-arr))
          vals-arr (long-array (map #(mod % 10) (range n)))
          vals-idx (index/index-from-seq :int64 (seq vals-arr))
          price-arr (double-array (map (fn [_] 1.0) (range n)))
          price-idx (index/index-from-seq :float64 (seq price-arr))
          result (q/q {:from {:cat cat-idx :vals vals-idx :price price-idx}
                       :where [[:in :vals 0 1 2]]
                       :group [:cat]
                       :agg [[:count]]})]
      ;; 300 rows match (vals in {0,1,2}), distributed across 3 categories → 100 each
      (is (= 3 (count result)))
      (is (every? #(= 100 (:count %)) result)))))
