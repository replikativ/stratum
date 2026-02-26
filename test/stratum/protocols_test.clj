(ns stratum.protocols-test
  "Tests for Clojure standard protocol implementations on PersistentColumnIndex."
  (:require [clojure.test :refer [deftest testing is are]]
            [stratum.index :as index]))

;; ============================================================================
;; Counted Protocol Tests
;; ============================================================================

(deftest counted-protocol-test
  (testing "count works on index"
    (let [idx (index/index-from-seq :float64 (range 100))]
      (is (= 100 (count idx)))
      (is (= 100 (index/idx-length idx)))))

  (testing "count on empty index"
    (let [idx (index/make-index :float64)]
      (is (= 0 (count idx))))))

;; ============================================================================
;; Indexed Protocol Tests
;; ============================================================================

(deftest indexed-protocol-test
  (testing "nth on index"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0 40.0 50.0])]
      (is (= 10.0 (nth idx 0)))
      (is (= 30.0 (nth idx 2)))
      (is (= 50.0 (nth idx 4)))))

  (testing "nth with not-found"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0])]
      (is (= ::not-found (nth idx -1 ::not-found)))
      (is (= ::not-found (nth idx 100 ::not-found)))
      (is (= 20.0 (nth idx 1 ::not-found)))))

  (testing "nth throws on out of bounds without default"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0])]
      (is (thrown? IndexOutOfBoundsException (nth idx -1)))
      (is (thrown? IndexOutOfBoundsException (nth idx 100))))))

;; ============================================================================
;; ILookup Protocol Tests
;; ============================================================================

(deftest ilookup-protocol-test
  (testing "get on index with integer keys"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0 40.0 50.0])]
      (is (= 10.0 (get idx 0)))
      (is (= 30.0 (get idx 2)))
      (is (= 50.0 (get idx 4)))))

  (testing "get returns nil for out of bounds"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0])]
      (is (nil? (get idx -1)))
      (is (nil? (get idx 100)))))

  (testing "get with not-found"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0])]
      (is (= ::not-found (get idx -1 ::not-found)))
      (is (= ::not-found (get idx 100 ::not-found)))
      (is (= 20.0 (get idx 1 ::not-found)))))

  (testing "get with non-integer keys returns nil"
    (let [idx (index/index-from-seq :float64 [10.0 20.0 30.0])]
      (is (nil? (get idx :foo)))
      (is (nil? (get idx "bar")))
      (is (= ::not-found (get idx :foo ::not-found))))))

;; ============================================================================
;; IReduce Protocol Tests
;; ============================================================================

(deftest ireduce-protocol-test
  (testing "reduce without init"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])]
      (is (= 15.0 (reduce + idx)))
      (is (= 120.0 (reduce * idx)))))

  (testing "reduce with init"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])]
      (is (= 115.0 (reduce + 100.0 idx)))
      (is (= 15.0 (reduce + 0.0 idx)))))

  (testing "reduce with long array"
    (let [idx (index/index-from-seq :int64 [1 2 3 4 5])]
      (is (= 15 (reduce + idx)))
      (is (= 120 (reduce * idx)))))

  (testing "reduce with early termination (reduced)"
    (let [idx (index/index-from-seq :float64 (range 1000))]
      (is (= 10.0 (reduce (fn [acc x]
                            (if (>= x 10.0)
                              (reduced x)
                              (+ acc x)))
                          0.0
                          idx)))))

  (testing "reduce processes chunks efficiently"
    ;; Large index spanning multiple chunks
    (let [idx (index/index-from-seq :float64 (range 10000))]
      (is (= 49995000.0 (reduce + 0.0 idx)))))

  (testing "reduce on empty index with init"
    (let [idx (index/make-index :float64)]
      (is (= 100.0 (reduce + 100.0 idx)))))

  (testing "reduce collects to vector"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0])]
      (is (= [1.0 2.0 3.0] (reduce conj [] idx))))))

;; ============================================================================
;; Reversible Protocol Tests
;; ============================================================================

(deftest reversible-protocol-test
  (testing "rseq reverses index"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])]
      (is (= [5.0 4.0 3.0 2.0 1.0] (vec (rseq idx))))))

  (testing "rseq on long index"
    (let [idx (index/index-from-seq :int64 [10 20 30 40 50])]
      (is (= [50 40 30 20 10] (vec (rseq idx))))))

  (testing "rseq on large index spanning multiple chunks"
    (let [idx (index/index-from-seq :float64 (range 100))]
      (is (= (vec (map double (reverse (range 100))))
             (vec (rseq idx))))))

  (testing "rseq on empty index"
    (let [idx (index/make-index :float64)]
      (is (empty? (rseq idx))))))

;; ============================================================================
;; Protocol Integration Tests
;; ============================================================================

(deftest protocol-integration-test
  (testing "Use index with Clojure sequence functions"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])]
      ;; count
      (is (= 5 (count idx)))

      ;; nth
      (is (= 3.0 (nth idx 2)))

      ;; get
      (is (= 4.0 (get idx 3)))

      ;; reduce
      (is (= 15.0 (reduce + idx)))

      ;; rseq
      (is (= [5.0 4.0 3.0 2.0 1.0] (vec (rseq idx))))))

  (testing "Protocols work after transient operations"
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient)]
      (dotimes [i 10]
        (index/idx-append! idx (double i)))
      (let [persistent-idx (index/idx-persistent! idx)]
        ;; All protocols should work on persistent index
        (is (= 10 (count persistent-idx)))
        (is (= 5.0 (nth persistent-idx 5)))
        (is (= 7.0 (get persistent-idx 7)))
        (is (= 45.0 (reduce + persistent-idx)))
        (is (= 9.0 (first (rseq persistent-idx))))))))
