(ns stratum.persistent-transient-test
  "Comprehensive tests for persistent/transient API semantics"
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.index :as index]))

;; ============================================================================
;; Persistent Operations Tests
;; ============================================================================

(deftest persistent-idx-set-test
  (testing "Persistent idx-set returns new index, original unchanged"
    (let [idx1 (index/index-from-seq :float64 [1.0 2.0 3.0])
          idx2 (index/idx-set idx1 1 99.0)]
      (is (= 2.0 (index/idx-get-double idx1 1)) "Original unchanged")
      (is (= 99.0 (index/idx-get-double idx2 1)) "New index has modification")
      (is (not (identical? idx1 idx2)) "Different objects"))))

(deftest persistent-idx-insert-test
  (testing "Persistent idx-insert returns new index, original unchanged"
    (let [idx1 (index/index-from-seq :float64 [1.0 3.0 5.0])
          idx2 (index/idx-insert idx1 1 2.0)]
      (is (= 3 (index/idx-length idx1)) "Original unchanged length")
      (is (= 4 (index/idx-length idx2)) "New index has +1 length")
      (is (= 3.0 (index/idx-get-double idx1 1)) "Original value unchanged")
      (is (= 2.0 (index/idx-get-double idx2 1)) "New value inserted")
      (is (= 3.0 (index/idx-get-double idx2 2)) "Subsequent values shifted"))))

(deftest persistent-idx-delete-test
  (testing "Persistent idx-delete returns new index, original unchanged"
    (let [idx1 (index/index-from-seq :float64 [1.0 2.0 3.0 4.0])
          idx2 (index/idx-delete idx1 1)]
      (is (= 4 (index/idx-length idx1)) "Original unchanged length")
      (is (= 3 (index/idx-length idx2)) "New index has -1 length")
      (is (= 2.0 (index/idx-get-double idx1 1)) "Original value unchanged")
      (is (= 3.0 (index/idx-get-double idx2 1)) "Subsequent values shifted"))))

(deftest persistent-idx-append-test
  (testing "Persistent idx-append returns new index, original unchanged"
    (let [idx1 (index/index-from-seq :float64 [1.0 2.0 3.0])
          idx2 (index/idx-append idx1 4.0)]
      (is (= 3 (index/idx-length idx1)) "Original unchanged")
      (is (= 4 (index/idx-length idx2)) "New index has +1 length")
      (is (= 4.0 (index/idx-get-double idx2 3)) "Value appended"))))

;; ============================================================================
;; Transient Operations Tests
;; ============================================================================

(deftest transient-idx-set-test
  (testing "Transient idx-set! mutates in place, returns same object"
    (let [idx1 (-> (index/index-from-seq :float64 [1.0 2.0 3.0])
                   index/idx-transient)
          idx2 (index/idx-set! idx1 1 99.0)]
      (is (identical? idx1 idx2) "Same object returned")
      (is (= 99.0 (index/idx-get-double idx1 1)) "Original mutated")
      (is (= 99.0 (index/idx-get-double idx2 1)) "Both see same change"))))

(deftest transient-idx-insert-test
  (testing "Transient idx-insert! mutates in place, returns same object"
    (let [idx1 (-> (index/index-from-seq :float64 [1.0 3.0 5.0])
                   index/idx-transient)
          idx2 (index/idx-insert! idx1 1 2.0)]
      (is (identical? idx1 idx2) "Same object returned")
      (is (= 4 (index/idx-length idx1)) "Original mutated")
      (is (= 2.0 (index/idx-get-double idx1 1)) "Value inserted"))))

(deftest transient-idx-delete-test
  (testing "Transient idx-delete! mutates in place, returns same object"
    (let [idx1 (-> (index/index-from-seq :float64 [1.0 2.0 3.0 4.0])
                   index/idx-transient)
          idx2 (index/idx-delete! idx1 1)]
      (is (identical? idx1 idx2) "Same object returned")
      (is (= 3 (index/idx-length idx1)) "Original mutated")
      (is (= 3.0 (index/idx-get-double idx1 1)) "Subsequent values shifted"))))

(deftest transient-idx-append-test
  (testing "Transient idx-append! mutates in place, returns same object"
    (let [idx1 (-> (index/index-from-seq :float64 [1.0 2.0 3.0])
                   index/idx-transient)
          idx2 (index/idx-append! idx1 4.0)]
      (is (identical? idx1 idx2) "Same object returned")
      (is (= 4 (index/idx-length idx1)) "Original mutated")
      (is (= 4.0 (index/idx-get-double idx1 3)) "Value appended"))))

;; ============================================================================
;; Error Handling Tests
;; ============================================================================

(deftest transient-operations-require-transient-mode-test
  (testing "Transient operations throw when called on persistent index"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0])]
      (is (thrown? IllegalStateException (index/idx-set! idx 0 99.0)))
      (is (thrown? IllegalStateException (index/idx-insert! idx 0 99.0)))
      (is (thrown? IllegalStateException (index/idx-delete! idx 0)))
      (is (thrown? IllegalStateException (index/idx-append! idx 99.0))))))

(deftest persistent-after-persistent-throws-test
  (testing "idx-persistent! throws when called on persistent index"
    (let [idx (index/index-from-seq :float64 [1.0 2.0 3.0])]
      (is (thrown? IllegalStateException (index/idx-persistent! idx))))))

(deftest transient-after-transient-throws-test
  (testing "idx-transient throws when called on transient index"
    (let [idx (-> (index/index-from-seq :float64 [1.0 2.0 3.0])
                  index/idx-transient)]
      (is (thrown? IllegalStateException (index/idx-transient idx))))))

;; ============================================================================
;; Structural Sharing Tests
;; ============================================================================

(deftest structural-sharing-independent-indices-test
  (testing "Persistent operations create independent indices via structural sharing"
    (let [original (index/index-from-seq :float64 (range 100))
          fork1 (index/idx-set original 50 999.0)
          fork2 (index/idx-set original 50 888.0)]
      ;; All three are different objects
      (is (not (identical? original fork1)))
      (is (not (identical? original fork2)))
      (is (not (identical? fork1 fork2)))

      ;; Each has different value at position 50
      (is (= 50.0 (index/idx-get-double original 50)))
      (is (= 999.0 (index/idx-get-double fork1 50)))
      (is (= 888.0 (index/idx-get-double fork2 50)))

      ;; All have same values at other positions
      (is (= 0.0 (index/idx-get-double original 0)))
      (is (= 0.0 (index/idx-get-double fork1 0)))
      (is (= 0.0 (index/idx-get-double fork2 0))))))

(deftest transient-from-persistent-independence-test
  (testing "Transient created from persistent is independent"
    (let [persistent-idx (index/index-from-seq :float64 [1.0 2.0 3.0])
          transient-idx (index/idx-transient persistent-idx)
          modified-idx (index/idx-set! transient-idx 0 99.0)]
      (is (= 1.0 (index/idx-get-double persistent-idx 0)) "Original persistent unchanged")
      (is (= 99.0 (index/idx-get-double transient-idx 0)) "Transient mutated")
      (is (identical? transient-idx modified-idx) "Transient operations return same object"))))

;; ============================================================================
;; Mixed Operations Tests
;; ============================================================================

(deftest mixed-persistent-transient-workflow-test
  (testing "Can mix persistent and transient operations"
    ;; Start with persistent
    (let [idx1 (index/index-from-seq :float64 [1.0 2.0 3.0])

          ;; Use persistent operation
          idx2 (index/idx-append idx1 4.0)

          ;; Convert to transient, do batch ops
          idx3 (-> idx2
                   index/idx-transient
                   (index/idx-append! 5.0)
                   (index/idx-append! 6.0)
                   index/idx-persistent!)

          ;; Use persistent operation again
          idx4 (index/idx-set idx3 0 99.0)]

      ;; Original unchanged
      (is (= 3 (index/idx-length idx1)))
      (is (= 1.0 (index/idx-get-double idx1 0)))

      ;; After first persistent append
      (is (= 4 (index/idx-length idx2)))
      (is (= 4.0 (index/idx-get-double idx2 3)))

      ;; After transient batch ops
      (is (= 6 (index/idx-length idx3)))
      (is (= 6.0 (index/idx-get-double idx3 5)))

      ;; After final persistent set
      (is (= 6 (index/idx-length idx4)))
      (is (= 99.0 (index/idx-get-double idx4 0)))
      (is (= 1.0 (index/idx-get-double idx3 0)) "idx3 unchanged by persistent op"))))

;; ============================================================================
;; Thread Safety Tests (Conceptual)
;; ============================================================================

(deftest persistent-operations-thread-safe-test
  (testing "Persistent operations can be used from multiple threads"
    (let [idx (index/index-from-seq :float64 (range 100))
          futures (doall (for [i (range 10)]
                           (future (index/idx-set idx 50 (double i)))))]
      ;; All futures complete without error
      (doseq [f futures]
        (is (some? @f)))

      ;; Original unchanged
      (is (= 50.0 (index/idx-get-double idx 50))))))

(deftest transient-operations-not-thread-safe-test
  (testing "Transient operations should only be used single-threaded"
    ;; This is a documentation test - transients are NOT thread-safe
    ;; Users should not share transients across threads
    (let [idx (-> (index/index-from-seq :float64 (range 100))
                  index/idx-transient)
          result (index/idx-append! idx 999.0)]
      (is (identical? idx result))
      (is (= 101 (index/idx-length result))))))

;; ============================================================================
;; Performance Characteristics Tests
;; ============================================================================

(deftest transient-batch-operations-efficient-test
  (testing "Transient mode is efficient for batch operations"
    (let [n 1000
          ;; Transient batch ops
          idx-transient (-> (index/make-index :float64)
                            index/idx-transient
                            (#(reduce (fn [idx i]
                                        (index/idx-append! idx (double i)))
                                      %
                                      (range n)))
                            index/idx-persistent!)]
      (is (= n (index/idx-length idx-transient)))
      (is (= 0.0 (index/idx-get-double idx-transient 0)))
      (is (= 999.0 (index/idx-get-double idx-transient 999))))))
