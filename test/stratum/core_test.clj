(ns stratum.core-test
  "Tests for stratum columnar index implementation."
  (:require [clojure.test :refer [deftest testing is are]]
            [stratum.chunk :as chunk]
            [stratum.index :as index]
            [stratum.stats :as stats]))

;; ============================================================================
;; Chunk Tests
;; ============================================================================

(deftest chunk-creation-test
  (testing "Create chunk with different datatypes"
    (let [c-f64 (chunk/make-chunk :float64 10)
          c-i64 (chunk/make-chunk :int64 10)]
      (is (= 10 (chunk/chunk-length c-f64)))
      (is (= 10 (chunk/chunk-length c-i64)))
      (is (= :float64 (chunk/chunk-datatype c-f64)))
      (is (= :int64 (chunk/chunk-datatype c-i64))))))

(deftest chunk-read-write-test
  (testing "Write and read values"
    (let [c (-> (chunk/make-chunk :float64 10)
                chunk/col-transient)]
      (chunk/write-double! c 0 42.0)
      (chunk/write-double! c 5 3.14)
      (chunk/col-persistent! c)
      (is (= 42.0 (chunk/read-double c 0)))
      (is (= 3.14 (chunk/read-double c 5)))
      (is (= 0.0 (chunk/read-double c 1))))))

(deftest chunk-cow-test
  (testing "Copy-on-write semantics"
    (let [c1 (-> (chunk/make-chunk :float64 10)
                 chunk/col-transient)]
      (chunk/write-double! c1 0 42.0)
      (chunk/col-persistent! c1)

      ;; Fork and modify
      (let [c2 (-> (chunk/col-fork c1)
                   chunk/col-transient)]
        (chunk/write-double! c2 0 99.0)
        (chunk/col-persistent! c2)

        ;; Original should be unchanged
        (is (= 42.0 (chunk/read-double c1 0)))
        ;; Fork should have new value
        (is (= 99.0 (chunk/read-double c2 0)))))))

(deftest chunk-cow-reverse-mutation-test
  (testing "Mutating original after fork does not affect fork (#40)"
    (let [c1 (-> (chunk/make-chunk :float64 10)
                 chunk/col-transient)]
      (chunk/write-double! c1 0 42.0)
      (chunk/write-double! c1 1 7.0)
      (chunk/col-persistent! c1)

      ;; Fork (snapshot should be frozen)
      (let [c2 (chunk/col-fork c1)]
        ;; Mutate the ORIGINAL
        (chunk/col-transient c1)
        (chunk/write-double! c1 0 999.0)
        (chunk/col-persistent! c1)

        ;; Original should have new value
        (is (= 999.0 (chunk/read-double c1 0)))
        ;; Fork should still have the old value — isolation guarantee
        (is (= 42.0 (chunk/read-double c2 0)))
        (is (= 7.0 (chunk/read-double c2 1)))))))

(deftest chunk-cow-long-fork-isolation-test
  (testing "Fork isolation for int64 chunks"
    (let [c1 (-> (chunk/make-chunk :int64 10)
                 chunk/col-transient)]
      (chunk/write-long! c1 0 42)
      (chunk/col-persistent! c1)

      (let [c2 (chunk/col-fork c1)]
        (chunk/col-transient c1)
        (chunk/write-long! c1 0 999)
        (chunk/col-persistent! c1)

        (is (= 999 (chunk/read-long c1 0)))
        (is (= 42 (chunk/read-long c2 0)))))))

(deftest chunk-from-seq-test
  (testing "Create chunk from sequence"
    (let [c (chunk/chunk-from-seq :float64 [1.0 2.0 3.0 4.0 5.0])]
      (is (= 5 (chunk/chunk-length c)))
      (is (= 1.0 (chunk/read-double c 0)))
      (is (= 3.0 (chunk/read-double c 2)))
      (is (= 5.0 (chunk/read-double c 4))))))

(deftest chunk-transient-errors-test
  (testing "Mutation throws when not transient"
    (let [c (chunk/make-chunk :float64 10)]
      (is (thrown? IllegalStateException
                   (chunk/write-double! c 0 42.0))))))

;; ============================================================================
;; Index Tests
;; ============================================================================

(deftest index-creation-test
  (testing "Create empty index"
    (let [idx (index/make-index :float64)]
      (is (= 0 (index/idx-length idx)))
      (is (= :float64 (index/idx-datatype idx)))))

  (testing "Create index from sequence"
    (let [idx (index/index-from-seq :float64 (range 100))]
      (is (= 100 (index/idx-length idx)))
      (is (= 0.0 (index/idx-get-double idx 0)))
      (is (= 50.0 (index/idx-get-double idx 50)))
      (is (= 99.0 (index/idx-get-double idx 99))))))

(deftest index-multi-chunk-test
  (testing "Index spanning multiple chunks"
    (let [idx (index/index-from-seq :float64 (range 10000))]
      (is (= 10000 (index/idx-length idx)))
      (is (= 0.0 (index/idx-get-double idx 0)))
      (is (= 5000.0 (index/idx-get-double idx 5000)))
      (is (= 9999.0 (index/idx-get-double idx 9999))))))

(deftest index-bounds-test
  (testing "Bounds checking"
    (let [idx (index/index-from-seq :float64 (range 100))]
      (is (thrown? IndexOutOfBoundsException
                   (index/idx-get-double idx -1)))
      (is (thrown? IndexOutOfBoundsException
                   (index/idx-get-double idx 100))))))

(deftest index-fork-cow-test
  (testing "Index fork with copy-on-write"
    (let [idx1 (index/index-from-seq :float64 (range 100))
          idx2 (index/idx-fork idx1)
          idx3 (-> idx2
                   index/idx-transient
                   (index/idx-set! 0 999.0)
                   index/idx-persistent!)]
      ;; Original unchanged
      (is (= 0.0 (index/idx-get-double idx1 0)))
      ;; Fork modified
      (is (= 999.0 (index/idx-get-double idx3 0)))
      ;; Other values unchanged
      (is (= 50.0 (index/idx-get-double idx3 50))))))

(deftest index-append-test
  (testing "Append values to index"
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient
                  (index/idx-append! 1.0)
                  (index/idx-append! 2.0)
                  (index/idx-append! 3.0)
                  index/idx-persistent!)]
      (is (= 3 (index/idx-length idx)))
      (is (= 1.0 (index/idx-get-double idx 0)))
      (is (= 2.0 (index/idx-get-double idx 1)))
      (is (= 3.0 (index/idx-get-double idx 2))))))

(deftest index-transient-errors-test
  (testing "Transient operations throw when not transient"
    (let [idx (index/index-from-seq :float64 (range 100))]
      (is (thrown? IllegalStateException
                   (index/idx-set! idx 0 42.0)))
      (is (thrown? IllegalStateException
                   (index/idx-append! idx 42.0))))))

(deftest index-dirty-chunks-test
  (testing "Dirty chunk tracking for storage"
    ;; Create empty index and make transient
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient)]
      ;; Initially no dirty chunks for empty index
      (is (empty? (index/idx-dirty-chunks idx)))

      ;; Append a value - should mark chunk as dirty
      (let [idx2 (index/idx-append! idx 42.0)]
        (is (contains? (index/idx-dirty-chunks idx2) [0]))

        ;; Clear dirty and verify
        (index/idx-clear-dirty! idx2)
        (is (empty? (index/idx-dirty-chunks idx2)))

        ;; Modify again - should mark dirty again
        (let [idx3 (index/idx-set! idx2 0 99.0)]
          (is (contains? (index/idx-dirty-chunks idx3) [0])))))))

;; ============================================================================
;; Stats After Mutation Tests
;; ============================================================================

(deftest stats-after-append-single-chunk-test
  (testing "Stats are correct after transient append within a single chunk"
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient)]
      ;; Append 100 values: 0.0, 1.0, ..., 99.0
      (dotimes [i 100]
        (index/idx-append! idx (double i)))
      (let [idx (index/idx-persistent! idx)
            s (index/idx-stats idx)]
        (is (= 100 (:count s)))
        (is (== 4950.0 (:sum s)))  ;; sum(0..99) = 4950
        (is (== 0.0 (:min-val s)))
        (is (== 99.0 (:max-val s)))))))

(deftest stats-after-append-multi-chunk-test
  (testing "Stats are correct after transient append spanning multiple chunks"
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient)
          n 10000]
      (dotimes [i n]
        (index/idx-append! idx (double i)))
      (let [idx (index/idx-persistent! idx)
            s (index/idx-stats idx)]
        (is (= n (:count s)))
        ;; sum(0..9999) = 9999*10000/2 = 49995000
        (is (== 4.9995E7 (:sum s)))
        (is (== 0.0 (:min-val s)))
        (is (== 9999.0 (:max-val s)))))))

(deftest stats-after-append-long-column-test
  (testing "Stats are correct after transient append for int64 columns"
    (let [idx (-> (index/make-index :int64)
                  index/idx-transient)]
      (dotimes [i 200]
        (index/idx-append! idx (long i)))
      (let [idx (index/idx-persistent! idx)
            s (index/idx-stats idx)]
        (is (= 200 (:count s)))
        ;; sum(0..199) = 199*200/2 = 19900
        (is (== 19900.0 (:sum s)))
        (is (== 0.0 (:min-val s)))
        (is (== 199.0 (:max-val s)))))))

(deftest stats-after-set-then-append-test
  (testing "Stats remain correct after idx-set! followed by idx-append!"
    (let [idx (-> (index/make-index :float64)
                  index/idx-transient)]
      (dotimes [i 50]
        (index/idx-append! idx (double i)))
      ;; Overwrite element 0 from 0.0 to 100.0
      (index/idx-set! idx 0 100.0)
      ;; Append a few more
      (dotimes [i 10]
        (index/idx-append! idx (double (+ 50 i))))
      (let [idx (index/idx-persistent! idx)
            s (index/idx-stats idx)]
        (is (= 60 (:count s)))
        (is (= 100.0 (index/idx-get-double idx 0)))
        (is (= 59.0 (index/idx-get-double idx 59)))))))

;; ============================================================================
;; Integration Tests
;; ============================================================================

(deftest large-index-test
  (testing "Large index operations"
    (let [n 100000
          idx (index/index-from-seq :float64 (range n))]
      (is (= n (index/idx-length idx)))
      (is (= 0.0 (index/idx-get-double idx 0)))
      (is (= (double (dec n)) (index/idx-get-double idx (dec n))))
      ;; sum(0..n-1) = n*(n-1)/2
      (is (= (/ (* n (dec n)) 2.0) (index/idx-sum-stats idx))))))

;; ============================================================================
;; Statistics Tests
;; ============================================================================

(deftest chunk-stats-test
  (testing "Chunk statistics are computed correctly"
    (let [idx (index/index-from-seq :float64 (range 100))
          stats (index/idx-stats idx)]
      ;; count
      (is (= 100 (:count stats)))
      ;; sum = 0+1+...+99 = 99*100/2 = 4950
      (is (= 4950.0 (:sum stats)))
      ;; min/max
      (is (= 0.0 (:min-val stats)))
      (is (= 99.0 (:max-val stats)))
      ;; sum-sq = 0² + 1² + ... + 99² = 99*100*199/6 = 328350
      (is (= 328350.0 (:sum-sq stats))))))

(deftest stats-aggregations-test
  (testing "O(chunks) aggregations using statistics"
    (let [idx (index/index-from-seq :float64 (range 100))]
      ;; Sum
      (is (= 4950.0 (index/idx-sum-stats idx)))
      ;; Mean = 49.5
      (is (= 49.5 (index/idx-mean-stats idx)))
      ;; Min/Max
      (is (= 0.0 (index/idx-min-stats idx)))
      (is (= 99.0 (index/idx-max-stats idx)))
      ;; Variance and Stddev
      (is (number? (index/idx-variance-stats idx)))
      (is (number? (index/idx-stddev-stats idx))))))

(deftest stats-multi-chunk-test
  (testing "Statistics work correctly across multiple chunks"
    ;; Force multiple chunks with small chunk size
    (let [idx (index/index-from-seq :float64 (range 1000) {:chunk-size 100})
          stats (index/idx-stats idx)]
      ;; Should have 10 chunks
      (is (= 10 (count (index/idx-all-chunk-stats idx))))
      ;; Aggregated stats should be correct
      (is (= 1000 (:count stats)))
      ;; sum = 0+1+...+999 = 999*1000/2 = 499500
      (is (= 499500.0 (:sum stats)))
      (is (= 0.0 (:min-val stats)))
      (is (= 999.0 (:max-val stats))))))

(deftest zone-map-filter-test
  (testing "Zone map filtering skips irrelevant chunks"
    ;; Create index with distinct ranges per chunk
    (let [idx (index/index-from-seq :float64 (range 1000) {:chunk-size 100})]
      ;; Filter for values > 850 - should only scan last 2 chunks
      (let [result (index/idx-filter-gt idx 850.0)]
        (is (= 149 (count result)))  ;; 851-999 = 149 values
        (is (= 851 (first result)))
        (is (= 999 (last result))))

      ;; Filter for values < 50 - should only scan first chunk
      (let [result (index/idx-filter-lt idx 50.0)]
        (is (= 50 (count result)))  ;; 0-49 = 50 values
        (is (= 0 (first result)))
        (is (= 49 (last result))))

      ;; Filter for range [400, 600) - should scan ~2-3 chunks
      (let [result (index/idx-filter-range idx 400.0 600.0)]
        (is (= 200 (count result)))  ;; 400-599 = 200 values
        (is (= 400 (first result)))
        (is (= 599 (last result)))))))

(deftest range-aggregations-test
  (testing "Range aggregations with zone map optimization"
    ;; Create index with distinct ranges per chunk (sorted data)
    (let [idx (index/index-from-seq :float64 (range 1000) {:chunk-size 100})]
      ;; Sum of [200, 400) - chunks 2,3 fully inside, no partial overlap
      ;; 200+201+...+399 = (200+399)*200/2 = 59900
      (is (= 59900.0 (index/idx-sum-range idx 200.0 400.0)))

      ;; Count of [200, 400)
      (is (= 200 (index/idx-count-range idx 200.0 400.0)))

      ;; Mean of [200, 400) = 299.5
      (is (= 299.5 (index/idx-mean-range idx 200.0 400.0)))

      ;; Min/Max of [200, 400)
      (is (= 200.0 (index/idx-min-range idx 200.0 400.0)))
      (is (= 399.0 (index/idx-max-range idx 200.0 400.0)))

      ;; Partial range [150, 250) - overlaps chunks 1 and 2
      (is (= 100 (index/idx-count-range idx 150.0 250.0)))
      ;; 150+151+...+249 = (150+249)*100/2 = 19950
      (is (= 19950.0 (index/idx-sum-range idx 150.0 250.0)))

      ;; Range outside all data
      (is (= 0 (index/idx-count-range idx 2000.0 3000.0)))
      (is (= 0.0 (index/idx-sum-range idx 2000.0 3000.0))))))

;; ============================================================================
;; PSS Aggregate Statistics Tests
;; ============================================================================

(deftest pss-stats-integration-test
  (testing "PSS aggregate statistics are enabled and working"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      ;; idx-stats now uses PSS fast path automatically (O(log chunks))
      (let [stats (index/idx-stats idx)]
        (is (some? stats))
        (is (= 25000 (:count stats)))
        ;; sum = 0+1+...+24999 = 24999*25000/2 = 312487500
        (is (= 312487500.0 (:sum stats)))
        (is (= 0.0 (:min-val stats)))
        (is (= 24999.0 (:max-val stats)))))))

(deftest idx-stats-range-single-chunk-test
  (testing "idx-stats-range for single partial chunk"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      ;; Query within first chunk [0, 8192)
      (let [stats (index/idx-stats-range idx 4000 6000)]
        (is (= 2000 (:count stats)))
        ;; sum = 4000+4001+...+5999 = (4000+5999)*2000/2 = 9999000
        (is (= 9999000.0 (:sum stats)))
        (is (= 4000.0 (:min-val stats)))
        (is (= 5999.0 (:max-val stats)))))))

(deftest idx-stats-range-multiple-chunks-test
  (testing "idx-stats-range across multiple chunks with overhangs"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      ;; Query [4000, 20000) spans 3 chunks:
      ;; - First chunk [0, 8192): overhang [4000, 8192)
      ;; - Second chunk [8192, 16384): fully covered
      ;; - Third chunk [16384, 24576): overhang [16384, 20000)
      (let [stats (index/idx-stats-range idx 4000 20000)]
        (is (= 16000 (:count stats)))
        ;; sum = 4000+4001+...+19999 = (4000+19999)*16000/2 = 191992000
        (is (= 191992000.0 (:sum stats)))
        (is (= 4000.0 (:min-val stats)))
        (is (= 19999.0 (:max-val stats)))))))

(deftest idx-stats-range-exact-boundaries-test
  (testing "idx-stats-range with exact chunk boundaries"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      ;; Query [0, 8192) - exactly first chunk
      (let [stats (index/idx-stats-range idx 0 8192)]
        (is (= 8192 (:count stats)))
        ;; sum = 0+1+...+8191 = 8191*8192/2 = 33550336
        (is (= 33550336.0 (:sum stats)))
        (is (= 0.0 (:min-val stats)))
        (is (= 8191.0 (:max-val stats))))

      ;; Query [0, 16384) - exactly first two chunks
      (let [stats (index/idx-stats-range idx 0 16384)]
        (is (= 16384 (:count stats)))
        ;; sum = 0+1+...+16383 = 16383*16384/2 = 134209536
        (is (= 134209536.0 (:sum stats)))
        (is (= 0.0 (:min-val stats)))
        (is (= 16383.0 (:max-val stats)))))))

(deftest idx-stats-range-full-index-test
  (testing "idx-stats-range for entire index"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      (let [range-stats (index/idx-stats-range idx 0 25000)
            full-stats (index/idx-stats idx)]
        ;; Should match full index stats
        (is (= (:count full-stats) (:count range-stats)))
        (is (= (:sum full-stats) (:sum range-stats)))
        (is (= (:min-val full-stats) (:min-val range-stats)))
        (is (= (:max-val full-stats) (:max-val range-stats)))))))

(deftest idx-stats-range-edge-cases-test
  (testing "idx-stats-range edge cases"
    (let [idx (index/index-from-seq :float64 (range 25000))]
      ;; Single element range
      (let [stats (index/idx-stats-range idx 100 101)]
        (is (= 1 (:count stats)))
        (is (= 100.0 (:sum stats)))
        (is (= 100.0 (:min-val stats)))
        (is (= 100.0 (:max-val stats))))

      ;; Small range at start
      (let [stats (index/idx-stats-range idx 0 10)]
        (is (= 10 (:count stats)))
        (is (= 45.0 (:sum stats)))  ;; 0+1+...+9 = 45
        (is (= 0.0 (:min-val stats)))
        (is (= 9.0 (:max-val stats))))

      ;; Small range at end
      (let [stats (index/idx-stats-range idx 24990 25000)]
        (is (= 10 (:count stats)))
        ;; 24990+24991+...+24999 = (24990+24999)*10/2 = 249945
        (is (= 249945.0 (:sum stats)))
        (is (= 24990.0 (:min-val stats)))
        (is (= 24999.0 (:max-val stats)))))))

(deftest idx-stats-range-error-handling-test
  (testing "idx-stats-range error handling"
    (let [idx (index/index-from-seq :float64 (range 1000))]
      ;; Invalid range: start >= end
      (is (thrown? Exception (index/idx-stats-range idx 100 100)))
      (is (thrown? Exception (index/idx-stats-range idx 100 99)))

      ;; Out of bounds
      (is (thrown? Exception (index/idx-stats-range idx -1 100)))
      (is (thrown? Exception (index/idx-stats-range idx 0 1001))))))
