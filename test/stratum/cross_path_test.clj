(ns stratum.cross-path-test
  "Cross-path consistency tests: verify that different execution paths
   (array vs index, dense vs hash, 1T vs NT, fused vs separate, etc.)
   produce identical results for the same logical query."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.query :as q]
            [stratum.index :as index])
  (:import [stratum.internal ColumnOps]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- approx=
  "Compare two doubles with tolerance for floating-point differences."
  ([a b] (approx= a b 1e-6))
  ([a b tol]
   (cond
     (and (nil? a) (nil? b)) true
     (or (nil? a) (nil? b)) false
     (and (Double/isNaN (double a)) (Double/isNaN (double b))) true
     (Double/isNaN (double a)) false
     (Double/isNaN (double b)) false
     :else (<= (Math/abs (- (double a) (double b)))
               (* tol (max 1.0 (Math/abs (double a)) (Math/abs (double b))))))))

(defn- results-match?
  "Compare two result sets (vectors of maps), allowing floating-point tolerance.
   Sorts by all keys to handle different orderings."
  [r1 r2]
  (let [sort-fn (fn [results]
                  (sort-by (fn [m] (mapv #(get m %) (sort (keys m)))) results))
        s1 (sort-fn r1)
        s2 (sort-fn r2)]
    (and (= (count s1) (count s2))
         (every? true?
                 (map (fn [m1 m2]
                        (and (= (set (keys m1)) (set (keys m2)))
                             (every? (fn [k]
                                       (let [v1 (get m1 k)
                                             v2 (get m2 k)]
                                         (if (and (number? v1) (number? v2))
                                           (approx= v1 v2)
                                           (= v1 v2))))
                                     (keys m1))))
                      s1 s2)))))

(defn- with-parallel-threshold
  "Run f with a specific parallel threshold, restoring default afterward."
  [threshold f]
  (let [default 200000]
    (try
      (ColumnOps/setParallelThreshold (int threshold))
      (f)
      (finally
        (ColumnOps/setParallelThreshold (int default))))))

;; ============================================================================
;; B1. Array vs Index — filter+aggregate
;; ============================================================================

(deftest b1-array-vs-index-filter-aggregate
  (testing "Array and Index inputs produce identical filter+aggregate results"
    (let [n 20000
          vals-arr (double-array (map #(+ 100.0 (* 0.5 %)) (range n)))
          cats-arr (long-array (map #(mod % 10) (range n)))
          vals-idx (index/index-from-seq :float64 (seq vals-arr))
          cats-idx (index/index-from-seq :int64 (seq cats-arr))]
      ;; SUM with predicate
      (let [q-map {:where [[:> :vals 5000.0]]
                   :agg [[:sum :vals]]}
            arr-result (q/q (assoc q-map :from {:vals vals-arr}))
            idx-result (q/q (assoc q-map :from {:vals vals-idx}))]
        (is (results-match? arr-result idx-result)
            "SUM with predicate: array vs index"))
      ;; COUNT with predicate
      (let [q-map {:where [[:< :vals 2000.0] [:> :vals 500.0]]
                   :agg [[:count]]}
            arr-result (q/q (assoc q-map :from {:vals vals-arr}))
            idx-result (q/q (assoc q-map :from {:vals vals-idx}))]
        (is (results-match? arr-result idx-result)
            "COUNT with predicate: array vs index"))
      ;; MIN/MAX
      (let [q-map {:where [[:> :vals 1000.0]]
                   :agg [[:min :vals] [:max :vals]]}
            arr-result (q/q (assoc q-map :from {:vals vals-arr}))
            idx-result (q/q (assoc q-map :from {:vals vals-idx}))]
        (is (results-match? arr-result idx-result)
            "MIN/MAX with predicate: array vs index"))
      ;; AVG
      (let [q-map {:agg [[:avg :vals]]}
            arr-result (q/q (assoc q-map :from {:vals vals-arr}))
            idx-result (q/q (assoc q-map :from {:vals vals-idx}))]
        (is (results-match? arr-result idx-result)
            "AVG: array vs index")))))

;; ============================================================================
;; B2. Dense vs Hash group-by
;; ============================================================================

(deftest b2-dense-vs-hash-group-by
  (testing "Dense and hash group-by produce identical results"
    (let [n 10000
          groups (long-array (map #(mod % 50) (range n)))
          vals   (double-array (map #(* 1.1 %) (range n)))]
      ;; SUM group-by
      (let [q-map {:from {:g groups :v vals}
                   :group [:g]
                   :agg [[:sum :v]]
                   :order [[:g :asc]]}
            dense-result (q/q q-map)
            hash-result  (binding [q/*dense-group-limit* 0]
                           (q/q q-map))]
        (is (results-match? dense-result hash-result)
            "SUM group-by: dense vs hash"))
      ;; COUNT + AVG group-by
      (let [q-map {:from {:g groups :v vals}
                   :group [:g]
                   :agg [[:count] [:avg :v]]
                   :order [[:g :asc]]}
            dense-result (q/q q-map)
            hash-result  (binding [q/*dense-group-limit* 0]
                           (q/q q-map))]
        (is (results-match? dense-result hash-result)
            "COUNT+AVG group-by: dense vs hash"))
      ;; MIN + MAX group-by
      (let [q-map {:from {:g groups :v vals}
                   :group [:g]
                   :agg [[:min :v] [:max :v]]
                   :order [[:g :asc]]}
            dense-result (q/q q-map)
            hash-result  (binding [q/*dense-group-limit* 0]
                           (q/q q-map))]
        (is (results-match? dense-result hash-result)
            "MIN+MAX group-by: dense vs hash")))))

;; ============================================================================
;; B3. 1T vs NT (parallel merge correctness)
;; ============================================================================

(deftest b3-single-thread-vs-parallel
  (testing "Single-threaded and parallel execution produce identical results"
    (let [n 500000
          vals   (double-array (map #(+ 1.0 (* 0.001 %)) (range n)))
          groups (long-array (map #(mod % 20) (range n)))]
      ;; Simple agg
      (let [q-map {:from {:v vals}
                   :where [[:> :v 100.0]]
                   :agg [[:sum :v] [:count] [:min :v] [:max :v]]}
            par-result (q/q q-map)
            seq-result (with-parallel-threshold Integer/MAX_VALUE
                         #(q/q q-map))]
        (is (results-match? par-result seq-result)
            "filter+agg: 1T vs NT"))
      ;; Group-by agg
      (let [q-map {:from {:g groups :v vals}
                   :group [:g]
                   :agg [[:sum :v] [:count] [:avg :v]]
                   :order [[:g :asc]]}
            par-result (q/q q-map)
            seq-result (with-parallel-threshold Integer/MAX_VALUE
                         #(q/q q-map))]
        (is (results-match? par-result seq-result)
            "group-by: 1T vs NT")))))

;; ============================================================================
;; B4. Mixed agg group-by — VARIANCE/CORR + MIN/MAX (regression test A1/A2)
;; ============================================================================

(defn- manual-stddev
  "Reference standard deviation (sample, N-1)."
  [xs]
  (let [n (count xs)]
    (if (<= n 1)
      Double/NaN
      (let [mean (/ (reduce + 0.0 xs) n)
            ss (reduce + 0.0 (map #(let [d (- % mean)] (* d d)) xs))]
        (Math/sqrt (/ ss (dec n)))))))

(defn- manual-corr
  "Reference Pearson correlation."
  [xs ys]
  (let [n (count xs)]
    (if (<= n 1)
      Double/NaN
      (let [mx (/ (reduce + 0.0 xs) n)
            my (/ (reduce + 0.0 ys) n)
            sxy (reduce + 0.0 (map #(* (- %1 mx) (- %2 my)) xs ys))
            sxx (reduce + 0.0 (map #(let [d (- % mx)] (* d d)) xs))
            syy (reduce + 0.0 (map #(let [d (- % my)] (* d d)) ys))]
        (if (or (zero? sxx) (zero? syy))
          Double/NaN
          (/ sxy (Math/sqrt (* sxx syy))))))))

(deftest b4-mixed-var-min-max-group-by
  (testing "STDDEV + MIN/MAX group-by produces correct results (regression A1/A2)"
    (let [n 300000
          groups-arr (long-array (map #(mod % 10) (range n)))
          vals-arr   (double-array (map #(+ 10.0 (* 0.01 %)) (range n)))
          vals2-arr  (double-array (map #(+ 5.0 (* 0.02 %)) (range n)))
          ;; Compute reference values per group
          grouped (group-by first (map vector (seq groups-arr) (seq vals-arr) (seq vals2-arr)))
          ref-results (into {}
                            (for [[g rows] grouped]
                              (let [vs (mapv second rows)
                                    v2s (mapv #(nth % 2) rows)]
                                [g {:stddev (manual-stddev vs)
                                    :min    (apply min vs)
                                    :max    (apply max vs)
                                    :corr   (manual-corr vs v2s)}])))]
      ;; Test STDDEV + MIN + MAX (triggers var path with MIN/MAX)
      (let [q-map {:from {:g groups-arr :v vals-arr}
                   :group [:g]
                   :agg [[:stddev :v] [:min :v] [:max :v]]
                   :order [[:g :asc]]}]
        ;; Test both 1T and NT
        (doseq [[label threshold] [["1T" Integer/MAX_VALUE] ["NT" 200000]]]
          (let [result (with-parallel-threshold threshold #(q/q q-map))]
            (doseq [row result]
              (let [g (:g row)
                    ref (get ref-results g)]
                (is (approx= (:stddev row) (:stddev ref))
                    (str label " STDDEV for group " g))
                (is (approx= (:min row) (:min ref))
                    (str label " MIN for group " g))
                (is (approx= (:max row) (:max ref))
                    (str label " MAX for group " g)))))))
      ;; Test CORR + MIN + SUM (triggers var path with MIN and non-var agg)
      (let [q-map {:from {:g groups-arr :v vals-arr :v2 vals2-arr}
                   :group [:g]
                   :agg [[:corr :v :v2] [:min :v] [:sum :v]]
                   :order [[:g :asc]]}]
        (doseq [[label threshold] [["1T" Integer/MAX_VALUE] ["NT" 200000]]]
          (let [result (with-parallel-threshold threshold #(q/q q-map))]
            (doseq [row result]
              (let [g (:g row)
                    ref (get ref-results g)
                    expected-sum (reduce + 0.0 (map second (get grouped g)))]
                (is (approx= (:corr row) (:corr ref))
                    (str label " CORR for group " g))
                (is (approx= (:min row) (:min ref))
                    (str label " MIN for group " g))
                (is (approx= (:sum row) expected-sum 1e-4)
                    (str label " SUM for group " g))))))))))

;; ============================================================================
;; B5. Fused join+group-by vs separate
;; ============================================================================

(deftest b5-fused-join-vs-separate
  (testing "Fused join+group-by matches non-fused (with WHERE to break eligibility)"
    (let [n 10000
          fact-keys (long-array (map #(mod % 100) (range n)))
          fact-vals (double-array (map #(* 1.5 %) (range n)))
          dim-keys  (long-array (range 100))
          dim-cats  (long-array (map #(mod % 5) (range 100)))
          ;; Fused: single INNER join, keyword group cols, no WHERE, no SELECT
          fused-result (q/q {:from {:fk fact-keys :v fact-vals}
                             :join [{:with {:dk dim-keys :cat dim-cats}
                                     :on [:= :fk :dk]}]
                             :group [:cat]
                             :agg [[:sum :v]]
                             :order [[:cat :asc]]})
          ;; Non-fused: add a WHERE that's always true to break fused eligibility
          separate-result (q/q {:from {:fk fact-keys :v fact-vals}
                                :join [{:with {:dk dim-keys :cat dim-cats}
                                        :on [:= :fk :dk]}]
                                :where [[:>= :v 0.0]]
                                :group [:cat]
                                :agg [[:sum :v]]
                                :order [[:cat :asc]]})]
      (is (results-match? fused-result separate-result)
          "fused vs separate join+group-by"))))

;; ============================================================================
;; B6. Multi-agg single-pass vs N-pass
;; ============================================================================

(deftest b6-multi-agg-vs-individual
  (testing "Multi-agg single-pass matches individual queries"
    (let [n 10000
          a-arr (double-array (map #(+ 1.0 %) (range n)))
          b-arr (double-array (map #(+ 100.0 (* 2.0 %)) (range n)))
          c-arr (long-array (map #(+ 1 %) (range n)))
          ;; Multi-agg query (→ fusedSimdMultiSumParallel for 3 SUMs)
          multi-result (q/q {:from {:a a-arr :b b-arr}
                             :agg [[:sum :a] [:sum :b] [:count]]})
          ;; Individual queries
          sum-a (q/q {:from {:a a-arr} :agg [[:sum :a]]})
          sum-b (q/q {:from {:b b-arr} :agg [[:sum :b]]})
          cnt   (q/q {:from {:a a-arr} :agg [[:count]]})]
      (is (approx= (:sum_a (first multi-result)) (:sum (first sum-a)))
          "multi-agg SUM :a matches individual")
      (is (approx= (:sum_b (first multi-result)) (:sum (first sum-b)))
          "multi-agg SUM :b matches individual")
      (is (= (:count (first multi-result)) (:count (first cnt)))
          "multi-agg COUNT matches individual"))
    ;; All-long multi-sum (→ fusedSimdMultiSumAllLongParallel)
    (let [n 10000
          a-arr (long-array (map #(+ 1 %) (range n)))
          b-arr (long-array (map #(+ 100 (* 2 %)) (range n)))
          multi-result (q/q {:from {:a a-arr :b b-arr}
                             :agg [[:sum :a] [:sum :b]]})
          sum-a (q/q {:from {:a a-arr} :agg [[:sum :a]]})
          sum-b (q/q {:from {:b b-arr} :agg [[:sum :b]]})]
      (is (approx= (:sum_a (first multi-result)) (:sum (first sum-a)))
          "all-long multi-agg SUM :a")
      (is (approx= (:sum_b (first multi-result)) (:sum (first sum-b)))
          "all-long multi-agg SUM :b"))))

;; ============================================================================
;; B7. SIMD tail edge cases
;; ============================================================================

(deftest b7-simd-tail-edge-cases
  (testing "Filter+agg correct for various lengths including SIMD lane boundaries"
    (doseq [n [0 1 3 4 5 7 8 9 15 16 17 31 32 33 63 64 65 999 1000 1001]]
      (when (pos? n)
        (let [vals (double-array (map #(+ 1.0 %) (range n)))
              expected-sum (reduce + 0.0 (seq vals))
              expected-count (long n)
              expected-min 1.0
              expected-max (+ 1.0 (dec n))
              result (q/q {:from {:v vals}
                           :agg [[:sum :v] [:count] [:min :v] [:max :v]]})]
          (is (approx= (:sum (first result)) expected-sum 1e-4)
              (str "SUM at length " n))
          (is (= (long (:count (first result))) expected-count)
              (str "COUNT at length " n))
          (is (approx= (:min (first result)) expected-min)
              (str "MIN at length " n))
          (is (approx= (:max (first result)) expected-max)
              (str "MAX at length " n)))))
    ;; Empty array
    (let [result (q/q {:from {:v (double-array 0)}
                       :agg [[:sum :v] [:count]]})]
      (is (= 0 (long (:count (first result)))) "COUNT on empty array"))))

;; ============================================================================
;; B8. NULL sentinel handling
;; ============================================================================

(deftest b8-null-sentinel-handling
  (testing "NaN and Long.MIN_VALUE null sentinels"
    ;; Double NaN in various positions
    (let [vals (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])]
      (let [result (q/q {:from {:v vals}
                         :where [[:is-null :v]]
                         :agg [[:count]]})]
        (is (= 2 (long (:count (first result)))) "is-null count for NaN"))
      (let [result (q/q {:from {:v vals}
                         :where [[:is-not-null :v]]
                         :agg [[:sum :v] [:count] [:min :v] [:max :v]]})]
        (is (= 3 (long (:count (first result)))) "is-not-null count")
        (is (approx= (:sum (first result)) 9.0) "sum excluding NaN")
        (is (approx= (:min (first result)) 1.0) "min excluding NaN")
        (is (approx= (:max (first result)) 5.0) "max excluding NaN")))
    ;; Long.MIN_VALUE nulls
    (let [vals (long-array [10 Long/MIN_VALUE 30 Long/MIN_VALUE 50])]
      (let [result (q/q {:from {:v vals}
                         :where [[:is-null :v]]
                         :agg [[:count]]})]
        (is (= 2 (long (:count (first result)))) "is-null count for Long.MIN_VALUE"))
      (let [result (q/q {:from {:v vals}
                         :where [[:is-not-null :v]]
                         :agg [[:count]]})]
        (is (= 3 (long (:count (first result)))) "is-not-null count for long")))
    ;; All NaN
    (let [vals (double-array [Double/NaN Double/NaN Double/NaN])]
      (let [result (q/q {:from {:v vals}
                         :where [[:is-null :v]]
                         :agg [[:count]]})]
        (is (= 3 (long (:count (first result)))) "all NaN → count 3"))
      (let [result (q/q {:from {:v vals}
                         :where [[:is-not-null :v]]
                         :agg [[:count]]})]
        (is (= 0 (long (:count (first result)))) "all NaN → is-not-null 0")))
    ;; NULL keys don't match in joins
    (let [left-keys  (long-array [1 Long/MIN_VALUE 3])
          left-vals  (double-array [10.0 20.0 30.0])
          right-keys (long-array [1 Long/MIN_VALUE 3])
          right-vals (double-array [100.0 200.0 300.0])
          result (q/q {:from {:lk left-keys :lv left-vals}
                       :join [{:with {:rk right-keys :rv right-vals}
                               :on [:= :lk :rk]}]
                       :agg [[:count]]})]
      ;; Long.MIN_VALUE should not match
      (is (= 2 (long (:count (first result)))) "NULL keys don't match in join"))))

;; ============================================================================
;; B9. Dense key normalization round-trip
;; ============================================================================

(deftest b9-dense-key-normalization-round-trip
  (testing "Group-by with large-offset keys produces correct decoded keys"
    ;; Epoch-second-like keys: large absolute values but small range
    (let [base-epoch 1700000000
          n 10000
          ;; 10 distinct groups: base, base+1, ..., base+9
          groups (long-array (map #(+ base-epoch (mod % 10)) (range n)))
          vals   (double-array (map #(* 1.0 %) (range n)))
          result (q/q {:from {:g groups :v vals}
                       :group [:g]
                       :agg [[:sum :v] [:count]]
                       :order [[:g :asc]]})]
      (is (= 10 (count result)) "10 groups expected")
      ;; Verify decoded keys match original epoch values
      (doseq [row result]
        (let [g (:g row)]
          (is (>= g base-epoch) (str "key " g " >= base epoch"))
          (is (< g (+ base-epoch 10)) (str "key " g " < base+10")))))))

;; ============================================================================
;; B10. Dictionary encoding round-trip
;; ============================================================================

(deftest b10-dict-encoding-round-trip
  (testing "String group-by: encode → query → correct decoded strings"
    (let [strings (into-array String
                              (flatten (repeat 200 ["apple" "banana" "cherry" "date" "elderberry"])))
          encoded (q/encode-column strings)
          vals    (double-array (map double (range (count strings))))
          result  (q/q {:from {:cat encoded :v vals}
                        :group [:cat]
                        :agg [[:sum :v] [:count]]
                        :order [[:cat :asc]]})]
      (is (= 5 (count result)) "5 distinct string groups")
      (is (= #{"apple" "banana" "cherry" "date" "elderberry"}
             (set (map :cat result)))
          "decoded group keys match original strings")
      ;; Each group has 200 rows
      (doseq [row result]
        (is (= 200 (long (:count row)))
            (str "count for " (:cat row))))))
  (testing "Dict encoding with duplicates and empty strings"
    (let [strings (into-array String ["" "a" "" "b" "a" "" "b" "a"])
          encoded (q/encode-column strings)
          result  (q/q {:from {:s encoded}
                        :group [:s]
                        :agg [[:count]]
                        :order [[:s :asc]]})]
      (is (= 3 (count result)) "3 distinct groups including empty string")
      (is (some #(= "" (:s %)) result) "empty string group present"))))

(deftest hash-groupby-empty-key-sentinel-test
  (testing "Hash group-by handles key equal to actual EMPTY_KEY sentinel (0xDEADBEEFDEADBEEFL)"
    ;; Regression test for EMPTY_KEY sentinel collision.
    ;; The actual hash table sentinel is 0xDEADBEEFDEADBEEFL = -2401053088876216321L.
    ;; Long.MIN_VALUE is the NULL sentinel and rows with that key are now excluded.
    ;; This test verifies the safeKey() remapping for the actual EMPTY_KEY sentinel.
    (let [n 2000
          empty-key (Long/parseUnsignedLong "DEADBEEFDEADBEEF" 16)]  ; 0xDEADBEEFDEADBEEFL
      (let [groups (long-array n)
            vals   (double-array n)
            _ (dotimes [i (dec n)]
                (aset groups i (long (+ 200000 i)))
                (aset vals i 1.0))
            ;; Last row: key = actual EMPTY_KEY sentinel (should be remapped by safeKey())
            _ (aset groups (dec n) empty-key)
            _ (aset vals (dec n) 42.0)
            result (ColumnOps/fusedFilterGroupAggregate
                    (int 0) (int-array 0) (make-array Long/TYPE 0 0) (long-array 0) (long-array 0)
                    (int 0) (int-array 0) (make-array Double/TYPE 0 0) (double-array 0) (double-array 0)
                    (int 1) (into-array (Class/forName "[J") [groups]) (long-array [1])
                    (int 1) (int-array [(int ColumnOps/AGG_SUM)])
                    (into-array (Class/forName "[D") [vals])
                    (into-array (Class/forName "[D") [(double-array 0)])
                    (int n))
            ^longs out-keys (aget ^objects result 0)
            ^doubles out-accs (aget ^objects result 1)
            n-groups (alength out-keys)]
        ;; The EMPTY_KEY sentinel row is remapped via safeKey() — find it as (EMPTY_KEY ^ 1)
        (let [remapped (bit-xor empty-key 1)
              remapped-idx (first (filter #(= remapped (aget out-keys %)) (range n-groups)))]
          (is (some? remapped-idx) "EMPTY_KEY sentinel must be remapped and stored")
          (when remapped-idx
            (is (== 42.0 (aget out-accs (* remapped-idx 2)))
                "Remapped EMPTY_KEY group must have correct sum")))
        ;; Also verify null group (Long.MIN_VALUE) is excluded
        (is (nil? (first (filter #(= Long/MIN_VALUE (aget out-keys %)) (range n-groups))))
            "Long.MIN_VALUE (null sentinel) must be excluded from hash group-by")))))
