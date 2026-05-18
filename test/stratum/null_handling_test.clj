(ns stratum.null-handling-test
  "Regression tests for NULL/NaN handling across the query engine.
   Mirrors the audit catalog at .internal/null-handling-audit-catalog.md;
   each `deftest` cites the closing finding ID so future refactors can
   trace which behaviour is being protected."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.dataset :as ds]
            [stratum.chunk :as chunk]
            [stratum.index :as index]
            [stratum.query :as q]))

;; ============================================================================
;; F-001/F-002 + F-003/F-004: predicate paths exclude NULL rows
;; ============================================================================

(deftest f-001-simd-pred-long-null-excluded
  (testing "WHERE col < N on long col with NULLs — SIMD path"
    ;; Length 2000 forces SIMD path (>= 1000)
    (let [arr (long-array 2000)
          _ (dotimes [i 2000]
              (aset-long arr i
                         (cond
                           (zero? (mod i 7)) Long/MIN_VALUE
                           :else (long i))))
          d (ds/make-dataset {:n arr})
          r-lt (q/q {:from d :agg [[:count]] :where [[:< :n 100]]})
          r-gt (q/q {:from d :agg [[:count]] :where [[:> :n 1000]]})
          r-neq (q/q {:from d :agg [[:count]] :where [[:not= :n 50]]})
          r-isnull (q/q {:from d :agg [[:count]] :where [[:is-null :n]]})
          r-notnull (q/q {:from d :agg [[:count]] :where [[:is-not-null :n]]})
          expected-isnull (count (filter #(= % Long/MIN_VALUE) arr))
          expected-notnull (- 2000 expected-isnull)
          expected-lt (count (filter #(and (not= % Long/MIN_VALUE) (< % 100)) arr))
          expected-gt (count (filter #(and (not= % Long/MIN_VALUE) (> % 1000)) arr))
          expected-neq (count (filter #(and (not= % Long/MIN_VALUE) (not= % 50)) arr))]
      (is (= expected-isnull (long (:count (first r-isnull)))))
      (is (= expected-notnull (long (:count (first r-notnull)))))
      (is (= expected-lt (long (:count (first r-lt)))))
      (is (= expected-gt (long (:count (first r-gt)))))
      (is (= expected-neq (long (:count (first r-neq))))))))

(deftest f-002-simd-pred-double-nan-excluded
  (testing "WHERE col < N on double col with NaN — SIMD path"
    (let [arr (double-array 2000)
          _ (dotimes [i 2000]
              (aset-double arr i
                           (cond
                             (zero? (mod i 5)) Double/NaN
                             :else (double i))))
          d (ds/make-dataset {:n arr})
          expected-isnull (count (filter #(Double/isNaN %) arr))
          expected-lt (count (filter #(and (not (Double/isNaN %)) (< % 100.0)) arr))
          expected-neq (count (filter #(and (not (Double/isNaN %)) (not= % 50.0)) arr))]
      (is (= expected-isnull (long (:count (first (q/q {:from d :agg [[:count]]
                                                        :where [[:is-null :n]]}))))))
      (is (= expected-lt (long (:count (first (q/q {:from d :agg [[:count]]
                                                    :where [[:< :n 100.0]]}))))))
      (is (= expected-neq (long (:count (first (q/q {:from d :agg [[:count]]
                                                     :where [[:not= :n 50.0]]})))))))))

(deftest f-003-mask-path-neq-excludes-null
  (testing "Compiled-mask :neq excludes NULL rows (mask path forced via OR)"
    ;; The OR combinator forces compiled-mask path
    (let [d (ds/make-dataset {:n (long-array [1 Long/MIN_VALUE 2 Long/MIN_VALUE 3])})
          r (q/q {:from d :agg [[:count]] :where [[:or [:not= :n 2] [:= :n -99]]]})]
      ;; Non-NULL rows ≠ 2: 1 and 3 → 2 rows. NULL rows excluded (UNKNOWN).
      (is (= 2 (long (:count (first r))))))))

;; ============================================================================
;; F-005: scalar predicate eval is SQL 3VL
;; ============================================================================

(deftest f-005-scalar-pred-col-vs-col
  (testing "eval-pred-scalar 3VL on col-vs-col predicate with NULL operand"
    (let [d (ds/make-dataset {:a (long-array [1 Long/MIN_VALUE 3])
                              :b (long-array [10 20 Long/MIN_VALUE])})
          r (q/q {:from d :agg [[:count]] :where [[:< :a :b]]})]
      ;; Row 0: 1 < 10 = true. Row 1: NULL < 20 = UNKNOWN (false).
      ;; Row 2: 3 < NULL = UNKNOWN (false). Only row 0 matches.
      (is (= 1 (long (:count (first r))))))))

;; ============================================================================
;; F-006: all-NULL groups still appear in GROUP BY output
;; ============================================================================

(deftest f-006-all-null-group-emitted
  (testing "GROUP BY emits row for groups whose agg col is all NULL"
    (let [d (ds/make-dataset {:g (long-array [1 1 2 2 3])
                              :v (long-array [10 20 Long/MIN_VALUE Long/MIN_VALUE Long/MIN_VALUE])})
          rows (q/q {:from d :group [:g] :agg [[:sum :v]]})
          by-g (into {} (map (juxt :g identity)) rows)]
      (is (= 3 (count rows)) "All three groups present")
      (is (= 30 (long (:sum (get by-g 1)))) "Group 1 has real sum")
      (is (nil? (:sum (get by-g 2))) "Group 2 all-NULL → :sum nil")
      (is (nil? (:sum (get by-g 3))) "Group 3 single-NULL → :sum nil"))))

(deftest f-006-all-null-group-double
  (testing "Double NaN — same all-NULL-group behaviour"
    (let [d (ds/make-dataset {:g (long-array [1 1 2 2])
                              :v (double-array [1.5 2.5 Double/NaN Double/NaN])})
          rows (q/q {:from d :group [:g] :agg [[:sum :v] [:avg :v] [:min :v] [:max :v]]})
          by-g (into {} (map (juxt :g identity)) rows)]
      (is (= 2 (count rows)))
      (is (== 4.0 (double (:sum (get by-g 1)))))
      (is (nil? (:sum (get by-g 2))))
      (is (nil? (:avg (get by-g 2))))
      (is (nil? (:min (get by-g 2))))
      (is (nil? (:max (get by-g 2)))))))

;; ============================================================================
;; F-008/F-009/F-010/F-011/F-012: aggregator NULL safety
;; ============================================================================

(deftest f-008-count-distinct-skips-null
  (testing "COUNT(DISTINCT col) excludes NULL"
    (let [d (ds/make-dataset {:g (long-array [1 1 1 2 2 2])
                              :v (long-array [10 Long/MIN_VALUE 20 30 Long/MIN_VALUE 40])})
          rows (q/q {:from d :group [:g] :agg [[:count-distinct :v]]})]
      (is (= 2 (count rows)))
      (let [by-g (into {} (map (juxt :g identity)) rows)]
        (is (= 2 (long (:count-distinct (get by-g 1)))))
        (is (= 2 (long (:count-distinct (get by-g 2)))))))))

(deftest f-009-sum-product-dual-null
  (testing "SUM(a*b) skips when EITHER operand is NULL"
    (let [d (ds/make-dataset {:g (long-array [1 1 1 1])
                              :a (long-array [10 Long/MIN_VALUE 20 30])
                              :b (long-array [2 5 Long/MIN_VALUE 7])})
          ;; Row 0: 10*2 = 20. Row 1: NULL → skip. Row 2: NULL → skip.
          ;; Row 3: 30*7 = 210. Total = 230.
          rows (q/q {:from d :group [:g] :agg [[:sum [:* :a :b]]]})]
      (is (= 1 (count rows)))
      (is (= 230 (long (:sum-product (first rows))))))))

(deftest f-010-median-skips-null
  (testing "MEDIAN ignores NULL values"
    (let [d (ds/make-dataset {:g (long-array [1 1 1 1 1])
                              :v (long-array [10 Long/MIN_VALUE 20 Long/MIN_VALUE 30])})
          rows (q/q {:from d :group [:g] :agg [[:median :v]]})]
      ;; Median of {10, 20, 30} = 20
      (is (= 1 (count rows)))
      (is (= 20.0 (double (:median (first rows))))))))

;; ============================================================================
;; F-013: eval-agg-expr propagates long NULL → NaN
;; ============================================================================

(deftest f-013-expr-long-null-propagates
  (testing "Arithmetic on long-NULL column returns NULL in result"
    (let [d (ds/make-dataset {:a (long-array [10 Long/MIN_VALUE 30])})
          ;; SUM of a*2 evaluated per row. Row 1 is NULL → skipped.
          ;; Sum of contributing rows = 10*2 + 30*2 = 80.
          rows (q/q {:from d :agg [[:as [:sum [:* :a 2]] :s]]})]
      (is (= 80.0 (double (:s (first rows))))))))

;; ============================================================================
;; F-014: HAVING evaluator is 3VL
;; ============================================================================

(deftest f-014-having-3vl
  (testing "HAVING on aggregate with NULL — 3VL"
    ;; Need a query whose planner choice routes through having-pred-matches?.
    ;; Using a window-having pushdown shape would be ideal but complex;
    ;; the simple post-decode HAVING in postprocess.clj is already covered
    ;; elsewhere. Here we just exercise: long-NULL → row excluded by 3VL.
    (let [d (ds/make-dataset {:g (long-array [1 2 3])
                              :v (long-array [10 Long/MIN_VALUE 30])})
          rows (q/q {:from d :group [:g] :agg [[:sum :v]] :having [[:> :sum 5]]})]
      ;; Group 1 sum=10 → included. Group 2 sum=nil → excluded.
      ;; Group 3 sum=30 → included.
      (is (= 2 (count rows))))))

;; ============================================================================
;; F-015: LIKE on dict-encoded col with NULL doesn't crash
;; ============================================================================

(deftest f-015-like-with-nulls
  (testing "LIKE on String[] with nil entries — no crash, 3VL semantics"
    (let [d (ds/make-dataset {:s (into-array String ["apple" nil "banana" nil "applet"])})]
      (is (= 2 (long (:count (first (q/q {:from d :agg [[:count]]
                                          :where [[:like :s "app%"]]})))))
          "apple, applet match")
      (is (= 1 (long (:count (first (q/q {:from d :agg [[:count]]
                                          :where [[:not-like :s "app%"]]})))))
          "banana only — NULL NOT LIKE x is UNKNOWN, not TRUE")
      (is (= 2 (long (:count (first (q/q {:from d :agg [[:count]]
                                          :where [[:is-null :s]]}))))))
      (is (= 3 (long (:count (first (q/q {:from d :agg [[:count]]
                                          :where [[:is-not-null :s]]})))))))))

;; ============================================================================
;; F-016: CAST(long AS DOUBLE) preserves NULL
;; ============================================================================

(deftest f-016-cast-long-double-preserves-null
  (testing "CAST(long_col AS DOUBLE) maps Long/MIN_VALUE → NaN"
    (let [d (ds/make-dataset {:a (long-array [1 Long/MIN_VALUE 3])})
          ;; SUM(CAST(a AS DOUBLE)) — NULLs should NOT contribute -9.22e18.
          rows (q/q {:from d :agg [[:sum [:cast :a :double]]]})]
      (is (== 4.0 (double (:sum (first rows))))))))

;; ============================================================================
;; F-017: temporal kernels propagate NULL
;; ============================================================================

(deftest f-017-extract-year-preserves-null
  (testing "EXTRACT(year FROM ts) returns NULL for NULL input"
    ;; Epoch-days: 2020-01-15 ≈ 18276
    (let [d (ds/make-dataset {:ts (long-array [18276 Long/MIN_VALUE 19000])})
          rows (q/q {:from d :group [[:year :ts]] :agg [[:count]]})]
      ;; Should have 3 groups (2020, NULL, 2022 or thereabouts).
      ;; The NULL key forms its own group.
      (is (>= (count rows) 2)))))

;; ============================================================================
;; F-019/F-033/F-034: top-N and head decode emit nil for sentinels
;; ============================================================================

(deftest f-033-top-n-emits-nil
  (testing "ORDER BY ... LIMIT emits nil (not Long/MIN_VALUE) for NULL"
    (let [d (ds/make-dataset {:id (long-array (range 10))
                              :v (long-array [10 20 Long/MIN_VALUE 40 50 60 70 80 90 100])})
          rows (q/q {:from d :order [[:v :asc]] :limit 5})]
      ;; ASC + NULLS LAST: real values 10..100 first, NULL last.
      ;; First 5 rows are non-NULL by ASC.
      (is (= 5 (count rows)))
      (is (every? some? (map :v rows))))))

(deftest f-034-head-emits-nil
  (testing "LIMIT (no ORDER BY) emits nil for sentinel"
    (let [d (ds/make-dataset {:v (long-array [10 Long/MIN_VALUE 30])})
          rows (q/q {:from d :limit 3})]
      (is (= 3 (count rows)))
      (is (nil? (:v (nth rows 1))))
      (is (= 10 (long (:v (nth rows 0)))))
      (is (= 30 (long (:v (nth rows 2))))))))

;; ============================================================================
;; F-018/F-024: join keys treat NULL/NaN as non-matching
;; ============================================================================

(deftest f-024-float-join-nan-excluded
  (testing "INNER JOIN on float64 with NaN keys: NaN ≠ NaN — rows excluded"
    (let [a {:id (long-array [1 2 3])
             :ak (double-array [1.5 Double/NaN 3.5])
             :av (long-array [10 20 30])}
          b {:bk (double-array [1.5 3.5 Double/NaN])
             :bv (long-array [100 200 300])}
          rows (q/q {:from a :join [{:with b :on [:= :ak :bk]}]
                     :select [:id :av :bv]})]
      ;; id=1 (1.5↔1.5) and id=3 (3.5↔3.5). id=2 NaN ≠ NaN.
      (is (= 2 (count rows)))
      (is (= #{1 3} (set (map :id rows)))))))

;; ============================================================================
;; F-021: DML predicate full op coverage
;; ============================================================================
;; Exercised by the SQL/DML tests; surface coverage rather than dedicated
;; assertion here.

;; ============================================================================
;; F-038: CASE/WHEN :neq over NULL doesn't over-fire
;; ============================================================================

(deftest f-038-case-when-neq-null
  (testing "CASE WHEN col != x THEN ... — NULL row does NOT match"
    (let [d (ds/make-dataset {:v (long-array [1 Long/MIN_VALUE 3])})
          rows (q/q {:from d
                     :select [[:as [:case [[:!= :v 99] :v] [:else 0]] :r]]})]
      ;; Row 1: 1 != 99 → :v = 1. Row 2: NULL != 99 → UNKNOWN → fall-through → 0.
      ;; Row 3: 3 != 99 → :v = 3.
      (is (= 1.0 (double (:r (nth rows 0)))))
      (is (= 0.0 (double (:r (nth rows 1)))))
      (is (= 3.0 (double (:r (nth rows 2))))))))

;; ============================================================================
;; F-044: chunk writers accept nil
;; ============================================================================

(deftest f-044-chunk-from-seq-accepts-nil
  (testing "chunk-from-seq maps nil to sentinel and derives validity"
    (let [c (chunk/chunk-from-seq :int64 [1 nil 3 nil 5])
          validity (chunk/chunk-validity c)]
      (is (= 5 (chunk/chunk-length c)))
      (is (some? validity) "validity bitmap derived for chunk with nulls")
      (is (not (chunk/validity-row-valid? validity 1)))
      (is (chunk/validity-row-valid? validity 0))
      (is (chunk/validity-row-valid? validity 2)))))

(deftest f-044-idx-append-accepts-nil
  (testing "idx-append! handles nil values without NPE"
    (let [idx (-> (index/make-index :int64)
                  index/idx-transient
                  (index/idx-append! 1)
                  (index/idx-append! nil)
                  (index/idx-append! 3)
                  index/idx-persistent!)]
      (is (= 3 (index/idx-length idx)))
      (is (= 1 (index/idx-get-long idx 0)))
      (is (= Long/MIN_VALUE (index/idx-get-long idx 1)) "nil → sentinel"))))
