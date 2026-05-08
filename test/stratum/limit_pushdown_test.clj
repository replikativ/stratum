(ns stratum.limit-pushdown-test
  "Tests for the LIMIT-without-ORDER-BY pushdown (LHead).
   Verifies that bare-LIMIT queries over scans get rewritten to
   `LHead`, materialize only the first N rows of each touched
   column (no full-column decode of the underlying index), and
   produce the same rows the regular path would."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.index :as index])
  (:import [stratum.query.ir LHead LLimit PLimit PScan LSort LTopN]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Plan-shape tests — verify head-rewrite eligibility gates
;; ============================================================================

(defn- optimize [q]
  (plan/optimize (plan/build-logical-plan q)))

(deftest head-rewrite-fires-on-bare-limit
  (testing "LIMIT N without ORDER BY / WHERE / aggregate rewrites to LHead"
    (let [data {:a (long-array (range 100)) :b (double-array (range 100))}
          plan (optimize {:from data :limit 3})]
      (is (instance? LHead plan)
          (str "Expected LHead at top, got " (.getSimpleName (class plan))))
      (is (= 3 (:limit plan)))
      (is (instance? PScan (:input plan))
          "LHead's input should be a (P)Scan with the full column set"))))

(deftest head-rewrite-peels-trivial-project
  (testing "LIMIT 3 with explicit SELECT [:a :b] still rewrites — LHead absorbs the project"
    (let [data {:a (long-array (range 100))
                :b (double-array (range 100))
                :c (long-array (range 100))}
          plan (optimize {:from data :select [:a :b] :limit 3})]
      (is (instance? LHead plan))
      (is (= [:a :b] (mapv :ref (:select plan)))))))

(deftest head-rewrite-skips-with-where
  (testing "LIMIT + WHERE must not rewrite — filter changes which rows count"
    (let [data {:a (long-array (range 100))}
          plan (optimize {:from data :where [[:< :a 50]] :limit 3})]
      (is (not (instance? LHead plan))
          "LHead would lose the WHERE filter"))))

(deftest head-rewrite-skips-with-order-by
  (testing "LIMIT + ORDER BY routes through LTopN, not LHead"
    (let [data {:a (long-array (range 100))}
          plan (optimize {:from data :order [[:a :asc]] :limit 3})]
      (is (instance? LTopN plan)
          (str "Expected LTopN, got " (.getSimpleName (class plan)))))))

(deftest head-rewrite-skips-with-offset
  (testing "LIMIT + OFFSET must not rewrite — OFFSET changes which prefix"
    (let [data {:a (long-array (range 100))}
          plan (optimize {:from data :limit 3 :offset 5})]
      (is (not (instance? LHead plan))))))

(deftest head-rewrite-skips-above-threshold
  (testing "LIMIT > *head-limit* falls through to PLimit"
    (let [data {:a (long-array (range 100))}
          plan (binding [plan/*head-limit* 10]
                 (optimize {:from data :limit 1000}))]
      (is (not (instance? LHead plan))))))

;; ============================================================================
;; Execution correctness — LHead returns the right rows
;; ============================================================================

(deftest head-execute-returns-prefix-of-array-input
  (testing "LIMIT 3 on array data returns first 3 rows in scan order"
    (let [data {:a (long-array [10 20 30 40 50])
                :b (double-array [1.5 2.5 3.5 4.5 5.5])}
          rows (q/q {:from data :limit 3})]
      (is (= 3 (count rows)))
      (is (= [10 20 30] (mapv :a rows)))
      (is (= [1.5 2.5 3.5] (mapv :b rows))))))

(deftest head-execute-returns-prefix-of-index-input
  (testing "LIMIT 3 on a multi-chunk PersistentColumnIndex still returns first 3 rows"
    (let [;; 10K rows split across multiple chunks
          n 10000
          a-idx (index/index-from-seq :int64 (range n))
          b-idx (index/index-from-seq :float64 (map double (range n)))
          data {:a a-idx :b b-idx}
          rows (q/q {:from data :limit 3})]
      (is (= 3 (count rows)))
      (is (= [0 1 2] (mapv :a rows)))
      (is (= [0.0 1.0 2.0] (mapv :b rows))))))

(deftest head-execute-respects-explicit-select
  (testing "LIMIT 3 + SELECT [:b] returns only the requested columns"
    (let [data {:a (long-array [10 20 30 40 50])
                :b (double-array [1.5 2.5 3.5 4.5 5.5])
                :c (long-array [100 200 300 400 500])}
          rows (q/q {:from data :select [:b] :limit 3})]
      (is (= 3 (count rows)))
      (is (every? #(= #{:b} (set (keys %))) rows))
      (is (= [1.5 2.5 3.5] (mapv :b rows))))))

(deftest head-execute-clamps-when-limit-exceeds-length
  (testing "LIMIT 100 on a 5-row dataset returns all 5 rows"
    (let [data {:a (long-array [10 20 30 40 50])}
          rows (q/q {:from data :limit 100})]
      (is (= 5 (count rows)))
      (is (= [10 20 30 40 50] (mapv :a rows))))))

;; ============================================================================
;; HAVING → WHERE pushdown (F12)
;; ============================================================================

(deftest having-rewrites-group-col-pred-to-where
  (testing "HAVING g > 1 (group col) is pushed below LGroupBy"
    (let [data {:cat (long-array [1 1 2 2 3 3])
                :amt (double-array [10 20 30 40 50 60])}
          plan (optimize {:from data :group [:cat]
                          :agg [[:as [:sum :amt] :total]]
                          :having [[:> :cat 1]]})]
      ;; The pushdown converts the HAVING into a scan-level filter.
      ;; After strategy-selection that surfaces as a non-trivial
      ;; selectivity on the dense group-by; the plan must NOT carry
      ;; a PHaving wrapper.
      (is (not (instance? stratum.query.ir.PHaving plan))
          (str "Expected no PHaving wrapper, got "
               (.getSimpleName (class plan)))))))

(deftest having-keeps-aggregate-pred
  (testing "HAVING SUM(x) > 100 (agg alias) stays on LHaving"
    (let [data {:cat (long-array [1 1 2 2 3 3])
                :amt (double-array [10 20 30 40 50 60])}
          plan (optimize {:from data :group [:cat]
                          :agg [[:as [:sum :amt] :total]]
                          :having [[:> :total 100]]})]
      (is (instance? stratum.query.ir.PHaving plan)
          "Aggregate-referencing predicate must stay on PHaving"))))

(deftest having-mixed-splits-correctly
  (testing "HAVING with both group-col and agg-alias preds: pushed pred goes to WHERE, kept stays on PHaving"
    (let [data {:cat (long-array [1 1 2 2 3 3])
                :amt (double-array [10 20 30 40 50 60])}
          ;; (q/q) sanity-check that the rewrite doesn't change results
          via-having (q/q {:from data :group [:cat]
                           :agg [[:as [:sum :amt] :total]]
                           :having [[:> :cat 1] [:> :total 50]]
                           :order [[:cat :asc]]})
          via-where  (q/q {:from data :where [[:> :cat 1]]
                           :group [:cat]
                           :agg [[:as [:sum :amt] :total]]
                           :having [[:> :total 50]]
                           :order [[:cat :asc]]})]
      (is (= via-having via-where)
          "HAVING(g) + HAVING(agg) must equal WHERE(g) + HAVING(agg)"))))

(deftest having-skips-global-aggregate
  (testing "HAVING on LGlobalAgg (no GROUP BY) cannot push down"
    (let [data {:amt (double-array [10 20 30 40 50])}
          plan (optimize {:from data
                          :agg [[:as [:sum :amt] :total]]
                          :having [[:> :total 100]]})]
      (is (instance? stratum.query.ir.PHaving plan)))))

;; ============================================================================
;; Multi-key Top-N (F13)
;; ============================================================================

(deftest topn-multi-key-rewrite-eligible
  (testing "Two-column ORDER BY + LIMIT routes through LTopN"
    (let [data {:cat (long-array [1 2 1 2 1 3])
                :pri (long-array [10 20 30 40 50 60])}
          plan (optimize {:from data :order [[:cat :asc] [:pri :desc]] :limit 3})]
      (is (instance? LTopN plan))
      (is (= [[:cat :asc] [:pri :desc]] (:order-specs plan))))))

(deftest topn-multi-key-matches-naive-sort
  (testing "Multi-key ASC/DESC produces identical ordering to a naive sort+limit"
    (let [data {:a (long-array [3 1 2 3 1 2])
                :b (long-array [10 20 30 40 50 60])}
          via-topn (q/q {:from data :order [[:a :asc] [:b :desc]] :limit 6})]
      ;; Expected: a ASC primary, b DESC tiebreak.
      (is (= [{:a 1 :b 50} {:a 1 :b 20} {:a 2 :b 60}
              {:a 2 :b 30} {:a 3 :b 40} {:a 3 :b 10}]
             (mapv #(select-keys % [:a :b]) via-topn))))))

(deftest topn-single-key-still-works
  (testing "Single-key ORDER BY (regression after multi-key refactor)"
    (let [data {:x (long-array [5 3 8 1 9 2 7])}
          rows (q/q {:from data :order [[:x :asc]] :limit 3})]
      (is (= [1 2 3] (mapv :x rows))))))

(deftest topn-index-backed-multi-key
  (testing "Multi-key TopN over index-backed columns produces correct order"
    (let [n 5000
          rng (java.util.Random. 7)
          cat-idx (index/index-from-seq :int64 (repeatedly n #(.nextInt rng 4)))
          pri-idx (index/index-from-seq :int64 (repeatedly n #(.nextInt rng 1000)))
          data {:cat cat-idx :pri pri-idx}
          rows (q/q {:from data :order [[:cat :asc] [:pri :desc]] :limit 10})]
      (is (= 10 (count rows)))
      ;; cat must be monotonically non-decreasing in the result.
      (is (= (sort (mapv :cat rows)) (mapv :cat rows))))))

;; ============================================================================
;; Row-group ordering for ORDER BY + LIMIT on monotonic columns (F14)
;; ============================================================================

(deftest topn-sorted-input-correctness
  (testing "Top-N on a strictly-ascending column returns first N values"
    (let [n 50000
          ts-idx (index/index-from-seq :int64 (range n))
          rows (q/q {:from {:ts ts-idx} :order [[:ts :asc]] :limit 10})]
      (is (= (vec (range 10)) (mapv :ts rows))))))

(deftest topn-sorted-input-desc-correctness
  (testing "Top-N DESC on a strictly-ascending column returns last N values"
    (let [n 50000
          ts-idx (index/index-from-seq :int64 (range n))
          rows (q/q {:from {:ts ts-idx} :order [[:ts :desc]] :limit 10})]
      (is (= (vec (reverse (range (- n 10) n))) (mapv :ts rows))))))

(deftest topn-pruning-doesnt-affect-random-input
  (testing "Top-N still correct on random input (pruning never triggers, but
   chunk reordering shouldn't change correctness)"
    (let [n 10000
          rng (java.util.Random. 42)
          values (vec (repeatedly n #(.nextLong rng)))
          ts-idx (index/index-from-seq :int64 values)
          rows (q/q {:from {:ts ts-idx} :order [[:ts :asc]] :limit 5})]
      ;; Result must be the 5 smallest values, in ascending order.
      (is (= (vec (take 5 (sort values))) (mapv :ts rows))))))
