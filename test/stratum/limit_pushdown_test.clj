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

(deftest topn-same-key-range-rewrite-eligibility
  (let [data {:id (long-array (range 100))
              :rank (long-array (range 100))}]
    (testing "a conjunction of ranges on the sole order key is absorbed"
      (let [plan (optimize {:from data
                            :where [[:> :rank 80] [:<= :rank 90]]
                            :select [:id :rank]
                            :order [[:rank :asc]]
                            :limit 3})]
        (is (instance? LTopN plan))
        (is (= [[:rank :gt 80] [:rank :lte 90]]
               (:predicates plan)))))
    (testing "all five exact comparison operators qualify"
      (doseq [predicate [[:= :rank 50] [:> :rank 50] [:>= :rank 50]
                         [:< :rank 50] [:<= :rank 50]]]
        (is (instance? LTopN
                       (optimize {:from data :where [predicate]
                                  :order [[:rank :asc]] :limit 3})))))
    (testing "other columns, composite order, and non-scalar ranges decline"
      (doseq [query [{:where [[:> :id 80]]
                      :order [[:rank :asc]]}
                     {:where [[:> :rank 80]]
                      :order [[:rank :asc] [:id :asc]]}
                     {:where [[:between :rank 80 90]]
                      :order [[:rank :asc]]}
                     {:where [[:> :rank 9223372036854775808N]]
                      :order [[:rank :asc]]}
                     {:where [[:or [:> :rank 80] [:< :rank 10]]]
                      :order [[:rank :asc]]}]]
        (is (not (instance? LTopN
                            (optimize (merge {:from data :limit 3} query)))))))))

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

(deftest topn-preserves-full-int64-order
  (testing "adjacent integers above double's exact range remain distinct"
    (let [lo 9007199254740992
          hi 9007199254740993
          data {:id (long-array [1 2])
                :x (long-array [hi lo])}]
      (is (= [{:id 2 :x lo}]
             (q/q {:from data
                   :select [:id :x]
                   :order [[:x :asc]]
                   :limit 1})))
      (is (= [{:id 1 :x hi}]
             (q/q {:from data
                   :select [:id :x]
                   :order [[:x :desc]]
                   :limit 1}))))))

(deftest topn-same-key-ranges-preserve-sql-semantics
  (let [lo 9007199254740992
        values [Long/MIN_VALUE (+ lo 3) lo (+ lo 2) (+ lo 1)]
        data {:x (long-array values)}]
    (testing "every comparison remains full-width and excludes NULL"
      (is (= [(+ lo 1) (+ lo 2)]
             (mapv :x (q/q {:from data :where [[:> :x lo] [:<= :x (+ lo 2)]]
                            :order [[:x :asc]] :limit 10}))))
      (is (= [lo]
             (mapv :x (q/q {:from data :where [[:= :x lo]]
                            :order [[:x :asc]] :limit 10}))))
      (is (= [lo (+ lo 1)]
             (mapv :x (q/q {:from data :where [[:< :x (+ lo 2)]]
                            :order [[:x :asc]] :limit 10}))))
      (is (= [(+ lo 3) (+ lo 2)]
             (mapv :x (q/q {:from data :where [[:>= :x (+ lo 2)]]
                            :order [[:x :desc]] :limit 10}))))))
  (testing "float ranges use the same exact gate"
    (let [data {:x (double-array [Double/NaN 4.0 1.0 3.0 2.0])}]
      (is (= [2.0 3.0]
             (mapv :x (q/q {:from data :where [[:> :x 1.0] [:< :x 4.0]]
                            :order [[:x :asc]] :limit 10})))))))

(deftest topn-mixed-keys-preserve-int64-order
  (testing "an int64 primary key is not collapsed before a double tiebreak"
    (let [lo 9007199254740992
          hi 9007199254740993
          rows (q/q {:from {:a (long-array [hi lo])
                            :b (double-array [0.0 100.0])}
                     :order [[:a :asc] [:b :asc]]
                     :limit 1})]
      (is (= [{:a lo :b 100.0}] rows))))
  (testing "an int64 tiebreak keeps full precision after a double key"
    (let [lo 9007199254740992
          hi 9007199254740993
          rows (q/q {:from {:a (double-array [1.0 1.0])
                            :b (long-array [hi lo])}
                     :order [[:a :asc] [:b :asc]]
                     :limit 1})]
      (is (= [{:a 1.0 :b lo}] rows)))))

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

(deftest topn-chunk-pruning-preserves-full-int64-bounds
  (testing "a later chunk whose minimum differs only below double precision is not pruned"
    (let [lo 9007199254740992
          hi 9007199254740993
          ;; The default chunk size is 8192. Put only the true minimum in the
          ;; second chunk: double-valued chunk bounds compare equal and used to
          ;; prune it after filling the heap from the first chunk.
          values (conj (vec (repeat 8192 hi)) lo)
          idx (index/index-from-seq :int64 values)
          rows (q/q {:from {:x idx}
                     :order [[:x :asc]]
                     :limit 1})]
      (is (= [lo] (mapv :x rows))))))

(deftest topn-int64-chunk-stats-are-never-used-as-exact-bounds
  (let [two-to-53 9007199254740992]
    (testing "DESC scans a later value rounded down by double ChunkStats"
      (let [winner (inc two-to-53)
            idx (index/index-from-seq
                 :int64 (conj (vec (repeat 8192 two-to-53)) winner))]
        (is (= [winner]
               (mapv :x (q/q {:from {:x idx}
                              :order [[:x :desc]]
                              :limit 1}))))))
    (testing "ASC scans a later value rounded up by double ChunkStats"
      (let [first-value (+ two-to-53 4)
            winner (+ two-to-53 3)
            idx (index/index-from-seq
                 :int64 (conj (vec (repeat 8192 first-value)) winner))]
        (is (= [winner]
               (mapv :x (q/q {:from {:x idx}
                              :order [[:x :asc]]
                              :limit 1}))))))))

(deftest topn-equal-primary-chunk-bound-cannot-prune-secondary-key
  (testing "a later equal-primary chunk may have the winning tiebreaker"
    (let [a-idx (index/index-from-seq :int64 (repeat 8193 1))
          b-idx (index/index-from-seq
                 :int64 (conj (vec (repeat 8192 100)) 0))]
      (is (= [{:a 1 :b 0}]
             (q/q {:from {:a a-idx :b b-idx}
                   :order [[:a :asc] [:b :asc]]
                   :limit 1}))))))

(deftest topn-all-null-chunks-do-not-produce-numeric-bounds
  (testing "an all-NULL int64 chunk neither throws nor hides a real value"
    (let [idx (index/index-from-seq
               :int64 (conj (vec (repeat 8192 Long/MIN_VALUE)) 7))]
      (is (= [7]
             (mapv :x (q/q {:from {:x idx}
                            :order [[:x :asc]]
                            :limit 1}))))
      (is (= [nil]
             (mapv :x (q/q {:from {:x idx}
                            :order [[:x :desc]]
                            :limit 1})))))))

(deftest topn-desc-null-and-nan-are-not-hidden-by-chunk-stats
  (testing "a later int64 NULL sorts first in DESC"
    (let [idx (index/index-from-seq
               :int64 (conj (vec (repeat 8192 1)) Long/MIN_VALUE))]
      (is (= [nil]
             (mapv :x (q/q {:from {:x idx}
                            :order [[:x :desc]]
                            :limit 1}))))))
  (testing "a later float64 NaN/NULL sorts first in DESC"
    (let [idx (index/index-from-seq
               :float64 (conj (vec (repeat 8192 1.0)) Double/NaN))]
      (is (= [nil]
             (mapv :x (q/q {:from {:x idx}
                            :order [[:x :desc]]
                            :limit 1})))))))

(deftest fallback-desc-null-order-matches-streaming-topn
  (testing "OFFSET disables streaming top-N without changing PostgreSQL NULL order"
    (let [data {:eid (long-array [1 2 3])
                :x (long-array [10 20 Long/MIN_VALUE])}]
      (is (= [[3 nil] [2 20]]
             (mapv (juxt :eid :x)
                   (q/q {:from data
                         :select [:eid :x]
                         :order [[:x :desc]]
                         :limit 2}))))
      (is (= [[2 20] [1 10]]
             (mapv (juxt :eid :x)
                   (q/q {:from data
                         :select [:eid :x]
                         :order [[:x :desc]]
                         :limit 2
                         :offset 1})))))))

(deftest topn-range-prunes-chunks-with-full-int64-bounds
  (testing "a >2^53 boundary is evaluated row-wise without rounding"
    (let [lo 9007199254740992
          hi 9007199254740993
          winner 9007199254740994
          values (conj (vec (repeat 8192 hi)) winner)
          idx (index/index-from-seq :int64 values)
          rows (q/q {:from {:x idx}
                     :where [[:> :x hi]]
                     :order [[:x :asc]]
                     :limit 1})]
      (is (= [winner] (mapv :x rows)))
      (is (not-any? #{lo hi} (map :x rows))))))

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
