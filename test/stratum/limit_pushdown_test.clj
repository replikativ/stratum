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
