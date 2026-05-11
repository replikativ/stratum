(ns stratum.explain-test
  "Tests for EXPLAIN, EXPLAIN ANALYZE, EXPLAIN (FORMAT JSON) — Postgres-style
   text renderer + DuckDB-shape JSON renderer + per-node ANALYZE timing."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [stratum.api :as api]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.sql :as sql]))

(def ^:private sample-cols
  {:a (long-array (range 1000))
   :b (double-array (map double (range 1000)))
   :c (let [arr (make-array String 1000)]
        (dotimes [i 1000] (aset arr i (str "k" (mod i 10))))
        arr)})

(defn- sample-tables []
  {"t" sample-cols})

;; ============================================================================
;; Text renderer — Postgres-style indented tree
;; ============================================================================

(deftest text-renderer-basic-shape
  (testing "Bare SELECT renders as indented tree with -> arrows"
    (let [text (:plan-tree (api/explain "SELECT SUM(a) FROM t" (sample-tables)))]
      (is (re-find #"PFusedSIMDAgg" text))
      (is (re-find #"->\s+PScan" text))
      (is (re-find #"Aggregate: sum\(a\)" text))
      (is (re-find #"Columns:" text))
      (is (re-find #"Length:" text))))

  (testing "Cost suffix includes est-rows"
    (let [text (:plan-tree (api/explain "SELECT SUM(a) FROM t" (sample-tables)))]
      (is (re-find #"est-rows=\d+" text))))

  (testing "Selectivity surfaces when planner annotated it"
    (let [text (:plan-tree
                (api/explain "SELECT c, MIN(a) FROM t GROUP BY c" (sample-tables)))]
      (is (re-find #"sel=\d\.\d{3}" text)))))

(deftest text-renderer-filter-pushdown-visibility
  (testing "WHERE clause shows as Filter: on the node that holds it"
    (let [text (:plan-tree
                (api/explain "SELECT SUM(a) FROM t WHERE a > 100"
                             (sample-tables)))]
      (is (re-find #"Filter:\s*\(a > 100\)" text)))))

(deftest text-renderer-split-agg-shows-filters
  (testing "PSplitAgg sub-plans surface the WHERE clause (regression test)"
    ;; Bug fixed in this branch: previously the plan-tree for
    ;; `SELECT MIN(a), MEDIAN(a), MAX(a) FROM t WHERE a > 100` was
    ;; byte-identical to the no-WHERE plan because the per-node
    ;; formatter dropped predicates on PDenseGroupBy / PPercentileAgg.
    (let [text-with    (:plan-tree
                        (api/explain "SELECT MIN(a), MEDIAN(a), MAX(a) FROM t WHERE a > 100"
                                     (sample-tables)))
          text-without (:plan-tree
                        (api/explain "SELECT MIN(a), MEDIAN(a), MAX(a) FROM t"
                                     (sample-tables)))]
      (is (re-find #"PSplitAgg" text-with))
      ;; Both sub-plans show the filter
      (is (>= (count (re-seq #"Filter:" text-with)) 2)
          "both PSplitAgg sub-plans should display Filter:")
      ;; Plans differ between with/without — proves the filter changed the output
      (is (not= text-with text-without)
          "WHERE-vs-no-WHERE plans must differ in their printed form"))))

(deftest text-renderer-group-by-shows-keys-and-aggs
  (let [text (:plan-tree
              (api/explain "SELECT c, MIN(a), MAX(a) FROM t GROUP BY c"
                           (sample-tables)))]
    (is (re-find #"Group Keys:" text))
    (is (re-find #"Aggregates:\s*\[min\(a\), max\(a\)\]" text))))

(deftest text-renderer-join-shows-conditions
  (let [dim  {:d (long-array (range 100))
              :v (long-array (map #(* 10 %) (range 100)))}
        fact {:f (long-array (repeatedly 1000 #(rand-int 100)))
              :amt (double-array (repeatedly 1000 #(rand)))}
        text (:plan-tree
              (api/explain
               "SELECT d.v, SUM(f.amt) FROM fact f JOIN dim d ON f.f = d.d GROUP BY d.v"
               {"fact" fact "dim" dim}))]
    ;; Some kind of join info present (either fused PFusedJoinGroupAgg's
    ;; "Join: {...}" or a plain PHashJoin's "Join Type:" / "Hash Cond:")
    (is (or (re-find #"Join Type:" text)
            (re-find #"Join:" text)))
    ;; Branch tags appear on multi-input nodes
    (is (or (re-find #"\[fact\]" text)
            (re-find #"\[build\]" text)
            (re-find #"\[left\]" text)))))

;; ============================================================================
;; JSON renderer — DuckDB-shape
;; ============================================================================

(deftest json-renderer-shape
  (let [r    (api/explain "SELECT SUM(a) FROM t WHERE a > 100"
                          (sample-tables)
                          {:format :json})
        json (:plan-json r)]
    (is (vector? json))
    (is (= 1 (count json)))
    (let [root (first json)]
      (is (string? (get root "name")))
      (is (vector? (get root "children")))
      (is (map? (get root "extra_info")))
      (is (contains? (get root "extra_info") "__estimated_cardinality__"))
      ;; Filter surfaces in extra_info
      (let [scan (first (get root "children"))]
        (is (= "PScan" (get scan "name")))))))

(deftest json-renderer-analyze-fields
  (let [r    (api/explain "SELECT SUM(a) FROM t WHERE a > 100"
                          (sample-tables)
                          {:analyze? true :format :json})
        root (first (:plan-json r))]
    (is (contains? (get root "extra_info") "__cardinality__"))
    (is (contains? (get root "extra_info") "__timing__"))))

;; ============================================================================
;; ANALYZE — per-node timing collection
;; ============================================================================

(deftest analyze-attaches-timings
  (let [r (api/explain "SELECT SUM(a) FROM t WHERE a > 100"
                       (sample-tables)
                       {:analyze? true})]
    (is (number? (:execution-time-ms r)))
    (is (pos? (:execution-time-ms r)))
    (let [data (:plan-data r)]
      (is (number? (:time-ms data)))
      (is (= 1 (:actual-rows data)) "global SUM produces 1 row"))
    (is (re-find #"actual-rows=" (:plan-tree r)))
    (is (re-find #"time=\d+\.\d+ms" (:plan-tree r)))
    (is (re-find #"Execution Time:" (:plan-tree r)))))

(deftest analyze-actual-rows-reflects-selectivity
  (let [r (api/explain "SELECT COUNT(*) FROM t WHERE a > 100"
                       (sample-tables)
                       {:analyze? true})
        scan-data (loop [d (:plan-data r)]
                    (if (= "PScan" (:op d))
                      d
                      (recur (first (:children d)))))]
    (is (= 1000 (:actual-rows scan-data))
        "PScan output is full table; selectivity reflects in downstream nodes")))

;; ============================================================================
;; SQL parser — EXPLAIN modifier variants
;; ============================================================================

(deftest sql-explain-syntax-variants
  (let [reg (sample-tables)]
    (testing "Bare EXPLAIN"
      (let [{:keys [options inner]} (:explain (sql/parse-sql
                                               "EXPLAIN SELECT a FROM t" reg))]
        (is (false? (:analyze? options)))
        (is (= :text (:format options)))
        (is (:query inner))))

    (testing "EXPLAIN ANALYZE"
      (let [{:keys [options]} (:explain (sql/parse-sql
                                         "EXPLAIN ANALYZE SELECT a FROM t" reg))]
        (is (true? (:analyze? options)))
        (is (= :text (:format options)))))

    (testing "EXPLAIN (ANALYZE)"
      (let [{:keys [options]} (:explain (sql/parse-sql
                                         "EXPLAIN (ANALYZE) SELECT a FROM t" reg))]
        (is (true? (:analyze? options)))))

    (testing "EXPLAIN (FORMAT JSON)"
      (let [{:keys [options]} (:explain (sql/parse-sql
                                         "EXPLAIN (FORMAT JSON) SELECT a FROM t" reg))]
        (is (= :json (:format options)))
        (is (false? (:analyze? options)))))

    (testing "EXPLAIN (ANALYZE, FORMAT JSON)"
      (let [{:keys [options]} (:explain (sql/parse-sql
                                         "EXPLAIN (ANALYZE, FORMAT JSON) SELECT a FROM t" reg))]
        (is (true? (:analyze? options)))
        (is (= :json (:format options)))))

    (testing "Unknown option surfaces as :error (caught by parse-sql)"
      (let [r (sql/parse-sql "EXPLAIN (FOO) SELECT a FROM t" reg)]
        (is (:error r))
        (is (str/includes? (:error r) "FOO"))))))

;; ============================================================================
;; End-to-end through api/explain SQL prefix
;; ============================================================================

(deftest sql-prefix-runs-analyze-and-renders
  (testing "EXPLAIN ANALYZE via SQL prefix produces timings"
    (let [r (api/explain "EXPLAIN ANALYZE SELECT SUM(a) FROM t WHERE a > 100"
                         (sample-tables))]
      (is (re-find #"actual-rows=" (:plan-tree r)))
      (is (re-find #"Execution Time:" (:plan-tree r)))))

  (testing "EXPLAIN (FORMAT JSON) via SQL prefix returns :plan-json"
    (let [r (api/explain "EXPLAIN (FORMAT JSON) SELECT SUM(a) FROM t"
                         (sample-tables))]
      (is (vector? (:plan-json r)))
      (is (= "PFusedSIMDAgg" (get (first (:plan-json r)) "name"))))))

;; ============================================================================
;; Back-compat with legacy explain shape
;; ============================================================================

(deftest legacy-keys-still-present
  (testing ":strategy / :n-rows / :columns still in result (api_test.clj contract)"
    (let [r (api/explain {:from sample-cols
                          :where [[:> :a 100]]
                          :agg [[:sum :a]]})]
      (is (keyword? (:strategy r)))
      (is (= 1000 (:n-rows r)))
      (is (pos? (:columns r))))))
