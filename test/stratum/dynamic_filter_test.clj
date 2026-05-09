(ns stratum.dynamic-filter-test
  "Tests for F19 — dynamic filter pushdown from a join's build side onto
   the probe-side scan. Mirrors DuckDB's `DynamicTableFilterSet`: after
   the build side materializes, we derive `[min,max]` of the build join
   key and push it as a `[:gte … :lte …]` predicate pair onto the
   probe-side scan's `:dynamic-filters` volatile, before the probe
   executes. Probe-side zone-map pruning then skips whole chunks whose
   stats fall outside the range.

   These tests exercise:
   - correctness invariance (results match a plain join)
   - the gate (only `:inner` joins push; outer joins must not)
   - the type gate (dict-encoded string keys must not push)
   - the volatile actually carries the derived range after execute
   - chunk-pruning pays off when the probe is index-backed (PChunkedScan)"
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.query.ir :as ir]
            [stratum.index :as index])
  (:import [stratum.query.ir PScan PChunkedScan PHashJoin
            PFusedJoinGlobalAgg PFusedJoinGroupAgg]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- physical [query]
  (-> query plan/build-logical-plan plan/optimize))

(defn- find-first
  "Walk plan tree top-down, return the first node satisfying `pred`."
  [plan pred]
  (let [found (volatile! nil)]
    (letfn [(walk [n]
              (when (and (not @found) n)
                (if (pred n)
                  (vreset! found n)
                  (cond
                    (instance? PHashJoin n)
                    (do (walk (:build-side n)) (walk (:probe-side n)))
                    (or (instance? PFusedJoinGlobalAgg n)
                        (instance? PFusedJoinGroupAgg n))
                    (do (walk (:left n)) (walk (:right n)))
                    :else
                    (when-let [in (:input n)] (walk in))))))]
      (walk plan)
      @found)))

(defn- find-probe-scan
  "Return the scan reachable down the join's probe side."
  [plan]
  (let [join (find-first plan #(or (instance? PHashJoin %)
                                   (instance? PFusedJoinGlobalAgg %)
                                   (instance? PFusedJoinGroupAgg %)))
        probe (cond
                (instance? PHashJoin join) (:probe-side join)
                :else                       (:left join))]
    (find-first probe #(or (instance? PScan %) (instance? PChunkedScan %)))))

;; ============================================================================
;; Correctness invariance
;; ============================================================================

(deftest dynamic-filter-correctness-inner-join
  (testing "INNER join with build keys covering subset of probe range — result matches"
    (let [;; Probe has 1..1000, build has 100..200 only — dynamic
          ;; filter narrows probe to that range.
          probe-keys  (long-array (range 1 1001))
          probe-vals  (double-array (mapv #(* 2.0 %) (range 1 1001)))
          probe       {:k probe-keys :v probe-vals}
          build       {:bk (long-array (range 100 201))
                       :tag (long-array (repeat 101 7))}
          result (q/q {:from probe
                       :join [{:with build :on [[:= :k :bk]]}]
                       :agg  [[:sum :v]]})]
      ;; Sum of 2*k for k in 100..200 inclusive = 2 * (100+200)*101/2 = 30300
      (is (= 30300.0 (:sum (first result)))))))

(deftest dynamic-filter-correctness-fused-group-agg
  (testing "Fused join+group-by — result identical to plain path"
    (let [fact   {:fk (long-array (mapv #(mod % 5) (range 1000)))
                  :v  (double-array (range 1000))}
          dim    {:id (long-array [2 3])
                  :grp (long-array [99 99])}
          result (q/q {:from fact
                       :join [{:with dim :on [[:= :fk :id]]}]
                       :group [:grp]
                       :agg [[:sum :v]]})]
      ;; fact rows whose fk in {2,3}; sum of v over those rows.
      (let [^longs ks (long-array (mapv #(mod % 5) (range 1000)))
            expected  (reduce + (for [i (range 1000)
                                      :when (#{2 3} (aget ks i))]
                                  (double i)))]
        (is (= 1 (count result)))
        (is (= 99 (:grp (first result))))
        (is (= expected (:sum (first result))))))))

;; ============================================================================
;; Volatile is populated after a join executes
;; ============================================================================

(deftest dynamic-filter-fires-when-narrow-build-narrows-probe
  (testing "Build is narrow vs probe range → estimated probe-side
            selectivity is low → push fires (volatile holds the range)."
    ;; probe range [0..99], build keys {25,50,75} → build range [25..75]
    ;; → ~50% of probe values fall outside → selectivity ≈ 0.5 → push.
    (let [probe {:k (long-array (range 0 100)) :v (double-array (range 0 100))}
          build {:bk (long-array [25 50 75])}
          plan-tree (physical {:from probe
                               :join [{:with build :on [[:= :k :bk]]}]
                               :agg [[:count]]})
          probe-scan (find-probe-scan plan-tree)
          vol (:dynamic-filters probe-scan)]
      (is (some? vol) "scan should carry a volatile for dynamic-filters")
      (is (nil? @vol) "before execute, volatile is empty")
      (exec/execute-physical plan-tree)
      (is (= [[:k :gte 25] [:k :lte 75]] @vol)
          "F19 fires when build narrows probe enough — derived range pushed"))))

(deftest dynamic-filter-skips-when-build-covers-probe-range
  (testing "FK shape (build covers full probe range) → estimated selectivity
            ≈ 1.0 → push must skip; otherwise mask-alloc dwarfs savings.
            JOIN-Q1 was 6× slower with an unconditional push."
    ;; probe range [0..999], build also [0..999] → all probe values in range
    ;; → selectivity ≈ 1.0 → no push.
    (let [n 1000
          probe {:k (long-array (range n)) :v (double-array (range n))}
          build {:bk (long-array (range n))}
          plan-tree (physical {:from probe
                               :join [{:with build :on [[:= :k :bk]]}]
                               :agg [[:count]]})
          probe-scan (find-probe-scan plan-tree)]
      (exec/execute-physical plan-tree)
      (is (nil? @(:dynamic-filters probe-scan))
          "build range == probe range → push skipped (cost-model gate)"))))

;; ============================================================================
;; Negative gates — must NOT push
;; ============================================================================

(deftest dynamic-filter-skips-left-join
  (testing "LEFT outer join must NOT push a probe-side range filter"
    (let [probe {:k (long-array [1 2 3 4 5]) :v (double-array [10 20 30 40 50])}
          build {:bk (long-array [3]) :extra (long-array [99])}
          plan-tree (physical {:from probe
                               :join [{:with build :on [:= :k :bk] :type :left}]})
          probe-scan (find-probe-scan plan-tree)
          result (q/q {:from probe
                       :join [{:with build :on [:= :k :bk] :type :left}]
                       :select [:k :v :extra]})]
      ;; Trigger the executor so the volatile would be set if the gate were wrong.
      (exec/execute-physical plan-tree)
      (is (nil? @(:dynamic-filters probe-scan))
          "LEFT join should leave dynamic-filters unset")
      (is (= 5 (count result))
          "LEFT join must preserve all 5 probe rows even if only 1 matches"))))

(deftest dynamic-filter-skips-string-keys
  (testing "Dict-encoded string keys must NOT push (codes are dict-local)"
    (let [fact {:n (into-array String ["a" "b" "c" "d" "e"])
                :v (double-array [1 2 3 4 5])}
          dim  {:n (into-array String ["b" "d"])
                :tag (long-array [99 99])}
          plan-tree (physical {:from fact
                               :join [{:with dim :on [[:= :n :n]]}]
                               :agg [[:sum :v]]})
          probe-scan (find-probe-scan plan-tree)
          result (q/q {:from fact
                       :join [{:with dim :on [[:= :n :n]]}]
                       :agg [[:sum :v]]})]
      (exec/execute-physical plan-tree)
      (is (nil? @(:dynamic-filters probe-scan))
          "string join key should leave dynamic-filters unset")
      (is (= 6.0 (:sum (first result)))
          "result is still correct (b=2, d=4 → sum=6)"))))

;; ============================================================================
;; Plan structure: lift folds wrappers into scan
;; ============================================================================

(deftest lift-folds-simd-wrapper-into-scan
  (testing "SELECT * WHERE p plan ends in a bare PScan with :predicates"
    (let [data {:k (long-array (range 100)) :v (double-array (range 100))}
          plan-tree (physical {:from data :where [[:< :k 10]] :select [:v]})
          ;; Plan is `PProject (PScan)` — no PSIMDFilter wrapper.
          inp (:input plan-tree)]
      (is (instance? PScan inp))
      (is (= [[:k :lt 10]] (:predicates inp)))
      (is (= 10 (count (q/q {:from data :where [[:< :k 10]] :select [:v]})))))))

(deftest lift-folds-mask-and-simd-into-scan
  (testing "Mixed SIMD + non-SIMD preds: SIMD folds onto scan, mask
            stays as wrapper (PMaskFilter requires the prepare-preds
            pipeline that lift would bypass)."
    (let [data {:k (long-array (range 100))
                :s (into-array String (mapv #(format "row_%03d" %) (range 100)))}
          rows (q/q {:from data
                     :where [[:< :k 50]
                             [:like :s "row_01%"]]
                     :select [:k]})]
      ;; Result is correct regardless of where each pred lives in the IR.
      ;; Rows: k=10..19 → 10 rows.
      (is (= 10 (count rows))))))

;; ============================================================================
;; Index-backed probe: dynamic filter should reduce work
;; ----------------------------------------------------------------------------
;; This is a soft check — the scan exposes :preds via ctx and the
;; chunked aggregate's Java path skips zone-map-pruned chunks. We
;; simply verify correctness on a chunked probe with a build whose
;; key range is much narrower than the probe range.
;; ============================================================================

(deftest dynamic-filter-chunked-probe-correctness
  (testing "Chunked (index-backed) probe with narrow build range — correct sum"
    (let [n 50000
          probe {:k (index/index-from-seq :int64 (range n))
                 :v (index/index-from-seq :float64 (mapv double (range n)))}
          ;; Build keys cover only [1000..1100] — narrow vs probe [0..n-1].
          ;; Selectivity ≈ 100/50000 ≈ 0.002 → push fires.
          build {:bk (long-array (range 1000 1101))}
          plan-tree (physical {:from probe
                               :join [{:with build :on [[:= :k :bk]]}]
                               :agg [[:sum :v]]})
          probe-scan (find-probe-scan plan-tree)
          result (exec/execute-physical plan-tree)]
      ;; Sum of i in 1000..1100 inclusive = (1000+1100)*101/2 = 106050
      (is (= 106050.0 (:sum (first result))))
      ;; Volatile records the derived bounds — proves the runtime push fired.
      (is (= [[:k :gte 1000] [:k :lte 1100]]
             @(:dynamic-filters probe-scan))))))
