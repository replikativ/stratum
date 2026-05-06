(ns stratum.asof-join-test
  "Tests for ASOF join: plan-shape, all four operators × {INNER, LEFT},
   tie behavior, no-match handling, multi-col equality keys, radix
   partitioning, and NULL semantics."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.ir :as ir])
  (:import [stratum.query.ir LAsofJoin PAsofJoin]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Plan / IR shape
;; ============================================================================

(deftest asof-builds-LAsofJoin-test
  (testing "build-logical-plan emits LAsofJoin for :type :asof"
    (let [trades {:sym (long-array [1 1 2])
                  :ts  (long-array [10 20 30])}
          prices {:sym (long-array [1 1 2 2])
                  :ts  (long-array [5 15 25 35])
                  :px  (double-array [100.0 110.0 200.0 210.0])}
          plan (plan/build-logical-plan
                {:from trades
                 :join [{:with prices
                         :type :asof
                         :on [[:= :sym :sym]
                              [:>= :ts :ts]]}]})
          n (or (when (instance? LAsofJoin plan) plan) (:input plan))]
      (is (instance? LAsofJoin n))
      (is (= :inner (:join-type n)))
      (is (= [[:sym :sym]] (:on-pairs n)))
      (is (= [:>= :ts :ts] (:match-condition n))))))

(deftest asof-left-builds-LAsofJoin-with-left-type-test
  (testing ":type :asof-left maps to LAsofJoin :join-type :left, no equality keys"
    (let [trades {:ts (long-array [10 20])}
          prices {:ts (long-array [5 15])
                  :px (double-array [1.0 2.0])}
          plan (plan/build-logical-plan
                {:from trades
                 :join [{:with prices
                         :type :asof-left
                         :on [:>= :ts :ts]}]})
          n (or (when (instance? LAsofJoin plan) plan) (:input plan))]
      (is (instance? LAsofJoin n))
      (is (= :left (:join-type n)))
      (is (= [] (:on-pairs n)))
      (is (= [:>= :ts :ts] (:match-condition n))))))

(deftest asof-strategy-selection-test
  (testing "optimize maps LAsofJoin → PAsofJoin"
    (let [trades {:sym (long-array [1 2])
                  :ts  (long-array [10 20])}
          prices {:sym (long-array [1 2])
                  :ts  (long-array [5 15])
                  :px  (double-array [100.0 200.0])}
          phys (plan/optimize
                (plan/build-logical-plan
                 {:from trades
                  :join [{:with prices
                          :type :asof
                          :on [[:= :sym :sym]
                               [:>= :ts :ts]]}]}))
          n (or (when (instance? PAsofJoin phys) phys) (:input phys))]
      (is (instance? PAsofJoin n))
      (is (= [[:sym :sym]] (:on-pairs n)))
      (is (= [:>= :ts :ts] (:match-condition n))))))

(deftest asof-rejects-no-inequality-test
  (testing "ASOF :on with only equalities throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"exactly one inequality"
         (plan/build-logical-plan
          {:from {:ts (long-array [1])}
           :join [{:with {:ts (long-array [1])} :type :asof
                   :on [:= :ts :ts]}]})))))

(deftest asof-rejects-multiple-inequalities-test
  (testing "ASOF :on with two inequalities throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"exactly one inequality"
         (plan/build-logical-plan
          {:from {:ts (long-array [1])}
           :join [{:with {:ts (long-array [1])} :type :asof
                   :on [[:>= :ts :ts] [:<= :ts :ts]]}]})))))

(deftest asof-explain-mentions-match-test
  (testing "explain output includes match condition"
    (let [phys (plan/optimize
                (plan/build-logical-plan
                 {:from {:ts (long-array [1])}
                  :join [{:with {:ts (long-array [1]) :px (double-array [1.0])}
                          :type :asof
                          :on [:>= :ts :ts]}]}))
          s (plan/explain phys)]
      (is (re-find #"PAsofJoin" s))
      (is (re-find #"match=" s)))))

;; ============================================================================
;; End-to-end execution — INNER, >=, no equality keys
;; ============================================================================

(deftest asof-inner-gte-basic-test
  (testing "Probe rows match the most recent build row at-or-before their ts"
    (let [;; trades arrive at ts 5, 15, 30
          probe {:probe_ts (long-array [5 15 30])
                 :qty      (long-array [100 200 300])}
          ;; prices known at ts 0, 10, 20
          build {:build_ts (long-array [0 10 20])
                 :px       (double-array [1.0 2.0 3.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :qty :px]})]
      (is (= 3 (count result)))
      ;; ts=5  → newest build at-or-before 5 is ts=0  → px=1.0
      ;; ts=15 → newest build at-or-before 15 is ts=10 → px=2.0
      ;; ts=30 → newest build at-or-before 30 is ts=20 → px=3.0
      (let [by-ts (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
        (is (= 1.0 (get by-ts 5)))
        (is (= 2.0 (get by-ts 15)))
        (is (= 3.0 (get by-ts 30)))))))

(deftest asof-inner-gte-drops-unmatched-test
  (testing "INNER drops probe rows with no build row at-or-before"
    (let [;; first probe ts is before all build ts → no match → dropped
          probe {:probe_ts (long-array [1 50])}
          build {:build_ts (long-array [10 20 30])
                 :px       (double-array [10.0 20.0 30.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      ;; ts=1 has no match (1 < 10), dropped. ts=50 → newest at-or-before is ts=30.
      (is (= 1 (count result)))
      (is (= 50 (long (:probe_ts (first result)))))
      (is (= 30.0 (:px (first result)))))))

(deftest asof-inner-gte-output-in-probe-input-order-test
  (testing "Output preserves probe input order regardless of internal sort"
    (let [;; deliberately unsorted probe
          probe {:probe_ts (long-array [50 10 30 20])
                 :tag      (long-array [4 1 3 2])}
          build {:build_ts (long-array [5 15 25 35 45])
                 :px       (double-array [10.0 20.0 30.0 40.0 50.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:tag :probe_ts :px]})]
      (is (= 4 (count result)))
      ;; Probe ts=50 → newest build at-or-before 50 is ts=45 → px=50
      ;; Probe ts=10 → newest build at-or-before 10 is ts=5  → px=10
      ;; Probe ts=30 → newest build at-or-before 30 is ts=25 → px=30
      ;; Probe ts=20 → newest build at-or-before 20 is ts=15 → px=20
      (is (= [4 1 3 2] (mapv #(long (:tag %)) result)))
      (is (= [50 10 30 20] (mapv #(long (:probe_ts %)) result)))
      (is (= [50.0 10.0 30.0 20.0] (mapv :px result))))))

(deftest asof-inner-gte-tie-test
  (testing "When multiple build rows share the same ts, ties are non-deterministic"
    (let [probe {:probe_ts (long-array [10])}
          build {:build_ts (long-array [10 10 10])
                 :px       (double-array [1.0 2.0 3.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      ;; Tie-breaker is non-deterministic per DuckDB/Snowflake; just check we got one match
      (is (= 1 (count result)))
      (is (contains? #{1.0 2.0 3.0} (:px (first result)))))))

(deftest asof-inner-gte-empty-build-test
  (testing "Empty build → INNER returns no rows"
    (let [probe {:probe_ts (long-array [10 20 30])}
          build {:build_ts (long-array [])
                 :px       (double-array [])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      (is (= 0 (count result))))))

(deftest asof-inner-gte-empty-probe-test
  (testing "Empty probe → no rows"
    (let [probe {:probe_ts (long-array [])}
          build {:build_ts (long-array [10])
                 :px       (double-array [1.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      (is (= 0 (count result))))))

;; ============================================================================
;; All four operators × {INNER, LEFT}
;; ============================================================================

(defn- run-asof
  "Helper: run an ASOF query with the given op and join-type, return result."
  [op join-type probe build]
  (q/q {:from probe
        :join [{:with build
                :type join-type
                :on [op :probe_ts :build_ts]}]
        :select [:probe_ts :px]}))

(defn- by-probe-ts
  "Index result rows by probe_ts → px for assertion convenience.
   NOTE: this collapses duplicate probe_ts values; only call when probe ts are unique."
  [result]
  (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result))

;; Common fixture: build ts=10,20,30 with px 1,2,3
(def ^:private build-fixture
  {:build_ts (long-array [10 20 30])
   :px       (double-array [1.0 2.0 3.0])})

(deftest asof-inner-gt-strict-before-test
  (testing ":> finds latest build strictly before probe"
    (let [probe {:probe_ts (long-array [9 10 15 20 31])}
          result (run-asof :> :asof probe build-fixture)
          m (by-probe-ts result)]
      ;; ts=9  → no build < 9 → dropped
      ;; ts=10 → no build < 10 → dropped (10 is not strictly before)
      ;; ts=15 → 10 < 15 → 1.0
      ;; ts=20 → 10 < 20 → 1.0 (20 is not strictly before)
      ;; ts=31 → 30 < 31 → 3.0
      (is (= 3 (count result)))
      (is (= 1.0 (get m 15)))
      (is (= 1.0 (get m 20)))
      (is (= 3.0 (get m 31))))))

(deftest asof-inner-lte-test
  (testing ":<= finds earliest build at-or-after probe"
    (let [probe {:probe_ts (long-array [5 10 15 30 31])}
          result (run-asof :<= :asof probe build-fixture)
          m (by-probe-ts result)]
      ;; ts=5  → smallest build ≥ 5 is 10 → 1.0
      ;; ts=10 → 10 (eq) → 1.0
      ;; ts=15 → 20 → 2.0
      ;; ts=30 → 30 → 3.0
      ;; ts=31 → no build ≥ 31 → dropped
      (is (= 4 (count result)))
      (is (= 1.0 (get m 5)))
      (is (= 1.0 (get m 10)))
      (is (= 2.0 (get m 15)))
      (is (= 3.0 (get m 30))))))

(deftest asof-inner-lt-strict-after-test
  (testing ":< finds earliest build strictly after probe"
    (let [probe {:probe_ts (long-array [5 10 15 30 31])}
          result (run-asof :< :asof probe build-fixture)
          m (by-probe-ts result)]
      ;; ts=5  → smallest build > 5 is 10 → 1.0
      ;; ts=10 → smallest build > 10 is 20 → 2.0
      ;; ts=15 → 20 → 2.0
      ;; ts=30 → no build > 30 → dropped
      ;; ts=31 → no build > 31 → dropped
      (is (= 3 (count result)))
      (is (= 1.0 (get m 5)))
      (is (= 2.0 (get m 10)))
      (is (= 2.0 (get m 15))))))

;; ----------------------------------------------------------------------------
;; LEFT outer

(deftest asof-left-gte-keeps-unmatched-test
  (testing "LEFT keeps unmatched probe rows with NULL right-side columns"
    (let [probe {:probe_ts (long-array [5 15 35])}
          result (run-asof :>= :asof-left probe build-fixture)
          ;; ts=5  → no build ≤ 5 → unmatched, px nil
          ;; ts=15 → 10 → px=1.0
          ;; ts=35 → 30 → px=3.0
          tagged (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
      (is (= 3 (count result)))
      (is (nil? (get tagged 5)))
      (is (= 1.0 (get tagged 15)))
      (is (= 3.0 (get tagged 35))))))

(deftest asof-left-gt-keeps-unmatched-test
  (testing "LEFT + :> keeps unmatched"
    (let [probe {:probe_ts (long-array [5 10])}
          result (run-asof :> :asof-left probe build-fixture)
          tagged (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
      ;; ts=5  → no build < 5 → nil
      ;; ts=10 → no build < 10 → nil
      (is (= 2 (count result)))
      (is (nil? (get tagged 5)))
      (is (nil? (get tagged 10))))))

(deftest asof-left-lte-keeps-unmatched-test
  (testing "LEFT + :<= keeps unmatched"
    (let [probe {:probe_ts (long-array [10 31])}
          result (run-asof :<= :asof-left probe build-fixture)
          tagged (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
      ;; ts=10 → 10 → 1.0
      ;; ts=31 → no build ≥ 31 → nil
      (is (= 2 (count result)))
      (is (= 1.0 (get tagged 10)))
      (is (nil? (get tagged 31))))))

(deftest asof-left-lt-keeps-unmatched-test
  (testing "LEFT + :< keeps unmatched"
    (let [probe {:probe_ts (long-array [9 30])}
          result (run-asof :< :asof-left probe build-fixture)
          tagged (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
      ;; ts=9  → 10 → 1.0
      ;; ts=30 → no build > 30 → nil
      (is (= 2 (count result)))
      (is (= 1.0 (get tagged 9)))
      (is (nil? (get tagged 30))))))

;; ----------------------------------------------------------------------------
;; Direction-of-sort sanity: descending probe input still yields probe-input-order output

(deftest asof-input-order-preserved-all-ops-test
  (testing "All four ops preserve probe input order"
    (let [probe {:probe_ts (long-array [30 5 15])}     ; deliberately unsorted
          gte (run-asof :>= :asof-left probe build-fixture)
          gt  (run-asof :>  :asof-left probe build-fixture)
          lte (run-asof :<= :asof-left probe build-fixture)
          lt  (run-asof :<  :asof-left probe build-fixture)]
      (is (= [30 5 15] (mapv #(long (:probe_ts %)) gte)))
      (is (= [30 5 15] (mapv #(long (:probe_ts %)) gt)))
      (is (= [30 5 15] (mapv #(long (:probe_ts %)) lte)))
      (is (= [30 5 15] (mapv #(long (:probe_ts %)) lt))))))

;; ----------------------------------------------------------------------------
;; Tie-handling per operator

(deftest asof-tie-on-equal-ts-test
  (testing "Equal ts: :>= matches, :> doesn't; :<= matches, :< doesn't"
    (let [probe {:probe_ts (long-array [20])}
          gte (run-asof :>= :asof probe build-fixture)
          gt  (run-asof :>  :asof probe build-fixture)
          lte (run-asof :<= :asof probe build-fixture)
          lt  (run-asof :<  :asof probe build-fixture)]
      ;; build has ts=20 (px=2.0)
      (is (= 1 (count gte))) (is (= 2.0 (:px (first gte))))
      ;; :> needs strictly before. Latest before 20 is 10.
      (is (= 1 (count gt)))  (is (= 1.0 (:px (first gt))))
      ;; :<= matches at-or-after; smallest at-or-after 20 is 20.
      (is (= 1 (count lte))) (is (= 2.0 (:px (first lte))))
      ;; :< strictly after; smallest >20 is 30.
      (is (= 1 (count lt)))  (is (= 3.0 (:px (first lt)))))))

;; ============================================================================
;; Equality keys + radix partitioning
;; ============================================================================

(deftest asof-with-equality-key-single-col-test
  (testing "Single equality key: rows match only within the same group"
    ;; Two symbols, 1 and 2. Build has ticks for both; probe wants the latest tick
    ;; for its symbol at-or-before its ts.
    (let [probe {:sym      (long-array [1 2 1 2])
                 :probe_ts (long-array [15 25 35 5])}
          build {:sym      (long-array [1 1 2 2])
                 :build_ts (long-array [10 30 20 40])
                 :px       (double-array [101.0 103.0 202.0 204.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:sym :probe_ts :px]})]
      ;; sym=1 ts=15 → newest sym=1 build at-or-before 15 is ts=10 → 101.0
      ;; sym=2 ts=25 → newest sym=2 build at-or-before 25 is ts=20 → 202.0
      ;; sym=1 ts=35 → newest sym=1 build at-or-before 35 is ts=30 → 103.0
      ;; sym=2 ts=5  → no sym=2 build at-or-before 5 → INNER drops
      (is (= 3 (count result)))
      (let [;; Use [sym ts] tuple as key since :probe_ts may repeat across syms
            m (into {} (map (fn [r] [[(long (:sym r)) (long (:probe_ts r))] (:px r)])) result)]
        (is (= 101.0 (get m [1 15])))
        (is (= 202.0 (get m [2 25])))
        (is (= 103.0 (get m [1 35])))
        (is (nil? (get m [2 5])))))))

(deftest asof-with-equality-key-isolation-test
  (testing "Symbols never cross-match — row in sym=1 must not match build in sym=2 even when ts is closer"
    (let [;; build has sym=2 with very close ts that would tempt a cross-match
          probe {:sym      (long-array [1])
                 :probe_ts (long-array [10])}
          build {:sym      (long-array [2 1])
                 :build_ts (long-array [9 1])     ; sym=2 ts=9 is closer to 10 than sym=1 ts=1
                 :px       (double-array [200.0 100.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:sym :probe_ts :px]})]
      ;; Must match sym=1 only: 100.0 (NOT 200.0 even though closer)
      (is (= 1 (count result)))
      (is (= 100.0 (:px (first result)))))))

(deftest asof-with-multi-col-equality-key-test
  (testing "Multi-col equality key: (sym, exchange) must match exactly"
    (let [probe {:sym      (long-array [1 1 2])
                 :exch     (long-array [1 2 1])
                 :probe_ts (long-array [10 10 10])}
          build {:sym      (long-array [1 1 2 2])
                 :exch     (long-array [1 2 1 2])
                 :build_ts (long-array [5 6 7 8])
                 :px       (double-array [11.0 12.0 21.0 22.0])}
          result (q/q {:from probe
                       :join [{:with build
                               :type :asof
                               :on [[:= :sym :sym]
                                    [:= :exch :exch]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:sym :exch :probe_ts :px]})]
      ;; (1,1) ts=10 → build (1,1,5)  → 11.0
      ;; (1,2) ts=10 → build (1,2,6)  → 12.0
      ;; (2,1) ts=10 → build (2,1,7)  → 21.0
      (is (= 3 (count result)))
      (let [m (into {} (map (fn [r] [[(long (:sym r)) (long (:exch r))] (:px r)])) result)]
        (is (= 11.0 (get m [1 1])))
        (is (= 12.0 (get m [1 2])))
        (is (= 21.0 (get m [2 1])))))))

(deftest asof-equality-key-no-match-INNER-vs-LEFT-test
  (testing "INNER drops sym with no build matches; LEFT keeps with NULL"
    (let [probe {:sym      (long-array [1 2 3])  ; sym=3 has no build entries
                 :probe_ts (long-array [100 100 100])}
          build {:sym      (long-array [1 2])
                 :build_ts (long-array [50 50])
                 :px       (double-array [10.0 20.0])}
          inner (q/q {:from probe
                      :join [{:with build :type :asof
                              :on [[:= :sym :sym]
                                   [:>= :probe_ts :build_ts]]}]
                      :select [:sym :px]})
          left  (q/q {:from probe
                      :join [{:with build :type :asof-left
                              :on [[:= :sym :sym]
                                   [:>= :probe_ts :build_ts]]}]
                      :select [:sym :px]})]
      (is (= 2 (count inner)))
      (is (= 3 (count left)))
      ;; LEFT should have sym=3 with nil px
      (is (some #(and (== 3 (long (:sym %))) (nil? (:px %))) left)))))

(deftest asof-large-cardinality-equality-test
  (testing "Many distinct equality keys exercise the partitioning path"
    (let [n 10000
          syms (long-array (range n))
          ;; build: each sym has a tick at ts = sym (so probe.ts = sym should match exactly)
          probe {:sym      syms
                 :probe_ts (long-array (range n))}
          build {:sym      syms
                 :build_ts (long-array (range n))
                 :px       (double-array (map double (range n)))}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:sym :probe_ts :px]})]
      (is (= n (count result)))
      ;; Every row should match its own sym (px == probe_ts)
      (is (every? #(== (double (:probe_ts %)) (double (:px %))) result)))))

(deftest asof-equality-many-matches-per-group-test
  (testing "Multiple build rows per sym: pick the newest at-or-before per probe row"
    (let [;; sym=1 has 4 build entries at ts=1,2,3,4 with px 11..14
          probe {:sym      (long-array [1 1 1 1])
                 :probe_ts (long-array [4 3 2 1])}    ; descending probe order
          build {:sym      (long-array [1 1 1 1])
                 :build_ts (long-array [1 2 3 4])
                 :px       (double-array [11.0 12.0 13.0 14.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:probe_ts :px]})]
      ;; Output preserves probe input order: [4,3,2,1] → [14,13,12,11]
      (is (= [4 3 2 1] (mapv #(long (:probe_ts %)) result)))
      (is (= [14.0 13.0 12.0 11.0] (mapv :px result))))))

;; ============================================================================
;; NULL handling + edge cases
;; ============================================================================

(def ^:private NULL-LONG Long/MIN_VALUE)

(deftest asof-null-build-ts-filtered-test
  (testing "Build rows with NULL ts (Long/MIN_VALUE) are filtered from match candidates"
    (let [probe {:probe_ts (long-array [10 20])}
          ;; Build has a NULL-ts row that would otherwise be the closest match for ts=10
          build {:build_ts (long-array [NULL-LONG 5 15])
                 :px       (double-array [99.0 50.0 150.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})
          m (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
      ;; ts=10 → newest non-NULL build at-or-before 10 is 5 → 50.0 (NOT 99.0)
      ;; ts=20 → 15 → 150.0
      (is (= 50.0 (get m 10)))
      (is (= 150.0 (get m 20))))))

(deftest asof-null-probe-ts-skipped-INNER-test
  (testing "INNER: probe rows with NULL ts produce no output"
    (let [probe {:probe_ts (long-array [NULL-LONG 15 NULL-LONG])}
          build {:build_ts (long-array [10])
                 :px       (double-array [1.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      ;; Only the ts=15 row survives
      (is (= 1 (count result)))
      (is (= 15 (long (:probe_ts (first result))))))))

(deftest asof-null-probe-ts-skipped-LEFT-test
  (testing "LEFT: probe rows with NULL ts are still skipped (DuckDB lhs-valid-mask behavior)"
    (let [probe {:probe_ts (long-array [NULL-LONG 15])
                 :tag      (long-array [99 1])}
          build {:build_ts (long-array [10])
                 :px       (double-array [1.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof-left
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:tag :probe_ts :px]})]
      ;; Per DuckDB: NULL inequality value on probe → row is filtered, not emitted with NULL.
      (is (= 1 (count result)))
      (is (= 1 (long (:tag (first result))))))))

(deftest asof-null-eq-key-LEFT-emits-null-test
  (testing "LEFT: probe with NULL composite eq key emits with NULL right cols"
    (let [probe {:sym      (long-array [1 NULL-LONG 2])
                 :probe_ts (long-array [10 10 10])
                 :tag      (long-array [101 102 103])}
          build {:sym      (long-array [1 2])
                 :build_ts (long-array [5 5])
                 :px       (double-array [11.0 22.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof-left
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:tag :px]})
          m (into {} (map (fn [r] [(long (:tag r)) (:px r)])) result)]
      (is (= 3 (count result)))
      (is (= 11.0 (get m 101)))
      (is (nil?  (get m 102)))   ; NULL sym → no match, but LEFT keeps it
      (is (= 22.0 (get m 103))))))

(deftest asof-null-eq-key-INNER-dropped-test
  (testing "INNER: probe with NULL composite eq key is dropped"
    (let [probe {:sym      (long-array [1 NULL-LONG])
                 :probe_ts (long-array [10 10])}
          build {:sym      (long-array [1])
                 :build_ts (long-array [5])
                 :px       (double-array [11.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [[:= :sym :sym]
                                    [:>= :probe_ts :build_ts]]}]
                       :select [:sym :px]})]
      (is (= 1 (count result)))
      (is (= 11.0 (:px (first result)))))))

(deftest asof-all-null-build-ts-test
  (testing "All-NULL build ts: INNER returns nothing, LEFT keeps probes with -1"
    (let [probe {:probe_ts (long-array [10 20])}
          build {:build_ts (long-array [NULL-LONG NULL-LONG])
                 :px       (double-array [1.0 2.0])}
          inner (q/q {:from probe
                      :join [{:with build :type :asof
                              :on [:>= :probe_ts :build_ts]}]
                      :select [:probe_ts :px]})
          left  (q/q {:from probe
                      :join [{:with build :type :asof-left
                              :on [:>= :probe_ts :build_ts]}]
                      :select [:probe_ts :px]})]
      (is (= 0 (count inner)))
      (is (= 2 (count left)))
      (is (every? #(nil? (:px %)) left)))))

(deftest asof-single-row-each-side-test
  (testing "Single row build, single row probe"
    (let [probe {:probe_ts (long-array [10])}
          build {:build_ts (long-array [5])
                 :px       (double-array [42.0])}
          inner (q/q {:from probe
                      :join [{:with build :type :asof
                              :on [:>= :probe_ts :build_ts]}]
                      :select [:px]})]
      (is (= 1 (count inner)))
      (is (= 42.0 (:px (first inner)))))))

(deftest asof-double-ts-column-test
  (testing "ASOF works on double[] timestamp columns (converted to long)"
    (let [probe {:probe_ts (double-array [1.5 2.5 3.5])}
          build {:build_ts (double-array [1.0 2.0 3.0])
                 :px       (double-array [10.0 20.0 30.0])}
          result (q/q {:from probe
                       :join [{:with build :type :asof
                               :on [:>= :probe_ts :build_ts]}]
                       :select [:probe_ts :px]})]
      ;; double 1.5 → long 1; latest build long ≤ 1 is 1 → 10.0
      ;; double 2.5 → long 2; latest build long ≤ 2 is 2 → 20.0
      ;; double 3.5 → long 3; latest build long ≤ 3 is 3 → 30.0
      (is (= 3 (count result)))
      (is (= [10.0 20.0 30.0] (mapv :px result))))))
