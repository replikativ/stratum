(ns stratum.predicate-pushdown-test
  "Tests for predicate pushdown through joins.
   Verifies that predicates on fact/dim sides are correctly pushed below
   the join and applied via masks, producing correct results."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.query.ir :as ir])
  (:import [stratum.query.ir
            LJoin LFilter LScan
            PFusedJoinGroupAgg PFusedJoinGlobalAgg PHashJoin PBitmapSemiJoin]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test data
;; ============================================================================

(defn- make-fact-data
  "10-row fact table: fk cycles 1,2,3; amounts 100..1000."
  []
  {:fk     (long-array [1 2 3 1 2 3 1 2 3 1])
   :amount (double-array [100 200 300 400 500 600 700 800 900 1000])})

(defn- make-dim-data
  "3-row dimension table: id=1,2,3; category=10,20,30."
  []
  {:id       (long-array [1 2 3])
   :category (long-array [10 20 30])})

(defn- run-plan
  "Build logical plan, optimize, execute."
  [query]
  (let [logical  (plan/build-logical-plan query)
        physical (plan/optimize logical)]
    {:result (exec/execute-physical physical)
     :plan   physical}))

(defn- approx=
  ([a b] (approx= a b 1e-6))
  ([a b tol]
   (cond
     (and (nil? a) (nil? b)) true
     (or (nil? a) (nil? b)) false
     :else (<= (Math/abs (- (double a) (double b)))
               (* tol (max 1.0 (Math/abs (double a)) (Math/abs (double b))))))))

;; ============================================================================
;; Probe-side (fact) predicate pushdown
;; ============================================================================

(deftest probe-side-pushdown-group-agg
  (testing "Predicate on fact-side column pushed below join for group-by agg"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          ;; amount > 400 keeps indices 4..9: fk=2,3,1,2,3,1 amounts=500..1000
          ;; cat10(fk=1): 700+1000=1700, cat20(fk=2): 500+800=1300, cat30(fk=3): 600+900=1500
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim :on [:= :fk :id] :type :inner}]
                                  :where [[:> :amount 400.0]]
                                  :group [:category]
                                  :agg [[:sum :amount]]})
          by-cat (into {} (map (fn [r] [(:category r) r]) result))]
      (is (instance? PFusedJoinGroupAgg plan)
          "should fuse to PFusedJoinGroupAgg")
      (is (= 3 (count result)))
      (is (approx= 1700.0 (:sum (get by-cat 10))))
      (is (approx= 1300.0 (:sum (get by-cat 20))))
      (is (approx= 1500.0 (:sum (get by-cat 30)))))))

(deftest probe-side-pushdown-global-agg
  (testing "Predicate on fact-side column pushed below join for global agg"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          ;; amount > 400 → 500+600+700+800+900+1000 = 4500, count=6
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim :on [:= :fk :id] :type :inner}]
                                  :where [[:> :amount 400.0]]
                                  :agg [[:sum :amount]]})]
      (is (instance? PFusedJoinGlobalAgg plan)
          "should fuse to PFusedJoinGlobalAgg")
      (is (approx= 4500.0 (:sum (first result))))
      (is (= 6 (:_count (first result)))))))

;; ============================================================================
;; Build-side (dim) predicate pushdown
;; ============================================================================

(deftest build-side-pushdown-group-agg
  (testing "Predicate on dim-side column pushed below join"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          ;; category > 15 keeps dim rows id=2(cat20), id=3(cat30)
          ;; cat20(fk=2): 200+500+800=1500, cat30(fk=3): 300+600+900=1800
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim :on [:= :fk :id] :type :inner}]
                                  :where [[:> :category 15]]
                                  :group [:category]
                                  :agg [[:sum :amount]]})
          by-cat (into {} (map (fn [r] [(:category r) r]) result))]
      (is (instance? PFusedJoinGroupAgg plan))
      (is (= 2 (count result)))
      (is (approx= 1500.0 (:sum (get by-cat 20))))
      (is (approx= 1800.0 (:sum (get by-cat 30)))))))

;; ============================================================================
;; Both-side predicate pushdown
;; ============================================================================

(deftest both-side-pushdown
  (testing "Predicates on both fact and dim sides pushed independently"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          ;; amount > 400 AND category > 15
          ;; fact rows with amount>400: idx4..9 (fk=2,3,1,2,3,1)
          ;; dim rows with cat>15: id=2(cat20), id=3(cat30)
          ;; Combined: fk=2→cat20(500,800), fk=3→cat30(600,900)
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim :on [:= :fk :id] :type :inner}]
                                  :where [[:> :amount 400.0] [:> :category 15]]
                                  :group [:category]
                                  :agg [[:sum :amount]]})
          by-cat (into {} (map (fn [r] [(:category r) r]) result))]
      (is (instance? PFusedJoinGroupAgg plan))
      (is (= 2 (count result)))
      (is (approx= 1300.0 (:sum (get by-cat 20))))
      (is (approx= 1500.0 (:sum (get by-cat 30)))))))

;; ============================================================================
;; Multi-column join key + predicate pushdown
;; ============================================================================

(deftest multi-col-join-pushdown
  (testing "Predicate pushdown works with multi-column join keys"
    (let [fact {:fk1 (long-array [1 2 1 2 1 2 1 2 1 2])
                :fk2 (long-array [10 20 10 20 10 20 10 20 10 20])
                :val (double-array [100 200 300 400 500 600 700 800 900 1000])}
          dim  {:id1 (long-array [1 2])
                :id2 (long-array [10 20])
                :cat (long-array [1 2])}
          ;; val > 200 → idx2..9 (300..1000)
          ;; fk1=1,fk2=10 → cat1: 300+500+700+900=2400
          ;; fk1=2,fk2=20 → cat2: 400+600+800+1000=2800
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim
                                          :on [[:= :fk1 :id1] [:= :fk2 :id2]]
                                          :type :inner}]
                                  :where [[:> :val 200.0]]
                                  :group [:cat]
                                  :agg [[:sum :val]]})
          by-cat (into {} (map (fn [r] [(:cat r) r]) result))]
      (is (instance? PFusedJoinGroupAgg plan))
      (is (= 2 (count result)))
      (is (approx= 2400.0 (:sum (get by-cat 1))))
      (is (approx= 2800.0 (:sum (get by-cat 2)))))))

(deftest multi-col-join-pushdown-global-agg
  (testing "Multi-column join + global agg with predicate pushdown"
    (let [fact {:fk1 (long-array [1 2 1 2 1])
                :fk2 (long-array [10 20 10 20 10])
                :val (double-array [100 200 300 400 500])}
          dim  {:id1 (long-array [1 2])
                :id2 (long-array [10 20])
                :cat (long-array [1 2])}
          ;; val > 200 → 300+400+500 = 1200, count=3
          {:keys [result]} (run-plan
                            {:from fact
                             :join [{:with dim
                                     :on [[:= :fk1 :id1] [:= :fk2 :id2]]
                                     :type :inner}]
                             :where [[:> :val 200.0]]
                             :agg [[:sum :val]]})]
      (is (approx= 1200.0 (:sum (first result))))
      (is (= 3 (:_count (first result)))))))

;; ============================================================================
;; Non-fused hash join path (projection, no aggregation)
;; ============================================================================

(deftest non-fused-join-pushdown
  (testing "Predicate pushdown works through non-fused hash join (projection)"
    (let [fact {:fk (long-array [1 2 3 1 2])
                :amount (double-array [100 200 300 400 500])}
          dim  {:id (long-array [1 2 3])
                :category (long-array [10 20 30])}
          ;; amount > 200 → (3,300,30), (1,400,10), (2,500,20)
          {:keys [result]} (run-plan
                            {:from fact
                             :join [{:with dim :on [:= :fk :id] :type :inner}]
                             :where [[:> :amount 200.0]]
                             :select [:fk :amount :category]})
          sorted (sort-by :amount result)]
      (is (= 3 (count sorted)))
      (is (= 300.0 (:amount (first sorted))))
      (is (= 30 (:category (first sorted))))
      (is (= 500.0 (:amount (last sorted))))
      (is (= 20 (:category (last sorted)))))))

;; ============================================================================
;; Pushdown IR verification — predicates actually move below the join
;; ============================================================================

(deftest pushdown-ir-structure
  (testing "Predicate pushdown transforms IR: filter moves below join"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             :where [[:> :amount 400.0] [:> :category 15]]
             :group [:category]
             :agg [[:sum :amount]]}
          logical  (plan/build-logical-plan q)
          annotated (plan/annotate logical)
          pushed   (plan/predicate-pushdown annotated)
          ;; After pushdown: LGlobalAgg/LGroupBy > LJoin(left=LFilter, right=LFilter)
          core-input (:input pushed)  ;; should be LJoin (filter removed from above)
          ]
      (is (instance? LJoin core-input)
          "cross-side filter should be removed, leaving LJoin directly")
      (is (instance? LFilter (:left core-input))
          "left (probe) side should have pushed-down filter")
      (is (= [[:amount :gt 400.0]] (:predicates (:left core-input)))
          "left filter should have fact-side predicate")
      (is (instance? LFilter (:right core-input))
          "right (build) side should have pushed-down filter")
      (is (= [[:category :gt 15]] (:predicates (:right core-input)))
          "right filter should have dim-side predicate"))))

;; ============================================================================
;; Cross-side predicates stay above the join
;; ============================================================================

(deftest cross-side-preds-stay-above
  (testing "Predicates referencing both sides are not pushed down"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             ;; This pred references :amount (left) only → pushed
             ;; A cross-pred would reference both sides. Since we don't have
             ;; cross-column predicates in the current pred format, test that
             ;; an unknown-column pred stays above.
             :where [[:> :amount 400.0] [:> :nonexistent 0]]
             :group [:category]
             :agg [[:sum :amount]]}
          logical  (plan/build-logical-plan q)
          annotated (plan/annotate logical)
          pushed   (plan/predicate-pushdown annotated)
          core-input (:input pushed)]
      ;; :nonexistent is in neither side → :cross → stays above as LFilter
      (is (instance? LFilter core-input)
          "cross-side pred should keep filter above join")
      (is (= 1 (count (:predicates core-input)))
          "only the cross-side pred should remain above")
      ;; The left-side pred should be pushed below
      (let [join (:input core-input)]
        (is (instance? LJoin join))
        (is (instance? LFilter (:left join))
            "left-side pred still pushed below")))))

;; ============================================================================
;; Multi-level pushdown through chained joins
;; ============================================================================

(deftest multi-level-pushdown
  (testing "Predicates push through chained joins to reach the correct level"
    (let [fact {:fk1 (long-array [1 2 3 1 2 3])
                :val (double-array [100 200 300 400 500 600])}
          dim1 {:id1 (long-array [1 2 3])
                :fk2 (long-array [10 20 30])}
          dim2 {:id2 (long-array [10 20 30])
                :label (long-array [1 2 3])}
          ;; val>200: keeps rows 2..5 (300,400,500,600)
          ;; label>1: keeps dim2 rows id2=20(label=2), id2=30(label=3)
          ;; Combined: fk1=3→label3(300+600=900 through both joins)
          ;;           fk1=2→label2(500 through both joins)
          {:keys [result plan]} (run-plan
                                 {:from fact
                                  :join [{:with dim1 :on [:= :fk1 :id1] :type :inner}
                                         {:with dim2 :on [:= :fk2 :id2] :type :inner}]
                                  :where [[:> :val 200.0] [:> :label 1]]
                                  :group [:label]
                                  :agg [[:sum :val]]})
          by-label (into {} (map (fn [r] [(:label r) r]) result))]
      (is (= 2 (count result))
          "should have 2 groups (label=2 and label=3)")
      (is (approx= 500.0 (:sum (get by-label 2)))
          "label=2 sum")
      (is (approx= 900.0 (:sum (get by-label 3)))
          "label=3 sum"))))

(deftest multi-level-pushdown-ir-structure
  (testing "Predicates are pushed to correct IR positions through chained joins"
    (let [fact {:fk1 (long-array [1 2 3 1 2])
                :val (double-array [100 200 300 400 500])}
          dim1 {:id1 (long-array [1 2 3])
                :fk2 (long-array [10 20 30])}
          dim2 {:id2 (long-array [10 20 30])
                :label (long-array [1 2 3])}
          q {:from fact
             :join [{:with dim1 :on [:= :fk1 :id1] :type :inner}
                    {:with dim2 :on [:= :fk2 :id2] :type :inner}]
             :where [[:> :val 200.0] [:> :label 1]]
             :group [:label]
             :agg [[:sum :val]]}
          logical  (plan/build-logical-plan q)
          annotated (plan/annotate logical)
          pushed   (plan/predicate-pushdown annotated)
          ;; Structure: LGroupBy > LJoin(outer) > ...
          outer-join (:input pushed)]
      ;; No filter above outer join — all preds pushed down
      (is (instance? LJoin outer-join)
          "all preds should be pushed, no filter above outer join")
      ;; :label > 1 → pushed to outer join's right (dim2)
      (is (instance? LFilter (:right outer-join))
          "dim2-side pred pushed to outer right")
      (is (= [[:label :gt 1]] (:predicates (:right outer-join))))
      ;; :val > 200 → pushed through outer join to inner join's left (fact scan)
      (let [inner-join (:left outer-join)]
        (is (instance? LJoin inner-join)
            "inner join is left child of outer")
        (is (instance? LFilter (:left inner-join))
            "fact-side pred pushed below inner join")
        (is (= [[:val :gt 200.0]] (:predicates (:left inner-join))))))))

;; ============================================================================
;; Consistency: planner result matches legacy non-planner result
;; ============================================================================

(deftest pushdown-matches-legacy
  (testing "Planner with pushdown matches legacy q/q for join+filter+agg"
    (let [n 5000
          fact-fk (long-array (map #(inc (mod % 50)) (range n)))
          fact-val (double-array (map #(* 1.0 %) (range n)))
          dim-id (long-array (range 1 51))
          dim-cat (long-array (map #(mod % 5) (range 50)))
          base-q {:from {:fk fact-fk :v fact-val}
                  :join [{:with {:dk dim-id :cat dim-cat}
                          :on [:= :fk :dk]}]
                  :where [[:> :v 2500.0]]
                  :group [:cat]
                  :agg [[:sum :v]]
                  :order [[:cat :asc]]}
          ;; Planner path
          planner-result (:result (run-plan base-q))
          ;; Legacy path (q/q without planner binding)
          legacy-result (binding [stratum.query/*use-planner* false]
                          (stratum.query/q base-q))]
      (is (= (count planner-result) (count legacy-result))
          "same number of groups")
      (doseq [[p l] (map vector
                         (sort-by :cat planner-result)
                         (sort-by :cat legacy-result))]
        (is (= (:cat p) (:cat l))
            (str "same category: " (:cat p)))
        (is (approx= (:sum p) (:sum l))
            (str "same sum for cat " (:cat p)))))))

;; ============================================================================
;; Bitmap semi-join
;; ============================================================================

(deftest bitmap-semi-join-project
  (testing "PProject over join with unused dim cols → PBitmapSemiJoin"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             :where [[:> :category 15]]
             :select [:fk :amount]}
          {:keys [result plan]} (run-plan q)]
      ;; Plan should use bitmap semi-join
      (is (instance? PBitmapSemiJoin (:input plan))
          "should use PBitmapSemiJoin when dim cols unused in output")
      ;; Only fk=2 (cat=20) and fk=3 (cat=30) survive
      (is (= 6 (count result))
          "6 rows match (4 fk=2 + 3 fk=3 minus one... actually 3+3)")
      (is (every? #{2 3} (map :fk result))
          "only fk=2 and fk=3 survive the dim filter"))))

(deftest bitmap-semi-join-group-agg
  (testing "GroupBy over join with fact-side groups → bitmap semi-join fallback"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             :where [[:> :category 15]]
             :group [:fk]
             :agg [[:sum :amount]]}
          {:keys [result plan]} (run-plan q)
          ;; Manually compute expected: fk=2 rows (200,500,800), fk=3 rows (300,600,900)
          expected {2 1500.0, 3 1800.0}]
      ;; Should NOT use fused join (group keys from fact side)
      (is (not (instance? PFusedJoinGroupAgg plan))
          "fused-join-group-agg needs dim-side group keys")
      ;; Results correct
      (is (= 2 (count result)))
      (doseq [row result]
        (is (approx= (expected (:fk row)) (:sum row))
            (str "sum for fk=" (:fk row)))))))

(deftest bitmap-semi-join-global-agg
  (testing "Global agg over join with unused dim cols → bitmap semi-join"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             :where [[:> :category 15]]
             :agg [[:sum :amount]]}
          {:keys [result]} (run-plan q)
          ;; Expected: fk=2 rows (200,500,800) + fk=3 rows (300,600,900) = 3300
          expected-sum 3300.0]
      (is (= 1 (count result)))
      (is (approx= expected-sum (:sum (first result)))))))

(deftest bitmap-semi-join-not-when-dim-cols-used
  (testing "Hash join retained when dim columns are in output"
    (let [fact (make-fact-data)
          dim  (make-dim-data)
          q {:from fact
             :join [{:with dim :on [:= :fk :id] :type :inner}]
             :where [[:> :category 15]]
             :select [:fk :amount :category]}
          {:keys [plan]} (run-plan q)]
      ;; Should use PHashJoin since :category is a dim col in output
      (is (instance? PHashJoin (:input plan))
          "PHashJoin when dim cols referenced in output"))))
