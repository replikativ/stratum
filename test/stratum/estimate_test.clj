(ns stratum.estimate-test
  "Tests for selectivity estimation (zone-map + sample + heuristic)."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query.estimate :as est]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.index :as index]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- approx=
  "Check if two numbers are approximately equal within relative tolerance."
  ([a b] (approx= a b 0.15))
  ([a b ^double tol]
   (let [a (double a) b (double b)]
     (if (and (zero? a) (zero? b))
       true
       (<= (Math/abs (- a b))
           (* tol (max 1.0 (Math/abs a) (Math/abs b))))))))

(defn- make-index-columns
  "Create index-backed columns from data map. Returns columns + length."
  [data-map & {:keys [chunk-size] :or {chunk-size 8192}}]
  (let [columns (into {}
                      (map (fn [[k v]]
                             (let [arr (cond (instance? (Class/forName "[J") v) v
                                             (instance? (Class/forName "[D") v) v
                                             :else (long-array v))
                                   dtype (if (instance? (Class/forName "[D") arr) :float64 :int64)
                                   idx (index/index-from-array arr {:chunk-size chunk-size})]
                               [k {:type dtype :index idx :source :index}])))
                      data-map)
        length (count (val (first data-map)))]
    [columns length]))

(defn- make-array-columns
  "Create array-backed columns. Returns columns + length."
  [data-map]
  (let [columns (into {}
                      (map (fn [[k v]]
                             (let [arr (cond (instance? (Class/forName "[J") v) v
                                             (instance? (Class/forName "[D") v) v
                                             :else (long-array v))]
                               [k {:type (if (instance? (Class/forName "[D") arr) :float64 :int64)
                                   :data arr}])))
                      data-map)
        length (count (val (first data-map)))]
    [columns length]))

;; ============================================================================
;; Zone-map estimation tests (Tier 1)
;; ============================================================================

(deftest zone-map-range-selectivity
  (testing "Zone-map estimation for range predicates on index columns"
    (let [[columns _] (make-index-columns
                       {:val (long-array (range 100000))})]
      ;; :gt 50000 → ~50% selectivity
      (is (approx= 0.50 (est/estimate-selectivity [:val :gt 50000.0] columns)))
      ;; :lt 10000 → ~10%
      (is (approx= 0.10 (est/estimate-selectivity [:val :lt 10000.0] columns)))
      ;; :gt 99000 → ~1%
      (is (approx= 0.01 (est/estimate-selectivity [:val :gt 99000.0] columns)))
      ;; :lt 1000 → ~1%
      (is (approx= 0.01 (est/estimate-selectivity [:val :lt 1000.0] columns))))))

(deftest zone-map-range-selectivity-range-pred
  (testing "Zone-map estimation for BETWEEN predicate"
    (let [[columns _] (make-index-columns
                       {:val (long-array (range 100000))})]
      ;; :range 20000 40000 → ~20%
      (is (approx= 0.20 (est/estimate-selectivity [:val :range 20000.0 40000.0] columns)))
      ;; :range 0 99999 → ~100%
      (is (approx= 1.0 (est/estimate-selectivity [:val :range 0.0 99999.0] columns))))))

(deftest zone-map-equality-selectivity
  (testing "Zone-map estimation for equality on sorted column"
    (let [[columns _] (make-index-columns
                       {:val (long-array (range 100000))})]
      ;; Equality on unique sorted data → very selective
      (is (< (est/estimate-selectivity [:val :eq 50000.0] columns) 0.001)))))

;; ============================================================================
;; Sample-based estimation tests (Tier 2)
;; ============================================================================

(deftest sample-array-selectivity
  (testing "Sample-based estimation for array-backed columns"
    (let [[columns _] (make-array-columns
                       {:val (double-array (repeatedly 50000 #(* 100.0 (rand))))})]
      ;; :gt 50.0 → ~50%
      (is (approx= 0.50 (est/estimate-selectivity [:val :gt 50.0] columns) 0.20))
      ;; :lt 20.0 → ~20%
      (is (approx= 0.20 (est/estimate-selectivity [:val :lt 20.0] columns) 0.20)))))

(deftest sample-categorical-selectivity
  (testing "Sample-based estimation for categorical data"
    (let [[columns _] (make-array-columns
                       {:cat (long-array (repeatedly 50000 #(rand-int 10)))})]
      ;; :gt 8 → ~10% (only value 9 passes)
      (is (approx= 0.10 (est/estimate-selectivity [:cat :gt 8.0] columns) 0.20))
      ;; :in [1 2 3] → ~30%
      (is (approx= 0.30 (est/estimate-selectivity [:cat :in [1.0 2.0 3.0]] columns) 0.20)))))

(deftest sample-index-selectivity
  (testing "Sample-based fallback for unsorted index columns"
    (let [[columns _] (make-index-columns
                       {:cat (long-array (repeatedly 50000 #(rand-int 20)))})]
      ;; Zone maps can't prune (all chunks have min~0, max~19)
      ;; But sample works: :eq 5 → ~5%
      (is (approx= 0.05 (est/estimate-selectivity [:cat :eq 5.0] columns) 0.25)))))

;; ============================================================================
;; Heuristic fallback tests (Tier 3)
;; ============================================================================

(deftest heuristic-fallback
  (testing "Heuristic estimate for unknown columns"
    (let [columns {}]
      (is (= 0.33 (est/estimate-selectivity [:unknown :gt 5] columns)))
      (is (= 0.02 (est/estimate-selectivity [:unknown :eq 5] columns)))
      (is (= 0.50 (est/estimate-selectivity [:unknown :fn identity] columns))))))

;; ============================================================================
;; Combined selectivity
;; ============================================================================

(deftest combined-selectivity
  (testing "Combined selectivity (independence assumption)"
    (let [[columns _] (make-index-columns
                       {:a (long-array (range 100000))
                        :b (long-array (range 100000))})]
      ;; Two 50% predicates → ~25% combined
      (is (approx= 0.25 (est/estimate-combined-selectivity
                         [[:a :gt 50000.0] [:b :gt 50000.0]]
                         columns)
                   0.10))
      ;; No preds → 1.0
      (is (= 1.0 (est/estimate-combined-selectivity [] columns))))))

(deftest output-row-estimation
  (testing "Estimated output rows"
    (let [[columns _] (make-index-columns
                       {:val (long-array (range 100000))})]
      ;; :gt 99000 on 100K rows → ~1000 estimated rows
      (let [est (est/estimate-output-rows [[:val :gt 99000.0]] columns 100000)]
        (is (> est 500))
        (is (< est 2000)))
      ;; No preds → full length
      (is (= 100000 (est/estimate-output-rows [] columns 100000))))))

;; ============================================================================
;; OR predicate estimation
;; ============================================================================

(deftest or-selectivity
  (testing "OR predicate uses 1 - product(1 - sub_sel)"
    (let [[columns _] (make-index-columns
                       {:val (long-array (range 100000))})]
      ;; OR of val > 90000 (~10%) and val < 10000 (~10%) → ~20%
      ;; No overlap, so should be close to sum
      (is (approx= 0.20
                   (est/estimate-selectivity
                    [:__or :or [:val :gt 90000.0] [:val :lt 10000.0]]
                    columns)
                   0.10)))))

;; ============================================================================
;; Explain shows estimates
;; ============================================================================

(deftest explain-shows-selectivity
  (testing "Plan explain output includes selectivity annotations"
    (let [;; Use raw arrays for :from (build-logical-plan expects raw data)
          q {:from {:val (long-array (range 100000))
                    :cat (long-array (repeatedly 100000 #(rand-int 10)))}
             :where [[:> :val 90000]]
             :group [:cat]
             :agg [[:sum :val]]}
          logical  (plan/build-logical-plan q)
          physical (plan/optimize logical)
          explanation (plan/explain physical)]
      ;; Should contain selectivity info
      (is (re-find #"sel=" explanation)
          "explain should show selectivity estimate"))))

;; ============================================================================
;; Integration: estimation doesn't change results
;; ============================================================================

(deftest estimation-preserves-correctness
  (testing "Adding selectivity estimation doesn't change query results"
    (let [n 10000
          vals (double-array (repeatedly n #(* 100.0 (rand))))
          cats (long-array (repeatedly n #(rand-int 5)))
          q {:from {:val vals :cat cats}
             :where [[:> :val 50.0]]
             :group [:cat]
             :agg [[:sum :val]]}
          result (exec/run-query q false)
          ;; Verify by computing manually
          manual (reduce (fn [acc i]
                           (let [v (aget vals i)
                                 c (aget cats i)]
                             (if (> v 50.0)
                               (update acc c (fnil + 0.0) v)
                               acc)))
                         {} (range n))]
      (doseq [row result]
        (let [cat (:cat row)
              expected (get manual cat)]
          (is (approx= expected (:sum row) 0.001)
              (str "sum for cat " cat " should match")))))))
