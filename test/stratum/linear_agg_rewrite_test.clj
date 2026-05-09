(ns stratum.linear-agg-rewrite-test
  "Tests for F21-A — algebraic rewrite of `SUM/AVG/MIN/MAX(s·col + o)`
   into a base agg on `col` plus a decode-time recipe. The rewrite
   eliminates the otherwise-mandatory `PMaterializeExpr` shim and is
   sound across all global-agg execution paths (scalar, fused SIMD,
   fused multi-sum, stats-only)."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.query.executor :as exec]
            [stratum.index :as index])
  (:import [stratum.query.ir
            PScan PChunkedScan PScalarAgg PFusedSIMDAgg PFusedMultiSum
            PStatsOnlyAgg PChunkedSIMDAgg PMaterializeExpr]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- physical [query] (-> query plan/build-logical-plan plan/optimize))

(defn- recipe-on
  "Return the `:linear-recipe` metadata attached to the first agg of
   the (optimized) physical node, or nil. Different physical
   strategies use either `:agg` (single) or `:aggs` (multi) — try
   both."
  [phys]
  (let [agg (or (:agg phys) (some-> phys :aggs first))]
    (:linear-recipe (meta agg))))

;; The two interesting size regimes — under the SIMD threshold (1000)
;; the planner picks `PScalarAgg`, above it picks `PFusedSIMDAgg` /
;; `PFusedMultiSum`. Both decoders must apply the recipe.
(def small-n 5)
(def large-n 2000)

(defn- mk-long-arr [n] (long-array (range n)))
(defn- mk-double-arr [n] (double-array (mapv double (range n))))

;; ============================================================================
;; Recipe shapes — exhaustive matcher coverage
;; ============================================================================

(deftest matcher-covers-canonical-shapes
  (let [data {:x (mk-long-arr large-n)}
        cases [;; [query-expr expected-recipe-keys-subset]
               [[:* :x 2]   {:scale 2.0  :offset 0.0}]
               [[:* 2 :x]   {:scale 2.0  :offset 0.0}]
               [[:+ :x 5]   {:scale 1.0  :offset 5.0}]
               [[:+ 5 :x]   {:scale 1.0  :offset 5.0}]
               [[:- :x 5]   {:scale 1.0  :offset -5.0}]
               [[:- 5 :x]   {:scale -1.0 :offset 5.0}]
               [[:/ :x 2]   {:scale 0.5  :offset 0.0}]]]
    (doseq [[expr expected] cases]
      (let [phys (physical {:from data :agg [[:sum expr]]})
            r    (recipe-on phys)]
        (is (some? r) (str "recipe expected for " expr))
        (is (= (:scale expected) (:scale r)) (str "scale for " expr))
        (is (= (:offset expected) (:offset r)) (str "offset for " expr))))))

(deftest matcher-rejects-non-linear
  (let [data {:x (mk-long-arr large-n) :y (mk-long-arr large-n)}]
    (testing "two-column expr is not linear in either"
      (is (nil? (recipe-on (physical {:from data :agg [[:sum [:+ :x :y]]]}))))
      ;; [:* :x :y] is converted by normalize-agg to :sum-product, also non-linear
      )
    (testing "constant divided by column is not linear"
      (is (nil? (recipe-on (physical {:from data :agg [[:sum [:/ 2 :x]]]})))))
    (testing "no expr — pass-through, no recipe"
      (is (nil? (recipe-on (physical {:from data :agg [[:sum :x]]})))))))

;; ============================================================================
;; SUM correctness across regimes
;; ============================================================================

(deftest sum-linear-correctness
  (testing "SUM(s·x + o) = s·SUM(x) + o·N across scalar and SIMD paths"
    (doseq [n [small-n large-n]]
      (let [data {:x (mk-long-arr n)}
            sum-x (reduce + 0 (range n))]
        (is (= (* 2.0 sum-x)
               (:sum (first (q/q {:from data :agg [[:sum [:* :x 2]]]}))))
            (str "SUM(2x) at n=" n))
        (is (= (+ (double sum-x) (* 5.0 n))
               (:sum (first (q/q {:from data :agg [[:sum [:+ :x 5]]]}))))
            (str "SUM(x+5) at n=" n))
        (is (= (- (* 100.0 n) (double sum-x))
               (:sum (first (q/q {:from data :agg [[:sum [:- 100 :x]]]}))))
            (str "SUM(100-x) at n=" n))))))

(deftest avg-linear-correctness
  (testing "AVG(s·x + o) = s·AVG(x) + o"
    (doseq [n [small-n large-n]]
      (let [data {:x (mk-long-arr n)}
            avg-x (/ (reduce + 0.0 (range n)) (double n))]
        (is (= (* 2.0 avg-x)
               (:avg (first (q/q {:from data :agg [[:avg [:* :x 2]]]}))))
            (str "AVG(2x) at n=" n))
        (is (= (+ avg-x 7.0)
               (:avg (first (q/q {:from data :agg [[:avg [:+ :x 7]]]}))))
            (str "AVG(x+7) at n=" n))))))

(deftest min-max-monotonicity
  (testing "MIN/MAX with positive scale: same op, with negative scale: flipped op"
    (doseq [n [small-n large-n]]
      (let [data {:x (mk-long-arr n)}
            min-x (double 0)
            max-x (double (dec n))]
        (is (= (* 2.0 min-x) (:min (first (q/q {:from data :agg [[:min [:* :x 2]]]})))))
        (is (= (* 2.0 max-x) (:max (first (q/q {:from data :agg [[:max [:* :x 2]]]})))))
        ;; Negative scale flips op internally
        (is (= (* -2.0 max-x) (:min (first (q/q {:from data :agg [[:min [:* :x -2]]]})))))
        (is (= (* -2.0 min-x) (:max (first (q/q {:from data :agg [[:max [:* :x -2]]]})))))
        ;; Offset
        (is (= (+ min-x 10.0) (:min (first (q/q {:from data :agg [[:min [:+ :x 10]]]})))))))))

;; ============================================================================
;; Result aliasing
;; ============================================================================

(deftest preserves-user-alias
  (let [data {:x (mk-long-arr large-n)}]
    (testing "user-supplied :as wins"
      (is (contains? (first (q/q {:from data :agg [[:as [:sum [:* :x 2]] :doubled]]}))
                     :doubled)))
    (testing "no :as — user-facing op is the original (sum/min/max), even when internal op flipped"
      (is (contains? (first (q/q {:from data :agg [[:sum [:* :x 2]]]})) :sum))
      (is (contains? (first (q/q {:from data :agg [[:min [:* :x -2]]]})) :min))
      (is (contains? (first (q/q {:from data :agg [[:max [:* :x -2]]]})) :max)))))

;; ============================================================================
;; All execution paths apply recipes
;; ============================================================================

(deftest scalar-path-applies-recipe
  (let [data {:x (mk-long-arr small-n)}
        phys (physical {:from data :agg [[:sum [:* :x 2]]]})]
    (is (instance? PScalarAgg phys))
    ;; SUM(2*[0..4]) = 2*10 = 20
    (is (= 20.0 (:sum (first (q/q {:from data :agg [[:sum [:* :x 2]]]})))))))

(deftest fused-simd-path-applies-recipe
  (let [data {:x (mk-long-arr large-n)}
        phys (physical {:from data :agg [[:sum [:* :x 2]]]})]
    (is (instance? PFusedSIMDAgg phys))
    (is (= 3998000.0 (:sum (first (q/q {:from data :agg [[:sum [:* :x 2]]]})))))))

(deftest multi-sum-path-applies-recipes
  (let [data {:x (mk-long-arr large-n)}
        phys (physical {:from data :agg [[:as [:sum [:* :x 2]] :s2]
                                         [:as [:avg [:+ :x 1]] :a1]]})
        result (first (q/q {:from data :agg [[:as [:sum [:* :x 2]] :s2]
                                             [:as [:avg [:+ :x 1]] :a1]]}))
        sum-x (reduce + 0 (range large-n))]
    (is (instance? PFusedMultiSum phys))
    (is (= (* 2.0 sum-x) (:s2 result)) "SUM(2x) — multi-agg path")
    (is (= (+ (/ (double sum-x) large-n) 1.0) (:a1 result))
        "AVG(x+1) — multi-agg path")))

(deftest stats-only-path-applies-recipe
  ;; Index-backed columns with no preds → PStatsOnlyAgg path.
  (let [data {:x (index/index-from-array (mk-long-arr large-n))}
        phys (physical {:from data :agg [[:sum [:* :x 2]]]})]
    (is (instance? PStatsOnlyAgg phys))
    (is (= 3998000.0 (:sum (first (q/q {:from data :agg [[:sum [:* :x 2]]]})))))))

(deftest chunked-simd-path-applies-recipe
  ;; Index-backed columns WITH preds → chunked path. Single agg →
  ;; PChunkedSIMDAgg → format-fused-result.
  (let [data {:x (index/index-from-array (mk-long-arr large-n))}
        phys (physical {:from data :where [[:< :x 100]] :agg [[:sum [:* :x 2]]]})
        sum-x (reduce + 0 (range 100))]
    (is (instance? PChunkedSIMDAgg phys))
    (is (= (* 2.0 sum-x)
           (:sum (first (q/q {:from data :where [[:< :x 100]] :agg [[:sum [:* :x 2]]]})))))))

;; ============================================================================
;; Empty-result NULL semantics
;; ============================================================================

(deftest empty-result-returns-nil
  (let [data {:x (mk-long-arr large-n)}]
    (testing "WHERE matches no rows → recipe still produces nil for SUM/AVG/MIN/MAX"
      (is (nil? (:sum (first (q/q {:from data :where [[:< :x -1]]
                                   :agg [[:sum [:* :x 2]]]})))))
      (is (nil? (:avg (first (q/q {:from data :where [[:< :x -1]]
                                   :agg [[:avg [:+ :x 5]]]})))))
      (is (nil? (:min (first (q/q {:from data :where [[:< :x -1]]
                                   :agg [[:min [:* :x 2]]]}))))))))

;; ============================================================================
;; Plan structure: PMaterializeExpr is bypassed
;; ============================================================================

(deftest rewrite-bypasses-materialize-expr
  (testing "Linear-rewrite eliminates the temp-column materialisation"
    (let [data {:x (mk-long-arr large-n)}
          ;; Without rewrite, [:sum [:* :x 2]] would force a PMaterializeExpr
          ;; child. After rewrite, the agg's :expr is gone, so no
          ;; PMaterializeExpr is inserted.
          phys (physical {:from data :agg [[:sum [:* :x 2]]]})
          has-mat-expr? (loop [n phys]
                          (cond
                            (nil? n) false
                            (instance? PMaterializeExpr n) true
                            :else (recur (:input n))))]
      (is (false? has-mat-expr?)
          "rewritten linear agg should not insert PMaterializeExpr"))))

;; ============================================================================
;; Group-by: per-group recipe application
;; ============================================================================

(defn- expected-by-cat
  [n-rows n-cats f]
  (into {} (for [c (range n-cats)]
             [c (f (filterv #(= c (mod % n-cats)) (range n-rows)))])))

(deftest group-by-linear-correctness
  (let [n      large-n
        n-cats 5
        data   {:cat (long-array (mapv #(mod % n-cats) (range n)))
                :x   (mk-long-arr n)}
        sum-results (q/q {:from data :group [:cat] :agg [[:sum [:* :x 2]]]})
        avg-results (q/q {:from data :group [:cat] :agg [[:as [:avg [:+ :x 5]] :a]]})
        min-results (q/q {:from data :group [:cat] :agg [[:min [:* :x -2]]]})
        sum-by-cat (into {} (map (juxt :cat :sum) sum-results))
        avg-by-cat (into {} (map (juxt :cat :a) avg-results))
        min-by-cat (into {} (map (juxt :cat :min) min-results))]
    (testing "GROUP BY cat, SUM(2*x)"
      (let [expected (expected-by-cat n n-cats #(* 2.0 (reduce + 0.0 %)))]
        (doseq [c (range n-cats)]
          (is (= (get expected c) (get sum-by-cat c))
              (str "cat " c)))))
    (testing "GROUP BY cat, AVG(x+5)"
      (let [expected (expected-by-cat
                      n n-cats
                      #(+ 5.0 (/ (reduce + 0.0 %) (double (count %)))))]
        (doseq [c (range n-cats)]
          (is (= (get expected c) (get avg-by-cat c))
              (str "cat " c)))))
    (testing "GROUP BY cat, MIN(-2*x) (op flip via negative scale)"
      ;; MIN(-2x) over each group's x values = -2 * MAX(x in group)
      (let [expected (expected-by-cat
                      n n-cats
                      #(* -2.0 (double (apply max %))))]
        (doseq [c (range n-cats)]
          (is (= (get expected c) (get min-by-cat c))
              (str "cat " c)))))))

(deftest group-by-bypasses-materialize-expr
  (let [data {:cat (long-array [0 1 0 1])
              :x   (long-array [1 2 3 4])}
        phys (physical {:from data :group [:cat] :agg [[:sum [:* :x 2]]]})
        has-mat-expr? (loop [n phys]
                        (cond
                          (nil? n) false
                          (instance? PMaterializeExpr n) true
                          :else (recur (:input n))))]
    (is (false? has-mat-expr?)
        "group-by linear rewrite eliminates PMaterializeExpr too")))

(deftest group-by-columnar-output
  (let [data {:cat (long-array [0 1 0 1 0 1])
              :x   (long-array [1 2 3 4 5 6])}
        r    (q/q {:from data :group [:cat] :agg [[:sum [:* :x 2]]] :result :columns})
        sums (vec (:sum r))]
    (testing "columnar group-by output applies recipe element-wise"
      ;; cat 0: x={1,3,5} → sum*2 = 18; cat 1: x={2,4,6} → sum*2 = 24
      (is (= 2 (:n-rows r)))
      (is (= [18.0 24.0] sums)))))
