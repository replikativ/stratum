(ns stratum.generative-test
  "Generative (property-based) tests for the Stratum query engine.
   Verifies SIMD/parallel paths match naive Clojure reference implementations."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [stratum.query :as q])
  (:import [stratum.internal ColumnOps]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- approx=
  "Compare two numbers with relative tolerance."
  ([a b] (approx= a b 1e-6))
  ([a b tol]
   (cond
     (and (nil? a) (nil? b)) true
     (or (nil? a) (nil? b)) false
     (and (Double/isNaN (double a)) (Double/isNaN (double b))) true
     (Double/isNaN (double a)) false
     (Double/isNaN (double b)) false
     :else (<= (Math/abs (- (double a) (double b)))
               (* tol (max 1.0 (Math/abs (double a)) (Math/abs (double b))))))))

;; ============================================================================
;; Generators
;; ============================================================================

(def gen-small-double
  "Generates doubles in a reasonable range to avoid overflow."
  (gen/double* {:min -1000.0 :max 1000.0 :NaN? false :infinite? false}))

(def gen-pred-op
  "Generates a predicate operator."
  (gen/elements [:< :> :<= :>= := :!=]))

(def gen-agg-op
  "Generates a simple aggregation operator."
  (gen/elements [:sum :count :min :max :avg]))

;; ============================================================================
;; C1. Filter+aggregate oracle
;; ============================================================================

(defn- ref-agg
  "Reference aggregation on a seq of doubles."
  [op vals]
  (let [n (count vals)]
    (case op
      :sum (if (zero? n) nil (reduce + 0.0 vals))
      :count (long n)
      :min (if (zero? n) nil (apply min vals))
      :max (if (zero? n) nil (apply max vals))
      :avg (if (zero? n) nil (/ (reduce + 0.0 vals) n)))))

(defn- ref-pred
  "Reference predicate evaluation."
  [op val bound]
  (case op
    :< (< val bound)
    :> (> val bound)
    :<= (<= val bound)
    :>= (>= val bound)
    := (== val bound)
    :!= (not= val bound)))

(def gen-filter-agg-scenario
  "Generates a filter+aggregate test scenario."
  (gen/let [n (gen/choose 1 5000)
            vals (gen/vector gen-small-double n)
            agg-op gen-agg-op
            pred-op gen-pred-op
            pred-bound gen-small-double]
    {:vals vals
     :agg-op agg-op
     :pred-op pred-op
     :pred-bound pred-bound}))

(defspec c1-filter-aggregate-oracle 200
  (prop/for-all [scenario gen-filter-agg-scenario]
                (let [{:keys [vals agg-op pred-op pred-bound]} scenario
                      arr (double-array vals)
          ;; Reference: filter then aggregate
                      filtered (filter #(ref-pred pred-op % pred-bound) vals)
                      expected (ref-agg agg-op filtered)
          ;; Query engine
                      result (q/q {:from {:v arr}
                                   :where [[pred-op :v pred-bound]]
                                   :agg [[agg-op :v]]})]
                  (if (and (nil? expected) (some? result))
        ;; nil from ref means no matches for min/max → query returns nil
                    (let [actual (get (first result) agg-op)]
                      (nil? actual))
                    (let [actual (get (first result) agg-op)]
                      (approx= actual expected 1e-4))))))

;; ============================================================================
;; C2. Group-by oracle
;; ============================================================================

(def gen-group-by-scenario
  "Generates a group-by test scenario."
  (gen/let [n (gen/choose 100 5000)
            n-groups (gen/choose 2 50)
            vals (gen/vector gen-small-double n)
            agg-op (gen/elements [:sum :count :min :max :avg])]
    {:n n
     :n-groups n-groups
     :vals vals
     :agg-op agg-op}))

(defspec c2-group-by-oracle 100
  (prop/for-all [scenario gen-group-by-scenario]
                (let [{:keys [n n-groups vals agg-op]} scenario
                      groups-vec (mapv #(mod % n-groups) (range n))
                      groups-arr (long-array groups-vec)
                      vals-arr (double-array vals)
          ;; Reference: group then aggregate
                      grouped (group-by first (map vector groups-vec vals))
                      ref-results (into {}
                                        (for [[g rows] grouped]
                                          [g (ref-agg agg-op (mapv second rows))]))
          ;; Query engine
                      result (q/q {:from {:g groups-arr :v vals-arr}
                                   :group [:g]
                                   :agg [[agg-op :v]]
                                   :order [[:g :asc]]})]
                  (and (= (count ref-results) (count result))
                       (every? (fn [row]
                                 (let [g (:g row)
                                       actual (get row agg-op)
                                       expected (get ref-results g)]
                                   (approx= actual expected 1e-4)))
                               result)))))

;; ============================================================================
;; C3. Multi-agg consistency
;; ============================================================================

(def gen-multi-agg-scenario
  "Generates a multi-agg test scenario."
  (gen/let [n (gen/choose 100 5000)
            vals (gen/vector gen-small-double n)
            ;; Generate 2-4 agg ops (can repeat)
            agg-ops (gen/vector (gen/elements [:sum :count :min :max :avg]) 2 4)]
    {:vals vals
     :agg-ops agg-ops}))

(defspec c3-multi-agg-consistency 100
  (prop/for-all [scenario gen-multi-agg-scenario]
                (let [{:keys [vals agg-ops]} scenario
                      arr (double-array vals)
          ;; Individual queries
                      individual-results (mapv (fn [op]
                                                 (let [r (q/q {:from {:v arr}
                                                               :agg [[op :v]]})]
                                                   (get (first r) op)))
                                               agg-ops)
          ;; Multi-agg query
                      multi-result (q/q {:from {:v arr}
                                         :agg (mapv (fn [op] [op :v]) agg-ops)})
                      multi-row (first multi-result)]
      ;; When all ops are the same, auto-aliasing appends _v
      ;; We check that values are present and match
                  (let [distinct-ops (distinct agg-ops)]
                    (every? (fn [op]
                              (let [expected (ref-agg op vals)
                        ;; Find the value in multi-result - could be :op or :op_v
                                    actual (or (get multi-row op)
                                               (get multi-row (keyword (str (name op) "_v"))))]
                                (approx= actual expected 1e-4)))
                            distinct-ops)))))

;; ============================================================================
;; C4. Join oracle
;; ============================================================================

(def gen-join-scenario
  "Generates a join test scenario."
  (gen/let [n-fact (gen/choose 50 2000)
            n-dim (gen/choose 5 50)
            fact-keys (gen/vector (gen/choose 0 (dec n-dim)) n-fact)
            fact-vals (gen/vector gen-small-double n-fact)
            dim-vals (gen/vector gen-small-double n-dim)
            join-type (gen/elements [:inner :left])]
    {:n-fact n-fact
     :n-dim n-dim
     :fact-keys fact-keys
     :fact-vals fact-vals
     :dim-vals dim-vals
     :join-type join-type}))

(defspec c4-join-oracle 50
  (prop/for-all [scenario gen-join-scenario]
                (let [{:keys [n-dim fact-keys fact-vals dim-vals join-type]} scenario
                      fk-arr (long-array fact-keys)
                      fv-arr (double-array fact-vals)
                      dk-arr (long-array (range n-dim))
                      dv-arr (double-array dim-vals)
          ;; Reference: naive nested-loop join
                      ref-pairs (case join-type
                                  :inner (for [[i fk] (map-indexed vector fact-keys)
                                               :let [dk-idx fk]
                                               :when (< dk-idx n-dim)]
                                           {:fv (nth fact-vals i)
                                            :dv (nth dim-vals dk-idx)})
                                  :left (for [[i fk] (map-indexed vector fact-keys)
                                              :let [dk-idx fk
                                                    matched? (< dk-idx n-dim)]]
                                          {:fv (nth fact-vals i)
                                           :dv (if matched? (nth dim-vals dk-idx) nil)}))
                      ref-count (count ref-pairs)
          ;; Query engine
                      result (q/q {:from {:fk fk-arr :fv fv-arr}
                                   :join [{:with {:dk dk-arr :dv dv-arr}
                                           :on [:= :fk :dk]
                                           :type join-type}]
                                   :agg [[:count]]})]
                  (= ref-count (long (:count (first result)))))))

;; ============================================================================
;; C5. Predicate composition — AND = intersection
;; ============================================================================

(def gen-pred-composition-scenario
  "Generates a predicate composition test scenario."
  (gen/let [n (gen/choose 100 5000)
            vals (gen/vector gen-small-double n)
            op1 (gen/elements [:< :> :<= :>=])
            bound1 gen-small-double
            op2 (gen/elements [:< :> :<= :>=])
            bound2 gen-small-double]
    {:vals vals
     :op1 op1 :bound1 bound1
     :op2 op2 :bound2 bound2}))

(defspec c5-predicate-composition 100
  (prop/for-all [scenario gen-pred-composition-scenario]
                (let [{:keys [vals op1 bound1 op2 bound2]} scenario
                      arr (double-array vals)
          ;; Reference: filter by both preds
                      ref-count (count (filter #(and (ref-pred op1 % bound1)
                                                     (ref-pred op2 % bound2))
                                               vals))
          ;; Combined WHERE (AND)
                      combined (q/q {:from {:v arr}
                                     :where [[op1 :v bound1]
                                             [op2 :v bound2]]
                                     :agg [[:count]]})
                      combined-count (long (:count (first combined)))]
                  (= ref-count combined-count))))
