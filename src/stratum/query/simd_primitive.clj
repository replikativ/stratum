(ns stratum.query.simd-primitive
  "Low-level bridge between Clojure query specs and Java SIMD methods.

   Packs normalized predicate and aggregate descriptions into typed Java
   arrays and calls ColumnOps/fusedSimdParallel (SUM/MIN/MAX/SUM_PRODUCT)
   or ColumnOps/fusedSimdCountParallel (COUNT). These are kept in separate
   Java methods for JIT isolation — see MEMORY.md for details.

   This is not a query compiler: it makes no strategy decisions and handles
   no expressions, grouping, or joins. It is called by stratum.query for
   the single-agg fused filter+aggregate hot path.

   == Input spec format ==

   {:columns   {:shipdate {:type :int64  :data some-long-array}
                :discount {:type :float64 :data some-double-array}
                :quantity {:type :int64  :data some-long-array}
                :price    {:type :float64 :data some-double-array}}
    :predicates [[:shipdate :range 8766 9131]
                 [:discount :range 0.05 0.07]
                 [:quantity :lt 24]]
    :aggregate  [:sum-product :price :discount]
    :length     6000000}"
  (:import [stratum.internal ColumnOps]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Predicate type constants
;; ============================================================================

(def op->pred-type
  "Map Clojure predicate ops to ColumnOps predicate type constants."
  {:range     ColumnOps/PRED_RANGE
   :lt        ColumnOps/PRED_LT
   :gt        ColumnOps/PRED_GT
   :eq        ColumnOps/PRED_EQ
   :lte       ColumnOps/PRED_LTE
   :gte       ColumnOps/PRED_GTE
   :neq       ColumnOps/PRED_NEQ
   :not-range ColumnOps/PRED_NOT_RANGE})

(def ^:private agg-op->type
  {:sum-product ColumnOps/AGG_SUM_PRODUCT
   :sum         ColumnOps/AGG_SUM
   :count       ColumnOps/AGG_COUNT
   :min         ColumnOps/AGG_MIN
   :max         ColumnOps/AGG_MAX})

;; ============================================================================
;; Predicate ordering
;; ============================================================================

(defn- estimate-selectivity
  "Estimate predicate selectivity for ordering.
   More selective predicates should come first for short-circuit benefit."
  [[_col op & _args]]
  (case op
    :eq    0.01
    :neq   0.99
    :range 0.15
    :lt    0.40
    :gt    0.40
    :lte   0.45
    :gte   0.45
    0.50))

;; ============================================================================
;; Java array packing
;; ============================================================================

(defn- prepare-predicates
  "Separate predicates into long and double groups and prepare Java arrays.

   Returns a map with:
     :num-long-preds, :long-pred-types, :long-cols, :long-lo, :long-hi
     :num-dbl-preds,  :dbl-pred-types,  :dbl-cols,  :dbl-lo,  :dbl-hi"
  [predicates columns]
  (let [long-preds (filterv #(= :int64  (:type (get columns (first %)))) predicates)
        dbl-preds  (filterv #(= :float64 (:type (get columns (first %)))) predicates)
        n-long     (count long-preds)
        n-dbl      (count dbl-preds)

        long-pred-types (int-array (mapv #(get op->pred-type (second %)) long-preds))
        long-cols       (into-array (Class/forName "[J")
                                    (mapv #(:data (get columns (first %))) long-preds))
        long-lo         (long-array (mapv (fn [[_col op & args]]
                                            (case op
                                              (:range :not-range :gt :eq :gte :neq) (long (first args))
                                              0))
                                          long-preds))
        long-hi         (long-array (mapv (fn [[_col op & args]]
                                            (case op
                                              (:range :not-range) (long (second args))
                                              (:lt :lte) (long (first args))
                                              0))
                                          long-preds))

        dbl-pred-types (int-array (mapv #(get op->pred-type (second %)) dbl-preds))
        dbl-cols       (into-array (Class/forName "[D")
                                   (mapv #(:data (get columns (first %))) dbl-preds))
        dbl-lo         (double-array (mapv (fn [[_col op & args]]
                                             (case op
                                               (:range :not-range :gt :eq :gte :neq) (double (first args))
                                               0.0))
                                           dbl-preds))
        dbl-hi         (double-array (mapv (fn [[_col op & args]]
                                             (case op
                                               (:range :not-range) (double (second args))
                                               (:lt :lte) (double (first args))
                                               0.0))
                                           dbl-preds))]
    {:num-long-preds n-long
     :long-pred-types long-pred-types
     :long-cols long-cols
     :long-lo long-lo
     :long-hi long-hi
     :num-dbl-preds n-dbl
     :dbl-pred-types dbl-pred-types
     :dbl-cols dbl-cols
     :dbl-lo dbl-lo
     :dbl-hi dbl-hi}))

(defn- ensure-doubles
  "Ensure column data is double[], converting long[] if needed."
  ^doubles [col-info ^long length]
  (let [data (:data col-info)]
    (if (instance? (Class/forName "[D") data)
      data
      (ColumnOps/longToDouble ^longs data (int length)))))

(defn- prepare-aggregation
  "Prepare aggregation arrays for Java methods.
   Returns [agg-type agg-col1 agg-col2].
   Converts long[] columns to double[] as needed."
  [aggregate columns length]
  (case (first aggregate)
    :sum-product
    (let [[_ col-a col-b] aggregate]
      [(int ColumnOps/AGG_SUM_PRODUCT)
       (ensure-doubles (get columns col-a) length)
       (ensure-doubles (get columns col-b) length)])

    :sum
    (let [[_ col] aggregate]
      [(int ColumnOps/AGG_SUM)
       (ensure-doubles (get columns col) length)
       (double-array 0)])

    :min
    (let [[_ col] aggregate]
      [(int ColumnOps/AGG_MIN)
       (ensure-doubles (get columns col) length)
       (double-array 0)])

    :max
    (let [[_ col] aggregate]
      [(int ColumnOps/AGG_MAX)
       (ensure-doubles (get columns col) length)
       (double-array 0)])

    :count
    [(int ColumnOps/AGG_COUNT)
     (double-array 0)
     (double-array 0)]))

;; ============================================================================
;; Execution
;; ============================================================================

(defn- execute-fused-simd
  [{:keys [predicates aggregate]} columns length]
  (let [pp (prepare-predicates predicates columns)
        [agg-type ^doubles agg-col1 ^doubles agg-col2] (prepare-aggregation aggregate columns length)
        ^doubles result
        (if (= (int agg-type) (int ColumnOps/AGG_COUNT))
          (ColumnOps/fusedSimdCountParallel
           (int (:num-long-preds pp))
           ^ints (:long-pred-types pp)
           ^"[[J" (:long-cols pp)
           ^longs (:long-lo pp)
           ^longs (:long-hi pp)
           (int (:num-dbl-preds pp))
           ^ints (:dbl-pred-types pp)
           ^"[[D" (:dbl-cols pp)
           ^doubles (:dbl-lo pp)
           ^doubles (:dbl-hi pp)
           (int length))
          (let [nan-safe (boolean
                          (or (and agg-col1 (ColumnOps/arrayHasNaN agg-col1 (alength agg-col1)))
                              (and agg-col2 (ColumnOps/arrayHasNaN agg-col2 (alength agg-col2)))))]
            (ColumnOps/fusedSimdParallel
             (int (:num-long-preds pp))
             ^ints (:long-pred-types pp)
             ^"[[J" (:long-cols pp)
             ^longs (:long-lo pp)
             ^longs (:long-hi pp)
             (int (:num-dbl-preds pp))
             ^ints (:dbl-pred-types pp)
             ^"[[D" (:dbl-cols pp)
             ^doubles (:dbl-lo pp)
             ^doubles (:dbl-hi pp)
             (int agg-type)
             agg-col1
             agg-col2
             (int length)
             nan-safe)))]
    {:result (aget result 0)
     :count  (long (aget result 1))}))

(defn- execute-fused-scalar
  [{:keys [predicates aggregate]} columns length]
  (let [pp (prepare-predicates predicates columns)
        [agg-type ^doubles agg-col1 ^doubles agg-col2] (prepare-aggregation aggregate columns length)
        ^doubles result
        (ColumnOps/fusedFilterAggregate
         (int (:num-long-preds pp))
         ^ints (:long-pred-types pp)
         ^"[[J" (:long-cols pp)
         ^longs (:long-lo pp)
         ^longs (:long-hi pp)
         (int (:num-dbl-preds pp))
         ^ints (:dbl-pred-types pp)
         ^"[[D" (:dbl-cols pp)
         ^doubles (:dbl-lo pp)
         ^doubles (:dbl-hi pp)
         (int agg-type)
         agg-col1
         agg-col2
         (int length))]
    {:result (aget result 0)
     :count  (long (aget result 1))}))

;; ============================================================================
;; Public API
;; ============================================================================

(defn compile-query
  "Compile a query spec into an executable zero-arg function.
   Selects SIMD (>=1000 rows) or scalar (<1000 rows) strategy.
   Returns (fn [] {:result double :count long})."
  [{:keys [length] :as spec}]
  (let [ordered (sort-by estimate-selectivity (:predicates spec))
        plan    (assoc spec :predicates (vec ordered))
        simd?   (>= (or length 0) 1000)]
    (if simd?
      (fn [] (execute-fused-simd plan (:columns spec) length))
      (fn [] (execute-fused-scalar plan (:columns spec) length)))))

(defn execute-query
  "One-shot query execution. Compiles and immediately runs."
  [spec]
  ((compile-query spec)))

(comment
  ;; Example: TPC-H Q06
  (import '[stratum.internal ColumnOps])

  (def n 6000000)
  (def shipdate (long-array (repeatedly n #(+ 8002 (rand-int 2559)))))
  (def discount (double-array (repeatedly n #(* 0.01 (rand-int 11)))))
  (def quantity (long-array (repeatedly n #(+ 1 (rand-int 50)))))
  (def price (double-array n))
  (dotimes [i n] (aset price i (* (aget quantity i) (+ 900.0 (* (Math/random) 1100.0)))))

  (def q06 (compile-query
            {:columns    {:shipdate {:type :int64  :data shipdate}
                          :discount {:type :float64 :data discount}
                          :quantity {:type :int64  :data quantity}
                          :price    {:type :float64 :data price}}
             :predicates [[:shipdate :range 8766 9131]
                          [:discount :range 0.05 0.07]
                          [:quantity :lt 24]]
             :aggregate  [:sum-product :price :discount]
             :length     n}))
  (time (q06)))
