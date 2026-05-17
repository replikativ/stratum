(ns stratum.column-ops-nullable-test
  "Phase 1a/1b — validate the validity-aware SIMD predicate kernels in
   `ColumnOpsNullable.java` in isolation (1a) and end-to-end through
   the query engine (1b)."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [stratum.chunk :as chunk]
            [stratum.index :as index]
            [stratum.query :as q])
  (:import [stratum.internal ColumnOps ColumnOpsNullable]))

(defn- build-bitmap
  "Build a packed long[] validity bitmap from a vector of booleans
   (true = valid, false = null)."
  ^longs [bools]
  (let [n (count bools)
        n-entries (chunk/bitmap-entry-count n)
        v (long-array n-entries)]
    ;; default all bits = 0 (all NULL); set bits where valid
    (dotimes [i n]
      (when (nth bools i)
        (let [entry-idx (quot i 64)
              bit-pos (rem i 64)]
          (aset v entry-idx
                (bit-or (aget v entry-idx)
                        (bit-shift-left 1 bit-pos))))))
    v))

(defn- pred-count-nullable
  "Run a single-long-predicate filter+count via ColumnOpsNullable.
   Returns a long. Args boxed because Clojure caps primitive-hinted
   defns at 4 args; this is test-only code."
  [col pred-type lo hi validity]
  (let [^longs col col
        ^longs validity validity
        n (alength col)
        ^doubles result
        (ColumnOpsNullable/fusedSimdCountParallel
         1 (int-array [pred-type])
         (into-array (Class/forName "[J") [col])
         (long-array [lo]) (long-array [hi])
         0 (int-array 0)
         (into-array (Class/forName "[D") [])
         (double-array 0) (double-array 0)
         validity
         (int n))]
    (long (aget result 1))))

;; ============================================================================
;; F-001 regression: LT / NEQ on long col with NULLs must NOT include NULL rows
;; ============================================================================

(deftest f001-lt-skips-null-rows
  (testing "WHERE col < 100 on long col with Long/MIN_VALUE NULL sentinels"
    (let [data [10 Long/MIN_VALUE 50 Long/MIN_VALUE 200 Long/MIN_VALUE]
          col (long-array data)
          validity (build-bitmap (mapv #(not= % Long/MIN_VALUE) data))
          cnt (pred-count-nullable col ColumnOps/PRED_LT Long/MIN_VALUE 100 validity)]
      ;; Real values < 100: 10 and 50 → 2 rows
      ;; Pre-fix (audit F-001), Long.MIN_VALUE rows would also pass < 100,
      ;; inflating the count to 5.
      (is (= 2 cnt)))))

(deftest f001-neq-skips-null-rows
  (testing "WHERE col != 0 on long col with NULL sentinels"
    (let [data [1 Long/MIN_VALUE 2 Long/MIN_VALUE 0 3]
          col (long-array data)
          validity (build-bitmap (mapv #(not= % Long/MIN_VALUE) data))
          cnt (pred-count-nullable col ColumnOps/PRED_NEQ 0 0 validity)]
      ;; Real values != 0: 1, 2, 3 → 3 rows
      ;; Pre-fix, Long.MIN_VALUE != 0 is true, count would be 5.
      (is (= 3 cnt)))))

(deftest f001-range-skips-null-rows
  (testing "WHERE col BETWEEN 0 AND 100 on long col with NULL sentinels"
    (let [data [10 Long/MIN_VALUE 50 200 Long/MIN_VALUE 0]
          col (long-array data)
          validity (build-bitmap (mapv #(not= % Long/MIN_VALUE) data))
          cnt (pred-count-nullable col ColumnOps/PRED_RANGE 0 100 validity)]
      ;; Real values in [0, 100]: 10, 50, 0 → 3 rows
      (is (= 3 cnt)))))

;; ============================================================================
;; Larger scale: verify SIMD path (must process > LONG_LANES rows in vector loop)
;; ============================================================================

(deftest large-array-with-sparse-nulls
  (testing "10K rows with ~20% NULL density, WHERE col >= 500"
    (let [n 10000
          rng (java.util.Random. 42)
          data (vec (for [_ (range n)]
                      (if (< (.nextDouble rng) 0.2)
                        Long/MIN_VALUE
                        (.nextInt rng 1000))))
          col (long-array data)
          validity (build-bitmap (mapv #(not= % Long/MIN_VALUE) data))
          expected (count (filter #(and (not= % Long/MIN_VALUE) (>= ^long % 500)) data))
          cnt (pred-count-nullable col ColumnOps/PRED_GTE 500 0 validity)]
      (is (= expected cnt)))))

(deftest dense-data-fast-path-matches-existing
  ;; When validity is *all-1s* (every row valid), the Nullable kernel
  ;; must produce identical results to the original ColumnOps kernel.
  ;; This isn't the production fast path (Clojure dispatch would route
  ;; to ColumnOps directly), but it locks in the equivalence so the
  ;; sibling can't silently drift.
  (testing "all-valid bitmap → same count as no-bitmap kernel"
    (let [n 1000
          rng (java.util.Random. 7)
          col (long-array (repeatedly n #(.nextInt rng 1000)))
          all-valid (build-bitmap (vec (repeat n true)))
          ;; Existing ColumnOps method:
          ^doubles ref-r
          (ColumnOps/fusedSimdCountParallel
           1 (int-array [ColumnOps/PRED_LT])
           (into-array (Class/forName "[J") [col])
           (long-array [Long/MIN_VALUE]) (long-array [500])
           0 (int-array 0)
           (into-array (Class/forName "[D") [])
           (double-array 0) (double-array 0)
           (int n))
          ref-count (long (aget ref-r 1))
          nullable-count (pred-count-nullable col ColumnOps/PRED_LT Long/MIN_VALUE 500 all-valid)]
      (is (= ref-count nullable-count)))))

;; ============================================================================
;; Property-based: random data + random bitmaps, cross-check with Clojure ref
;; ============================================================================

(def ^:private gen-spec
  (gen/let [n (gen/choose 16 256)
            data (gen/vector (gen/choose -1000 1000) n)
            null-flags (gen/vector gen/boolean n)
            lo (gen/choose -1000 1000)
            span (gen/choose 0 1000)]
    (let [hi (+ lo span)]
      {:n n
       ;; Replace null positions with Long/MIN_VALUE sentinel in the data
       :data (mapv (fn [v nullit?] (if nullit? Long/MIN_VALUE v)) data null-flags)
       ;; Validity bitmap matches the null flags
       :validity (mapv not null-flags)
       :lo lo
       :hi hi})))

(defn- clojure-ref-count
  "Reference implementation in pure Clojure."
  [data validity pred-type lo hi]
  (->> (map vector data validity)
       (filter (fn [[_ v]] v))                 ; row must be valid
       (map first)
       (filter (fn [v]
                 (case pred-type
                   :range (and (>= v lo) (<= v hi))
                   :lt    (< v hi)
                   :gt    (> v lo)
                   :eq    (= v lo)
                   :lte   (<= v hi)
                   :gte   (>= v lo)
                   :neq   (not= v lo)
                   :not-range (not (and (>= v lo) (<= v hi))))))
       count))

;; ============================================================================
;; Phase 1b — end-to-end through the query engine on indexed input
;; ============================================================================

;; ============================================================================
;; Phase 1b wiring is in place at the array-fused-count path
;; (`stratum.query.simd-primitive`). However, the planner picks
;; `PChunkedSIMDCount` for indexed inputs and `PBlockSkipCount` for
;; filtered array-mode counts; both go through OTHER kernels
;; (`fusedSimdChunkedCountParallel`, `fusedSimdCountBlockSkipParallel`)
;; not yet covered. End-to-end tests assertion-style come in Phase 1d
;; once the planner-picked kernels for COUNT all have Nullable siblings.
;; The Phase 1a kernel-direct tests above still exercise the bitmap
;; correctness in isolation.
;; ============================================================================

(deftest end-to-end-indexed-count-with-nulls
  ;; Phase 1d-count — end-to-end through the planner's PChunkedSIMDCount
  ;; path on an indexed column with NULLs. The dispatch routes to
  ;; ColumnOpsChunkedSimdNullable when any chunk has non-nil validity.
  (testing "WHERE col < 100 on indexed long col with NULLs"
    (let [n 10000
          data (vec (for [i (range n)]
                      (if (zero? (mod i 7)) Long/MIN_VALUE
                          (mod (* i 13) 1000))))
          idx (index/index-from-seq :int64 data)
          result (q/q {:from {:c idx}
                       :where [[:< :c 100]]
                       :agg [[:count]]})
          ;; Reference: non-NULL rows with value < 100
          expected (count (filter (fn [v] (and (not= v Long/MIN_VALUE) (< v 100))) data))]
      (is (= expected (:_count (first result))))))
  (testing "WHERE col != 0 on indexed long col with NULLs"
    (let [;; 10000 rows: NULLs at every 7th position, zeros at every 11th
          n 10000
          data (vec (for [i (range n)]
                      (cond (zero? (mod i 7))  Long/MIN_VALUE
                            (zero? (mod i 11)) 0
                            :else              (mod (* i 13) 1000))))
          idx (index/index-from-seq :int64 data)
          result (q/q {:from {:c idx}
                       :where [[:!= :c 0]]
                       :agg [[:count]]})
          ;; Reference
          expected (count (filter (fn [v] (and (not= v Long/MIN_VALUE) (not= v 0))) data))]
      (is (= expected (:_count (first result))))))
  (testing "dense indexed col (no NULLs) takes the no-bitmap fast path"
    (let [data (vec (range 10000))
          idx (index/index-from-seq :int64 data)
          result (q/q {:from {:c idx}
                       :where [[:< :c 5000]]
                       :agg [[:count]]})]
      (is (= 5000 (:_count (first result)))))))

(deftest f001-fused-simd-sum-skips-nulls
  ;; Phase 1c — fusedSimdParallel sibling. Same shape as count but
  ;; the SUM aggregator side keeps its existing IEEE-NaN sentinel-
  ;; skip semantics; the bitmap fixes the predicate side.
  (testing "SUM(v) WHERE col < 100 on long col with NULLs"
    (let [n 10000
          ;; Pred col with NULLs at every 7th position; agg col is double
          pred-data (vec (for [i (range n)]
                           (if (zero? (mod i 7)) Long/MIN_VALUE
                               (mod (* i 13) 1000))))
          agg-data (vec (for [i (range n)] (* 0.5 (mod i 100))))
          pred-arr (long-array pred-data)
          agg-arr  (double-array agg-data)
          validity (let [v (long-array (chunk/bitmap-entry-count n))]
                     (dotimes [i n]
                       (when (not= (nth pred-data i) Long/MIN_VALUE)
                         (let [eidx (quot i 64), bit (rem i 64)]
                           (aset v eidx (bit-or (aget v eidx)
                                                (bit-shift-left 1 bit))))))
                     v)
          ;; Expected: SUM agg over rows where pred-data IS NOT NULL AND < 100
          expected (->> (range n)
                        (filter (fn [i] (and (not= (nth pred-data i) Long/MIN_VALUE)
                                             (< (nth pred-data i) 100))))
                        (map #(nth agg-data %))
                        (reduce + 0.0))
          ^doubles result
          (ColumnOpsNullable/fusedSimdParallel
           1 (int-array [ColumnOps/PRED_LT])
           (into-array (Class/forName "[J") [pred-arr])
           (long-array [Long/MIN_VALUE]) (long-array [100])
           0 (int-array 0)
           (into-array (Class/forName "[D") [])
           (double-array 0) (double-array 0)
           ColumnOps/AGG_SUM ^doubles agg-arr (double-array 0)
           ^longs validity
           (int n) true)]
      (is (= (Math/round (* 100.0 expected))
             (Math/round (* 100.0 (aget result 0))))))))

(deftest dispatch-survives-validity-passthrough
  ;; Lock in the wiring: `cols/materialize-column` preserves an
  ;; existing `:validity` field rather than dropping it during the
  ;; materialise pass; `prepare-predicates` reads `:validity` and
  ;; assembles the combined bitmap.
  (testing "column with :validity flows through to combined-validity"
    (let [arr (long-array [1 2 3 4 5])
          bm  (let [v (long-array 1)] (java.util.Arrays/fill v -1) v)
          col {:type :int64 :data arr :validity bm}
          ;; Drop down to simd-primitive's prepare-predicates directly:
          prep (#'stratum.query.simd-primitive/prepare-predicates
                [[:c :lt 100]] {:c col})]
      (is (some? (:combined-validity prep)))))
  (testing "all-no-validity column → combined-validity nil (fast path)"
    (let [arr (long-array [1 2 3 4 5])
          col {:type :int64 :data arr}
          prep (#'stratum.query.simd-primitive/prepare-predicates
                [[:c :lt 100]] {:c col})]
      (is (nil? (:combined-validity prep))))))

(defspec each-pred-type-matches-clojure-reference 50
  (prop/for-all [{:keys [data validity lo hi]} gen-spec]
    (let [col (long-array data)
          bm (build-bitmap validity)]
      (every? true?
              (for [[op pred-type] [[:range ColumnOps/PRED_RANGE]
                                    [:lt ColumnOps/PRED_LT]
                                    [:gt ColumnOps/PRED_GT]
                                    [:eq ColumnOps/PRED_EQ]
                                    [:lte ColumnOps/PRED_LTE]
                                    [:gte ColumnOps/PRED_GTE]
                                    [:neq ColumnOps/PRED_NEQ]
                                    [:not-range ColumnOps/PRED_NOT_RANGE]]]
                (let [expected (clojure-ref-count data validity op lo hi)
                      actual (pred-count-nullable col pred-type lo hi bm)]
                  (= expected actual)))))))
