(ns stratum.column-ops-nullable-test
  "Phase 1a — validate the validity-aware SIMD predicate kernels in
   `ColumnOpsNullable.java` in isolation, before wiring any production
   query path through them.

   The kernels are siblings of the all-valid kernels in `ColumnOps`;
   they're intended to be called by the Clojure dispatch layer when
   `any-nullable?` is true for the input columns. This test calls
   the Java method directly, constructs a known-shape validity
   bitmap, and verifies F-001/F-002-style cases."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [stratum.chunk :as chunk])
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
