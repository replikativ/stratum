(ns stratum.chunk-validity-test
  "Phase 0 + 1b — round-trip + invariant tests for the per-row validity
   bitmap on `stratum.chunk/PersistentColChunk` and for the
   chunk→flat concatenation in `idx-materialize-to-array-with-validity`."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [stratum.chunk :as chunk]
            [stratum.index :as index]))

;; ============================================================================
;; Generators
;; ============================================================================

(def ^:private gen-long-or-null
  "Either a long value or the NULL sentinel Long/MIN_VALUE. Skewed
   toward real values so generated chunks aren't dominated by nulls."
  (gen/frequency [[8 gen/large-integer]
                  [2 (gen/return Long/MIN_VALUE)]]))

(def ^:private gen-double-or-nan
  "Either a finite double or the NULL sentinel Double/NaN."
  (gen/frequency [[8 (gen/double* {:NaN? false :infinite? false})]
                  [2 (gen/return Double/NaN)]]))

(def ^:private gen-int64-data
  "Generator for an int64 data vector + a sentinel-aware expected
   validity vector. Length 1..128 to exercise both single-entry and
   multi-entry bitmap shapes (boundary at 64)."
  (gen/let [vs (gen/vector gen-long-or-null 1 128)]
    {:data vs
     :expected-validity (mapv #(not= % Long/MIN_VALUE) vs)}))

(def ^:private gen-float64-data
  (gen/let [vs (gen/vector gen-double-or-nan 1 128)]
    {:data vs
     :expected-validity (mapv #(not (Double/isNaN %)) vs)}))

(defn- bitmap->vec
  "Materialise a validity bitmap (or nil) to a per-row boolean vector
   for comparison against the expected vector."
  [validity ^long n-rows]
  (if (nil? validity)
    (vec (repeat n-rows true))
    (mapv #(chunk/validity-row-valid? validity %) (range n-rows))))

;; ============================================================================
;; Properties
;; ============================================================================

(defspec int64-chunk-validity-matches-sentinels 200
  (prop/for-all [{:keys [data expected-validity]} gen-int64-data]
                (let [arr (long-array data)
                      c (chunk/chunk-from-array arr)
                      v (chunk/chunk-validity c)
                      n (chunk/chunk-length c)]
                  (and
        ;; Fast-path: nil iff there are no nulls in the data
                   (= (every? identity expected-validity)
                      (nil? v))
        ;; Bitmap matches expected per-row when materialised
                   (= expected-validity (bitmap->vec v n))))))

(defspec float64-chunk-validity-matches-sentinels 200
  (prop/for-all [{:keys [data expected-validity]} gen-float64-data]
                (let [arr (double-array data)
                      c (chunk/chunk-from-array arr)
                      v (chunk/chunk-validity c)
                      n (chunk/chunk-length c)]
                  (and
                   (= (every? identity expected-validity)
                      (nil? v))
                   (= expected-validity (bitmap->vec v n))))))

(defspec serialization-round-trip-preserves-bitmap 100
  (prop/for-all [{:keys [data expected-validity]} gen-int64-data]
                (let [c (chunk/chunk-from-array (long-array data))
                      restored (chunk/chunk-from-bytes (chunk/chunk-to-bytes c))
                      v (chunk/chunk-validity restored)
                      n (chunk/chunk-length restored)]
                  (= expected-validity (bitmap->vec v n)))))

(defspec serialization-round-trip-preserves-bitmap-double 100
  (prop/for-all [{:keys [data expected-validity]} gen-float64-data]
                (let [c (chunk/chunk-from-array (double-array data))
                      restored (chunk/chunk-from-bytes (chunk/chunk-to-bytes c))
                      v (chunk/chunk-validity restored)
                      n (chunk/chunk-length restored)]
                  (= expected-validity (bitmap->vec v n)))))

(defspec transient-write-then-persistent-rebuilds-validity 100
  (prop/for-all [{:keys [data expected-validity]} gen-int64-data]
                (let [;; Start with all-zeros, transient-write each row, then persistent.
                      c (-> (chunk/make-chunk :int64 (count data))
                            (chunk/col-transient))
                      _ (dotimes [i (count data)]
                          (chunk/write-long! c i (nth data i)))
                      _ (chunk/col-persistent! c)
                      v (chunk/chunk-validity c)
                      n (chunk/chunk-length c)]
                  (= expected-validity (bitmap->vec v n)))))

;; ============================================================================
;; Explicit unit tests (corner cases + regression locks)
;; ============================================================================

(deftest all-valid-chunk-returns-nil-validity
  (testing "long[]"
    (let [c (chunk/chunk-from-array (long-array [1 2 3 4 5]))]
      (is (nil? (chunk/chunk-validity c)))))
  (testing "double[]"
    (let [c (chunk/chunk-from-array (double-array [1.0 2.0 3.0]))]
      (is (nil? (chunk/chunk-validity c))))))

(deftest constant-chunk-real-value-returns-nil-validity
  (let [c (chunk/make-constant-chunk :int64 1000 42)]
    (is (nil? (chunk/chunk-validity c)))))

(deftest constant-chunk-sentinel-value-returns-all-zeros-bitmap
  (testing "int64 Long/MIN_VALUE constant → all-NULL"
    (let [c (chunk/make-constant-chunk :int64 130 Long/MIN_VALUE)
          v (chunk/chunk-validity c)]
      (is (some? v))
      (is (every? false?
                  (mapv #(chunk/validity-row-valid? v %) (range 130))))))
  (testing "float64 Double/NaN constant → all-NULL"
    (let [c (chunk/make-constant-chunk :float64 130 Double/NaN)
          v (chunk/chunk-validity c)]
      (is (some? v))
      (is (every? false?
                  (mapv #(chunk/validity-row-valid? v %) (range 130)))))))

(deftest bitmap-spans-multiple-long-entries-correctly
  (testing "65-row chunk: one bit in the second entry"
    (let [data (vec (concat [Long/MIN_VALUE] (repeat 64 1)))
          c (chunk/chunk-from-array (long-array data))
          v (chunk/chunk-validity c)]
      (is (some? v))
      (is (= 2 (alength v)))
      (is (false? (chunk/validity-row-valid? v 0)))
      (is (every? #(chunk/validity-row-valid? v %) (range 1 65)))))
  (testing "128-row chunk with NULL spanning entry boundary"
    (let [data (vec (concat (repeat 64 1) [Long/MIN_VALUE] (repeat 63 2)))
          c (chunk/chunk-from-array (long-array data))
          v (chunk/chunk-validity c)]
      (is (some? v))
      (is (= 2 (alength v)))
      (is (every? #(chunk/validity-row-valid? v %) (range 0 64)))
      (is (false? (chunk/validity-row-valid? v 64)))
      (is (every? #(chunk/validity-row-valid? v %) (range 65 128))))))

(deftest col-fork-shares-validity-of-persistent-source
  ;; Forking a *persistent* chunk produces a sibling that shares the
  ;; computed bitmap by reference. Both are persistent; neither will
  ;; mutate the bitmap directly. A subsequent `col-transient` on either
  ;; side clears its own validity, and `col-persistent!` rebuilds.
  (let [src (chunk/chunk-from-array (long-array [1 Long/MIN_VALUE 3]))
        forked (chunk/col-fork src)]
    (is (= (mapv #(chunk/validity-row-valid? (chunk/chunk-validity src) %) (range 3))
           (mapv #(chunk/validity-row-valid? (chunk/chunk-validity forked) %) (range 3))))))

(deftest chunk-slice-rescans-validity
  (let [c (chunk/chunk-from-array (long-array [1 Long/MIN_VALUE 3 Long/MIN_VALUE 5]))
        s1 (chunk/chunk-slice c 0 2)  ;; rows 0,1 → [1, NULL]
        s2 (chunk/chunk-slice c 2 5)] ;; rows 2,3,4 → [3, NULL, 5]
    (is (= [true false]
           (let [v (chunk/chunk-validity s1)]
             (mapv #(chunk/validity-row-valid? v %) (range 2)))))
    (is (= [true false true]
           (let [v (chunk/chunk-validity s2)]
             (mapv #(chunk/validity-row-valid? v %) (range 3)))))))

;; ============================================================================
;; Phase 1b — idx-materialize-to-array-with-validity (concat chunk bitmaps)
;; ============================================================================

(deftest materialize-with-validity-all-valid-returns-nil-bitmap
  (testing "long index built from sentinel-free data → :validity nil"
    (let [data (vec (range 20000))   ;; spans 3 chunks at default size 8192
          idx (index/index-from-seq :int64 data)
          {:keys [data validity]} (index/idx-materialize-to-array-with-validity idx)]
      (is (nil? validity))
      (is (= 20000 (alength ^longs data)))
      (is (= 0     (aget ^longs data 0)))
      (is (= 19999 (aget ^longs data 19999))))))

(deftest materialize-with-validity-flags-nulls-spanning-chunks
  (testing "NULLs in multiple chunks land at the right flat-bitmap positions"
    (let [;; NULLs at rows 5 (chunk 0), 8500 (chunk 1), 18000 (chunk 2)
          ;; Use Long/MIN_VALUE directly — index-from-seq doesn't translate
          ;; nil today, that's the sentinel convention.
          data (->> (range 20000)
                    (map (fn [i] (case i
                                   5     Long/MIN_VALUE
                                   8500  Long/MIN_VALUE
                                   18000 Long/MIN_VALUE
                                   i)))
                    vec)
          idx (index/index-from-seq :int64 data)
          {:keys [validity]} (index/idx-materialize-to-array-with-validity idx)]
      (is (some? validity))
      (testing "exact NULL positions"
        (is (false? (chunk/validity-row-valid? validity 5)))
        (is (false? (chunk/validity-row-valid? validity 8500)))
        (is (false? (chunk/validity-row-valid? validity 18000))))
      (testing "neighbouring positions stay valid"
        (is (true? (chunk/validity-row-valid? validity 4)))
        (is (true? (chunk/validity-row-valid? validity 6)))
        (is (true? (chunk/validity-row-valid? validity 8499)))
        (is (true? (chunk/validity-row-valid? validity 8501)))
        (is (true? (chunk/validity-row-valid? validity 17999)))
        (is (true? (chunk/validity-row-valid? validity 18001)))))))

(deftest materialize-with-validity-handles-partial-last-chunk
  (testing "last chunk smaller than default chunk-size; bitmap entries copied correctly"
    (let [;; 8200 rows: chunk 0 = 8192 (all valid), chunk 1 = 8 rows with NULL at idx 8195
          data (->> (range 8200)
                    (map (fn [i] (if (= i 8195) Long/MIN_VALUE i)))
                    vec)
          idx (index/index-from-seq :int64 data)
          {:keys [validity]} (index/idx-materialize-to-array-with-validity idx)]
      (is (some? validity))
      (is (false? (chunk/validity-row-valid? validity 8195)))
      (is (true?  (chunk/validity-row-valid? validity 8194)))
      (is (true?  (chunk/validity-row-valid? validity 8199))))))

(deftest materialize-with-validity-double-col
  (testing "float64 path via NaN sentinel"
    (let [data [1.0 2.0 Double/NaN 4.0 Double/NaN]
          idx (index/index-from-seq :float64 data)
          {:keys [validity]} (index/idx-materialize-to-array-with-validity idx)]
      (is (some? validity))
      (is (true?  (chunk/validity-row-valid? validity 0)))
      (is (true?  (chunk/validity-row-valid? validity 1)))
      (is (false? (chunk/validity-row-valid? validity 2)))
      (is (true?  (chunk/validity-row-valid? validity 3)))
      (is (false? (chunk/validity-row-valid? validity 4))))))
