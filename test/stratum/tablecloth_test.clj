(ns stratum.tablecloth-test
  "Tests for tablecloth integration via protocol extensions on StratumDataset"
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.dataset :as dataset]
            [stratum.tablecloth]  ;; Load protocol extensions
            [stratum.csv :as csv]
            [stratum.index :as idx]
            [stratum.query :as q]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.protocols :as ds-proto]))

;; ============================================================================
;; Phase 1: MVP Tests
;; ============================================================================

(deftest arrays->dataset-test
  (testing "Create dataset from arrays"
    (let [dataset (dataset/make-dataset
                   {:x (double-array [1.0 2.0 3.0])
                    :y (long-array [10 20 30])}
                   {:name "test-data"})]

      (is (some? dataset))
      (is (= 3 (ds/row-count dataset)))
      (is (= 2 (ds/column-count dataset)))
      (is (= #{:x :y} (set (dataset/column-names dataset))))
      (is (= "test-data" (dataset/ds-name dataset))))))

(deftest make-dataset-from-indices-test
  (testing "Create dataset from PersistentColumnIndex"
    (let [price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0])
          qty-idx (idx/index-from-seq :int64 [10 20 30])
          dataset (dataset/make-dataset {:price price-idx :qty qty-idx})]

      (is (some? dataset))
      (is (= 3 (ds/row-count dataset)))
      (is (= 2 (ds/column-count dataset)))
      (is (= #{:price :qty} (set (dataset/column-names dataset)))))))

(deftest column-access-test
  (testing "Access individual columns"
    (let [dataset (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})]

      (let [col (ds/column dataset :x)]
        (is (some? col))
        (is (= 3 (count col)))
        (is (= 1.0 (nth col 0)))
        (is (= 2.0 (nth col 1)))
        (is (= 3.0 (nth col 2)))))))

(deftest dataset->arrays-test
  (testing "Convert dataset back to arrays"
    (let [orig-x (double-array [1.0 2.0 3.0])
          orig-y (long-array [10 20 30])
          ds (dataset/make-dataset {:x orig-x :y orig-y})
          ;; Direct column access instead of dataset->arrays
          cols (dataset/columns ds)
          x-arr (:data (get cols :x))
          y-arr (:data (get cols :y))]

      (is (= #{:x :y} (set (keys cols))))
      (is (= 3 (alength ^doubles x-arr)))
      (is (= 3 (alength ^longs y-arr)))

      ;; Check values
      (is (= [1.0 2.0 3.0] (vec x-arr)))
      (is (= [10 20 30] (vec y-arr))))))

(deftest select-columns-test
  (testing "Select subset of columns"
    (let [dataset (dataset/make-dataset
                   {:a (double-array [1 2 3])
                    :b (double-array [4 5 6])
                    :c (double-array [7 8 9])})
          selected (ds/select-columns dataset [:a :c])]

      (is (= 2 (ds/column-count selected)))
      (is (= #{:a :c} (set (dataset/column-names selected))))
      (is (= 3 (ds/row-count selected))))))

(deftest select-rows-test
  (testing "Select subset of rows"
    (let [dataset (dataset/make-dataset
                   {:x (double-array [1 2 3 4 5])
                    :y (long-array [10 20 30 40 50])})
          selected (ds/select-rows dataset [0 2 4])]

      (is (= 3 (ds/row-count selected)))
      (is (= 2 (ds/column-count selected)))

      ;; Verify values
      (let [cols (dataset/columns selected)
            x-arr (:data (get cols :x))
            y-arr (:data (get cols :y))]
        (is (= [1.0 3.0 5.0] (vec x-arr)))
        (is (= [10 30 50] (vec y-arr)))))))

(deftest missing-values-test
  (testing "Missing value detection (via NaN sentinel)"
    (let [data (double-array [1.0 Double/NaN 3.0 Double/NaN 5.0])
          dataset (dataset/make-dataset {:x data})
          col (ds/column dataset :x)]

      ;; Access values - NaN becomes nil (readObject converts it)
      (is (= 1.0 (nth col 0)))
      (is (nil? (nth col 1)))  ; NaN → nil via readObject
      (is (= 3.0 (nth col 2)))
      (is (nil? (nth col 3)))  ; NaN → nil via readObject
      (is (= 5.0 (nth col 4))))))

(deftest empty-dataset-test
  (testing "Empty dataset handling"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dataset/make-dataset {})))))

(deftest mismatched-lengths-test
  (testing "Columns with different lengths throw error"
    (is (thrown? clojure.lang.ExceptionInfo
                 (dataset/make-dataset
                  {:x (double-array [1 2 3])
                   :y (double-array [1 2])})))))

(deftest metadata-test
  (testing "Dataset metadata"
    (let [dataset (dataset/make-dataset
                   {:x (double-array [1 2 3])}
                   {:name "test" :metadata {:custom-key "custom-value"}})]

      (is (= "test" (dataset/ds-name dataset)))

      ;; Create new dataset with updated name
      (let [ds2 (dataset/make-dataset
                 {:x (double-array [1 2 3])}
                 {:name "updated"})]
        (is (= "updated" (dataset/ds-name ds2)))))))

(deftest large-dataset-no-materialization-test
  (testing "Large dataset doesn't materialize unnecessarily (zero-copy)"
    (let [;; Create 1M row index
          large-idx (idx/index-from-seq :float64 (range 1000000))
          dataset (dataset/make-dataset {:x large-idx})]

      ;; These operations should not trigger materialization
      (is (= 1000000 (ds/row-count dataset)))
      (is (= 1 (ds/column-count dataset)))
      (is (= [:x] (dataset/column-names dataset)))

      ;; Access a few values - no full materialization
      (let [col (ds/column dataset :x)]
        (is (= 0.0 (nth col 0)))
        (is (= 999999.0 (nth col 999999)))))))

;; ============================================================================
;; Phase 2: Query Integration Tests
;; ============================================================================

(deftest dataset-preserves-indices-test
  (testing "Dataset preserves indices (no materialization)"
    (let [price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0])
          qty-idx (idx/index-from-seq :int64 [10 20 30])
          dataset (dataset/make-dataset {:price price-idx :qty qty-idx})]

      ;; Should preserve indices (not materialize)
      (is (dataset/has-index? dataset :price))
      (is (dataset/has-index? dataset :qty))
      (is (= 3 (dataset/row-count dataset)))
      ;; Can get indices from columns
      (let [cols (dataset/columns dataset)]
        (is (satisfies? idx/IColumnIndex (:index (get cols :price))))
        (is (satisfies? idx/IColumnIndex (:index (get cols :qty))))))))

(deftest dataset-query-integration-test
  (testing "Query execution with Dataset input (preserves zero-copy indices)"
    (let [price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0 400.0])
          qty-idx (idx/index-from-seq :int64 [10 20 30 40])
          dataset (dataset/make-dataset {:price price-idx :qty qty-idx})
          result (q/q {:from dataset
                       :agg [[:sum :price] [:sum :qty]]})]

      ;; Verify zero-copy preservation via dataset
      (is (dataset/has-index? dataset :price))
      (is (dataset/has-index? dataset :qty))

      ;; Verify query results
      (is (= 1 (count result)))
      (is (= 4 (:_count (first result))))
      (is (= 1000.0 (:sum_price (first result))))
      (is (= 100.0 (:sum_qty (first result))))))

  (testing "Query execution with filtering on Dataset"
    (let [price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0 400.0])
          qty-idx (idx/index-from-seq :int64 [10 20 30 40])
          dataset (dataset/make-dataset {:price price-idx :qty qty-idx})
          result (q/q {:from dataset
                       :where [[:>= :price 200.0]]
                       :agg [[:sum :price] [:count]]})]

      (is (= 1 (count result)))
      (is (= 3 (:_count (first result))))  ;; 200, 300, 400
      (is (= 3 (:count (first result))))   ;; Explicit count agg
      (is (= 900.0 (:sum (first result)))))))

;; ============================================================================
;; Phase 4C: StratumDataset Protocol Implementation Tests
;; ============================================================================
;;
;; These tests verify that StratumDataset implements tech.ml.dataset protocols
;; directly, enabling seamless use with tablecloth WITHOUT explicit conversion.

(deftest stratum-dataset-pdataset-test
  (testing "StratumDataset implements PDataset protocol"
    (let [ds (dataset/make-dataset
              {:price (double-array [100.0 200.0 300.0])
               :qty (long-array [10 20 30])}
              {:name "trades"})]

      ;; PDataset protocol methods
      (is (ds-proto/is-dataset? ds))
      (is (= 3 (ds-proto/row-count ds)))
      (is (= 2 (ds-proto/column-count ds)))

      ;; Column access returns proper Column object
      (let [price-col (ds-proto/column ds :price)]
        (is (some? price-col))
        (is (= 3 (count price-col)))
        (is (= 100.0 (nth price-col 0))))

      ;; Missing column throws
      (is (thrown? RuntimeException
                   (ds-proto/column ds :nonexistent))))))

(deftest stratum-dataset-rows-rowvecs-test
  (testing "StratumDataset rows and rowvecs"
    (let [ds (dataset/make-dataset
              {:a (double-array [1.0 2.0])
               :b (long-array [10 20])})]

      ;; rows returns ObjectReader of maps
      (let [rows (ds-proto/rows ds {})]
        (is (= 2 (.lsize rows)))
        (let [row0 (.readObject rows 0)]
          (is (map? row0))
          (is (= 1.0 (:a row0)))
          (is (= 10 (:b row0)))))

      ;; rowvecs returns ObjectReader of vectors
      (let [rowvecs (ds-proto/rowvecs ds {})]
        (is (= 2 (.lsize rowvecs)))
        (let [row0 (.readObject rowvecs 0)]
          (is (vector? row0))
          (is (= 2 (count row0))))))))

(deftest stratum-dataset-missing-values-test
  (testing "StratumDataset PMissing protocol with NaN and Long.MIN_VALUE sentinels"
    (let [ds (dataset/make-dataset
              {:x (double-array [1.0 Double/NaN 3.0])
               :y (long-array [10 Long/MIN_VALUE 30])})]

      ;; PMissing protocol via extend-protocol
      (let [missing-bm (ds-proto/missing ds)]
        (is (instance? org.roaringbitmap.RoaringBitmap missing-bm))
        (is (= 1 (.getCardinality missing-bm)))  ;; Row 1 has missing values (both cols)
        (is (.contains missing-bm 1))  ;; Row 1 has missing values
        (is (not (.contains missing-bm 0)))
        (is (not (.contains missing-bm 2)))))))

(testing "Dataset with no missing values"
  (let [ds (dataset/make-dataset
            {:x (double-array [1.0 2.0 3.0])})]
    (let [missing-bm (ds-proto/missing ds)]
      (is (= 0 (.getCardinality missing-bm))))))

(deftest stratum-dataset-select-columns-test
  (testing "PSelectColumns returns StratumDataset (not tech.ml.dataset)"
    (let [ds (dataset/make-dataset
              {:a (double-array [1 2 3])
               :b (double-array [4 5 6])
               :c (double-array [7 8 9])})
          selected (ds-proto/select-columns ds [:a :c])]

      ;; Result should be StratumDataset (type consistency)
      (is (instance? stratum.dataset.StratumDataset selected))
      (is (= 2 (count (dataset/column-names selected))))
      (is (= #{:a :c} (set (dataset/column-names selected))))
      (is (= 3 (dataset/row-count selected)))

      ;; Metadata preserved
      (is (= :select-columns (get-in (dataset/metadata selected) [:derived-from]))))))

(deftest stratum-dataset-select-rows-test
  (testing "PSelectRows returns StratumDataset with selected rows"
    (let [ds (dataset/make-dataset
              {:x (double-array [1 2 3 4 5])
               :y (long-array [10 20 30 40 50])})
          selected (ds-proto/select-rows ds [0 2 4])]

      ;; Result should be StratumDataset
      (is (instance? stratum.dataset.StratumDataset selected))
      (is (= 3 (dataset/row-count selected)))
      (is (= 2 (count (dataset/column-names selected))))

      ;; Values match selected rows
      (let [x-col (dataset/column selected :x)
            y-col (dataset/column selected :y)]
        (is (= [1.0 3.0 5.0] (vec (:data x-col))))
        (is (= [10 30 50] (vec (:data y-col))))))))

(deftest csv-returns-stratum-dataset-test
  (testing "CSV import returns StratumDataset that works with tech.ml.dataset protocols"
    (let [path (.getAbsolutePath
                (doto (java.io.File/createTempFile "test" ".csv")
                  (.deleteOnExit)
                  (spit "name,age,score\nAlice,30,95.5\nBob,25,87.3")))
          ds (csv/from-csv path)]

      ;; Should be StratumDataset
      (is (instance? stratum.dataset.StratumDataset ds))

      ;; tech.ml.dataset protocols work directly (no conversion!)
      (is (ds-proto/is-dataset? ds))
      (is (= 2 (ds-proto/row-count ds)))
      (is (= 3 (ds-proto/column-count ds)))

      ;; Can access columns
      (let [age-col (ds-proto/column ds :age)]
        (is (some? age-col)))

      ;; Selection works and returns StratumDataset
      (let [selected (ds-proto/select-columns ds [:name :age])]
        (is (instance? stratum.dataset.StratumDataset selected))
        (is (= 2 (count (dataset/column-names selected))))))))

(deftest stratum-dataset-tablecloth-integration-test
  (testing "StratumDataset works directly with ds/* functions (tablecloth compatibility)"
    (let [ds (dataset/make-dataset
              {:product (into-array String ["Widget" "Gadget" "Widget"])
               :price (double-array [10.5 25.0 10.5])
               :qty (long-array [5 3 2])})]

      ;; dataset/column-names works
      (is (= [:product :price :qty] (dataset/column-names ds)))

      ;; ds/row-count works  
      (is (= 3 (ds/row-count ds)))

      ;; ds/column-count works
      (is (= 3 (ds/column-count ds)))

      ;; ds/select-columns works
      (let [selected (ds/select-columns ds [:price :qty])]
        (is (instance? stratum.dataset.StratumDataset selected)))

      ;; ds/select-rows works
      (let [selected (ds/select-rows ds [0 2])]
        (is (instance? stratum.dataset.StratumDataset selected))
        (is (= 2 (ds/row-count selected)))))))

;; ============================================================================
;; Bidirectional Support Tests: tech.ml.Dataset → Stratum Query Engine
;; ============================================================================

(deftest tech-dataset-idataset-protocol-test
  (testing "tech.ml.Dataset implements IDataset protocol for query engine"
    (let [tds (ds/->dataset {:x (double-array [1.0 2.0 3.0 4.0 5.0])
                             :y (long-array [10 20 30 40 50])})]

      ;; IDataset protocol methods work
      (is (= [:x :y] (dataset/column-names tds)))
      (is (= 5 (dataset/row-count tds)))
      (is (= :float64 (dataset/column-type tds :x)))
      (is (= :int64 (dataset/column-type tds :y)))

      ;; Columns are converted to Stratum format
      (let [cols (dataset/columns tds)
            x-col (get cols :x)
            y-col (get cols :y)]
        (is (= :float64 (:type x-col)))
        (is (= :int64 (:type y-col)))
        ;; Zero-copy: underlying arrays preserved
        (is (instance? (Class/forName "[D") (:data x-col)))
        (is (instance? (Class/forName "[J") (:data y-col)))))))

(deftest tech-dataset-query-execution-test
  (testing "tech.ml.Dataset works in Stratum queries"
    (let [tds (ds/->dataset {:x (double-array [1.0 2.0 3.0 4.0 5.0])
                             :y (long-array [10 20 30 40 50])})]

      ;; Simple aggregation
      (let [result (q/q {:from tds
                         :agg [[:sum :x] [:sum :y]]})]
        (is (= 1 (count result)))
        (is (= 5 (:_count (first result))))
        (is (= 15.0 (:sum_x (first result))))
        (is (= 150.0 (:sum_y (first result)))))

      ;; With WHERE clause
      (let [result (q/q {:from tds
                         :where [[:> :x 2.0]]
                         :agg [[:sum :y] [:count]]})]
        (is (= 1 (count result)))
        (is (= 3 (:_count (first result))))
        (is (= 120.0 (:sum (first result))))
        (is (= 3 (:count (first result))))))))

(deftest tech-dataset-group-by-test
  (testing "tech.ml.Dataset works with GROUP BY"
    (let [tds (ds/->dataset {:category (into-array String ["A" "B" "A" "B" "A"])
                             :value (double-array [10.0 20.0 15.0 25.0 12.0])})]

      (let [result (q/q {:from tds
                         :group [:category]
                         :agg [[:sum :value] [:count]]})
            by-category (into {} (map (fn [r] [(:category r) r])) result)]
        (is (= 2 (count result)))
        (is (= 37.0 (:sum (get by-category "A"))))
        (is (= 3 (:count (get by-category "A"))))
        (is (= 45.0 (:sum (get by-category "B"))))
        (is (= 2 (:count (get by-category "B"))))))))
