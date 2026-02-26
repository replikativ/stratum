(ns stratum.parquet-test
  "Tests for Parquet import."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.parquet :as parquet]
            [stratum.dataset :as dataset]))

(set! *warn-on-reflection* true)

(deftest from-parquet-missing-file-test
  (testing "Reading non-existent file throws"
    (is (thrown? Exception
                 (parquet/from-parquet "/tmp/nonexistent-stratum-test.parquet")))))

(deftest from-parquet-integration-test
  (testing "Read parquet file if available in data directory"
    ;; Checks basic API with real files. Skipped if no .parquet files exist.
    (let [test-files (filter #(.endsWith (str %) ".parquet")
                             (try (file-seq (clojure.java.io/file "data"))
                                  (catch Exception _ [])))]
      (when (seq test-files)
        (let [path (str (first test-files))
              ds (parquet/from-parquet path :limit 100)]
          (is (satisfies? dataset/IDataset ds))
          (is (pos? (count (dataset/column-names ds))))
          (is (= 100 (dataset/row-count ds)))
          (doseq [col-name (dataset/column-names ds)]
            (is (keyword? col-name))
            (is (some? (dataset/column-type ds col-name)))))))))

(deftest from-parquet-column-filter-test
  (testing "Column filtering option"
    (let [test-files (filter #(.endsWith (str %) ".parquet")
                             (try (file-seq (clojure.java.io/file "data"))
                                  (catch Exception _ [])))]
      (when (seq test-files)
        (let [path (str (first test-files))
              all-cols-ds (parquet/from-parquet path :limit 10)
              ;; :columns option takes string names (matching parquet schema)
              first-col-name (name (first (dataset/column-names all-cols-ds)))]
          (when first-col-name
            (let [one-col-ds (parquet/from-parquet path :limit 10
                                                   :columns [first-col-name])]
              (is (= 1 (count (dataset/column-names one-col-ds))))
              (is (contains? (set (dataset/column-names one-col-ds))
                             (keyword first-col-name))))))))))
