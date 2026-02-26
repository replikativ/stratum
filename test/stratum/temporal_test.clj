(ns stratum.temporal-test
  "Tests for as-of temporal query layer."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.api :as st]
            [stratum.dataset :as dataset]
            [stratum.index :as idx]
            [stratum.storage :as storage]
            [konserve.store :as kstore]))

(defn- fresh-store []
  (let [config {:backend :file
                :path (str "/tmp/stratum-temporal-test-" (System/currentTimeMillis))
                :id (java.util.UUID/randomUUID)}]
    (when (kstore/store-exists? config {:sync? true})
      (kstore/delete-store config {:sync? true}))
    (kstore/create-store config {:sync? true})))

(deftest ds-resolve-branch-test
  (testing "ds-resolve loads from branch HEAD"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])}
              {:name "test"})
          _ (dataset/sync! ds store "main")]
      (let [resolved (dataset/resolve store "main" {:branch "main"})]
        (is (= 3 (dataset/row-count resolved)))))))

(deftest ds-resolve-as-of-test
  (testing "ds-resolve loads specific commit by UUID"
    (let [store (fresh-store)
          ds1 (dataset/make-dataset
               {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])}
               {:name "test"})
          saved1 (dataset/sync! ds1 store "main")
          commit1 (:id (:commit-info saved1))
          ;; Fork and modify
          ds2 (dataset/fork saved1)
          t2 (transient ds2)
          _ (dataset/set-at! t2 :price 0 99.0)
          p2 (persistent! t2)
          _ (dataset/sync! p2 store "main")]
      ;; Resolve at first commit
      (let [resolved (dataset/resolve store "main" {:as-of commit1})]
        (is (= 3 (dataset/row-count resolved)))))))

(deftest ds-resolve-default-test
  (testing "ds-resolve with empty opts loads branch by ref name"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])}
              {:name "test"})
          _ (dataset/sync! ds store "develop")]
      (let [resolved (dataset/resolve store "develop" {})]
        (is (= 3 (dataset/row-count resolved)))))))

(deftest ds-with-metadata-test
  (testing "ds-with-metadata merges metadata"
    (let [ds (dataset/make-dataset
              {:x (idx/index-from-seq :int64 [1 2 3])}
              {:name "test" :metadata {:source "csv"}})]
      (let [ds2 (dataset/with-metadata ds {"datahike/tx" 42})]
        (is (= {"datahike/tx" 42 :source "csv"} (dataset/metadata ds2)))))))

(deftest ds-resolve-as-of-tx-test
  (testing "ds-resolve with :as-of-tx finds commit by metadata"
    (let [store (fresh-store)
          ;; Commit 1 with tx=10
          ds1 (-> (dataset/make-dataset
                   {:val (idx/index-from-seq :float64 [1.0 2.0])}
                   {:name "test" :metadata {"datahike/tx" 10}})
                  (dataset/sync! store "main"))
          ;; Fork and commit 2 with tx=20
          ds2 (dataset/fork ds1)
          t2 (transient ds2)
          _ (dataset/set-at! t2 :val 0 99.0)
          p2 (persistent! t2)
          ds2m (dataset/with-metadata p2 {"datahike/tx" 20})
          _ (dataset/sync! ds2m store "main")]
      ;; Resolve at tx=15 should find tx=10 commit (floor)
      (let [resolved (dataset/resolve store "main" {:as-of-tx 15})]
        (is (= 2 (dataset/row-count resolved))))
      ;; Resolve at tx=20 should find tx=20 commit
      (let [resolved (dataset/resolve store "main" {:as-of-tx 20})]
        (is (= 2 (dataset/row-count resolved)))))))

(deftest find-commit-by-metadata-test
  (testing "find-commit-by-metadata walks history"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:x (idx/index-from-seq :int64 [1 2 3])}
              {:name "test" :metadata {"tag" "v1"}})
          saved (dataset/sync! ds store "main")]
      (let [found (storage/find-commit-by-metadata
                   store "main"
                   #(= "v1" (get % "tag")))]
        (is (some? found))
        (is (uuid? found))))))

(deftest api-q-temporal-opts-test
  (testing "api/q with temporal opts resolves from store"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])
               :qty (idx/index-from-seq :int64 [1 2 3])}
              {:name "trades"})
          _ (dataset/sync! ds store "main")]
      (let [result (st/q {:from "trades" :agg [[:sum :price]]}
                         {:store store :branch "main"})]
        (is (= 60.0 (:sum (first result))))))))

(deftest sql-dataset-table-test
  (testing "SQL query with StratumDataset as table value (2-arity)"
    (let [ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])
               :qty (idx/index-from-seq :int64 [1 2 3])}
              {:name "test"})]
      (let [result (st/q "SELECT SUM(price) AS total FROM t" {"t" ds})]
        (is (== 60.0 (:total (first result))))))))

(deftest sql-dataset-table-3arity-test
  (testing "SQL query with StratumDataset as table value (3-arity)"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])
               :qty (idx/index-from-seq :int64 [1 2 3])}
              {:name "test"})]
      (let [result (st/q "SELECT SUM(price) AS total FROM t"
                         {"t" ds}
                         {:store store})]
        (is (== 60.0 (:total (first result))))))))

(deftest sql-storage-backed-table-3arity-test
  (testing "SQL 3-arity resolves storage-backed table references"
    (let [store (fresh-store)
          ds (dataset/make-dataset
              {:price (idx/index-from-seq :float64 [10.0 20.0 30.0])}
              {:name "test"})
          _ (dataset/sync! ds store "main")]
      ;; Pass string ref as table value — resolved from store
      (let [result (st/q "SELECT SUM(price) AS total FROM t"
                         {"t" "main"}
                         {:store store :branch "main"})]
        (is (== 60.0 (:total (first result))))))))
