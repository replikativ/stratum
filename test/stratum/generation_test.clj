(ns stratum.generation-test
  (:require [clojure.test :refer [deftest is testing]]
            [konserve.memory :refer [new-mem-store]]
            [stratum.dataset :as dataset]
            [stratum.index :as index]
            [stratum.storage :as storage]))

(defn- store []
  (new-mem-store (atom {}) {:sync? true}))

(defn- sample-dataset []
  (dataset/make-dataset
   {:eid (index/index-from-seq :int64 [1 2 3])
    :score (index/index-from-seq :float64 [1.5 2.5 3.5])}
   {:name "items"}))

(deftest sealing-a-generation-does-not-publish-a-ref
  (let [kv (store)
        source (sample-dataset)
        sealed (dataset/seal-generation! source kv)
        root (dataset/generation-id sealed)
        reopened (dataset/open-generation kv root)
        snapshot (storage/load-dataset-commit kv root)
        index-ids (set (map :index-commit (vals (:columns snapshot))))
        pss-roots (set (keep (fn [index-id]
                               (:pss-root
                                (storage/load-index-commit kv index-id)))
                             index-ids))
        reachable (storage/generation-reachable-keys kv root)]
    (testing "sealing writes an exact restorable generation"
      (is (uuid? root))
      (is (= 3 (dataset/row-count reopened)))
      (is (== 2.5 (index/idx-get-double
                   (:index (dataset/column reopened :score)) 1))))

    (testing "no native branch or head becomes visible"
      (is (nil? (storage/list-dataset-branches kv)))
      (is (nil? (storage/load-dataset-head kv "main")))
      (is (nil? (:branch snapshot)))
      (is (nil? (get-in sealed [:commit-info :branch]))))

    (testing "the exact mark excludes mutable refs and parent history"
      (is (contains? reachable [:datasets :commits root]))
      (is (every? #(contains? reachable [:indices :commits %]) index-ids))
      (is (every? #(contains? reachable %) pss-roots))
      (is (not-any? #(and (vector? %)
                          (= :heads (second %)))
                    reachable)))))

(deftest independent-generations-can-seal-from-one-base
  (let [kv (store)
        base (dataset/seal-generation! (sample-dataset) kv)
        left (-> base dataset/fork transient)
        right (-> base dataset/fork transient)]
    (dataset/set-at! left :score 0 10.0)
    (dataset/set-at! right :score 0 20.0)
    (let [left-root (-> left persistent! (dataset/seal-generation! kv)
                        dataset/generation-id)
          right-root (-> right persistent! (dataset/seal-generation! kv)
                         dataset/generation-id)
          base-root (dataset/generation-id base)
          left-reachable (storage/generation-reachable-keys kv left-root)]
      (is (= 3 (count #{base-root left-root right-root})))
      (is (not (contains? left-reachable
                          [:datasets :commits base-root]))
          "an embedding root retains its exact generation, not Stratum history")
      (is (== 1.5 (index/idx-get-double
                   (:index (dataset/column
                            (dataset/open-generation kv base-root) :score)) 0)))
      (is (== 10.0 (index/idx-get-double
                    (:index (dataset/column
                             (dataset/open-generation kv left-root) :score)) 0)))
      (is (== 20.0 (index/idx-get-double
                    (:index (dataset/column
                             (dataset/open-generation kv right-root) :score)) 0)))
      (is (nil? (storage/list-dataset-branches kv))))))

(deftest standalone-sync-composes-seal-with-branch-publication
  (let [kv (store)
        saved (dataset/sync! (sample-dataset) kv "main")
        root (dataset/generation-id saved)]
    (is (= root (storage/load-dataset-head kv "main")))
    (is (= #{"main"} (storage/list-dataset-branches kv)))
    (is (= "main" (get-in saved [:commit-info :branch])))
    (is (= root (dataset/generation-id (dataset/load kv "main"))))))

(deftest open-generation-refuses-branch-names
  (let [kv (store)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"generation ID must be a UUID"
                          (dataset/open-generation kv "main")))))
