(ns ^{:yggdrasil true} stratum.yggdrasil-test
  "Yggdrasil compliance tests for Stratum adapter.

   The fixture bridges key-value semantics to columnar storage using a
   key→position side map. The index stays dense (append-only), and the
   key-map tracks which logical key lives at which physical position.
   Both are persisted in index metadata for branch isolation."
  (:require [clojure.test :refer [deftest testing is]]
            [yggdrasil.compliance :as compliance]
            [yggdrasil.protocols :as p]
            [stratum.yggdrasil :as ygg]
            [stratum.index :as index]
            [stratum.dataset :as dataset]
            [stratum.storage :as storage]
            [stratum.api :as st]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]))

;; ============================================================
;; Key-map helpers
;;
;; ::key-map : {string-key → {:pos long, :value any}}
;;
;; The key-map is the source of truth for the compliance tests.
;; The index stores numeric representations for analytics.
;; ============================================================

(defn- get-key-map [sys]
  (or (::key-map sys) {}))

(defn- value->double
  "Convert a value to double for storage in the columnar index."
  [value]
  (cond
    (nil? value) 0.0
    (number? value) (double value)
    (string? value) (double (.hashCode ^String value))
    :else 0.0))

;; ============================================================
;; Test Fixture
;; ============================================================

(defn create-test-system
  "Create a fresh Stratum system for testing."
  []
  (ygg/create-system {:datatype :float64 :system-name "test-stratum"}))

(defn mutate-system
  "Perform a mutation on the system (append data to index)."
  [sys]
  (let [idx (:idx sys)
        updated-idx (-> idx
                        index/idx-transient
                        (index/idx-append! (double (rand-int 1000)))
                        index/idx-persistent!)]
    (assoc sys :idx updated-idx)))

(defn commit-system
  "Commit the system with a message.
   Persists the key-map in index metadata for branch isolation.
   Writes index commit + yggdrasil branch HEAD."
  [sys msg]
  (let [idx (:idx sys)
        store (:store sys)
        branch (get-in (meta idx) [:branch] :main)
        ;; Include key-map in metadata so it gets persisted with the snapshot
        km (get-key-map sys)
        idx-with-meta (vary-meta idx assoc ::key-map km)]
    ;; Sync to storage - this creates an index commit (no branch)
    (try
      (let [synced-idx (index/idx-sync! idx-with-meta store)
            commit-id (get-in (meta synced-idx) [:commit :id])
            ;; Also write branch HEAD for yggdrasil's branch management
            snapshot (storage/load-index-commit store commit-id)]
        (when snapshot
          (konserve.core/assoc store [:yggdrasil :heads branch] snapshot {:sync? true}))
        (assoc sys :idx synced-idx))
      (catch Exception _e
        ;; If there's nothing to commit (no dirty chunks), just return system
        sys))))

(defn close-system
  "Close/cleanup the system."
  [sys]
  ;; konserve mem-store doesn't need explicit cleanup
  nil)

(defn write-entry
  "Write a key-value pair. Append-only: new keys get appended to the
   index, existing keys get updated in place. The key-map tracks the
   mapping from string keys to physical positions."
  [sys key value]
  (let [km (get-key-map sys)
        idx (:idx sys)
        existing (get km key)]
    (if existing
      ;; Update existing position
      (let [pos (long (:pos existing))
            val (value->double value)
            updated-idx (-> idx
                            index/idx-transient
                            (index/idx-set! pos val)
                            index/idx-persistent!)]
        (-> sys
            (assoc :idx updated-idx)
            (assoc ::key-map (assoc km key {:pos pos :value value}))))
      ;; Append new entry
      (let [pos (index/idx-length idx)
            val (value->double value)
            updated-idx (-> idx
                            index/idx-transient
                            (index/idx-append! val)
                            index/idx-persistent!)]
        (-> sys
            (assoc :idx updated-idx)
            (assoc ::key-map (assoc km key {:pos pos :value value})))))))

(defn read-entry
  "Read a value by key. Looks up position from the key-map and
   returns the original value stored there."
  [sys key]
  (when-let [entry (get (get-key-map sys) key)]
    (:value entry)))

(defn count-entries
  "Count entries in the key-map."
  [sys]
  (count (get-key-map sys)))

(defn delete-entry
  "Delete a key. Removes from index (shifts elements) and updates
   the key-map positions accordingly."
  [sys key]
  (let [km (get-key-map sys)]
    (if-let [entry (get km key)]
      (let [pos (long (:pos entry))
            idx (:idx sys)
            ;; Delete from index (shifts subsequent elements down)
            updated-idx (-> idx
                            index/idx-transient
                            (index/idx-delete! pos)
                            index/idx-persistent!)
            ;; Remove key and shift positions for keys after the deleted one
            updated-km (into {}
                             (for [[k v] km
                                   :when (not= k key)]
                               [k (if (> (:pos v) pos)
                                    (update v :pos dec)
                                    v)]))]
        (-> sys
            (assoc :idx updated-idx)
            (assoc ::key-map updated-km)))
      ;; Key doesn't exist - return unchanged
      sys)))

;; ============================================================
;; Compliance Test
;; ============================================================

(def stratum-fixture
  {:create-system create-test-system
   :mutate mutate-system
   :commit commit-system
   :close! close-system
   :write-entry write-entry
   :read-entry read-entry
   :count-entries count-entries
   :delete-entry delete-entry
   ;; Disable concurrent tests - konserve sync mode doesn't provide CAS semantics
   ;; for branch registration, and single-writer-per-branch is enforced at app level
   :supports-concurrent? false})

(deftest stratum-yggdrasil-compliance
  (testing "Stratum adapter passes Yggdrasil compliance tests"
    (compliance/run-compliance-tests stratum-fixture)))

;; ============================================================
;; Additional Stratum-specific tests
;; ============================================================

(deftest stratum-system-identity-test
  (testing "System identity"
    (let [sys (create-test-system)]
      (try
        (is (= :stratum (p/system-type sys)))
        (is (string? (p/system-id sys)))
        (let [caps (p/capabilities sys)]
          (is (:snapshotable caps))
          (is (:branchable caps))
          (is (:graphable caps))
          (is (not (:mergeable caps))))
        (finally (close-system sys))))))

(deftest stratum-basic-snapshot-test
  (testing "Basic snapshot operations"
    (let [sys (create-test-system)]
      (try
        ;; Mutate and commit
        (let [sys (-> sys mutate-system (commit-system "first commit"))
              snap-id (p/snapshot-id sys)]
          (is (string? snap-id))
          (is (seq snap-id))

          ;; Check snapshot metadata
          (let [meta (p/snapshot-meta sys snap-id)]
            (is (some? meta))
            (is (= snap-id (:snapshot-id meta)))
            (is (= :main (:branch meta)))))
        (finally (close-system sys))))))

(deftest stratum-branch-operations-test
  (testing "Branch operations"
    (let [sys (create-test-system)]
      (try
        ;; Initial state
        (is (= :main (p/current-branch sys)))
        (is (contains? (p/branches sys) :main))

        ;; Create feature branch
        (let [sys (-> sys mutate-system (commit-system "base commit"))
              sys (p/branch! sys :feature)]
          (is (contains? (p/branches sys) :feature))
          (is (= :main (p/current-branch sys)) "branch! doesn't switch")

          ;; Checkout feature
          (let [sys (p/checkout sys :feature)]
            (is (= :feature (p/current-branch sys)))))
        (finally (close-system sys))))))

(deftest stratum-history-test
  (testing "Commit history"
    (let [sys (create-test-system)]
      (try
        (let [sys (-> sys mutate-system (commit-system "first"))
              sys (-> sys mutate-system (commit-system "second"))
              sys (-> sys mutate-system (commit-system "third"))
              hist (p/history sys)]
          (is (>= (count hist) 3))
          (is (= (p/snapshot-id sys) (first hist)) "Most recent first"))
        (finally (close-system sys))))))

;; ============================================================
;; StratumDatasetSystem tests
;; ============================================================

(defn- make-test-store [] (new-mem-store (atom {}) {:sync? true}))

(deftest dataset-system-basic-test
  (testing "Dataset system identity and capabilities"
    (let [store (make-test-store)
          ds (dataset/make-dataset
              {:price (index/index-from-seq :float64 [10.0 20.0 30.0])
               :qty (index/index-from-seq :int64 [1 2 3])}
              {:name "test"})
          saved (dataset/sync! ds store "main")
          sys (ygg/create-dataset-system saved store {:system-name "test-ds"})]
      (is (= "test-ds" (p/system-id sys)))
      (is (= :stratum-dataset (p/system-type sys)))
      (let [caps (p/capabilities sys)]
        (is (:snapshotable caps))
        (is (:branchable caps))
        (is (:graphable caps))
        (is (not (:committable caps)))))))

(deftest dataset-system-snapshot-test
  (testing "Dataset system snapshot-id round-trip"
    (let [store (make-test-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])}
              {:name "test"})
          saved (dataset/sync! ds store "main")
          sys (ygg/create-dataset-system saved store)]
      ;; snapshot-id returns the commit UUID as string
      (let [snap-id (p/snapshot-id sys)]
        (is (string? snap-id))
        (is (some? (parse-uuid snap-id)))))))

(deftest dataset-system-as-of-test
  (testing "as-of returns queryable StratumDataset"
    (let [store (make-test-store)
          ;; Version 1: 3 rows
          ds1 (dataset/make-dataset
               {:price (index/index-from-seq :float64 [10.0 20.0 30.0])}
               {:name "test"})
          saved1 (dataset/sync! ds1 store "main")
          snap1 (str (:id (:commit-info saved1)))
          ;; Version 2: 2 rows
          ds2 (dataset/make-dataset
               {:price (index/index-from-seq :float64 [100.0 200.0])}
               {:name "test"})
          saved2 (dataset/sync! ds2 store "main")
          sys (ygg/create-dataset-system saved2 store)]
      ;; Current is v2 (2 rows)
      (is (= 2 (dataset/row-count @(ygg/dataset-system-atom sys))))
      ;; as-of v1 returns 3 rows
      (let [v1-ds (p/as-of sys snap1)]
        (is (some? v1-ds))
        (is (= 3 (dataset/row-count v1-ds)))
        ;; Queryable
        (let [result (st/q {:from v1-ds :agg [[:sum :price]]})]
          (is (== 60.0 (:sum (first result)))))))))

(deftest dataset-system-branch-test
  (testing "Dataset system branch! and checkout"
    (let [store (make-test-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])}
              {:name "test"})
          saved (dataset/sync! ds store "main")
          sys (ygg/create-dataset-system saved store)]
      ;; Current branch
      (is (= "main" (p/current-branch sys)))
      (is (contains? (p/branches sys) "main"))

      ;; Create feature branch
      (let [feature-sys (p/branch! sys "feature")]
        (is (contains? (p/branches feature-sys) "feature"))

        ;; Checkout feature branch
        (let [checked-out (p/checkout sys "feature")]
          (is (= "feature" (p/current-branch checked-out)))
          ;; Data preserved
          (let [ds-data @(ygg/dataset-system-atom checked-out)]
            (is (= 3 (dataset/row-count ds-data)))))))))

(deftest dataset-system-history-test
  (testing "Dataset system commit history traversal"
    (let [store (make-test-store)
          ;; Commit 1
          ds1 (dataset/make-dataset
               {:x (index/index-from-seq :float64 [1.0])}
               {:name "test"})
          saved1 (dataset/sync! ds1 store "main")
          ;; Commit 2 (fork from 1 to establish parent chain)
          ds2 (dataset/fork saved1)
          t2 (transient ds2)
          _ (dataset/set-at! t2 :x 0 99.0)
          p2 (persistent! t2)]
      (let [saved2 (dataset/sync! p2 store "main")
            sys (ygg/create-dataset-system saved2 store)
            hist (p/history sys)]
        ;; At least 2 commits in history
        (is (>= (count hist) 2))
        ;; Most recent first
        (is (= (p/snapshot-id sys) (first hist)))))))

(deftest dataset-system-snapshot-meta-test
  (testing "Dataset system snapshot-meta returns metadata"
    (let [store (make-test-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])}
              {:name "test" :metadata {"tag" "v1"}})
          saved (dataset/sync! ds store "main")
          sys (ygg/create-dataset-system saved store)
          snap-id (p/snapshot-id sys)
          meta-map (p/snapshot-meta sys snap-id)]
      (is (some? meta-map))
      (is (= snap-id (:snapshot-id meta-map)))
      (is (= "test" (:name meta-map)))
      (is (= 2 (:row-count meta-map)))
      (is (= {"tag" "v1"} (:metadata meta-map))))))
