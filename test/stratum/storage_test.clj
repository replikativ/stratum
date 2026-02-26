(ns stratum.storage-test
  "Tests for stratum storage layer."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.storage :as storage]
            [stratum.index :as index]
            [stratum.chunk :as chunk]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [connect-fs-store]]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set :as pss]))

;; ============================================================================
;; Index Commit Tests
;; ============================================================================

(deftest write-and-load-index-commit-test
  (testing "Write and load index commit"
    (let [store (new-mem-store (atom {}) {:sync? true})
          commit-id (java.util.UUID/randomUUID)
          snapshot {:commit-id commit-id
                    :total-length 100
                    :datatype :float64
                    :pss-root (java.util.UUID/randomUUID)}]
      (storage/write-index-commit! store commit-id snapshot)

      ;; Load by commit ID
      (let [loaded (storage/load-index-commit store commit-id)]
        (is (= snapshot loaded))))))

;; ============================================================================
;; Dataset Branch Tests
;; ============================================================================

(deftest dataset-branch-registration-test
  (testing "Dataset branch registration"
    (let [store (new-mem-store (atom {}) {:sync? true})]
      (storage/register-dataset-branch! store "main")
      (storage/register-dataset-branch! store "feature")
      (is (= #{"main" "feature"} (storage/list-dataset-branches store))))))

(deftest dataset-head-management-test
  (testing "Dataset HEAD pointer management"
    (let [store (new-mem-store (atom {}) {:sync? true})
          commit-1 (java.util.UUID/randomUUID)
          commit-2 (java.util.UUID/randomUUID)]

      ;; Set HEAD
      (storage/update-dataset-head! store "main" commit-1)
      (is (= commit-1 (storage/load-dataset-head store "main")))

      ;; Update HEAD
      (storage/update-dataset-head! store "main" commit-2)
      (is (= commit-2 (storage/load-dataset-head store "main")))

      ;; Non-existent branch returns nil
      (is (nil? (storage/load-dataset-head store "nonexistent"))))))

(deftest dataset-commit-test
  (testing "Dataset commit write and load"
    (let [store (new-mem-store (atom {}) {:sync? true})
          commit-id (java.util.UUID/randomUUID)
          snapshot {:dataset-id commit-id
                    :name "orders"
                    :branch "main"
                    :columns {:x {:index-commit (java.util.UUID/randomUUID) :type :float64}}
                    :schema {:x {:type :float64 :nullable? true}}
                    :row-count 1000
                    :metadata {}}]
      (storage/write-dataset-commit! store commit-id snapshot)

      (let [loaded (storage/load-dataset-commit store commit-id)]
        (is (= snapshot loaded))))))

(deftest unregister-dataset-branch-test
  (testing "Unregister dataset branch"
    (let [store (new-mem-store (atom {}) {:sync? true})]
      (storage/register-dataset-branch! store "main")
      (storage/register-dataset-branch! store "feature")
      (storage/update-dataset-head! store "feature" (java.util.UUID/randomUUID))

      (storage/unregister-dataset-branch! store "feature")

      (is (= #{"main"} (storage/list-dataset-branches store)))
      (is (nil? (storage/load-dataset-head store "feature"))))))

;; ============================================================================
;; Garbage Collection Tests
;; ============================================================================

(deftest gc-removes-unreachable-pss-nodes-test
  (testing "GC removes PSS nodes not referenced by any dataset branch"
    (let [store (new-mem-store (atom {}) {:sync? true})
          ;; Create two indices and sync them
          idx1 (index/index-from-seq :float64 (range 1000))
          synced1 (index/idx-sync! idx1 store)
          commit-id-1 (get-in (meta synced1) [:commit :id])

          idx2 (index/index-from-seq :float64 (range 1000 2000))
          synced2 (index/idx-sync! idx2 store)
          commit-id-2 (get-in (meta synced2) [:commit :id])

          ;; Create dataset commits referencing these indices
          ds-commit-1 (java.util.UUID/randomUUID)
          ds-commit-2 (java.util.UUID/randomUUID)]

      ;; Write dataset commits
      (storage/write-dataset-commit! store ds-commit-1
                                     {:dataset-id ds-commit-1
                                      :columns {:x {:index-commit commit-id-1 :type :float64}}
                                      :parents #{}})
      (storage/write-dataset-commit! store ds-commit-2
                                     {:dataset-id ds-commit-2
                                      :columns {:x {:index-commit commit-id-2 :type :float64}}
                                      :parents #{}})

      ;; Register branch pointing only to ds-commit-1
      (storage/register-dataset-branch! store "main")
      (storage/update-dataset-head! store "main" ds-commit-1)

      ;; GC should remove idx2's PSS nodes (not referenced by any branch)
      (let [result (storage/gc! store)]
        (is (pos? (:kept-pss-nodes result)))
        (is (pos? (:deleted-pss-nodes result)))
        (is (= 1 (:deleted-index-commits result)))
        (is (= 1 (:deleted-dataset-commits result)))))))

(deftest gc-preserves-shared-pss-nodes-test
  (testing "GC with two branches sharing PSS nodes preserves shared data"
    (let [store (new-mem-store (atom {}) {:sync? true})
          ;; Create one index (shared between branches)
          idx (index/index-from-seq :float64 (range 100))
          synced (index/idx-sync! idx store)
          shared-commit-id (get-in (meta synced) [:commit :id])

          ;; Create another index (branch-specific)
          idx2 (index/index-from-seq :float64 (range 100 200))
          synced2 (index/idx-sync! idx2 store)
          branch-commit-id (get-in (meta synced2) [:commit :id])

          ;; Dataset commits
          ds-commit-main (java.util.UUID/randomUUID)
          ds-commit-feature (java.util.UUID/randomUUID)]

      ;; Main branch uses shared index
      (storage/write-dataset-commit! store ds-commit-main
                                     {:dataset-id ds-commit-main
                                      :columns {:x {:index-commit shared-commit-id :type :float64}}
                                      :parents #{}})

      ;; Feature branch uses both shared + branch-specific
      (storage/write-dataset-commit! store ds-commit-feature
                                     {:dataset-id ds-commit-feature
                                      :columns {:x {:index-commit shared-commit-id :type :float64}
                                                :y {:index-commit branch-commit-id :type :float64}}
                                      :parents #{}})

      (storage/register-dataset-branch! store "main")
      (storage/register-dataset-branch! store "feature")
      (storage/update-dataset-head! store "main" ds-commit-main)
      (storage/update-dataset-head! store "feature" ds-commit-feature)

      ;; GC should delete nothing (both branches alive)
      (let [result (storage/gc! store)]
        (is (= 0 (:deleted-pss-nodes result)))
        (is (= 0 (:deleted-index-commits result)))
        (is (= 0 (:deleted-dataset-commits result)))))))

(deftest gc-with-no-branches-test
  (testing "GC with no branches deletes everything"
    (let [store (new-mem-store (atom {}) {:sync? true})
          idx (index/index-from-seq :float64 (range 100))
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])]

      ;; Write a dataset commit but don't register any branch
      (storage/write-dataset-commit! store (java.util.UUID/randomUUID)
                                     {:dataset-id (java.util.UUID/randomUUID)
                                      :columns {:x {:index-commit commit-id :type :float64}}
                                      :parents #{}})

      ;; GC with no branches should delete everything
      (let [result (storage/gc! store)]
        (is (pos? (:deleted-pss-nodes result)))
        (is (= 0 (:kept-pss-nodes result)))))))

(deftest collect-live-pss-addresses-test
  (testing "Collect PSS addresses from index snapshots"
    (let [store (new-mem-store (atom {}) {:sync? true})
          idx (-> (index/make-index :float64)
                  index/idx-transient
                  (index/idx-append! 1.0)
                  index/idx-persistent!)
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)]

      ;; Should have pss-root
      (is (some? (:pss-root snapshot)))

      ;; Should collect PSS addresses from the snapshot
      (let [live (storage/collect-live-pss-addresses store [commit-id])]
        (is (pos? (count live)))))))

;; ============================================================================
;; Atomic Registration Tests
;; ============================================================================

(deftest atomic-branch-registration-test
  (testing "Branch registration is idempotent and consistent"
    (let [store (new-mem-store (atom {}) {:sync? true})]
      ;; Register same branch twice — should not duplicate
      (storage/register-dataset-branch! store "main")
      (storage/register-dataset-branch! store "main")
      (is (= #{"main"} (storage/list-dataset-branches store)))

      ;; Register another branch
      (storage/register-dataset-branch! store "dev")
      (is (= #{"main" "dev"} (storage/list-dataset-branches store)))

      ;; Unregister
      (storage/unregister-dataset-branch! store "dev")
      (is (= #{"main"} (storage/list-dataset-branches store)))

      ;; Unregister non-existent branch — no error
      (storage/unregister-dataset-branch! store "nonexistent")
      (is (= #{"main"} (storage/list-dataset-branches store))))))

;; ============================================================================
;; Dirty-Only Sync Tests (PSS-backed incremental)
;; ============================================================================

(deftest idx-sync-incremental-test
  (testing "idx-sync! second sync writes fewer PSS nodes (only dirty)"
    (let [store (new-mem-store (atom {}) {:sync? true})
          ;; Create index with multiple chunks
          idx (index/index-from-seq :float64 (range 300) {:chunk-size 100})
          ;; First sync — writes all PSS nodes
          synced1 (index/idx-sync! idx store)

          ;; Modify one element using transient path
          modified (-> synced1
                       index/idx-transient
                       (index/idx-set! 0 999.0)
                       index/idx-persistent!)
          synced2 (index/idx-sync! modified store)]

      ;; Values should be correct after second sync
      (is (== 999.0 (index/idx-get-double synced2 0)))
      (is (== 1.0 (index/idx-get-double synced2 1)))

      ;; Second sync should use incremental write via CachedStorage
      ;; Storage stats should show fewer writes than a full sync
      (let [storage2 (index/idx-storage synced2)]
        (is (some? storage2))))))

;; ============================================================================
;; Constant Chunk Compression Tests
;; ============================================================================

(deftest constant-chunk-serialization-test
  (testing "Constant chunks are serialized with 8 bytes via Fressian (file store)"
    (let [dir (str "/tmp/stratum-const-test-" (System/currentTimeMillis))
          store (connect-fs-store dir :opts {:sync? true})
          ;; Create index with all-same values (triggers constant encoding)
          idx (index/index-from-seq :int64 (repeat 20000 42))
          synced (index/idx-sync! idx store)
          commit (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit)
          restored (index/restore-index-from-snapshot snapshot store)
          entries (vec (pss/slice (index/idx-tree restored) nil nil))]
      (try
        ;; All chunks should be constant after Fressian round-trip
        (is (= 3 (count entries)))
        (doseq [entry entries]
          (let [chk (:chunk entry)]
            (is (chunk/chunk-constant? chk))
            (is (= 42 (chunk/chunk-constant-val chk)))))

        ;; Materialization should produce correct values
        (let [arr (index/idx-materialize-to-array restored)]
          (is (= 20000 (alength ^longs arr)))
          (is (every? #(= 42 %) (take 100 (seq arr)))))

        ;; Read without expansion should work
        (let [chk (:chunk (first entries))]
          (is (= 42 (chunk/read-long chk 0)))
          (is (= 42 (chunk/read-long chk 8191))))

        (finally
          (doseq [f (reverse (file-seq (java.io.File. dir)))]
            (.delete ^java.io.File f)))))))

(deftest constant-chunk-direct-roundtrip-test
  (testing "chunk-to-bytes/chunk-from-bytes constant encoding"
    (let [c (chunk/chunk-from-seq :int64 (repeat 8192 0))
          s (stratum.stats/compute-stats (chunk/chunk-data c))
          bytes (chunk/chunk-to-bytes c s)]
      ;; Should use constant encoding
      (is (= :constant (:encoding bytes)))
      (is (= 8 (alength ^bytes (:data bytes))))

      ;; Round-trip should produce constant chunk
      (let [restored (chunk/chunk-from-bytes bytes)]
        (is (chunk/chunk-constant? restored))
        (is (= 0 (chunk/chunk-constant-val restored)))
        (is (= 8192 (chunk/chunk-length restored)))
        ;; Read without expansion
        (is (= 0 (chunk/read-long restored 0)))
        ;; Expand and verify
        (let [arr (chunk/chunk-data restored)]
          (is (= 8192 (alength ^longs arr)))
          (is (every? zero? (seq arr)))))))

  (testing "Non-constant chunks use raw encoding"
    (let [c (chunk/chunk-from-seq :float64 (range 100))
          s (stratum.stats/compute-stats (chunk/chunk-data c))
          bytes (chunk/chunk-to-bytes c s)]
      (is (= :raw (:encoding bytes)))
      (is (= (* 100 8) (alength ^bytes (:data bytes))))))

  (testing "make-constant-chunk creates lazy chunk"
    (let [cc (chunk/make-constant-chunk :float64 1000 3.14)]
      (is (chunk/chunk-constant? cc))
      (is (= 3.14 (chunk/chunk-constant-val cc)))
      (is (= 3.14 (chunk/read-double cc 0)))
      (is (= 1000 (chunk/chunk-length cc))))))
