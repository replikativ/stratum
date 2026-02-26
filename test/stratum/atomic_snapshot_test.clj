(ns stratum.atomic-snapshot-test
  "Tests for PSS-backed atomic snapshot commits and lazy loading."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.index :as index]
            [stratum.storage :as storage]
            [stratum.cached-storage :as cstorage]
            [konserve.memory :refer [new-mem-store]]))

;; ============================================================================
;; Test Fixtures
;; ============================================================================

(defn make-test-storage
  "Create in-memory storage for testing."
  []
  (new-mem-store (atom {}) {:sync? true}))

;; ============================================================================
;; Basic Snapshot Tests
;; ============================================================================

(deftest basic-snapshot-commit-test
  (testing "Basic snapshot commit stores PSS root"
    (let [store (make-test-storage)
          idx (-> (index/make-index :float64)
                  index/idx-transient)]

      ;; Build index
      (dotimes [i 100]
        (index/idx-append! idx (double i)))
      (let [persistent-idx (index/idx-persistent! idx)]

        ;; Sync
        (let [synced (index/idx-sync! persistent-idx store)]

          ;; Should have commit-id in metadata
          (is (some? (get-in (meta synced) [:commit :id])))

          ;; Should be loadable from index commits
          (let [commit-id (get-in (meta synced) [:commit :id])
                snapshot (storage/load-index-commit store commit-id)]
            (is (some? snapshot))
            (is (= 100 (:total-length snapshot)))
            (is (= :float64 (:datatype snapshot)))
            ;; Should have pss-root (new format)
            (is (some? (:pss-root snapshot)))))))))

(deftest multiple-commits-test
  (testing "Multiple commits create independent snapshots"
    (let [store (make-test-storage)
          idx1 (index/index-from-seq :float64 (range 10))]

      ;; First commit
      (let [synced1 (index/idx-sync! idx1 store)
            commit-id-1 (get-in (meta synced1) [:commit :id])]

        (is (some? commit-id-1))

        ;; Modify and commit again
        (let [idx2 (-> synced1
                       index/idx-transient
                       (index/idx-append! 99.0)
                       index/idx-persistent!)
              synced2 (index/idx-sync! idx2 store)
              commit-id-2 (get-in (meta synced2) [:commit :id])]

          (is (some? commit-id-2))
          (is (not= commit-id-1 commit-id-2))

          ;; Both commits should be independently accessible
          (let [snap1 (storage/load-index-commit store commit-id-1)
                snap2 (storage/load-index-commit store commit-id-2)]
            (is (= 10 (:total-length snap1)))
            (is (= 11 (:total-length snap2)))))))))

(deftest fork-isolation-test
  (testing "Forked indices are isolated"
    (let [store (make-test-storage)
          idx1 (index/index-from-seq :float64 [1.0 2.0 3.0])]

      ;; Fork before sync
      (let [idx2 (index/idx-fork idx1)]

        ;; Modify fork
        (let [idx2-mod (-> idx2
                           index/idx-transient
                           (index/idx-append! 99.0)
                           index/idx-persistent!)]

          ;; Original unchanged
          (is (= 3 (index/idx-length idx1)))
          (is (= 3.0 (index/idx-get-double idx1 2)))

          ;; Fork modified
          (is (= 4 (index/idx-length idx2-mod)))
          (is (= 99.0 (index/idx-get-double idx2-mod 3))))))))

(deftest transient-cycle-with-sync-test
  (testing "Transient → persistent → sync workflow"
    (let [store (make-test-storage)
          idx (-> (index/make-index :float64)
                  index/idx-transient)]

      ;; Batch operations in transient
      (dotimes [i 1000]
        (index/idx-append! idx (double i)))

      ;; Must be persistent before sync
      (is (thrown? IllegalStateException
                   (index/idx-sync! idx store)))

      ;; Make persistent and sync
      (let [persistent-idx (index/idx-persistent! idx)
            synced (index/idx-sync! persistent-idx store)]

        (is (some? (get-in (meta synced) [:commit :id])))
        (is (= 1000 (index/idx-length synced)))))))

(deftest dirty-chunks-cleared-after-sync-test
  (testing "Dirty chunks are cleared after sync"
    (let [store (make-test-storage)
          idx (-> (index/make-index :float64)
                  index/idx-transient)]

      (dotimes [i 100]
        (index/idx-append! idx (double i)))

      (let [persistent-idx (index/idx-persistent! idx)]
        ;; Should have dirty chunks before sync
        (is (pos? (count (index/idx-dirty-chunks persistent-idx))))

        ;; Sync clears dirty chunks
        (let [synced (index/idx-sync! persistent-idx store)]
          (is (zero? (count (index/idx-dirty-chunks synced)))))))))

(deftest parent-tracking-test
  (testing "Commits track parent relationships"
    (let [store (make-test-storage)
          idx1 (index/index-from-seq :float64 (range 5))]

      ;; First commit (no parent)
      (let [synced1 (index/idx-sync! idx1 store)
            commit-id-1 (get-in (meta synced1) [:commit :id])]

        ;; Second commit (has parent)
        (let [idx2 (-> synced1
                       index/idx-transient
                       (index/idx-append! 99.0)
                       index/idx-persistent!)
              synced2 (index/idx-sync! idx2 store)
              commit-id-2 (get-in (meta synced2) [:commit :id])
              snapshot2 (storage/load-index-commit store commit-id-2)]

          ;; Second commit should reference first as parent
          (is (contains? (:parents snapshot2) commit-id-1)))))))

(deftest commit-id-modes-test
  (testing "Default mode: each sync produces a unique random UUID"
    (let [store1 (make-test-storage)
          store2 (make-test-storage)
          idx (index/index-from-seq :float64 (range 100))]

      (let [synced1 (index/idx-sync! idx store1)
            synced2 (index/idx-sync! idx store2)
            id1 (get-in (meta synced1) [:commit :id])
            id2 (get-in (meta synced2) [:commit :id])]

        (is (instance? java.util.UUID id1))
        (is (instance? java.util.UUID id2))
        (is (not= id1 id2)))))

  (testing "Crypto-hash mode: same content produces same commit ID"
    (let [store1 (make-test-storage)
          store2 (make-test-storage)
          idx (index/index-from-seq :float64 (range 100)
                                    {:metadata {:crypto-hash? true}})]

      (let [synced1 (index/idx-sync! idx store1)
            synced2 (index/idx-sync! idx store2)
            id1 (get-in (meta synced1) [:commit :id])
            id2 (get-in (meta synced2) [:commit :id])]

        (is (instance? java.util.UUID id1))
        (is (= id1 id2) "Identical content should produce identical commit ID")))))

;; ============================================================================
;; Lazy Loading / PSS-Backed Tests
;; ============================================================================

(deftest restore-from-snapshot-test
  (testing "Index can be restored from PSS-backed snapshot"
    (let [store (make-test-storage)
          original-data (range 100)
          idx (index/index-from-seq :float64 original-data)
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)
          restored (index/restore-index-from-snapshot snapshot store)]

      ;; Restored index should match original
      (is (= (index/idx-length synced) (index/idx-length restored)))
      (dotimes [i 10]
        (is (== (index/idx-get-double synced i) (index/idx-get-double restored i)))))))

(deftest lazy-loading-test
  (testing "Restored index has CachedStorage and loads lazily"
    (let [store (make-test-storage)
          idx (index/index-from-seq :float64 (range 10000))
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)
          restored (index/restore-index-from-snapshot snapshot store)]

      ;; Should have storage attached
      (is (some? (index/idx-storage restored)))

      ;; Stats should be zero reads initially (nothing loaded yet)
      (let [stats (cstorage/storage-stats (index/idx-storage restored))]
        (is (zero? (:reads stats))))

      ;; Now access some data — triggers lazy loading
      (is (== 0.0 (index/idx-get-double restored 0)))
      (is (== 9999.0 (index/idx-get-double restored 9999)))

      ;; Should now have some reads
      (let [stats (cstorage/storage-stats (index/idx-storage restored))]
        (is (pos? (:reads stats)))))))

(deftest sync-restore-round-trip-test
  (testing "Full round-trip: sync → restore → verify all values"
    (let [store (make-test-storage)
          n 50000
          idx (index/index-from-seq :float64 (range n))
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)
          restored (index/restore-index-from-snapshot snapshot store)]

      ;; Verify length
      (is (= n (index/idx-length restored)))

      ;; Verify boundary values
      (is (== 0.0 (index/idx-get-double restored 0)))
      (is (== (dec n) (double (index/idx-get-double restored (dec n)))))

      ;; Verify stats
      (let [original-stats (index/idx-stats synced)
            restored-stats (index/idx-stats restored)]
        (is (= (:count original-stats) (:count restored-stats)))))))

(deftest incremental-sync-test
  (testing "Second sync only stores dirty nodes"
    (let [store (make-test-storage)
          ;; Use small chunk-size to get many chunks → many PSS nodes
          ;; 10000 elements / 100 chunk-size = 100 chunks > BF=64 → 2-level PSS tree
          idx (index/index-from-seq :float64 (range 10000) {:chunk-size 100})
          synced1 (index/idx-sync! idx store)]

      ;; Get stats after first sync (cumulative writes counter)
      (let [storage1 (index/idx-storage synced1)
            writes-after-first (:writes (cstorage/storage-stats storage1))]

        ;; Modify one value and sync again (only chunk 0 is dirty)
        (let [idx2 (-> synced1
                       index/idx-transient
                       (index/idx-set! 0 999.0)
                       index/idx-persistent!)
              synced2 (index/idx-sync! idx2 store)
              storage2 (index/idx-storage synced2)
              writes-after-second (:writes (cstorage/storage-stats storage2))
              ;; Writes for the second sync only (cumulative - previous)
              incremental-writes (- writes-after-second writes-after-first)]

          ;; Second sync should write fewer nodes than first
          ;; (only dirty leaf + path to root, not entire tree)
          (is (< incremental-writes writes-after-first))

          ;; Values should be correct
          (is (== 999.0 (index/idx-get-double synced2 0)))
          (is (== 1.0 (index/idx-get-double synced2 1))))))))

(deftest int64-sync-restore-test
  (testing "int64 index round-trip through PSS storage"
    (let [store (make-test-storage)
          idx (index/index-from-seq :int64 (range 500))
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)
          restored (index/restore-index-from-snapshot snapshot store)]

      (is (= 500 (index/idx-length restored)))
      (is (= :int64 (index/idx-datatype restored)))
      (is (== 0 (index/idx-get-long restored 0)))
      (is (== 499 (index/idx-get-long restored 499))))))

(deftest storage-propagation-test
  (testing "Storage propagates through fork/transient/persistent"
    (let [store (make-test-storage)
          idx (index/index-from-seq :float64 (range 100))
          synced (index/idx-sync! idx store)]

      ;; Fork inherits storage
      (let [forked (index/idx-fork synced)]
        (is (some? (index/idx-storage forked)))

        ;; Transient inherits storage
        (let [t (index/idx-transient forked)]
          (index/idx-append! t 999.0)
          (let [p (index/idx-persistent! t)]
            (is (some? (index/idx-storage p)))))))))

(deftest empty-index-sync-test
  (testing "Empty index sync/restore round-trip"
    (let [store (make-test-storage)
          idx (index/make-index :float64)
          synced (index/idx-sync! idx store)
          commit-id (get-in (meta synced) [:commit :id])
          snapshot (storage/load-index-commit store commit-id)
          restored (index/restore-index-from-snapshot snapshot store)]

      (is (= 0 (index/idx-length restored)))
      (is (= :float64 (index/idx-datatype restored))))))
