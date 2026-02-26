(ns stratum.dataset-test
  "Tests for dataset persistence, lifecycle, and branch management."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.dataset :as dataset]
            [stratum.index :as index]
            [stratum.storage :as storage]
            [konserve.memory :refer [new-mem-store]]))

(defn- make-store [] (new-mem-store (atom {}) {:sync? true}))

;; ============================================================================
;; Transient/Persistent Lifecycle
;; ============================================================================

(deftest transient-persistent-round-trip-test
  (testing "ds-transient → modify → ds-persistent! round-trip"
    (let [x-idx (index/index-from-seq :float64 [1.0 2.0 3.0])
          y-idx (index/index-from-seq :int64 [10 20 30])
          ds (dataset/make-dataset {:x x-idx :y y-idx}
                                   {:name "test"})]

      ;; Make transient (returns NEW instance)
      (let [t (transient ds)]
        (is (dataset/transient? t))

        ;; Modify
        (dataset/set-at! t :x 0 99.0)

        ;; Make persistent
        (let [p (persistent! t)]
          (is (not (dataset/transient? p)))

          ;; Verify modification
          (let [x-col (dataset/column p :x)]
            (is (== 99.0 (index/idx-get-double (:index x-col) 0)))))))))

(deftest double-transient-throws-test
  (testing "Double transient throws"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (let [t (transient ds)]
        (is (thrown? IllegalStateException (transient t)))))))

(deftest modify-persistent-throws-test
  (testing "Modify persistent throws"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (is (thrown? IllegalStateException (dataset/set-at! ds :x 0 99.0))))))

(deftest double-persistent-throws-test
  (testing "Double persistent throws"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (is (thrown? IllegalStateException (persistent! ds))))))

(deftest ieditable-collection-test
  (testing "IEditableCollection interface works"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})]
      ;; asTransient via Clojure's transient function
      (let [t (transient ds)]
        (is (dataset/transient? t))
        ;; persistent! via Clojure's persistent! function
        (let [p (persistent! t)]
          (is (not (dataset/transient? p))))))))

(deftest ds-append-test
  (testing "ds-append! adds rows to all columns"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])
               :y (index/index-from-seq :int64 [10 20])})]
      (let [t (transient ds)]
        (dataset/append! t {:x 3.0 :y 30})
        (let [p (persistent! t)]

          (is (= 3 (dataset/row-count p)))
          (let [x-col (dataset/column p :x)
                y-col (dataset/column p :y)]
            (is (== 3.0 (index/idx-get-double (:index x-col) 2)))
            (is (== 30 (index/idx-get-long (:index y-col) 2)))))))))

(deftest ds-append-missing-column-throws-test
  (testing "ds-append! with missing column value throws"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0])
               :y (index/index-from-seq :int64 [10])})]
      (let [t (transient ds)]
        (is (thrown? clojure.lang.ExceptionInfo
                     (dataset/append! t {:x 2.0})))))))

;; ============================================================================
;; Fork Independence
;; ============================================================================

(deftest fork-independence-test
  (testing "Fork → modify fork → original unchanged"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})]

      (let [fork (dataset/fork ds)
            t (transient fork)]
        ;; Modify fork
        (dataset/set-at! t :x 0 99.0)
        (let [p (persistent! t)]

          ;; Original unchanged
          (let [orig-x (dataset/column ds :x)]
            (is (== 1.0 (index/idx-get-double (:index orig-x) 0))))

          ;; Fork modified
          (let [fork-x (dataset/column p :x)]
            (is (== 99.0 (index/idx-get-double (:index fork-x) 0)))))))))

(deftest fork-both-directions-test
  (testing "Fork → modify both forks → independent results"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})]

      (let [fork-a (dataset/fork ds)
            fork-b (dataset/fork ds)]

        ;; Modify fork-a
        (let [ta (transient fork-a)]
          (dataset/set-at! ta :x 0 100.0)
          (let [pa (persistent! ta)]

            ;; Modify fork-b
            (let [tb (transient fork-b)]
              (dataset/set-at! tb :x 0 200.0)
              (let [pb (persistent! tb)]

                ;; Both independent
                (is (== 100.0 (index/idx-get-double (:index (dataset/column pa :x)) 0)))
                (is (== 200.0 (index/idx-get-double (:index (dataset/column pb :x)) 0)))

                ;; Original unchanged
                (is (== 1.0 (index/idx-get-double (:index (dataset/column ds :x)) 0)))))))))))

;; ============================================================================
;; Sync/Load Round-trip
;; ============================================================================

(deftest sync-load-round-trip-test
  (testing "Create dataset → sync → load → verify identical data"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:price (index/index-from-seq :float64 [100.0 200.0 300.0])
               :qty (index/index-from-seq :int64 [10 20 30])}
              {:name "trades" :metadata {:source "test"}})]

      (let [saved (dataset/sync! ds store "main")
            loaded (dataset/load store "main")]

        ;; Metadata preserved
        (is (= "trades" (dataset/ds-name loaded)))
        (is (= {:source "test"} (dataset/metadata loaded)))
        (is (= 3 (dataset/row-count loaded)))

        ;; Schema preserved
        (is (= (dataset/schema saved) (dataset/schema loaded)))

        ;; Data preserved
        (let [price-col (dataset/column loaded :price)
              qty-col (dataset/column loaded :qty)]
          (is (= :index (:source price-col)))
          (is (= :index (:source qty-col)))
          (is (== 100.0 (index/idx-get-double (:index price-col) 0)))
          (is (== 300.0 (index/idx-get-double (:index price-col) 2)))
          (is (== 10 (index/idx-get-long (:index qty-col) 0)))
          (is (== 30 (index/idx-get-long (:index qty-col) 2))))))))

(deftest sync-dirty-columns-test
  (testing "Sync with dirty columns → load → verify"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})]

      ;; Modify and persist
      (let [fork (dataset/fork ds)
            t (transient fork)]
        (dataset/set-at! t :x 1 99.0)
        (let [p (persistent! t)]

          ;; Sync modified dataset
          (let [saved (dataset/sync! p store "main")
                loaded (dataset/load store "main")]
            (is (== 99.0 (index/idx-get-double
                          (:index (dataset/column loaded :x)) 1)))))))))

(deftest multiple-syncs-same-branch-test
  (testing "Multiple syncs to same branch (HEAD updates)"
    (let [store (make-store)
          ds1 (dataset/make-dataset
               {:x (index/index-from-seq :float64 [1.0 2.0])})]

      ;; First sync
      (let [saved1 (dataset/sync! ds1 store "main")
            commit-1 (:id (:commit-info saved1))]

        ;; Second sync with different data
        (let [ds2 (dataset/make-dataset
                   {:x (index/index-from-seq :float64 [10.0 20.0 30.0])})]
          (let [saved2 (dataset/sync! ds2 store "main")
                commit-2 (:id (:commit-info saved2))]

            ;; Commits are different
            (is (not= commit-1 commit-2))

            ;; Load gets latest
            (let [loaded (dataset/load store "main")]
              (is (= 3 (dataset/row-count loaded)))
              (is (== 10.0 (index/idx-get-double
                            (:index (dataset/column loaded :x)) 0))))))))))

(deftest sync-to-different-branches-test
  (testing "Sync to different branches → both loadable"
    (let [store (make-store)
          ds1 (dataset/make-dataset
               {:x (index/index-from-seq :float64 [1.0 2.0])}
               {:name "ds1"})
          ds2 (dataset/make-dataset
               {:x (index/index-from-seq :float64 [10.0 20.0 30.0])}
               {:name "ds2"})]

      (dataset/sync! ds1 store "main")
      (dataset/sync! ds2 store "feature")

      ;; Both loadable
      (let [loaded-main (dataset/load store "main")
            loaded-feature (dataset/load store "feature")]
        (is (= 2 (dataset/row-count loaded-main)))
        (is (= 3 (dataset/row-count loaded-feature)))))))

(deftest load-by-commit-id-test
  (testing "Load dataset by commit UUID"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})]

      (let [saved (dataset/sync! ds store "main")
            commit-id (:id (:commit-info saved))
            loaded (dataset/load store commit-id)]
        (is (= 3 (dataset/row-count loaded)))))))

;; ============================================================================
;; Branch Management
;; ============================================================================

(deftest list-branches-test
  (testing "list-dataset-branches after syncing"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0])})]

      (dataset/sync! ds store "main")
      (dataset/sync! ds store "feature")

      (is (= #{"main" "feature"} (storage/list-dataset-branches store))))))

(deftest delete-branch-test
  (testing "ds-delete-branch! removes HEAD but not data"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])})]

      (dataset/sync! ds store "main")
      (dataset/sync! ds store "feature")

      ;; Delete feature branch
      (dataset/delete-branch! store "feature")

      ;; Branch removed from registry
      (is (= #{"main"} (storage/list-dataset-branches store)))

      ;; HEAD pointer removed
      (is (nil? (storage/load-dataset-head store "feature")))

      ;; Main still works
      (let [loaded (dataset/load store "main")]
        (is (= 2 (dataset/row-count loaded)))))))

;; ============================================================================
;; GC Correctness
;; ============================================================================

(deftest gc-removes-old-versions-test
  (testing "Sync → new version → GC → old chunks deleted"
    (let [store (make-store)
          ds1 (dataset/make-dataset
               {:x (index/index-from-seq :float64 (range 100))})]

      ;; First sync
      (dataset/sync! ds1 store "main")

      ;; New version
      (let [ds2 (dataset/make-dataset
                 {:x (index/index-from-seq :float64 (range 100 200))})]
        (dataset/sync! ds2 store "main"))

      ;; GC should remove old version's chunks
      (let [result (storage/gc! store)]
        ;; Old index + dataset commits should be cleaned
        (is (pos? (:deleted-pss-nodes result)))
        (is (pos? (:deleted-index-commits result)))
        (is (pos? (:deleted-dataset-commits result)))

        ;; Current version preserved
        (is (pos? (:kept-pss-nodes result)))))))

(deftest gc-preserves-shared-data-test
  (testing "Two branches sharing chunks → GC preserves shared chunks"
    (let [store (make-store)
          ;; Sync index once to get a single set of chunk addresses
          idx (index/index-from-seq :float64 (range 100))
          synced-idx (index/idx-sync! idx store)
          idx-commit-id (get-in (meta synced-idx) [:commit :id])

          ;; Both dataset branches reference the same index commit
          ds-commit-main (java.util.UUID/randomUUID)
          ds-commit-feature (java.util.UUID/randomUUID)]

      (storage/write-dataset-commit! store ds-commit-main
                                     {:dataset-id ds-commit-main
                                      :columns {:x {:index-commit idx-commit-id :type :float64}}
                                      :parents #{}})
      (storage/write-dataset-commit! store ds-commit-feature
                                     {:dataset-id ds-commit-feature
                                      :columns {:x {:index-commit idx-commit-id :type :float64}}
                                      :parents #{}})

      (storage/register-dataset-branch! store "main")
      (storage/register-dataset-branch! store "feature")
      (storage/update-dataset-head! store "main" ds-commit-main)
      (storage/update-dataset-head! store "feature" ds-commit-feature)

      ;; GC should delete nothing
      (let [result (storage/gc! store)]
        (is (= 0 (:deleted-pss-nodes result)))))))

(deftest gc-with-no-branches-test
  (testing "GC with no branches → everything deleted"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 (range 100))})]

      ;; Sync then delete the branch
      (dataset/sync! ds store "main")
      (dataset/delete-branch! store "main")

      ;; GC should clean everything
      (let [result (storage/gc! store)]
        (is (pos? (:deleted-pss-nodes result)))
        (is (= 0 (:kept-pss-nodes result)))))))

;; ============================================================================
;; Validation
;; ============================================================================

(deftest fork-with-arrays-throws-test
  (testing "ds-fork with array columns throws"
    (let [ds (dataset/make-dataset
              {:x (double-array [1.0 2.0 3.0])})]
      (is (thrown? clojure.lang.ExceptionInfo (dataset/fork ds))))))

(deftest sync-with-arrays-throws-test
  (testing "ds-sync! with array columns throws"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (double-array [1.0 2.0 3.0])})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (dataset/sync! ds store "main"))))))

(deftest sync-transient-throws-test
  (testing "ds-sync! on transient dataset throws"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (let [t (transient ds)]
        (is (thrown? IllegalStateException
                     (dataset/sync! t store "main")))))))

(deftest transient-with-arrays-throws-test
  (testing "ds-transient with array columns throws"
    (let [ds (dataset/make-dataset
              {:x (double-array [1.0 2.0 3.0])})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (transient ds))))))

;; ============================================================================
;; Query Integration
;; ============================================================================

(deftest dataset-satisfies-idataset-test
  (testing "StratumDataset satisfies IDataset protocol"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])
               :y (long-array [10 20 30])}
              {:name "test"})]
      (is (satisfies? dataset/IDataset ds))
      (is (= "test" (dataset/ds-name ds)))
      (is (= 3 (dataset/row-count ds)))
      (is (dataset/has-index? ds :x))
      (is (not (dataset/has-index? ds :y))))))

(deftest dataset-ilookup-test
  (testing "ILookup works for field access"
    (let [ds (dataset/make-dataset
              {:x (double-array [1.0 2.0])}
              {:name "test" :metadata {:k "v"}})]
      (is (= "test" (:name ds)))
      (is (= {:k "v"} (:metadata ds)))
      (is (= 2 (:row-count ds)))
      (is (nil? (:nonexistent ds))))))

;; ============================================================================
;; Column Operations (Associative)
;; ============================================================================

(deftest assoc-column-with-array-test
  (testing "assoc adds column with array auto-conversion"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ds2 (assoc ds :y (long-array [10 20 30]))]
      (is (= 3 (dataset/row-count ds2)))
      (is (= #{:x :y} (set (dataset/column-names ds2))))
      (let [y-col (dataset/column ds2 :y)]
        (is (= :int64 (:type y-col)))
        (is (= 10 (nth (:data y-col) 0)))))))

(deftest assoc-column-with-index-test
  (testing "assoc adds column with index (zero-copy)"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          y-idx (index/index-from-seq :int64 [10 20 30])
          ds2 (assoc ds :y y-idx)]
      (is (= 3 (dataset/row-count ds2)))
      (is (= #{:x :y} (set (dataset/column-names ds2))))
      (let [y-col (dataset/column ds2 :y)]
        (is (= :index (:source y-col)))
        (is (identical? y-idx (:index y-col)))))))  ;; Zero-copy!

(deftest assoc-column-with-sequence-test
  (testing "assoc adds column from sequence"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ds2 (assoc ds :y [10.5 20.5 30.5])]
      (is (= 3 (dataset/row-count ds2)))
      (is (= #{:x :y} (set (dataset/column-names ds2))))
      (let [y-col (dataset/column ds2 :y)]
        (is (= :float64 (:type y-col)))))))

(deftest assoc-column-length-mismatch-test
  (testing "assoc throws on length mismatch"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Column length mismatch"
                            (assoc ds :y (long-array [10 20])))))))

(deftest assoc-column-to-single-column-dataset-test
  (testing "assoc adds second column to single-column dataset"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ds2 (assoc ds :y [10 20 30])]
      (is (= 3 (dataset/row-count ds2)))
      (is (= #{:x :y} (set (dataset/column-names ds2)))))))

(deftest assoc-column-on-transient-throws-test
  (testing "assoc on transient dataset throws"
    (let [ds (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (let [t (transient ds)]
        (is (thrown-with-msg? IllegalStateException
                              #"Cannot assoc on transient dataset"
                              (assoc t :y [10 20])))))))

(deftest assoc-updates-existing-column-test
  (testing "assoc can update existing column"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ds2 (assoc ds :x (long-array [10 20 30]))]
      (is (= 3 (dataset/row-count ds2)))
      (let [x-col (dataset/column ds2 :x)]
        (is (= :int64 (:type x-col)))
        (is (= 10 (nth (:data x-col) 0)))))))

(deftest dissoc-column-test
  (testing "dissoc removes column"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0])
                                    :y (long-array [10 20])})
          ds2 (dissoc ds :y)]
      (is (= #{:x} (set (dataset/column-names ds2))))
      (is (= 2 (dataset/row-count ds2))))))

(deftest dissoc-nonexistent-column-test
  (testing "dissoc on nonexistent column returns unchanged dataset"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0])})
          ds2 (dissoc ds :nonexistent)]
      (is (identical? ds ds2)))))

(deftest dissoc-on-transient-throws-test
  (testing "dissoc on transient dataset throws"
    (let [ds (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0])})]
      (let [t (transient ds)]
        (is (thrown-with-msg? IllegalStateException
                              #"Cannot dissoc on transient dataset"
                              (dissoc t :x)))))))

(deftest ds-add-column-test
  (testing "ds-add-column explicit API"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ds2 (dataset/add-column ds :y [10 20 30])]
      (is (= #{:x :y} (set (dataset/column-names ds2)))))))

(deftest ds-drop-column-test
  (testing "ds-drop-column explicit API"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0])
                                    :y (long-array [10 20])})
          ds2 (dataset/drop-column ds :y)]
      (is (= #{:x} (set (dataset/column-names ds2)))))))

(deftest ds-rename-column-test
  (testing "ds-rename-column renames column"
    (let [ds (dataset/make-dataset {:old-name (double-array [1.0 2.0])})
          ds2 (dataset/rename-column ds :old-name :new-name)]
      (is (= #{:new-name} (set (dataset/column-names ds2))))
      (is (= 2 (dataset/row-count ds2))))))

(deftest containsKey-test
  (testing "containsKey checks column existence"
    (let [ds (dataset/make-dataset {:x (double-array [1.0 2.0])})]
      (is (contains? ds :x))
      (is (not (contains? ds :nonexistent))))))

(deftest move-column-between-datasets-test
  (testing "Move column from one dataset to another (zero-copy)"
    (let [ds1 (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0 3.0])
                                     :y (index/index-from-seq :int64 [10 20 30])})
          ds2 (dataset/make-dataset {:a (double-array [100.0 200.0 300.0])})
          ;; Extract column from ds1
          y-col (dataset/column ds1 :y)
          y-idx (:index y-col)
          ;; Add to ds2
          ds3 (assoc ds2 :y y-idx)]
      (is (= #{:a :y} (set (dataset/column-names ds3))))
      ;; Verify it's the same index (zero-copy)
      (is (identical? y-idx (:index (dataset/column ds3 :y)))))))

;; ============================================================================
;; ensure-indexed
;; ============================================================================

(deftest ensure-indexed-array-to-index-test
  (testing "ensure-indexed converts array-backed columns to index-backed"
    (let [ds (dataset/make-dataset
              {:x (double-array [1.0 2.0 3.0])
               :y (long-array [10 20 30])})]
      ;; Before: array-backed
      (is (not (dataset/has-index? ds :x)))
      (is (not (dataset/has-index? ds :y)))

      ;; Convert
      (let [indexed (dataset/ensure-indexed ds)]
        ;; After: all index-backed
        (is (dataset/has-index? indexed :x))
        (is (dataset/has-index? indexed :y))
        (is (= 3 (dataset/row-count indexed)))

        ;; Data preserved
        (let [x-col (dataset/column indexed :x)
              y-col (dataset/column indexed :y)]
          (is (== 1.0 (index/idx-get-double (:index x-col) 0)))
          (is (== 3.0 (index/idx-get-double (:index x-col) 2)))
          (is (== 10 (index/idx-get-long (:index y-col) 0)))
          (is (== 30 (index/idx-get-long (:index y-col) 2))))))))

(deftest ensure-indexed-passthrough-test
  (testing "ensure-indexed passes through already index-backed columns"
    (let [x-idx (index/index-from-seq :float64 [1.0 2.0 3.0])
          ds (dataset/make-dataset {:x x-idx})]
      ;; Already index-backed
      (is (dataset/has-index? ds :x))

      ;; ensure-indexed returns same dataset (identity)
      (let [result (dataset/ensure-indexed ds)]
        (is (identical? ds result))))))

(deftest ensure-indexed-dict-preservation-test
  (testing "ensure-indexed preserves dict/dict-type for string-encoded columns"
    (let [strings (into-array String ["US" "EU" "APAC" "US"])
          ds (dataset/make-dataset {:region strings})]
      ;; Before: array-backed with dict
      (let [col (dataset/column ds :region)]
        (is (some? (:dict col)))
        (is (= :string (:dict-type col))))

      ;; Convert
      (let [indexed (dataset/ensure-indexed ds)]
        (is (dataset/has-index? indexed :region))
        ;; Dict preserved
        (let [col (dataset/column indexed :region)]
          (is (some? (:dict col)))
          (is (= :string (:dict-type col))))))))

(deftest ensure-indexed-mixed-columns-test
  (testing "ensure-indexed converts only array-backed, passes index-backed through"
    (let [x-idx (index/index-from-seq :float64 [1.0 2.0 3.0])
          ds (dataset/make-dataset {:x x-idx
                                    :y (long-array [10 20 30])})]
      ;; Mixed: x is index, y is array
      (is (dataset/has-index? ds :x))
      (is (not (dataset/has-index? ds :y)))

      (let [indexed (dataset/ensure-indexed ds)]
        (is (dataset/has-index? indexed :x))
        (is (dataset/has-index? indexed :y))
        ;; x's index is the same object (passthrough)
        (is (identical? (:index (dataset/column ds :x))
                        (:index (dataset/column indexed :x))))))))

(deftest ds-with-parent-test
  (testing "ds-with-parent establishes commit parent chain"
    (let [store (make-store)
          ds1 (dataset/make-dataset
               {:x (index/index-from-seq :float64 [1.0 2.0])}
               {:name "test"})
          saved1 (dataset/sync! ds1 store "main")
          commit1 (:id (:commit-info saved1))
          ;; New dataset with parent
          ds2 (-> (dataset/make-dataset
                   {:x (index/index-from-seq :float64 [10.0 20.0 30.0])}
                   {:name "test"})
                  (dataset/with-parent saved1))
          saved2 (dataset/sync! ds2 store "main")
          commit2 (:id (:commit-info saved2))
          ;; Load commit2's snapshot to check parent
          snapshot (storage/load-dataset-commit store commit2)]
      ;; commit2 should have commit1 as parent
      (is (contains? (:parents snapshot) commit1))
      ;; commit1 has no parent
      (let [snap1 (storage/load-dataset-commit store commit1)]
        (is (empty? (:parents snap1)))))))

(deftest ensure-indexed-enables-sync-test
  (testing "ensure-indexed enables ds-sync! on previously array-backed datasets"
    (let [store (make-store)
          ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})
          ;; ds-sync! would throw on array-backed:
          _ (is (thrown? clojure.lang.ExceptionInfo
                         (dataset/sync! ds store "main")))
          ;; After ensure-indexed, sync works:
          indexed (dataset/ensure-indexed ds)
          saved (dataset/sync! indexed store "main")]
      (is (some? (:commit-info saved)))
      ;; Round-trip:
      (let [loaded (dataset/load store "main")]
        (is (= 3 (dataset/row-count loaded)))))))
