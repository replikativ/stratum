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

;; ============================================================================
;; Valid-Time Metadata Config
;; ============================================================================

(deftest bitemporal-valid-axis-stamps-temporal-unit
  (testing ":bitemporal {:valid {...}} tags the two named cols with :temporal-unit"
    (let [ds (dataset/make-dataset
              {:e (long-array [1 2])
               :_valid_from (long-array [1700000000000000 1710000000000000])
               :_valid_to   (long-array [1710000000000000 Long/MAX_VALUE])}
              {:name "vt"
               :metadata {:bitemporal
                          {:valid {:from-col :_valid_from
                                   :to-col   :_valid_to}}}})]
      (is (= :micros (:temporal-unit (dataset/column ds :_valid_from))))
      (is (= :micros (:temporal-unit (dataset/column ds :_valid_to))))
      (testing "non-vt columns are unaffected"
        (is (nil? (:temporal-unit (dataset/column ds :e)))))
      (testing "bitemporal-config helper returns the :valid axis with :unit defaulted"
        (is (= {:valid {:from-col :_valid_from :to-col :_valid_to :unit :micros}}
               (dataset/bitemporal-config ds))))
      (testing "valid-time-config / system-time-config per-axis helpers"
        (is (= {:from-col :_valid_from :to-col :_valid_to :unit :micros}
               (dataset/valid-time-config ds)))
        (is (nil? (dataset/system-time-config ds)))))))

(deftest bitemporal-both-axes-symmetric
  (testing "configuring :valid + :system at once stamps both pairs"
    (let [ds (dataset/make-dataset
              {:e (long-array [1])
               :_valid_from  (long-array [1700000000000000])
               :_valid_to    (long-array [Long/MAX_VALUE])
               :_system_from (long-array [1700000000000000])
               :_system_to   (long-array [Long/MAX_VALUE])}
              {:metadata
               {:bitemporal {:valid  {:from-col :_valid_from
                                      :to-col   :_valid_to}
                             :system {:from-col :_system_from
                                      :to-col   :_system_to}}}})]
      (is (= :micros (:temporal-unit (dataset/column ds :_valid_from))))
      (is (= :micros (:temporal-unit (dataset/column ds :_valid_to))))
      (is (= :micros (:temporal-unit (dataset/column ds :_system_from))))
      (is (= :micros (:temporal-unit (dataset/column ds :_system_to))))
      (testing "bitemporal-config exposes both axes"
        (let [cfg (dataset/bitemporal-config ds)]
          (is (some? (:valid cfg)))
          (is (some? (:system cfg))))))))

(deftest bitemporal-only-system-axis
  (testing "system-only datasets are valid — :valid axis is optional"
    (let [ds (dataset/make-dataset
              {:e (long-array [1])
               :_system_from (long-array [1700000000000000])
               :_system_to   (long-array [Long/MAX_VALUE])}
              {:metadata
               {:bitemporal {:system {:from-col :_system_from
                                      :to-col   :_system_to}}}})]
      (is (nil? (dataset/valid-time-config ds)))
      (is (= {:from-col :_system_from :to-col :_system_to :unit :micros}
             (dataset/system-time-config ds))))))

(deftest bitemporal-honours-explicit-unit
  (testing "explicit :unit overrides the :micros default per-axis"
    (let [ds (dataset/make-dataset
              {:_valid_from (long-array [19000 19365])
               :_valid_to   (long-array [19365 Long/MAX_VALUE])}
              {:metadata
               {:bitemporal {:valid {:from-col :_valid_from
                                     :to-col   :_valid_to
                                     :unit     :days}}}})]
      (is (= :days (:temporal-unit (dataset/column ds :_valid_from))))
      (is (= :days (:temporal-unit (dataset/column ds :_valid_to))))
      (is (= :days (:unit (dataset/valid-time-config ds)))))))

(deftest bitemporal-rejects-missing-column
  (testing "make-dataset throws if an axis names a missing column"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #":bitemporal :valid references missing column"
         (dataset/make-dataset
          {:_valid_from (long-array [1 2])}
          {:metadata
           {:bitemporal {:valid {:from-col :_valid_from
                                 :to-col   :_valid_to}}}})))))

(deftest bitemporal-rejects-wrong-type
  (testing "make-dataset throws if an axis column is not :int64"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #":bitemporal :valid column must be :int64"
         (dataset/make-dataset
          {:_valid_from (double-array [1.0 2.0])
           :_valid_to   (long-array   [3 4])}
          {:metadata
           {:bitemporal {:valid {:from-col :_valid_from
                                 :to-col   :_valid_to}}}})))))

(deftest bitemporal-rejects-conflicting-temporal-unit
  (testing "make-dataset throws if a column carries a different :temporal-unit"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #":bitemporal :valid unit conflicts"
         (dataset/make-dataset
          {:_valid_from {:type :int64
                         :data (long-array [1 2])
                         :temporal-unit :millis}
           :_valid_to   (long-array [3 4])}
          {:metadata
           {:bitemporal {:valid {:from-col :_valid_from
                                 :to-col   :_valid_to}}}})))))

(deftest bitemporal-survives-sync-load-round-trip
  (testing "after sync!/load, both axes' tags are restamped"
    (let [store (make-store)
          ds (dataset/make-dataset
              {:e            (index/index-from-seq :int64 [1 2 3])
               :_valid_from  (index/index-from-seq :int64 [1700000000000000
                                                           1710000000000000
                                                           1720000000000000])
               :_valid_to    (index/index-from-seq :int64 [1710000000000000
                                                           1720000000000000
                                                           Long/MAX_VALUE])
               :_system_from (index/index-from-seq :int64 [1700000000000000
                                                           1710000000000000
                                                           1720000000000000])
               :_system_to   (index/index-from-seq :int64 [Long/MAX_VALUE
                                                           Long/MAX_VALUE
                                                           Long/MAX_VALUE])}
              {:name "bt"
               :metadata
               {:bitemporal {:valid  {:from-col :_valid_from
                                      :to-col   :_valid_to}
                             :system {:from-col :_system_from
                                      :to-col   :_system_to}}}})]
      (dataset/sync! ds store "main")
      (let [loaded (dataset/load store "main")]
        (testing "metadata round-trips intact for both axes"
          (let [cfg (dataset/bitemporal-config loaded)]
            (is (= {:from-col :_valid_from :to-col :_valid_to :unit :micros}
                   (:valid cfg)))
            (is (= {:from-col :_system_from :to-col :_system_to :unit :micros}
                   (:system cfg)))))
        (testing "all four window columns carry :temporal-unit after load"
          (is (= :micros (:temporal-unit (dataset/column loaded :_valid_from))))
          (is (= :micros (:temporal-unit (dataset/column loaded :_valid_to))))
          (is (= :micros (:temporal-unit (dataset/column loaded :_system_from))))
          (is (= :micros (:temporal-unit (dataset/column loaded :_system_to)))))
        (testing "non-axis column unaffected"
          (is (nil? (:temporal-unit (dataset/column loaded :e)))))))))

(deftest bitemporal-config-nil-on-non-bitemporal-dataset
  (testing "bitemporal-config returns nil for a dataset with no :bitemporal metadata"
    (let [ds (dataset/make-dataset {:x (long-array [1 2 3])})]
      (is (nil? (dataset/bitemporal-config ds)))
      (is (nil? (dataset/valid-time-config ds)))
      (is (nil? (dataset/system-time-config ds))))))

;; ============================================================================
;; Phase B — temporal write primitives (append! / upsert! / retract!)
;; ============================================================================

(defn- vt-only-ds [rows]
  (let [eids       (mapv :eid rows)
        salaries   (mapv :salary rows)
        vfs        (mapv :_valid_from rows)
        vts        (mapv :_valid_to rows)]
    (dataset/make-dataset
     {:eid         (index/index-from-seq :int64 eids)
      :salary      (index/index-from-seq :int64 salaries)
      :_valid_from (index/index-from-seq :int64 vfs)
      :_valid_to   (index/index-from-seq :int64 vts)}
     {:name "emp"
      :metadata
      {:bitemporal {:valid {:from-col :_valid_from
                            :to-col   :_valid_to}}}})))

(defn- ds-rows [ds]
  (vec (for [i (range (dataset/row-count ds))]
         (into {} (for [k (dataset/column-names ds)]
                    [k (let [col (dataset/column ds k)]
                         (cond
                           (:data col)
                           (let [arr (:data col)]
                             (cond
                               (instance? (Class/forName "[J") arr) (aget ^longs arr i)
                               (instance? (Class/forName "[D") arr) (aget ^doubles arr i)
                               :else nil))
                           (:index col) (index/idx-get-long (:index col) i)))])))))

(deftest append!-auto-stamps-axis-columns-from-tx-meta
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds
                   transient
                   (dataset/append! {:eid 2 :salary 80000}
                                    {:valid-from 1709251200000000})
                   persistent!)
        rows (ds-rows result)
        bob (first (filter #(= 1 (:eid %)) rows))
        alice (first (filter #(= 2 (:eid %)) rows))]
    (testing "explicit :valid-from in tx-meta is stamped"
      (is (= 1709251200000000 (:_valid_from alice))))
    (testing ":valid-to defaults to Long/MAX_VALUE"
      (is (= Long/MAX_VALUE (:_valid_to alice))))
    (testing "previously-existing row untouched"
      (is (= 1704067200000000 (:_valid_from bob)))
      (is (= Long/MAX_VALUE (:_valid_to bob))))))

(deftest append!-defaults-valid-from-to-now-when-tx-meta-absent
  (let [ds  (vt-only-ds [])
        before (* 1000 (System/currentTimeMillis))
        result (-> ds transient
                   (dataset/append! {:eid 1 :salary 100000})
                   persistent!)
        after  (* 1000 (System/currentTimeMillis))
        row    (first (ds-rows result))]
    (testing ":valid-from defaults to now-in-micros within the call window"
      (is (<= before (:_valid_from row) after)))
    (testing ":valid-to defaults to Long/MAX_VALUE"
      (is (= Long/MAX_VALUE (:_valid_to row))))))

(deftest upsert!-closes-old-row-and-appends-new
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]]
                                     :set {:salary 110000}}
                                    {:valid-from 1719792000000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "two rows after upsert: closed-old + open-new"
      (is (= 2 (count rows))))
    (testing "old row's :_valid_to closed to new tx-meta's :valid-from"
      (let [closed (first (filter #(= 100000 (:salary %)) rows))]
        (is (= 1704067200000000 (:_valid_from closed)))
        (is (= 1719792000000000 (:_valid_to closed)))))
    (testing "new row carries merged values + open vt-window"
      (let [new-row (first (filter #(= 110000 (:salary %)) rows))]
        (is (= 1 (:eid new-row)))
        (is (= 1719792000000000 (:_valid_from new-row)))
        (is (= Long/MAX_VALUE (:_valid_to new-row)))))))

(deftest upsert!-skips-already-closed-rows
  ;; Multiple historical versions; only the open one should be closed.
  (let [ds (vt-only-ds [{:eid 1 :salary 90000
                         :_valid_from 1700000000000000
                         :_valid_to   1704067200000000}
                        {:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]]
                                     :set {:salary 110000}}
                                    {:valid-from 1719792000000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "three rows after upsert"
      (is (= 3 (count rows))))
    (testing "already-closed historical row untouched"
      (let [old (first (filter #(= 90000 (:salary %)) rows))]
        (is (= 1700000000000000 (:_valid_from old)))
        (is (= 1704067200000000 (:_valid_to old)))))))

(deftest retract!-closes-open-row-without-reopening
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/retract! {:where [[:= :eid 1]]}
                                     {:valid-from 1719792000000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "row count unchanged (retract is close-only, no append)"
      (is (= 1 (count rows))))
    (testing ":_valid_to closed to tx-meta :valid-from"
      (is (= 1719792000000000 (:_valid_to (first rows)))))))

(deftest upsert!-throws-on-non-bitemporal-dataset
  (let [ds (dataset/make-dataset
            {:eid (index/index-from-seq :int64 [1])
             :salary (index/index-from-seq :int64 [100])})]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"upsert! requires a :valid axis"
         (-> ds transient
             (dataset/upsert! {:where [[:= :eid 1]] :set {:salary 200}}))))))

(deftest retract!-throws-on-non-bitemporal-dataset
  (let [ds (dataset/make-dataset
            {:eid (index/index-from-seq :int64 [1])
             :salary (index/index-from-seq :int64 [100])})]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"retract! requires a :valid axis"
         (-> ds transient
             (dataset/retract! {:where [[:= :eid 1]]}))))))

(deftest append!-works-on-non-bitemporal-dataset-with-tx-meta
  (testing "non-bitemporal datasets ignore tx-meta and require all columns"
    (let [ds (dataset/make-dataset
              {:x (index/index-from-seq :int64 [])})
          result (-> ds transient
                     (dataset/append! {:x 42} {:valid-from 999})
                     persistent!)]
      (is (= 1 (dataset/row-count result)))
      (is (= 42 (index/idx-get-long (:index (dataset/column result :x)) 0))))))

(deftest both-axes-append!-stamps-both-pairs
  (let [ds (dataset/make-dataset
            {:eid          (index/index-from-seq :int64 [])
             :salary       (index/index-from-seq :int64 [])
             :_valid_from  (index/index-from-seq :int64 [])
             :_valid_to    (index/index-from-seq :int64 [])
             :_system_from (index/index-from-seq :int64 [])
             :_system_to   (index/index-from-seq :int64 [])}
            {:metadata
             {:bitemporal
              {:valid  {:from-col :_valid_from  :to-col :_valid_to}
               :system {:from-col :_system_from :to-col :_system_to}}}})
        result (-> ds transient
                   (dataset/append! {:eid 1 :salary 100000}
                                    {:valid-from  1704067200000000
                                     :system-from 1700000000000000})
                   persistent!)
        row (first (for [i (range (dataset/row-count result))]
                     (into {} (for [k (dataset/column-names result)]
                                [k (index/idx-get-long
                                    (:index (dataset/column result k)) i)]))))]
    (testing "both axes' :_from values come from tx-meta"
      (is (= 1704067200000000 (:_valid_from row)))
      (is (= 1700000000000000 (:_system_from row))))
    (testing "both axes' :_to default to Long/MAX_VALUE"
      (is (= Long/MAX_VALUE (:_valid_to row)))
      (is (= Long/MAX_VALUE (:_system_to row))))))

(deftest upsert!-merges-previous-row-values-when-set-is-partial
  ;; If :set only supplies some columns, the new row should inherit the
  ;; rest from the previous (closed) row.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   ;; :set only carries :salary; :eid should be inherited
                   (dataset/upsert! {:where [[:= :eid 1]]
                                     :set {:salary 110000}}
                                    {:valid-from 1719792000000000})
                   persistent!)
        new-row (first (filter #(= 110000 (:salary %)) (ds-rows result)))]
    (testing "new row inherits :eid from the previous open row"
      (is (= 1 (:eid new-row))))))

(deftest upsert!-supports-fn-where-predicate
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}
                        {:eid 2 :salary 80000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where (fn [r] (= 1 (:eid r)))
                                     :set {:salary 110000}}
                                    {:valid-from 1719792000000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "fn predicate matches only eid=1"
      (is (= 3 (count rows)))  ;; 2 original + 1 new
      (is (= 1 (count (filter #(and (= 1 (:eid %)) (= 110000 (:salary %))) rows)))))
    (testing "eid=2 row stays open"
      (let [bob2 (first (filter #(= 2 (:eid %)) rows))]
        (is (= Long/MAX_VALUE (:_valid_to bob2)))))))

;; ============================================================================
;; Phase C — overlap detection (reject by default; auto-split is deferred)
;; ============================================================================

(deftest upsert!-rejects-backdated-write-overlapping-open-row
  ;; Existing open row [Jul-2024, MAX). New write tries :valid-from
  ;; Apr-2024 (backdated). The new write's [Apr, MAX) would overlap
  ;; the open [Jul, MAX). Reject.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1719792000000000  ;; 2024-07-01
                         :_valid_to   Long/MAX_VALUE}])]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"upsert! would overlap"
         (-> ds transient
             (dataset/upsert! {:where [[:= :eid 1]] :set {:salary 110000}}
                              {:valid-from 1714521600000000})))))) ;; 2024-04-30

(deftest upsert!-rejects-overlap-with-already-closed-historical
  ;; Two historical rows: Jan-Jul (closed) and Jul-MAX (open).
  ;; New write :valid-from Apr-2024 would overlap BOTH the closed
  ;; Jan-Jul row AND the open row.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000  ;; 2024-01-01
                         :_valid_to   1719792000000000} ;; 2024-07-01
                        {:eid 1 :salary 110000
                         :_valid_from 1719792000000000
                         :_valid_to   Long/MAX_VALUE}])]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"upsert! would overlap"
         (-> ds transient
             (dataset/upsert! {:where [[:= :eid 1]] :set {:salary 95000}}
                              {:valid-from 1714521600000000}))))))

(deftest upsert!-auto-split-truncates-open-row-when-new-vf-is-inside-it
  ;; Single open row [Jul-2024, MAX). Backdated upsert at Apr-2024 with
  ;; :auto-split? true: row's open vt is in the future, so close-safe
  ;; doesn't apply (row-vf=Jul > new-vf=Apr); instead auto-split must
  ;; DROP the row (it's entirely inside [Apr, MAX)) and append the new
  ;; row from `:set`.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1719792000000000  ;; Jul-2024
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]] :set {:eid 1 :salary 110000}
                                     :auto-split? true}
                                    {:valid-from 1714521600000000}) ;; Apr-2024
                   persistent!)
        rows (ds-rows result)]
    (testing "exactly one row survives — the original was dropped, new appended"
      (is (= 1 (count rows)))
      (is (= 110000 (:salary (first rows))))
      (is (= 1714521600000000 (:_valid_from (first rows))))
      (is (= Long/MAX_VALUE (:_valid_to (first rows)))))))

(deftest upsert!-auto-split-truncates-closed-historical-with-partial-left-overlap
  ;; Closed row [Jan, Jul). Backdated upsert at Apr (inside the row).
  ;; row-vf=Jan < new-vf=Apr < row-vt=Jul → TRUNCATE row to [Jan, Apr),
  ;; then append new [Apr, MAX).
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000  ;; Jan-2024
                         :_valid_to   1719792000000000} ;; Jul-2024
                        {:eid 1 :salary 110000
                         :_valid_from 1719792000000000  ;; Jul-2024
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]] :set {:eid 1 :salary 95000}
                                     :auto-split? true}
                                    {:valid-from 1714521600000000}) ;; Apr-2024
                   persistent!)
        rows (sort-by :_valid_from (ds-rows result))]
    (testing "two rows after split: truncated historical + new write"
      (is (= 2 (count rows))))
    (testing "historical row truncated to [Jan, Apr)"
      (is (= 100000 (:salary (first rows))))
      (is (= 1704067200000000 (:_valid_from (first rows))))
      (is (= 1714521600000000 (:_valid_to (first rows)))))
    (testing "new row covers [Apr, MAX)"
      (is (= 95000 (:salary (second rows))))
      (is (= 1714521600000000 (:_valid_from (second rows))))
      (is (= Long/MAX_VALUE (:_valid_to (second rows)))))
    (testing "the future open row [Jul, MAX) was dropped (row-vf >= new-vf)"
      (is (not-any? #(= 110000 (:salary %)) rows)))))

(deftest upsert!-auto-split-leaves-other-entities-alone
  ;; eid=2 has an open row but doesn't match :where [:= :eid 1].
  ;; Auto-split on eid=1 must not touch eid=2.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1719792000000000
                         :_valid_to   Long/MAX_VALUE}
                        {:eid 2 :salary 80000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]] :set {:eid 1 :salary 999}
                                     :auto-split? true}
                                    {:valid-from 1714521600000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "eid=2 untouched"
      (let [r2 (first (filter #(= 2 (:eid %)) rows))]
        (is (some? r2))
        (is (= 80000 (:salary r2)))
        (is (= 1704067200000000 (:_valid_from r2)))
        (is (= Long/MAX_VALUE (:_valid_to r2)))))))

(deftest upsert!-tolerates-historical-rows-strictly-before-new-vf
  ;; Closed row entirely in the past (Nov-Jan), open row Jan-MAX,
  ;; new write Jul-2024. Only the open row's [Jan, MAX) is touched;
  ;; the closed Nov-Jan row is left alone (entirely before new-vf).
  (let [ds (vt-only-ds [{:eid 1 :salary 90000
                         :_valid_from 1700000000000000  ;; Nov-2023
                         :_valid_to   1704067200000000} ;; Jan-2024
                        {:eid 1 :salary 100000
                         :_valid_from 1704067200000000  ;; Jan-2024
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]] :set {:salary 110000}}
                                    {:valid-from 1719792000000000}) ;; Jul-2024
                   persistent!)
        rows (ds-rows result)]
    (testing "three rows after the historic-aware upsert"
      (is (= 3 (count rows))))
    (testing "ancient closed row untouched"
      (is (= 1700000000000000
             (:_valid_from (first (filter #(= 90000 (:salary %)) rows))))))))

(deftest upsert!-on-new-entity-degenerates-to-insert
  ;; No matching open row exists for eid=99 → upsert! falls through to
  ;; appending the :set payload as a brand-new row, auto-stamping the
  ;; axis columns. Matches the spirit of "INSERT … ON CONFLICT UPDATE".
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 99]]
                                     :set {:eid 99 :salary 50000}}
                                    {:valid-from 1719792000000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "two rows: original eid=1 + new eid=99"
      (is (= 2 (count rows))))
    (testing "new row is the inserted eid=99"
      (let [new-row (first (filter #(= 99 (:eid %)) rows))]
        (is (= 50000 (:salary new-row)))
        (is (= 1719792000000000 (:_valid_from new-row)))
        (is (= Long/MAX_VALUE (:_valid_to new-row)))))))

(deftest retract!-rejects-overlap-with-historical-window
  ;; Closing into a historical period (Jan-Jul is already closed; trying
  ;; to retract from Apr-2024 would touch a window we no longer own).
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   1719792000000000}
                        {:eid 1 :salary 110000
                         :_valid_from 1719792000000000
                         :_valid_to   Long/MAX_VALUE}])]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"retract! would touch rows"
         (-> ds transient
             (dataset/retract! {:where [[:= :eid 1]]}
                               {:valid-from 1714521600000000}))))))

(deftest retract!-auto-split-truncates-historical-and-drops-future
  ;; Closed [Jan, Jul) + open [Jul, MAX). Backdated retract at Apr:
  ;; closed row spans Apr → TRUNCATE to [Jan, Apr); open row's
  ;; vf=Jul >= Apr → DROP.
  (let [ds (vt-only-ds [{:eid 1 :salary 100000
                         :_valid_from 1704067200000000
                         :_valid_to   1719792000000000}
                        {:eid 1 :salary 110000
                         :_valid_from 1719792000000000
                         :_valid_to   Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/retract! {:where [[:= :eid 1]] :auto-split? true}
                                     {:valid-from 1714521600000000})
                   persistent!)
        rows (ds-rows result)]
    (testing "only the truncated historical row survives"
      (is (= 1 (count rows)))
      (is (= 100000 (:salary (first rows))))
      (is (= 1704067200000000 (:_valid_from (first rows))))
      (is (= 1714521600000000 (:_valid_to (first rows)))))))

(deftest ds-delete-rows!-shifts-correctly
  ;; Delete two rows from a 5-row dataset; verify the remaining rows
  ;; keep their column-aligned values.
  (let [ds (vt-only-ds (for [i (range 5)]
                         {:eid i :salary (* 100 (inc i))
                          :_valid_from i :_valid_to Long/MAX_VALUE}))
        out (-> ds transient
                (dataset/ds-delete-rows! [1 3])
                persistent!)
        rows (ds-rows out)]
    (testing "row count is reduced"
      (is (= 3 (count rows)))
      (is (= 3 (dataset/row-count out))))
    (testing "surviving rows keep column alignment"
      (is (= [0 2 4] (mapv :eid rows)))
      (is (= [100 300 500] (mapv :salary rows))))))

(deftest ds-delete-rows!-requires-transient
  (let [ds (vt-only-ds [{:eid 1 :salary 100
                         :_valid_from 1 :_valid_to Long/MAX_VALUE}])]
    (is (thrown? IllegalStateException
                 (dataset/ds-delete-rows! ds [0])))))

(deftest ds-delete-rows!-rejects-out-of-bounds
  (let [ds (vt-only-ds [{:eid 1 :salary 100
                         :_valid_from 1 :_valid_to Long/MAX_VALUE}])]
    (is (thrown? IndexOutOfBoundsException
                 (-> ds transient (dataset/ds-delete-rows! [5]))))))

;; ============================================================================
;; validate-period! — reject zero-width and reverse temporal windows
;; ============================================================================

(deftest append!-rejects-zero-width-valid-window
  (testing "append! with vf == vt throws"
    (let [ds (vt-only-ds [])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid window"
            (-> ds transient
                (dataset/append! {:eid 1 :salary 100
                                  :_valid_from 1000 :_valid_to 1000})))))))

(deftest append!-rejects-reverse-valid-window
  (testing "append! with vf > vt throws"
    (let [ds (vt-only-ds [])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid window"
            (-> ds transient
                (dataset/append! {:eid 1 :salary 100
                                  :_valid_from 2000 :_valid_to 1000})))))))

(deftest bounded-retract!-rejects-zero-width-period
  (testing "retract! with :valid-to == :valid-from throws"
    (let [ds (vt-only-ds [{:eid 1 :salary 100
                           :_valid_from 1000 :_valid_to Long/MAX_VALUE}])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid window"
            (-> ds transient
                (dataset/retract! {:where [[:= :eid 1]]}
                                  {:valid-from 2000 :valid-to 2000})))))))

(deftest bounded-update!-rejects-reverse-period
  (testing "bounded-update! with reverse period throws"
    (let [ds (vt-only-ds [{:eid 1 :salary 100
                           :_valid_from 1000 :_valid_to Long/MAX_VALUE}])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"Invalid valid window"
            (-> ds transient
                (dataset/bounded-update! {:where [[:= :eid 1]] :set {:salary 999}}
                                         {:valid-from 5000 :valid-to 3000})))))))

(deftest bounded-update!-requires-explicit-valid-from
  ;; Regression lock for copilot review-2 #3: `bounded-update!`
  ;; previously checked `:valid-to` is non-nil but not `:valid-from`.
  ;; A nil `:valid-from` propagated to `(long new-vf)` and threw a
  ;; bare NullPointerException instead of a clear `ex-info` —
  ;; misleading both for diagnosis and for any caller that tried
  ;; to `(catch ExceptionInfo …)` the bad-input case.
  (testing "bounded-update! with :valid-to but missing :valid-from throws a clear ex-info"
    (let [ds (vt-only-ds [{:eid 1 :salary 100
                           :_valid_from 1000 :_valid_to Long/MAX_VALUE}])]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"bounded-update! requires tx-meta :valid-from"
            (-> ds transient
                (dataset/bounded-update! {:where [[:= :eid 1]] :set {:salary 999}}
                                         {:valid-to 3000})))))))

;; ============================================================================
;; SCD2-on-both-axes — system-time symmetry on every vt mutation
;;
;; In a bitemporal dataset, an upsert/retract/bounded-update must NOT
;; mutate vt in place on the old row — that would silently rewrite
;; the past. Instead, close _system_to on the old row and append the
;; surviving slice(s) with fresh _system_from. AS OF queries at past
;; system-time then still see the pre-surgery state.
;; ============================================================================

(defn- bitemporal-ds [rows]
  (let [eids       (mapv :eid rows)
        salaries   (mapv :salary rows)
        vfs        (mapv :_valid_from rows)
        vts        (mapv :_valid_to rows)
        sfs        (mapv :_system_from rows)
        sts        (mapv :_system_to rows)]
    (dataset/make-dataset
     {:eid          (index/index-from-seq :int64 eids)
      :salary       (index/index-from-seq :int64 salaries)
      :_valid_from  (index/index-from-seq :int64 vfs)
      :_valid_to    (index/index-from-seq :int64 vts)
      :_system_from (index/index-from-seq :int64 sfs)
      :_system_to   (index/index-from-seq :int64 sts)}
     {:name "emp"
      :metadata
      {:bitemporal {:valid  {:from-col :_valid_from :to-col :_valid_to}
                    :system {:from-col :_system_from :to-col :_system_to}}}})))

(defn- bitemporal-rows [ds]
  (vec (for [i (range (dataset/row-count ds))]
         (into {} (for [k (dataset/column-names ds)]
                    [k (index/idx-get-long (:index (dataset/column ds k)) i)])))))

(deftest upsert!-on-bitemporal-closes-system-to-on-prior-row
  ;; Setup: at system-time 1000, the user asserted "eid 1 salary 100,
  ;; valid Jan-MAX". At system-time 2000, the user upserts a new
  ;; salary 110 valid from Jul. We expect:
  ;;   - The original [Jan, MAX) salary=100 row stays at vf=Jan vt=MAX
  ;;     (vt is NOT mutated) but its system-to is closed to 2000.
  ;;   - A successor [Jan, Jul) salary=100 row is appended at system 2000.
  ;;   - The new [Jul, MAX) salary=110 row is appended at system 2000.
  (let [ds (bitemporal-ds [{:eid 1 :salary 100
                            :_valid_from 1000000 :_valid_to Long/MAX_VALUE
                            :_system_from 1000 :_system_to Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]]
                                     :set {:eid 1 :salary 110}}
                                    {:valid-from 7000000
                                     :system-from 2000})
                   persistent!)
        rows (bitemporal-rows result)]
    (testing "three rows: original (system-closed), successor historical, new current"
      (is (= 3 (count rows))))
    (testing "original row retained vt=MAX but its system-to is now 2000"
      (let [orig (first (filter #(and (= 1000 (:_system_from %))
                                      (= 1000000 (:_valid_from %))
                                      (= Long/MAX_VALUE (:_valid_to %)))
                                rows))]
        (is (some? orig) "the originally-asserted row must still exist")
        (is (= 2000 (:_system_to orig))
            "its system-to should be closed at the surgery system-time")))
    (testing "successor for the truncated [Jan, Jul) at system 2000"
      (let [hist (first (filter #(and (= 2000 (:_system_from %))
                                      (= 1000000 (:_valid_from %))
                                      (= 7000000 (:_valid_to %)))
                                rows))]
        (is (some? hist))
        (is (= 100 (:salary hist)))))
    (testing "new [Jul, MAX) at system 2000 with merged salary"
      (let [new-row (first (filter #(and (= 2000 (:_system_from %))
                                         (= 7000000 (:_valid_from %)))
                                   rows))]
        (is (some? new-row))
        (is (= 110 (:salary new-row)))
        (is (= Long/MAX_VALUE (:_valid_to new-row)))))))

(deftest retract!-bounded-on-bitemporal-closes-system-on-split
  ;; Single row [Jan, Sep) salary=100 originally written at system 1000.
  ;; Retract over [Apr, Jul) at system 2000.
  ;; Expected after surgery:
  ;;   Original [Jan, Sep) system-closed to 2000.
  ;;   Successor [Jan, Apr) at system 2000.
  ;;   Successor [Jul, Sep) at system 2000.
  ;; AS OF system-time 1500: should see the original [Jan, Sep) row.
  ;; AS OF system-time 2500: should see two slices [Jan, Apr) + [Jul, Sep).
  (let [ds (bitemporal-ds [{:eid 1 :salary 100
                            :_valid_from 1000000 :_valid_to 9000000
                            :_system_from 1000 :_system_to Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/retract! {:where [[:= :eid 1]]}
                                     {:valid-from 4000000
                                      :valid-to 7000000
                                      :system-from 2000})
                   persistent!)
        rows (bitemporal-rows result)]
    (testing "three rows: closed original + two replacement slices"
      (is (= 3 (count rows))))
    (testing "original system-closed at 2000"
      (let [orig (first (filter #(and (= 1000 (:_system_from %))
                                      (= 1000000 (:_valid_from %))
                                      (= 9000000 (:_valid_to %)))
                                rows))]
        (is (some? orig))
        (is (= 2000 (:_system_to orig)))))
    (testing "left replacement [Jan, Apr) at system 2000"
      (let [left (first (filter #(and (= 2000 (:_system_from %))
                                      (= 1000000 (:_valid_from %))
                                      (= 4000000 (:_valid_to %)))
                                rows))]
        (is (some? left))))
    (testing "right replacement [Jul, Sep) at system 2000"
      (let [right (first (filter #(and (= 2000 (:_system_from %))
                                       (= 7000000 (:_valid_from %))
                                       (= 9000000 (:_valid_to %)))
                                 rows))]
        (is (some? right))))))

(deftest valid-only-upsert!-keeps-in-place-mutation
  ;; Regression: when no system axis is configured, the existing
  ;; in-place vt mutation behavior must be preserved (no extra
  ;; "historical successor" row appears).
  (let [ds (vt-only-ds [{:eid 1 :salary 100
                         :_valid_from 1000 :_valid_to Long/MAX_VALUE}])
        result (-> ds transient
                   (dataset/upsert! {:where [[:= :eid 1]]
                                     :set {:eid 1 :salary 110}}
                                    {:valid-from 5000})
                   persistent!)
        rows (ds-rows result)]
    (is (= 2 (count rows))
        "valid-only mode: original closed in place + new merged row = 2")))

;; ============================================================================
;; *clock-time-millis* dynamic var for repeatable test runs
;; ============================================================================

(deftest now-in-unit-honors-clock-time-millis-binding
  (testing "Binding *clock-time-millis* pins the time source"
    (let [ds (vt-only-ds [])
          pinned 1704067200000]   ;; Jan-01-2024 millis
      ;; append! without :valid-from defaults to now → should be the pinned value
      ;; in micros (×1000).
      (let [result (binding [dataset/*clock-time-millis* pinned]
                     (-> ds transient
                         (dataset/append! {:eid 1 :salary 100})
                         persistent!))
            row (first (ds-rows result))]
        (is (= (* 1000 pinned) (:_valid_from row))
            "valid-from must equal the pinned clock-time × 1000 (millis → micros)")))))

(deftest now-in-unit-without-binding-uses-system-clock
  (testing "When *clock-time-millis* is nil, falls back to wall-clock"
    (let [ds (vt-only-ds [])
          before-millis (System/currentTimeMillis)
          result (-> ds transient
                     (dataset/append! {:eid 1 :salary 100})
                     persistent!)
          after-millis (System/currentTimeMillis)
          row (first (ds-rows result))
          vf-millis (quot (:_valid_from row) 1000)]
      (is (<= before-millis vf-millis after-millis)
          "valid-from millis should fall in [before, after] window"))))
