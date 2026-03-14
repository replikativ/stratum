;; # Dataset Persistence
;;
;; Stratum datasets are immutable values that persist to
;; [Konserve](https://github.com/replikativ/konserve) stores
;; with branch-based versioning. This notebook walks through
;; the full lifecycle: transient mutation, fork independence,
;; sync/load round-trips, branch management, garbage collection,
;; and column operations.

(ns stratum-book.dataset-persistence
  (:require
   [stratum.dataset :as dataset]
   [stratum.index :as index]
   [stratum.storage :as storage]
   [konserve.memory :refer [new-mem-store]]
   [scicloj.kindly.v4.kind :as kind]))

(defn- make-store [] (new-mem-store (atom {}) {:sync? true}))

;; ---
;;
;; ## Transient / Persistent Protocol
;;
;; Like Clojure's
;; [transients](https://clojure.org/reference/transients),
;; a dataset must be made transient before mutation, then
;; persisted back. This enables batch-efficient, copy-on-write
;; updates.

(def ds
  (dataset/make-dataset
   {:x (index/index-from-seq :float64 [1.0 2.0 3.0])
    :y (index/index-from-seq :int64 [10 20 30])}
   {:name "test"}))

(let [t (transient ds)]
  (dataset/set-at! t :x 0 99.0)
  (let [p (persistent! t)]
    {:transient?  (dataset/transient? t)
     :persistent? (not (dataset/transient? p))
     :x0          (index/idx-get-double (:index (dataset/column p :x)) 0)}))

(kind/test-last
 [(fn [{:keys [persistent? x0]}]
    (and persistent? (== 99.0 x0)))])

;; ### Append rows

(let [t (transient ds)]
  (dataset/append! t {:x 4.0 :y 40})
  (let [p (persistent! t)]
    {:rows (dataset/row-count p)
     :x3   (index/idx-get-double (:index (dataset/column p :x)) 3)
     :y3   (index/idx-get-long (:index (dataset/column p :y)) 3)}))

(kind/test-last
 [(fn [{:keys [rows x3 y3]}]
    (and (= 4 rows) (== 4.0 x3) (== 40 y3)))])

;; ### Guards
;;
;; Modifying a persistent dataset, double-transient, and
;; double-persistent all throw.

(try (dataset/set-at! ds :x 0 99.0) :no-error
     (catch IllegalStateException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try (transient (transient ds)) :no-error
     (catch IllegalStateException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try (persistent! ds) :no-error
     (catch IllegalStateException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; Append with a missing column throws:

(try
  (let [t (transient ds)] (dataset/append! t {:x 2.0}))
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ---
;;
;; ## Fork Independence
;;
;; `fork` creates an O(1) structural copy. Subsequent mutations
;; are independent — the original is never affected.

(let [fork (dataset/fork ds)
      t    (transient fork)]
  (dataset/set-at! t :x 0 99.0)
  (let [p (persistent! t)]
    {:original (index/idx-get-double (:index (dataset/column ds :x)) 0)
     :fork     (index/idx-get-double (:index (dataset/column p :x)) 0)}))

(kind/test-last
 [(fn [{:keys [original fork]}]
    (and (== 1.0 original) (== 99.0 fork)))])

;; Two forks are independent of each other and the original:

(let [fa (dataset/fork ds)
      fb (dataset/fork ds)
      ta (transient fa)
      _  (dataset/set-at! ta :x 0 100.0)
      pa (persistent! ta)
      tb (transient fb)
      _  (dataset/set-at! tb :x 0 200.0)
      pb (persistent! tb)]
  {:original (index/idx-get-double (:index (dataset/column ds :x)) 0)
   :fork-a   (index/idx-get-double (:index (dataset/column pa :x)) 0)
   :fork-b   (index/idx-get-double (:index (dataset/column pb :x)) 0)})

(kind/test-last
 [(fn [{:keys [original fork-a fork-b]}]
    (and (== 1.0 original) (== 100.0 fork-a) (== 200.0 fork-b)))])

;; ---
;;
;; ## Sync / Load Round-Trip
;;
;; `sync!` persists a dataset and its indices atomically.
;; `load` restores it, preserving name, metadata, schema, and data.

(def store (make-store))

(def saved
  (dataset/sync!
   (dataset/make-dataset
    {:price (index/index-from-seq :float64 [100.0 200.0 300.0])
     :qty   (index/index-from-seq :int64 [10 20 30])}
    {:name "trades" :metadata {:source "test"}})
   store "main"))

(def loaded (dataset/load store "main"))

{:name     (dataset/ds-name loaded)
 :metadata (dataset/metadata loaded)
 :rows     (dataset/row-count loaded)
 :schema=  (= (dataset/schema saved) (dataset/schema loaded))
 :price0   (index/idx-get-double (:index (dataset/column loaded :price)) 0)
 :qty2     (index/idx-get-long (:index (dataset/column loaded :qty)) 2)}

(kind/test-last
 [(fn [{:keys [name metadata rows schema= price0 qty2]}]
    (and (= "trades" name)
         (= {:source "test"} metadata)
         (= 3 rows)
         schema=
         (== 100.0 price0)
         (== 30 qty2)))])

;; ### Dirty-column sync
;;
;; Only modified chunks are written on subsequent syncs.

(let [store2 (make-store)
      ds2    (dataset/make-dataset
              {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})
      fork   (dataset/fork ds2)
      t      (transient fork)]
  (dataset/set-at! t :x 1 99.0)
  (let [p      (persistent! t)
        _      (dataset/sync! p store2 "main")
        loaded (dataset/load store2 "main")]
    (index/idx-get-double (:index (dataset/column loaded :x)) 1)))

(kind/test-last
 [(fn [v] (== 99.0 v))])

;; ### Multiple syncs to same branch
;;
;; Each sync updates the HEAD pointer. Loading always gets the latest.

(let [store3 (make-store)
      ds-v1  (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0])})
      ds-v2  (dataset/make-dataset {:x (index/index-from-seq :float64 [10.0 20.0 30.0])})
      s1     (dataset/sync! ds-v1 store3 "main")
      s2     (dataset/sync! ds-v2 store3 "main")
      loaded (dataset/load store3 "main")]
  {:different-commits? (not= (:id (:commit-info s1)) (:id (:commit-info s2)))
   :rows               (dataset/row-count loaded)
   :x0                 (index/idx-get-double (:index (dataset/column loaded :x)) 0)})

(kind/test-last
 [(fn [{:keys [different-commits? rows x0]}]
    (and different-commits? (= 3 rows) (== 10.0 x0)))])

;; ### Load by commit ID

(let [store4 (make-store)
      ds4    (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})
      saved  (dataset/sync! ds4 store4 "main")
      cid    (:id (:commit-info saved))
      loaded (dataset/load store4 cid)]
  (dataset/row-count loaded))

(kind/test-last
 [(fn [n] (= 3 n))])

;; ---
;;
;; ## Branch Management

(let [store5 (make-store)
      ds5    (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0])})]
  (dataset/sync! ds5 store5 "main")
  (dataset/sync! ds5 store5 "feature")
  (storage/list-dataset-branches store5))

(kind/test-last
 [(fn [branches] (= #{"main" "feature"} branches))])

;; ### Delete branch

(let [store6 (make-store)
      ds6    (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0 2.0])})]
  (dataset/sync! ds6 store6 "main")
  (dataset/sync! ds6 store6 "feature")
  (dataset/delete-branch! store6 "feature")
  {:branches (storage/list-dataset-branches store6)
   :head-nil? (nil? (storage/load-dataset-head store6 "feature"))
   :main-rows (dataset/row-count (dataset/load store6 "main"))})

(kind/test-last
 [(fn [{:keys [branches head-nil? main-rows]}]
    (and (= #{"main"} branches) head-nil? (= 2 main-rows)))])

;; ---
;;
;; ## Garbage Collection
;;
;; `gc!` walks all reachable commits from branch HEADs and deletes
;; everything else — orphaned chunks, index commits, and dataset
;; commits.

;; ### Old versions cleaned

(let [store7 (make-store)
      ds-a   (dataset/make-dataset {:x (index/index-from-seq :float64 (range 100))})
      ds-b   (dataset/make-dataset {:x (index/index-from-seq :float64 (range 100 200))})]
  (dataset/sync! ds-a store7 "main")
  (dataset/sync! ds-b store7 "main")
  (let [gc (storage/gc! store7)]
    {:deleted-pss  (pos? (:deleted-pss-nodes gc))
     :deleted-idx  (pos? (:deleted-index-commits gc))
     :deleted-ds   (pos? (:deleted-dataset-commits gc))
     :kept         (pos? (:kept-pss-nodes gc))}))

(kind/test-last
 [(fn [{:keys [deleted-pss deleted-idx deleted-ds kept]}]
    (and deleted-pss deleted-idx deleted-ds kept))])

;; ### All branches deleted → everything cleaned

(let [store8 (make-store)
      ds8    (dataset/make-dataset {:x (index/index-from-seq :float64 (range 100))})]
  (dataset/sync! ds8 store8 "main")
  (dataset/delete-branch! store8 "main")
  (let [gc (storage/gc! store8)]
    {:deleted (pos? (:deleted-pss-nodes gc))
     :kept    (= 0 (:kept-pss-nodes gc))}))

(kind/test-last
 [(fn [{:keys [deleted kept]}]
    (and deleted kept))])

;; ---
;;
;; ## Column Operations
;;
;; `StratumDataset` supports `assoc`, `dissoc`, and named helpers
;; for column manipulation.

;; ### `assoc` — add or replace columns

(def base-ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])}))

;; Add column from array:

(let [ds2 (assoc base-ds :y (long-array [10 20 30]))]
  {:cols (set (dataset/column-names ds2))
   :type (:type (dataset/column ds2 :y))
   :v0   (nth (:data (dataset/column ds2 :y)) 0)})

(kind/test-last
 [(fn [{:keys [cols type v0]}]
    (and (= #{:x :y} cols) (= :int64 type) (= 10 v0)))])

;; Add column from index (zero copy):

(let [y-idx (index/index-from-seq :int64 [10 20 30])
      ds2   (assoc base-ds :y y-idx)]
  {:source   (:source (dataset/column ds2 :y))
   :same?    (identical? y-idx (:index (dataset/column ds2 :y)))})

(kind/test-last
 [(fn [{:keys [source same?]}]
    (and (= :index source) same?))])

;; Add column from sequence:

(let [ds2 (assoc base-ds :y [10.5 20.5 30.5])]
  (:type (dataset/column ds2 :y)))

(kind/test-last
 [(fn [t] (= :float64 t))])

;; Length mismatch throws:

(try (assoc base-ds :y (long-array [10 20])) :no-error
     (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; Replace existing column:

(let [ds2 (assoc base-ds :x (long-array [10 20 30]))]
  {:type (:type (dataset/column ds2 :x))
   :v0   (nth (:data (dataset/column ds2 :x)) 0)})

(kind/test-last
 [(fn [{:keys [type v0]}]
    (and (= :int64 type) (= 10 v0)))])

;; ### `dissoc` — remove columns

(let [ds2 (dataset/make-dataset {:x (double-array [1.0 2.0])
                                 :y (long-array [10 20])})
      ds3 (dissoc ds2 :y)]
  {:cols (set (dataset/column-names ds3))
   :rows (dataset/row-count ds3)})

(kind/test-last
 [(fn [{:keys [cols rows]}]
    (and (= #{:x} cols) (= 2 rows)))])

;; Dissoc on nonexistent column returns identical dataset:

(let [ds2 (dissoc base-ds :nonexistent)]
  (identical? base-ds ds2))

(kind/test-last
 [true?])

;; ### Rename column

(let [ds2 (dataset/rename-column base-ds :x :new-x)]
  {:cols (set (dataset/column-names ds2))
   :rows (dataset/row-count ds2)})

(kind/test-last
 [(fn [{:keys [cols rows]}]
    (and (= #{:new-x} cols) (= 3 rows)))])

;; ### `contains?`

{:has-x?    (contains? base-ds :x)
 :has-foo?  (contains? base-ds :foo)}

(kind/test-last
 [(fn [{:keys [has-x? has-foo?]}]
    (and has-x? (not has-foo?)))])

;; ### ILookup — keyword field access

(let [ds2 (dataset/make-dataset
           {:x (double-array [1.0 2.0])}
           {:name "test" :metadata {:k "v"}})]
  {:name      (:name ds2)
   :metadata  (:metadata ds2)
   :row-count (:row-count ds2)
   :nil-key   (:nonexistent ds2)})

(kind/test-last
 [(fn [{:keys [name metadata row-count nil-key]}]
    (and (= "test" name) (= {:k "v"} metadata)
         (= 2 row-count) (nil? nil-key)))])

;; ---
;;
;; ## ensure-indexed
;;
;; Converts array-backed columns to index-backed, enabling
;; persistence and zone-map pruning.

(let [ds2 (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])
                                 :y (long-array [10 20 30])})]
  {:before-x (dataset/has-index? ds2 :x)
   :before-y (dataset/has-index? ds2 :y)
   :after    (let [indexed (dataset/ensure-indexed ds2)]
               {:x (dataset/has-index? indexed :x)
                :y (dataset/has-index? indexed :y)
                :v (index/idx-get-double (:index (dataset/column indexed :x)) 2)})})

(kind/test-last
 [(fn [{:keys [before-x before-y after]}]
    (and (not before-x) (not before-y)
         (:x after) (:y after) (== 3.0 (:v after))))])

;; Already indexed columns pass through unchanged:

(let [x-idx (index/index-from-seq :float64 [1.0 2.0 3.0])
      ds2   (dataset/make-dataset {:x x-idx})
      result (dataset/ensure-indexed ds2)]
  (identical? ds2 result))

(kind/test-last
 [true?])

;; Dictionary encoding is preserved:

(let [ds2 (dataset/make-dataset {:region (into-array String ["US" "EU" "APAC" "US"])})
      indexed (dataset/ensure-indexed ds2)
      col (dataset/column indexed :region)]
  {:has-index? (dataset/has-index? indexed :region)
   :has-dict?  (some? (:dict col))
   :dict-type  (:dict-type col)})

(kind/test-last
 [(fn [{:keys [has-index? has-dict? dict-type]}]
    (and has-index? has-dict? (= :string dict-type)))])

;; ### ensure-indexed enables sync
;;
;; Array-backed datasets cannot be synced directly. After
;; `ensure-indexed`, sync works.

(let [store9 (make-store)
      ds2    (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})]
  ;; Sync on arrays throws:
  (try (dataset/sync! ds2 store9 "main")
       {:threw false}
       (catch clojure.lang.ExceptionInfo _
         ;; After ensure-indexed, sync works:
         (let [indexed (dataset/ensure-indexed ds2)
               saved   (dataset/sync! indexed store9 "main")
               loaded  (dataset/load store9 "main")]
           {:threw true
            :commit? (some? (:commit-info saved))
            :rows    (dataset/row-count loaded)}))))

(kind/test-last
 [(fn [{:keys [threw commit? rows]}]
    (and threw commit? (= 3 rows)))])

;; ---
;;
;; ## Validation Guards
;;
;; Certain operations are rejected on datasets that aren't in the
;; right state.

;; Fork requires index-backed columns:

(try (dataset/fork (dataset/make-dataset {:x (double-array [1.0])}))
     :no-error
     (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; Transient requires index-backed columns:

(try (transient (dataset/make-dataset {:x (double-array [1.0])}))
     :no-error
     (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; Sync on transient dataset throws:

(let [store10 (make-store)
      ds2    (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0])})]
  (try (dataset/sync! (transient ds2) store10 "main") :no-error
       (catch IllegalStateException _ :threw)))

(kind/test-last
 [(fn [r] (= :threw r))])
