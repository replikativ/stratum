(ns stratum.yggdrasil
  "Yggdrasil adapter for Stratum PersistentColumnIndex and StratumDataset.

  Wraps a PersistentColumnIndex or StratumDataset and konserve store to expose
  Snapshotable, Branchable, Graphable protocols.

  Value semantics: mutating operations return a new system value.

  Branch management uses yggdrasil-specific konserve keys:
    [:yggdrasil :branches]       → #{:main :feature ...}
    [:yggdrasil :heads branch]   → index-snapshot (full snapshot)"
  (:require [yggdrasil.protocols :as p]
            [stratum.index :as index]
            [stratum.dataset :as dataset]
            [stratum.storage :as storage]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]))

;; ============================================================
;; Internal helpers
;; ============================================================

(defn- commit-id-of [idx]
  (get-in (meta idx) [:commit :id]))

(defn- parent-ids-of [snapshot]
  (:parents snapshot))

(defn- branch-of [idx]
  (get-in (meta idx) [:branch] :main))

;; Yggdrasil-specific branch management (separate from dataset branches)

(defn- ygg-load-branch
  "Load the HEAD snapshot for a yggdrasil branch."
  [store branch]
  (k/get store [:yggdrasil :heads branch] nil {:sync? true}))

(defn- ygg-write-branch-head!
  "Write a snapshot as the HEAD for a yggdrasil branch."
  [store branch snapshot]
  (k/assoc store [:yggdrasil :heads branch] snapshot {:sync? true}))

(defn- ygg-list-branches
  "List all registered yggdrasil branches."
  [store]
  (k/get store [:yggdrasil :branches] nil {:sync? true}))

(defn- ygg-register-branch!
  "Register a yggdrasil branch."
  [store branch]
  (let [existing (or (k/get store [:yggdrasil :branches] nil {:sync? true}) #{})]
    (k/assoc store [:yggdrasil :branches] (conj existing branch) {:sync? true})))

(defn- walk-history
  "Walk commit graph from starting commit-ids, collecting snapshot-ids.
   Returns vector of commit UUIDs in traversal order."
  [store start-ids {:keys [limit] :or {limit 100}}]
  (loop [queue (vec start-ids)
         visited #{}
         result []]
    (if (or (empty? queue)
            (and limit (>= (count result) limit)))
      result
      (let [[current & rest] queue]
        (if (visited current)
          (recur (vec rest) visited result)
          (if-let [snapshot (storage/load-index-commit store current)]
            (let [parents (parent-ids-of snapshot)]
              (recur (into (vec rest) parents)
                     (conj visited current)
                     (conj result (str current))))
            (recur (vec rest) (conj visited current) result)))))))

;; ============================================================
;; StratumSystem record
;; ============================================================

(defrecord StratumSystem [idx store system-name]
  p/SystemIdentity
  (system-id [_]
    (or system-name
        (str "stratum:" (.hashCode idx))))
  (system-type [_] :stratum)
  (capabilities [_]
    {:snapshotable true
     :branchable true
     :graphable true
     :mergeable false  ;; Not yet implemented
     :overlayable false
     :watchable false})

  p/Snapshotable
  (snapshot-id [_]
    (str (commit-id-of idx)))

  (parent-ids [_]
    (let [cid (commit-id-of idx)]
      (if cid
        ;; Load commit by ID to get its parents
        (if-let [snapshot (storage/load-index-commit store cid)]
          (set (map str (filter uuid? (parent-ids-of snapshot))))
          #{})
        ;; No commit yet
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [store store
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))
          snapshot (storage/load-index-commit store uuid)]
      (when snapshot
        (index/restore-index-from-snapshot snapshot store))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [store store
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))
          snapshot (storage/load-index-commit store uuid)]
      (when snapshot
        ;; Derive branch by finding which branch HEAD matches this commit
        (let [branches (or (ygg-list-branches store) #{})
              matching-branch (first (filter (fn [b]
                                               (let [head (ygg-load-branch store b)]
                                                 (and head (= (:commit-id head) uuid))))
                                             branches))]
          {:snapshot-id (str (:commit-id snapshot))
           :parent-ids (set (map str (parent-ids-of snapshot)))
           :timestamp (:timestamp snapshot)
           :branch matching-branch
           :total-length (:total-length snapshot)
           :datatype (:datatype snapshot)}))))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [store store]
      (or (ygg-list-branches store) #{:main})))

  (current-branch [_]
    (branch-of idx))

  (branch! [this name] (p/branch! this name nil nil))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    ;; Create branch from current state (or from snapshot-id/branch)
    (let [store store
          source-branch (if from
                          (if (keyword? from) from (branch-of idx))
                          (branch-of idx))
          source-snapshot (if from
                            (if (keyword? from)
                              (ygg-load-branch store from)
                              (storage/load-index-commit store (parse-uuid (str from))))
                           ;; No from specified - use current branch
                            (ygg-load-branch store source-branch))]
      (ygg-register-branch! store name)
      ;; If source snapshot exists, copy it to new branch
      (when source-snapshot
        ;; Write both as index commit and as branch HEAD
        (storage/write-index-commit! store (:commit-id source-snapshot) source-snapshot)
        (ygg-write-branch-head! store name source-snapshot))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    ;; Remove branch from branches set
    (let [store store
          current-branches (or (ygg-list-branches store) #{})
          updated-branches (disj current-branches name)]
      ;; Update the branches set
      (k/assoc store [:yggdrasil :branches] updated-branches {:sync? true}))
    this)

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    ;; Switch to branch - load branch head and create new StratumSystem
    (let [store store
          snapshot (ygg-load-branch store name)]
      (if snapshot
        ;; Branch has commits - reconstruct index from snapshot
        (let [restored-idx (index/restore-index-from-snapshot snapshot store)
              ;; Update metadata to reflect branch
              updated-idx (vary-meta restored-idx assoc :branch name :commit {:id (:commit-id snapshot)})
              ;; Propagate user metadata from index to system
              idx-meta (meta updated-idx)
              sys (->StratumSystem updated-idx store system-name)]
          ;; Copy namespaced keys from index metadata to system
          (reduce-kv (fn [s k v]
                       (if (qualified-keyword? k) (assoc s k v) s))
                     sys idx-meta))
        ;; Branch exists but no commits yet - create fresh index for this branch
        (let [fresh-idx (-> (index/make-index (index/idx-datatype idx))
                            (vary-meta assoc :branch name))]
          (->StratumSystem fresh-idx store system-name)))))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [store store
          branch (branch-of idx)
          branch-snapshot (ygg-load-branch store branch)]
      (when branch-snapshot
        (walk-history store [(:commit-id branch-snapshot)] opts))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [store store
          uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (set (walk-history store [uuid] {:limit nil}))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [store store
          uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-of-b (set (walk-history store [uuid-b] {:limit nil}))]
      (contains? ancestors-of-b (str uuid-a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [store store
          uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-a (set (walk-history store [uuid-a] {:limit nil}))]
      ;; Walk b's history until we find something in a's ancestors
      (loop [queue [uuid-b]
             visited #{}]
        (when (seq queue)
          (let [[current & rest] queue]
            (if (visited current)
              (recur (vec rest) visited)
              (if (ancestors-a (str current))
                (str current)
                (if-let [snapshot (storage/load-index-commit store current)]
                  (recur (into (vec rest) (parent-ids-of snapshot))
                         (conj visited current))
                  (recur (vec rest) (conj visited current))))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [store store
          branches (p/branches this)
          branch-commits (for [b branches
                               :let [snap (ygg-load-branch store b)]
                               :when snap]
                           (:commit-id snap))
          all-ids (walk-history store branch-commits {:limit nil})]
      {:nodes (into {}
                    (for [id all-ids
                          :let [uuid (parse-uuid id)
                                snapshot (when uuid (storage/load-index-commit store uuid))]
                          :when snapshot]
                      [id {:parent-ids (set (map str (parent-ids-of snapshot)))
                           :meta (p/snapshot-meta this id)}]))
       :branches (into {}
                       (for [b branches
                             :let [snap (ygg-load-branch store b)]
                             :when snap]
                         [b (str (:commit-id snap))]))
       :roots (set (filter
                    (fn [id]
                      (let [uuid (parse-uuid id)
                            snapshot (when uuid (storage/load-index-commit store uuid))]
                        (or (nil? snapshot) (empty? (parent-ids-of snapshot)))))
                    all-ids))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (p/snapshot-meta this snap-id)))

;; ============================================================
;; Constructor
;; ============================================================

(defn create
  "Create a Stratum adapter from an existing index and storage.

   (create idx storage)
   (create idx storage {:system-name \"my-stratum-index\"})"
  ([idx storage] (create idx storage {}))
  ([idx storage opts]
   (->StratumSystem idx storage (:system-name opts))))

(defn create-system
  "Create a fresh Stratum system with in-memory storage.

   Options:
   - :datatype - :float64 (default), :int64
   - :system-name - optional system identifier"
  ([] (create-system {}))
  ([{:keys [datatype system-name] :or {datatype :float64}}]
   (let [;; Use :sync? true to get store directly (no channel)
         store (new-mem-store (atom {}) {:sync? true})
         idx (-> (index/make-index datatype)
                 (vary-meta assoc :branch :main))]
     ;; Register :main branch
     (ygg-register-branch! store :main)
     (->StratumSystem idx store system-name))))

;; ============================================================
;; Dataset Adapter: StratumDatasetSystem
;; ============================================================

(defn- walk-dataset-history
  "Walk dataset commit graph from starting commit-ids, collecting snapshot-ids.
   Uses dataset commits (not index commits). Returns vector of commit UUID strings."
  [store start-ids {:keys [limit] :or {limit 100}}]
  (loop [queue (vec start-ids)
         visited #{}
         result []]
    (if (or (empty? queue)
            (and limit (>= (count result) limit)))
      result
      (let [[current & rest] queue]
        (if (or (nil? current) (visited current))
          (recur (vec rest) visited result)
          (if-let [snapshot (storage/load-dataset-commit store current)]
            (let [parents (:parents snapshot)]
              (recur (into (vec rest) parents)
                     (conj visited current)
                     (conj result (str current))))
            (recur (vec rest) (conj visited current) result)))))))

(defrecord StratumDatasetSystem [ds-atom store system-name]
  p/SystemIdentity
  (system-id [_]
    (or system-name "stratum-dataset"))
  (system-type [_] :stratum-dataset)
  (capabilities [_]
    {:snapshotable true
     :branchable true
     :graphable true
     :committable false
     :mergeable false
     :overlayable false
     :watchable false})

  p/Snapshotable
  (snapshot-id [_]
    (when-let [ci (:commit-info @ds-atom)]
      (str (:id ci))))

  (parent-ids [_]
    (let [ds @ds-atom
          cid (get-in ds [:commit-info :id])]
      (if cid
        (if-let [snapshot (storage/load-dataset-commit store cid)]
          (set (map str (filter uuid? (:parents snapshot))))
          #{})
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (let [uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (when uuid
        (dataset/load store uuid))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))
          snapshot (when uuid (storage/load-dataset-commit store uuid))]
      (when snapshot
        {:snapshot-id (str (:dataset-id snapshot))
         :parent-ids (set (map str (:parents snapshot)))
         :timestamp (:timestamp snapshot)
         :branch (:branch snapshot)
         :name (:name snapshot)
         :row-count (:row-count snapshot)
         :metadata (:metadata snapshot)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (or (storage/list-dataset-branches store) #{}))

  (current-branch [_]
    (or (get-in @ds-atom [:commit-info :branch]) "main"))

  (branch! [this name] (p/branch! this name nil nil))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [_ name from _opts]
    (let [ds @ds-atom
          ;; Resolve source dataset
          source-ds (cond
                      (nil? from) ds
                      (string? from) (dataset/load store from)
                      :else (let [uuid (if (uuid? from) from (parse-uuid (str from)))]
                              (dataset/load store uuid)))
          ;; Fork and sync to new branch
          forked (dataset/fork source-ds)
          synced (dataset/sync! forked store (if (keyword? name) (clojure.core/name name) name))]
      (->StratumDatasetSystem (atom synced) store system-name)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (dataset/delete-branch! store (if (keyword? name) (clojure.core/name name) name))
    this)

  (checkout [this name] (p/checkout this name nil))
  (checkout [_ name _opts]
    (let [branch-name (if (keyword? name) (clojure.core/name name) name)
          loaded-ds (dataset/load store branch-name)]
      (->StratumDatasetSystem (atom loaded-ds) store system-name)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [ds @ds-atom
          branch (or (get-in ds [:commit-info :branch]) "main")
          head-id (storage/load-dataset-head store branch)]
      (when head-id
        (walk-dataset-history store [head-id] opts))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [uuid (if (uuid? snap-id) snap-id (parse-uuid (str snap-id)))]
      (set (walk-dataset-history store [uuid] {:limit nil}))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-of-b (set (walk-dataset-history store [uuid-b] {:limit nil}))]
      (contains? ancestors-of-b (str uuid-a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [uuid-a (if (uuid? a) a (parse-uuid (str a)))
          uuid-b (if (uuid? b) b (parse-uuid (str b)))
          ancestors-a (set (walk-dataset-history store [uuid-a] {:limit nil}))]
      (loop [queue [uuid-b]
             visited #{}]
        (when (seq queue)
          (let [[current & rest] queue]
            (if (visited current)
              (recur (vec rest) visited)
              (if (ancestors-a (str current))
                (str current)
                (if-let [snapshot (storage/load-dataset-commit store current)]
                  (recur (into (vec rest) (:parents snapshot))
                         (conj visited current))
                  (recur (vec rest) (conj visited current))))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [all-branches (p/branches this)
          branch-commits (for [b all-branches
                               :let [head (storage/load-dataset-head store b)]
                               :when head]
                           head)
          all-ids (walk-dataset-history store branch-commits {:limit nil})]
      {:nodes (into {}
                    (for [id all-ids
                          :let [uuid (parse-uuid id)
                                snapshot (when uuid (storage/load-dataset-commit store uuid))]
                          :when snapshot]
                      [id {:parent-ids (set (map str (:parents snapshot)))
                           :meta (p/snapshot-meta this id)}]))
       :branches (into {}
                       (for [b all-branches
                             :let [head (storage/load-dataset-head store b)]
                             :when head]
                         [b (str head)]))
       :roots (set (filter
                    (fn [id]
                      (let [uuid (parse-uuid id)
                            snapshot (when uuid (storage/load-dataset-commit store uuid))]
                        (or (nil? snapshot) (empty? (:parents snapshot)))))
                    all-ids))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (p/snapshot-meta this snap-id)))

;; ============================================================
;; Dataset System Constructor
;; ============================================================

(defn create-dataset-system
  "Create a Yggdrasil adapter for a StratumDataset.

   ds must be synced (has commit-info) or fresh (will get synced on first composite commit).

   Options:
   - :system-name - optional system identifier (default \"stratum-dataset\")"
  ([ds store] (create-dataset-system ds store {}))
  ([ds store {:keys [system-name]}]
   (->StratumDatasetSystem (atom ds) store system-name)))

(defn dataset-system-atom
  "Get the underlying ds-atom for external updates (e.g., d/listen! callback)."
  [sys]
  (:ds-atom sys))
