(ns stratum.storage
  "Storage support for stratum snapshots and chunks.

   Provides:
   - Dataset-level commit/branch tracking via konserve
   - Index-level commit storage (no branches)
   - PSS-backed chunk persistence (nodes stored as flat UUID keys)
   - Garbage collection for unreachable PSS nodes

   Storage layout:
     [:datasets :branches]           → #{\"main\" \"feature-1\"}
     [:datasets :heads \"main\"]     → <dataset-commit-uuid>
     [:datasets :commits <uuid>]     → dataset-snapshot
     [:indices  :commits <uuid>]     → index-snapshot
     <uuid>                          → serialized PSS node (Leaf or Branch)"
  (:require [konserve.core :as k]
            [hasch.core :as hasch]
            [clojure.set :as set]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; GC / Sync Coordination Lock
;; ============================================================================

(def ^:private gc-lock
  "Lock to serialize GC with sync operations. Prevents concurrent ds-sync!
   and ds-gc! from racing (sync writes chunks that GC hasn't yet seen)."
  (Object.))

(defn with-storage-lock
  "Execute f while holding the GC/sync coordination lock.
   Used by ds-sync! to prevent concurrent GC from deleting freshly written chunks."
  [f]
  (locking gc-lock (f)))

;; ============================================================================
;; Commit ID Generation
;; ============================================================================

(defn generate-commit-id
  "Generate a commit ID.
   When content-map is provided, computes a deterministic content-addressed UUID
   using hasch (for crypto-hash/merkle verification). Otherwise returns a random UUID."
  ([] (UUID/randomUUID))
  ([content-map] (hasch/uuid content-map)))

;; ============================================================================
;; Index-Level Storage (No Branches)
;; ============================================================================

(defn write-index-commit!
  "Write an index snapshot to [:indices :commits uuid].
   Indices do not own branches — branch management is at the dataset level."
  [store commit-id snapshot]
  (k/assoc store [:indices :commits commit-id] snapshot {:sync? true})
  snapshot)

(defn load-index-commit
  "Load an index snapshot by commit ID from [:indices :commits uuid]."
  [store commit-id]
  (k/get store [:indices :commits commit-id] nil {:sync? true}))

;; ============================================================================
;; Dataset-Level Storage (Branch Management)
;; ============================================================================

(defn write-dataset-commit!
  "Write a dataset snapshot to [:datasets :commits uuid].
   Immutable — once written, never changes."
  [store commit-id snapshot]
  (k/assoc store [:datasets :commits commit-id] snapshot {:sync? true})
  snapshot)

(defn update-dataset-head!
  "Update a dataset branch HEAD to point to a commit ID.
   [:datasets :heads branch] → commit-uuid"
  [store branch commit-id]
  (k/assoc store [:datasets :heads branch] commit-id {:sync? true}))

(defn load-dataset-head
  "Load the HEAD commit ID for a dataset branch.
   Returns UUID or nil if branch has no commits."
  [store branch]
  (k/get store [:datasets :heads branch] nil {:sync? true}))

(defn load-dataset-commit
  "Load a dataset snapshot by commit ID."
  [store commit-id]
  (k/get store [:datasets :commits commit-id] nil {:sync? true}))

(defn list-dataset-branches
  "List all registered dataset branches."
  [store]
  (k/get store [:datasets :branches] nil {:sync? true}))

(defn register-dataset-branch!
  "Register a new dataset branch in the branch set (atomic read-modify-write)."
  [store branch]
  (k/update store [:datasets :branches] #(conj (or % #{}) branch) {:sync? true}))

(defn unregister-dataset-branch!
  "Remove a dataset branch from the branch set and delete its HEAD pointer."
  [store branch]
  (k/update store [:datasets :branches] #(disj (or % #{}) branch) {:sync? true})
  (k/dissoc store [:datasets :heads branch] {:sync? true}))

;; ============================================================================
;; Garbage Collection (Dataset-Aware, PSS-backed)
;; ============================================================================

(defn collect-live-index-commits
  "Collect all index commit IDs referenced by dataset snapshots."
  [store dataset-commit-ids]
  (reduce
   (fn [live-idx-commits ds-commit-id]
     (if-let [ds-snapshot (load-dataset-commit store ds-commit-id)]
       (into live-idx-commits
             (keep (fn [[_col-name col-info]]
                     (:index-commit col-info))
                   (:columns ds-snapshot)))
       live-idx-commits))
   #{}
   dataset-commit-ids))

(defn collect-live-pss-addresses
  "Collect all PSS node addresses reachable from live index snapshots.
   Walks PSS trees via pss/walk-addresses to find all stored node UUIDs."
  [store index-commit-ids]
  (let [live-addrs (atom #{})]
    (doseq [idx-commit-id index-commit-ids]
      (when-let [snapshot (load-index-commit store idx-commit-id)]
        (when-let [pss-root (:pss-root snapshot)]
          ;; Create a CachedStorage to restore the tree for walking
          (let [cached-storage ((requiring-resolve 'stratum.cached-storage/create-storage)
                                store {:crypto-hash? (:crypto-hash? (:metadata snapshot))})
                cmp @(requiring-resolve 'stratum.index/chunk-entry-comparator)
                tree (pss/restore-by cmp pss-root cached-storage
                                     {:measure @(requiring-resolve 'stratum.index/chunk-entry-measure-ops)
                                      :branching-factor @(requiring-resolve 'stratum.cached-storage/BRANCHING_FACTOR)
                                      :ref-type :weak})]
            ;; walk-addresses calls consume-fn with each address; return true to recurse
            (pss/walk-addresses tree (fn [addr] (swap! live-addrs conj addr) true))))))
    @live-addrs))

(defn- list-all-keys-by-prefix
  "List all keys with a given 2-element vector prefix."
  [store prefix-1 prefix-2]
  (let [all-key-infos (k/keys store {:sync? true})
        all-keys (map :key all-key-infos)]
    (->> all-keys
         (filter #(and (vector? %)
                       (= 3 (count %))
                       (= prefix-1 (first %))
                       (= prefix-2 (second %))))
         (map #(nth % 2))
         set)))

(defn- list-all-flat-uuid-keys
  "List all flat UUID keys in konserve (PSS node addresses)."
  [store]
  (let [all-key-infos (k/keys store {:sync? true})
        all-keys (map :key all-key-infos)]
    (->> all-keys
         (filter #(instance? UUID %))
         set)))

(defn gc!
  "Mark-and-sweep GC from dataset branch heads.

   1. Mark: Load all branches from [:datasets :branches]
   2. For each branch: load HEAD commit, walk parent chain
   3. Collect reachable dataset commits → index commits → PSS node addresses
   4. Sweep: Delete unreachable PSS nodes, index commits, dataset commits

   Returns: map with :deleted-pss-nodes, :deleted-index-commits, :deleted-dataset-commits counts."
  [store]
  (locking gc-lock
    (let [;; 1. Find all branches and their HEAD commits
          branches (or (list-dataset-branches store) #{})

        ;; 2. Walk parent chains to find all reachable dataset commits
          reachable-ds-commits
          (loop [queue (vec (keep #(load-dataset-head store %) branches))
                 visited #{}]
            (if (empty? queue)
              visited
              (let [[current & rest] queue]
                (if (or (nil? current) (visited current))
                  (recur (vec rest) visited)
                  (let [snapshot (load-dataset-commit store current)
                        parents (when snapshot (seq (:parents snapshot)))]
                    (recur (into (vec rest) parents)
                           (conj visited current)))))))

        ;; 3. Collect reachable index commits from dataset snapshots
          reachable-idx-commits (collect-live-index-commits store reachable-ds-commits)

        ;; 4. Collect reachable PSS node addresses from index snapshots
          reachable-pss-addrs (collect-live-pss-addresses store reachable-idx-commits)

        ;; 5. List ALL stored keys
          all-pss-addrs (list-all-flat-uuid-keys store)
          all-idx-commits (list-all-keys-by-prefix store :indices :commits)
          all-ds-commits (list-all-keys-by-prefix store :datasets :commits)

        ;; 6. Compute dead sets
          dead-pss-addrs (set/difference all-pss-addrs reachable-pss-addrs)
          dead-idx-commits (set/difference all-idx-commits reachable-idx-commits)
          dead-ds-commits (set/difference all-ds-commits reachable-ds-commits)]

    ;; 7. Sweep
      (doseq [addr dead-pss-addrs]
        (k/dissoc store addr {:sync? true}))
      (doseq [idx-uuid dead-idx-commits]
        (k/dissoc store [:indices :commits idx-uuid] {:sync? true}))
      (doseq [ds-uuid dead-ds-commits]
        (k/dissoc store [:datasets :commits ds-uuid] {:sync? true}))

      {:deleted-pss-nodes (count dead-pss-addrs)
       :deleted-index-commits (count dead-idx-commits)
       :deleted-dataset-commits (count dead-ds-commits)
       :kept-pss-nodes (count reachable-pss-addrs)
       :kept-index-commits (count reachable-idx-commits)
       :kept-dataset-commits (count reachable-ds-commits)})))

;; ============================================================================
;; Temporal Lookups
;; ============================================================================

(defn find-commit-by-metadata
  "Walk commit history from branch HEAD, return first commit-id whose
   metadata satisfies pred-fn. Used for Datahike tx lookup.

   Walks from HEAD backwards through parent chain. Returns the commit UUID
   of the first commit where (pred-fn (:metadata snapshot)) is truthy,
   or nil if no match is found.

   Options:
     :limit - maximum commits to walk (default 1000)"
  ([store branch pred-fn]
   (find-commit-by-metadata store branch pred-fn {}))
  ([store branch pred-fn {:keys [limit] :or {limit 1000}}]
   (let [head-id (load-dataset-head store branch)]
     (when head-id
       (loop [queue [head-id]
              visited #{}
              n 0]
         (when (and (seq queue) (< n limit))
           (let [[current & rest] queue]
             (if (or (nil? current) (visited current))
               (recur (vec rest) visited (inc n))
               (let [snapshot (load-dataset-commit store current)]
                 (if (and snapshot (pred-fn (:metadata snapshot)))
                   current
                   (recur (into (vec rest) (when snapshot (seq (:parents snapshot))))
                          (conj visited current)
                          (inc n))))))))))))

(comment
  ;; Example usage
  (require '[konserve.memory :refer [new-mem-store]])
  (require '[stratum.index :as index])

  ;; Create store
  (def store (new-mem-store (atom {}) {:sync? true}))

  ;; Create and commit an index (no branch — indices don't own branches)
  (def idx (-> (index/make-index :float64)
               index/idx-transient
               (index/idx-append! 1.0)
               (index/idx-append! 2.0)
               index/idx-persistent!))
  (def synced (index/idx-sync! idx store))

  ;; GC from dataset branch heads
  (gc! store))
