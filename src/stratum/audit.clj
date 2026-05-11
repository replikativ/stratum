(ns stratum.audit
  "Audit-chain verification for `:crypto-hash? true` stratum stores.

   `verify-chain` walks the dataset-commit DAG from a branch HEAD
   backwards via `:parents`, recomputes each commit-id from the stored
   snapshot, and reports any mismatch. Tampering with a stored commit
   flips its recomputed cid (layer-1 detection).

   With `:deep? true` every column's PSS tree under the head dataset
   commit is additionally walked from konserve: each node is read
   directly, its content-addressed UUID is recomputed via
   `cached-storage/gen-address`, and any node whose bytes no longer
   hash back to its address is reported (layer-2 detection — catches
   bytes-level tampering on the underlying .ksv blobs).

   The protocol shape (`IAuditable`, `-merkle-root`,
   `-recompute-merkle-root`) and the result-map vocabulary
   (`{:status :ok|:mismatch|:unsupported|:advisory|:incomplete}`) are
   intentionally identical to datahike's. Bridges across repos can pass
   results through without translation."
  (:require [hasch.core :as hasch]
            [konserve.core :as k]
            [stratum.cached-storage :as cstorage]
            [stratum.dataset]
            [stratum.index]
            [stratum.storage :as storage])
  (:import [org.replikativ.persistent_sorted_set Branch Leaf]
           [stratum.dataset StratumDataset]
           [stratum.index PersistentColumnIndex]
           [java.util UUID]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocol — same shape as datahike.index.audit/IAuditable
;; ============================================================================

(defprotocol IAuditable
  (-merkle-root [this]
    "Cheap. Returns the cached/known content-addressed UUID of this
     thing's current state. Returns nil when no root is available
     (e.g. unsynced). Never throws.")

  (-recompute-merkle-root [this]
    "Expensive. Walks the underlying storage and re-derives the merkle
     root, asserting that every reachable node's bytes hash back to its
     address. Returns a result map; never throws on mismatch:

       {:status :ok          :root <uuid>}
       {:status :mismatch    :root <recomputed-uuid?>
                             :errors [{:address, :expected, :recomputed,
                                       :node-class, :type}]}
       {:status :unsupported :reason <kw>}"))

;; ============================================================================
;; PSS tree walk — read every node from konserve, recompute its address
;; ============================================================================

(defn- walk-pss-address!
  "Walk the PSS subtree rooted at `address`. For each node: read the
   bytes from konserve directly (so a hot in-memory cache cannot mask a
   tampered blob), recompute its content-addressed UUID, and accumulate
   anomalies into the `errors` atom rather than throwing. `verified`
   prunes already-checked subtrees so the cache can be threaded across
   multiple calls (e.g. multiple columns)."
  [store address verified errors]
  (when-not (contains? @verified address)
    (let [node (k/get store address nil {:sync? true})]
      (cond
        (nil? node)
        (swap! errors conj {:type :audit/node-missing :address address})

        :else
        (let [recomputed (cstorage/gen-address node true)]
          (cond
            (not= address recomputed)
            (swap! errors conj {:type :audit/merkle-mismatch
                                :address address
                                :expected address
                                :recomputed recomputed
                                :node-class (.getName (class node))})

            :else
            (do
              (when (instance? Branch node)
                (doseq [child-addr (.addresses ^Branch node)]
                  (walk-pss-address! store child-addr verified errors)))
              (swap! verified conj address))))))))

(defn verify-pss-tree-from-cold
  "Walk the PSS tree rooted at `pss-root` directly from `store`,
   recomputing every node's address. Returns the standard result map.

   `store` may be a raw konserve store; PSS nodes are stratum-specific
   tagged objects so the audit attaches the deserializer handlers
   itself (caller doesn't need a CachedStorage).

   Pass `:verified` (atom #{addresses}) to share dedup state across
   calls — useful when verifying multiple columns of one dataset, since
   stratum often reuses identical column trees across commits."
  ([store pss-root]
   (verify-pss-tree-from-cold store pss-root {}))
  ([store pss-root {:keys [verified]
                    :or {verified (atom #{})}}]
   (cond
     (nil? pss-root)
     {:status :unsupported :reason :no-pss-root}

     :else
     (let [errors (atom [])
           cfg-store (cstorage/attach-pss-handlers store)]
       (walk-pss-address! cfg-store pss-root verified errors)
       (if (seq @errors)
         {:status :mismatch :root nil :errors @errors}
         {:status :ok :root pss-root})))))

;; ============================================================================
;; Commit-id recomputation — mirrors the formulas in dataset.clj/index.clj
;; ============================================================================

(defn- recompute-index-commit-id
  "Re-derive the index commit-id from a stored index snapshot under
   :crypto-hash?. Mirrors stratum.index/idx-sync!'s formula:
   `(hasch/uuid {:pss-root :total-length :datatype :chunk-size})`."
  [snapshot]
  (hasch/uuid (select-keys snapshot [:pss-root :total-length :datatype :chunk-size])))

(defn- recompute-dataset-commit-id
  "Re-derive the dataset commit-id from a stored dataset snapshot under
   :crypto-hash?. Mirrors stratum.dataset/sync!'s formula:
   `(hasch/uuid {:columns :schema :metadata})`."
  [snapshot]
  (hasch/uuid {:columns  (:columns snapshot)
               :schema   (:schema snapshot)
               :metadata (:metadata snapshot)}))

(defn- crypto-hash-snapshot?
  "Whether a stored dataset snapshot was committed under :crypto-hash?."
  [snapshot]
  (boolean (get-in snapshot [:metadata :crypto-hash?])))

;; ============================================================================
;; Layer-1 commit-chain walk
;; ============================================================================

(defn- index-snapshot-crypto?
  "Whether a stored index snapshot was committed under :crypto-hash?.
   Stratum opts into crypto-hash per-index (via the index's `:metadata`),
   so a dataset committed under :crypto-hash? can still hold columns whose
   indexes use random UUID addressing — those indexes can only contribute
   advisory verification."
  [idx-snapshot]
  (boolean (get-in idx-snapshot [:metadata :crypto-hash?])))

(defn- verify-dataset-commit
  "Load the dataset commit at `cid`, recompute its commit-id, and
   return a per-commit verification entry. Returns nil when the commit
   isn't in the store (caller bumps it into :missing)."
  [store cid]
  (when-let [stored (storage/load-dataset-commit store cid)]
    (let [crypto?    (crypto-hash-snapshot? stored)
          recomputed (when crypto? (recompute-dataset-commit-id stored))
          parents    (or (:parents stored) #{})
          ;; Each column references an :index-commit. Only indexes that
          ;; opted into :crypto-hash? have a recomputable commit-id.
          col-checks (when crypto?
                       (vec
                        (keep
                         (fn [[col-name col-info]]
                           (let [idx-cid    (:index-commit col-info)
                                 idx-stored (when idx-cid
                                              (storage/load-index-commit store idx-cid))]
                             (cond
                               (nil? idx-cid)
                               nil

                               (nil? idx-stored)
                               {:column col-name :index-cid idx-cid
                                :status :missing}

                               (not (index-snapshot-crypto? idx-stored))
                               {:column col-name :index-cid idx-cid
                                :status :advisory
                                :reason :crypto-hash-disabled}

                               :else
                               (let [idx-rec (recompute-index-commit-id idx-stored)]
                                 (when (not= idx-cid idx-rec)
                                   {:column col-name :index-cid idx-cid
                                    :recomputed idx-rec :status :mismatch})))))
                         (:columns stored))))
          col-mismatches (filterv #(= :mismatch (:status %)) col-checks)
          col-missing    (filterv #(= :missing (:status %)) col-checks)
          col-advisory   (filterv #(= :advisory (:status %)) col-checks)
          status (cond
                   (not crypto?)              :advisory
                   (seq col-missing)          :incomplete
                   (seq col-mismatches)       :mismatch
                   (not= cid recomputed)      :mismatch
                   (seq col-advisory)         :advisory
                   :else                      :ok)]
      (cond-> {:cid cid :recomputed recomputed
               :parents parents :status status}
        (not crypto?)         (assoc :reason :crypto-hash-disabled)
        (seq col-mismatches)  (assoc :column-mismatches col-mismatches)
        (seq col-missing)     (assoc :column-missing col-missing)
        (seq col-advisory)    (assoc :column-advisory col-advisory)))))

;; ============================================================================
;; Layer-2 deep walk for the head snapshot
;; ============================================================================

(defn- deep-verify-head
  "Walk every column's PSS in `stored-head` and confirm each tree's
   nodes hash back to their addresses. Returns the same shape as
   datahike's deep block: `{:status :ok|:mismatch, :diffs […]}`. A
   shared `verified` atom is threaded across columns — datasets often
   reuse identical columnar trees across commits, so subtree dedup
   across columns is real (not just a no-op like within one tree)."
  [store stored-head]
  (let [verified (atom #{})
        diffs (vec
               (keep
                (fn [[col-name col-info]]
                  (let [idx-cid (:index-commit col-info)
                        idx-snap (when idx-cid
                                   (storage/load-index-commit store idx-cid))
                        pss-root (:pss-root idx-snap)]
                    (cond
                      (nil? idx-snap)
                      {:column col-name :index idx-cid :reason :missing-index}

                      (not (index-snapshot-crypto? idx-snap))
                      {:column col-name :index idx-cid
                       :reason :crypto-hash-disabled}

                      :else
                      (let [result (verify-pss-tree-from-cold
                                    store pss-root {:verified verified})]
                        (case (:status result)
                          :ok          nil
                          :unsupported {:column col-name :index idx-cid
                                        :reason (:reason result)}
                          :mismatch    {:column col-name :index idx-cid
                                        :pss-root pss-root
                                        :errors (:errors result)})))))
                (:columns stored-head)))]
    {:status (if (some :errors diffs) :mismatch :ok)
     :diffs  diffs}))

;; ============================================================================
;; Public entrypoint
;; ============================================================================

(defn verify-chain
  "Walk the dataset commit DAG anchored at `branch`'s HEAD and return
   `{:head, :status, :commits, :mismatches, :missing[, :deep]}`.

   Options:
     :head    — explicit dataset commit-id to start from (default: branch HEAD)
     :branch  — branch name (default: \"main\"). Ignored when :head is set.
     :deep?   — when true, additionally walk every column's PSS tree
                under the head snapshot and confirm each node's bytes
                hash back to its address. Adds a `:deep` entry.
     :limit   — max commits to walk (default: unbounded).

   :status is `:ok | :mismatch | :advisory | :incomplete`. Each entry
   in :commits is `{:cid, :recomputed, :parents, :status [, :reason
   :column-mismatches :column-missing]}`."
  ([store] (verify-chain store {}))
  ([store {:keys [head branch deep? limit]
           :or {branch "main"
                limit Long/MAX_VALUE
                deep? false}}]
   (let [head (or head (storage/load-dataset-head store branch))]
     (when-not head
       (throw (ex-info "verify-chain: no head for branch"
                       {:type :audit/no-head :branch branch})))
     (let [commits (volatile! [])
           missing (volatile! [])
           visited (volatile! #{})]
       (loop [frontier [head] n 0]
         (when (and (seq frontier) (< n limit))
           (let [cid (first frontier) rest-f (rest frontier)]
             (if (contains? @visited cid)
               (recur rest-f n)
               (do (vswap! visited conj cid)
                   (if-let [entry (verify-dataset-commit store cid)]
                     (do (vswap! commits conj entry)
                         (recur (into (vec rest-f)
                                      (remove @visited (:parents entry)))
                                (inc n)))
                     (do (vswap! missing conj cid)
                         (recur rest-f n))))))))
       (let [es @commits
             mism (filterv #(= :mismatch (:status %)) es)
             miss @missing
             adv  (some #(= :advisory (:status %)) es)
             deep (when deep?
                    (when-let [stored-head (storage/load-dataset-commit store head)]
                      (deep-verify-head store stored-head)))
             base-status (cond (seq mism) :mismatch
                               (seq miss) :incomplete
                               adv        :advisory
                               :else      :ok)
             deep-mismatch? (= :mismatch (:status deep))]
         (cond-> {:head head
                  :status (if deep-mismatch? :mismatch base-status)
                  :commits es :mismatches mism :missing miss}
           deep (assoc :deep deep)))))))

(defn ok? [report] (= :ok (:status report)))

;; ============================================================================
;; Live-instance protocol extension — useful for bridges (e.g. datahike's
;; stratum secondary) that hold a live dataset/index value and want a
;; uniform IAuditable surface across repos.
;;
;; -merkle-root returns the cached/known content-addressed UUID; nil pre-sync.
;; -recompute-merkle-root walks storage and returns a result map.
;; ============================================================================

(defn- pci-recompute
  "Walk a PersistentColumnIndex live value: returns a result map by
   reading its committed pss-root from the index snapshot in store."
  [^PersistentColumnIndex idx]
  (let [m       (meta idx)
        cid     (get-in m [:commit :id])
        store   (when-let [storage (.storage idx)]
                  (:store storage))]
    (cond
      (nil? cid)   {:status :unsupported :reason :unsynced}
      (nil? store) {:status :unsupported :reason :no-store}
      :else
      (let [snap (storage/load-index-commit store cid)]
        (cond
          (nil? snap)
          {:status :mismatch :root nil
           :errors [{:type :audit/snapshot-missing :address cid}]}

          (not (index-snapshot-crypto? snap))
          {:status :unsupported :reason :crypto-hash-disabled}

          :else
          (let [pss-result (verify-pss-tree-from-cold store (:pss-root snap))
                idx-rec    (recompute-index-commit-id snap)]
            (cond
              (= :mismatch (:status pss-result)) pss-result
              (not= cid idx-rec)
              {:status :mismatch :root idx-rec
               :errors [{:type :audit/merkle-mismatch
                         :address cid
                         :expected cid
                         :recomputed idx-rec
                         :node-class "stratum.index.PersistentColumnIndex"}]}
              :else {:status :ok :root cid})))))))

(defn- ds-recompute
  "Walk a StratumDataset live value: re-runs the dataset+columns
   verification from its committed dataset commit-id forward, deeply."
  [ds]
  (let [cid (-> ds :commit-info :id)]
    (if (nil? cid)
      {:status :unsupported :reason :unsynced}
      ;; The live dataset doesn't carry its store directly; column
      ;; indexes do (via their idx-storage). Use the first column's
      ;; storage to walk the dataset DAG from cid.
      (let [^PersistentColumnIndex first-idx
            (some-> ds :columns first val :index)
            storage (some-> first-idx .storage)
            store   (some-> storage :store)]
        (if (nil? store)
          {:status :unsupported :reason :no-store}
          (let [report (verify-chain store {:head cid :deep? true})]
            (case (:status report)
              :ok       {:status :ok :root cid}
              :mismatch {:status :mismatch :root nil
                         :errors (vec
                                  (concat
                                   (mapv (fn [m] {:type :audit/cid-mismatch
                                                  :address (:cid m)
                                                  :expected (:cid m)
                                                  :recomputed (:recomputed m)})
                                         (:mismatches report))
                                   (mapcat :errors (-> report :deep :diffs))))}
              {:status :unsupported :reason (:status report)})))))))

(extend-protocol IAuditable
  stratum.index.PersistentColumnIndex
  (-merkle-root [idx] (some-> idx meta :commit :id))
  (-recompute-merkle-root [idx] (pci-recompute idx))

  stratum.dataset.StratumDataset
  (-merkle-root [ds] (some-> ds :commit-info :id))
  (-recompute-merkle-root [ds] (ds-recompute ds)))
