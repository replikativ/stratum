(ns stratum.audit-test
  "Tests for stratum.audit/verify-chain.

   Builds a small file-backed dataset under :crypto-hash?, verifies it
   walks clean, then mutates the underlying konserve store directly to
   simulate three classes of tampering:

     1. Stored dataset commit (layer-1 cid recompute catches it)
     2. PSS leaf bytes      (layer-2 deep walk catches it)
     3. Stored index commit (layer-1 index-cid recompute catches it)

   Also exercises advisory mode (no :crypto-hash?), the IAuditable
   protocol surface for live `PersistentColumnIndex` and `StratumDataset`
   values, and the `verify-pss-tree-from-cold` helper."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :refer [<!!]]
            [konserve.core :as k]
            [konserve.filestore :refer [connect-fs-store]]
            [stratum.audit :as audit]
            [stratum.cached-storage :as cstorage]
            [stratum.dataset :as ds]
            [stratum.index :as idx]
            [stratum.stats :as stats]
            [stratum.storage :as storage])
  (:import [java.util UUID]
           [org.replikativ.persistent_sorted_set Leaf Settings]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- tmp-store-path []
  (str (System/getProperty "java.io.tmpdir") "/stratum-audit-" (UUID/randomUUID)))

(defn- new-store [path]
  (<!! (connect-fs-store path :id (UUID/randomUUID))))

(def ^:private ch-meta {:metadata {:crypto-hash? true}})

(defn- bootstrap-clean
  "Build a small dataset under :crypto-hash?, sync to a fresh file
   store on `path`, return [store path]."
  [path]
  (let [store (new-store path)
        small (ds/make-dataset
               {:price (idx/index-from-seq :float64 (mapv double (range 5000)) ch-meta)
                :qty   (idx/index-from-seq :int64 (range 5000) ch-meta)}
               {:name "trades" :metadata {:crypto-hash? true}})
        synced (ds/sync! small store "main")]
    [store path synced]))

(defn- tamper-leaf!
  "Read the Leaf at `pss-root` from the store and write back a Leaf
   with the same chunk but altered stats — same address, different
   content (by the leaf hash that includes count/sum/min/max)."
  [store pss-root]
  (let [cfg-store (cstorage/attach-pss-handlers store)
        ^Leaf orig (k/get cfg-store pss-root nil {:sync? true})
        keys-list (vec (.keys orig))
        first-entry (first keys-list)
        bad-stats (stats/->ChunkStats (:count (:stats first-entry))
                                      999999.0
                                      (:sum-sq (:stats first-entry))
                                      (:min-val (:stats first-entry))
                                      (:max-val (:stats first-entry))
                                      (:null-count (:stats first-entry)))
        ctor (resolve 'stratum.index/->ChunkEntry)
        tampered-entry (ctor (:chunk-id first-entry) (:chunk first-entry) bad-stats)
        tampered-keys (java.util.Arrays/asList (into-array Object [tampered-entry]))
        ^Settings settings (.-_settings orig)
        tampered-leaf (Leaf. tampered-keys settings)]
    (k/assoc cfg-store pss-root tampered-leaf {:sync? true})))

;; ============================================================================
;; Layer-1: dataset commit chain
;; ============================================================================

(deftest clean-chain-verifies-ok
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        report (audit/verify-chain store {:branch "main"})]
    (is (= :ok (:status report)))
    (is (audit/ok? report))
    (is (= 1 (count (:commits report))))
    (is (every? #{:ok} (map :status (:commits report))))))

(deftest dataset-cid-recipe-coverage
  (testing "dataset commit-id is hashed over (columns + schema + metadata)
            only — tampering with non-recipe fields like :row-count or
            :timestamp will NOT be caught by layer-1 verify-chain. This
            test documents that boundary so future readers don't expect
            otherwise; tampering with cid-bearing fields IS caught
            (see tamper-of-cid-bearing-field-detected)."
    (let [path (tmp-store-path)
          [store _ _] (bootstrap-clean path)
          head (storage/load-dataset-head store "main")
          stored (storage/load-dataset-commit store head)
          tampered (assoc stored :row-count 99999)
          _ (k/assoc store [:datasets :commits head] tampered {:sync? true})
          report (audit/verify-chain store {:branch "main"})]
      (is (= :ok (:status report))
          ":row-count is not in the cid recipe; layer-1 cannot catch this"))))

(deftest tamper-of-cid-bearing-field-detected
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        head (storage/load-dataset-head store "main")
        stored (storage/load-dataset-commit store head)
        tampered (assoc-in stored [:metadata :tampered] true)
        _ (k/assoc store [:datasets :commits head] tampered {:sync? true})
        report (audit/verify-chain store {:branch "main"})]
    (testing "changing :metadata (which IS hashed into the cid) makes
              the recomputed cid differ from the stored cid"
      (is (= :mismatch (:status report)))
      (is (= 1 (count (:mismatches report))))
      (is (= head (-> report :mismatches first :cid)))
      (is (not= head (-> report :mismatches first :recomputed))))))

(deftest tamper-on-index-commit-detected
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        head (storage/load-dataset-head store "main")
        ds-snap (storage/load-dataset-commit store head)
        idx-cid (-> ds-snap :columns first val :index-commit)
        idx-stored (storage/load-index-commit store idx-cid)
        ;; Change :datatype (in the index cid recipe)
        tampered (assoc idx-stored :datatype :float64-tampered)
        _ (k/assoc store [:indices :commits idx-cid] tampered {:sync? true})
        report (audit/verify-chain store {:branch "main"})]
    (is (= :mismatch (:status report)))
    (let [commit (first (:mismatches report))]
      (is (some? (:column-mismatches commit)))
      (is (some #(= idx-cid (:index-cid %)) (:column-mismatches commit))))))

;; ============================================================================
;; Layer-2: deep PSS walk
;; ============================================================================

(deftest pss-deep-verify-clean
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        r (audit/verify-chain store {:branch "main" :deep? true})]
    (is (= :ok (:status r)))
    (is (= :ok (-> r :deep :status)))
    (is (empty? (-> r :deep :diffs)))))

(deftest pss-deep-verify-detects-leaf-tampering
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        head (storage/load-dataset-head store "main")
        ds-snap (storage/load-dataset-commit store head)
        idx-cid (-> ds-snap :columns first val :index-commit)
        idx-snap (storage/load-index-commit store idx-cid)
        pss-root (:pss-root idx-snap)
        _ (tamper-leaf! store pss-root)
        r (audit/verify-chain store {:branch "main" :deep? true})
        ;; Find any diff — the price+qty columns share the same root
        ;; (content-addressed dedup) so both will surface.
        diffs (-> r :deep :diffs)
        first-err (-> diffs first :errors first)]
    (is (= :mismatch (:status r)))
    (is (= :mismatch (-> r :deep :status)))
    (is (pos? (count diffs)))
    (is (= :audit/merkle-mismatch (:type first-err)))
    (is (= pss-root (:expected first-err)))))

;; ============================================================================
;; Advisory mode (crypto-hash off)
;; ============================================================================

(deftest no-crypto-hash-is-advisory
  (let [path (tmp-store-path)
        store (new-store path)
        small (ds/make-dataset {:x (idx/index-from-seq :int64 [1 2 3])}
                               {:name "no-ch"})
        _ (ds/sync! small store "main")
        r (audit/verify-chain store {:branch "main"})]
    (is (= :advisory (:status r)))
    (is (= :crypto-hash-disabled (-> r :commits first :reason)))))

;; ============================================================================
;; verify-pss-tree-from-cold helper
;; ============================================================================

(deftest verify-pss-tree-from-cold-clean
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        head (storage/load-dataset-head store "main")
        idx-cid (-> (storage/load-dataset-commit store head)
                    :columns first val :index-commit)
        pss-root (:pss-root (storage/load-index-commit store idx-cid))
        r (audit/verify-pss-tree-from-cold store pss-root)]
    (is (= :ok (:status r)))
    (is (= pss-root (:root r)))))

(deftest verify-pss-tree-from-cold-tampered
  (let [path (tmp-store-path)
        [store _ _] (bootstrap-clean path)
        head (storage/load-dataset-head store "main")
        idx-cid (-> (storage/load-dataset-commit store head)
                    :columns first val :index-commit)
        pss-root (:pss-root (storage/load-index-commit store idx-cid))
        _ (tamper-leaf! store pss-root)
        r (audit/verify-pss-tree-from-cold store pss-root)]
    (is (= :mismatch (:status r)))
    (is (= :audit/merkle-mismatch (-> r :errors first :type)))
    (is (= pss-root (-> r :errors first :expected)))))

;; ============================================================================
;; IAuditable protocol on live values
;; ============================================================================

(deftest iauditable-on-live-index
  (let [path (tmp-store-path)
        [_ _ synced] (bootstrap-clean path)
        first-idx (-> synced :columns first val :index)
        root (audit/-merkle-root first-idx)
        r (audit/-recompute-merkle-root first-idx)]
    (is (uuid? root))
    (is (= :ok (:status r)))
    (is (= root (:root r)))))

(deftest iauditable-on-live-dataset
  (let [path (tmp-store-path)
        [_ _ synced] (bootstrap-clean path)
        root (audit/-merkle-root synced)
        r (audit/-recompute-merkle-root synced)]
    (is (uuid? root))
    (is (= :ok (:status r)))
    (is (= root (:root r)))))

(deftest iauditable-detects-tampering
  (let [path (tmp-store-path)
        [store _ synced] (bootstrap-clean path)
        first-idx (-> synced :columns first val :index)
        idx-cid (audit/-merkle-root first-idx)
        idx-snap (storage/load-index-commit store idx-cid)
        pss-root (:pss-root idx-snap)
        _ (tamper-leaf! store pss-root)
        r (audit/-recompute-merkle-root first-idx)]
    (is (= :mismatch (:status r)))
    (is (= :audit/merkle-mismatch (-> r :errors first :type)))))
