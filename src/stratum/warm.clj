(ns stratum.warm
  "Budget-bounded breadth-first warming of a dataset's column indices — pull the
   trees' upper levels into the storage cache in WAVES instead of discovering
   them one blocking round trip at a time.

   The walk itself lives in persistent-sorted-set
   (`org.replikativ.persistent-sorted-set.warm`); this namespace is the
   dataset-shaped entry: every indexed column contributes its PSS tree, and all
   of them share ONE budget round-robin, so no column can spend it before
   another gets any — which column the next query needs is not knowable at warm
   time. `:by-index` in the report says where the budget actually went, keyed
   by column name.

   ## Depth is a bigger lever here than in a thin-leaf tree

   A stratum leaf is FAT: each `ChunkEntry` in it carries its whole
   `PersistentColChunk`, so one leaf is up to 64 chunks of column data (~4MB at
   the default chunk size) and restoring the leaves IS materializing the
   column. So

     :depth :interior     (the default) warms the SPINE — branches carry the
                          aggregate measures (min/max/sum/count), so measure-
                          driven pruning, `idx-stats`, and chunk addressing all
                          answer without touching a chunk. Cheap and broadly
                          useful.
     :depth :with-leaves  materializes the COLUMN DATA itself. That is the
                          right call for a scan you know is coming, and a
                          budget-shaped mistake for a dataset larger than the
                          cache — size `:budget` to what the query will read.

   ## What is and is not warmed

   Only columns whose index has durable storage participate; an in-memory
   column has nothing to restore and is skipped (the PSS walk does this — a
   tree without storage contributes no entries). Dictionaries, schema and
   commit metadata are not trees and are loaded by `dataset/load` itself.

   Options and report are persistent-sorted-set's:
   `:depth` `:budget` `:width` `:from`/`:to` `:cache-size` →
   {:fetched :by-level :rounds :height :by-index :budget-left
    :budget-exhausted? :budget-clamped? :ms}."
  (:require [stratum.dataset :as dataset]
            [stratum.index :as index]
            [org.replikativ.persistent-sorted-set.warm :as pss-warm]))

(defn warm-column!
  "Warm ONE column's index tree. See [[warm!]] for options; `:from`/`:to` are
   chunk-id vectors in the tree's own `ChunkEntry` order, which makes them an
   implementation-facing option — most callers want the whole spine and should
   pass none."
  ([dataset col-name] (warm-column! dataset col-name {}))
  ([dataset col-name opts]
   (if-let [tree (some-> (dataset/column dataset col-name) :index index/idx-tree)]
     (pss-warm/warm! tree (assoc opts :key col-name))
     (pss-warm/warm-trees! [] opts))))

(defn warm!
  "Warm every indexed column of `dataset`, sharing one budget round-robin.

   The connect-time shape: after `dataset/load` the dataset's trees are
   address-rooted and every touched node is a storage round trip; one `warm!`
   at `:depth :interior` (the default) makes stats and pruning free, and
   `:depth :with-leaves` with a sized `:budget` prefetches the data a known
   scan will read.

   Returns persistent-sorted-set's warm report, `:by-index` keyed by column
   name."
  ([dataset] (warm! dataset {}))
  ([dataset opts]
   (pss-warm/warm-trees!
    (for [col-name (dataset/column-names dataset)
          :let [tree (some-> (dataset/column dataset col-name) :index index/idx-tree)]
          :when tree]
      {:key col-name :set tree})
    opts)))
