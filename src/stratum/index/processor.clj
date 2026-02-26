(ns stratum.index.processor
  "Leaf processor for automatic chunk compaction and splitting in Stratum columnar index.

   Implements PSS ILeafProcessor interface to merge small chunks and split large chunks
   automatically as the tree is modified."
  (:require [stratum.chunk :as chunk]
            [stratum.index :as index]
            [stratum.stats :as stats])
  (:import [org.replikativ.persistent_sorted_set ILeafProcessor]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn- can-merge?
  "Check if two ChunkEntries can be merged within chunk-size limit."
  [^ChunkEntry e1 ^ChunkEntry e2 ^long chunk-size]
  (< (+ (chunk/chunk-length (.chunk e1))
        (chunk/chunk-length (.chunk e2)))
     chunk-size))

(defn- merge-chunk-entries
  "Merge multiple ChunkEntries into a single entry.
   Creates a new chunk containing all data from the entries.
   Uses the minimum chunk-id from the merged entries for ordering."
  [entries]
  (let [chunks (map #(.chunk ^ChunkEntry %) entries)
        total-length (reduce + 0 (map chunk/chunk-length chunks))
        first-entry ^ChunkEntry (first entries)
        min-chunk-id (.chunk-id first-entry) ;; entries are sorted, first has smallest key
        datatype (chunk/chunk-datatype (.chunk first-entry))
        ;; Create new chunk with exact size
        new-chunk (chunk/make-chunk datatype total-length)
        new-chunk-t (chunk/col-transient new-chunk)]
    ;; Copy all data into new chunk
    (loop [remaining entries
           offset (long 0)]
      (when-let [entry (first remaining)]
        (let [src-chunk (.chunk ^ChunkEntry entry)
              len (long (chunk/chunk-length src-chunk))]
          ;; Copy chunk data
          (dotimes [i len]
            (chunk/write-value! new-chunk-t (+ offset i)
                                (chunk/read-value src-chunk i)))
          (recur (rest remaining) (long (+ offset len))))))
    ;; Persist and create new entry
    (chunk/col-persistent! new-chunk-t)
    (let [new-stats (stats/compute-stats (chunk/chunk-data new-chunk) total-length)]
      (index/->ChunkEntry min-chunk-id
                          new-chunk
                          new-stats))))

(defn- compact-adjacent-chunks
  "Merge consecutive ChunkEntries if combined length < chunk-size.
   Returns compacted list of entries."
  [entries ^long chunk-size]
  (loop [remaining entries
         result []]
    (if-let [entry (first remaining)]
      (let [;; Find all entries we can merge with this one
            pending (take-while #(can-merge? entry % chunk-size) (rest remaining))
            merged (if (seq pending)
                     ;; Merge entry with all pending entries
                     (merge-chunk-entries (cons entry pending))
                     ;; Can't merge, keep as-is
                     entry)]
        (recur (drop (inc (count pending)) remaining)
               (conj result merged)))
      result)))

(defn- compact-near
  "Merge entries near modifiedIndex if combined length < chunk-size.
   Only checks the immediate neighborhood (idx-1, idx, idx+1) — O(1) instead
   of scanning the entire entry list."
  [entries ^long modified-index ^long chunk-size]
  (let [n (count entries)]
    (if (< n 2)
      entries
      ;; Find the window of entries around modifiedIndex that could merge
      (let [lo (max 0 (dec modified-index))
            hi (min (dec n) (inc modified-index))
            ;; Try to merge entries in [lo..hi] greedily from lo
            window (subvec entries lo (inc hi))]
        (loop [remaining window
               merged-window []]
          (if-let [entry (first remaining)]
            (let [pending (take-while #(can-merge? entry % chunk-size) (rest remaining))
                  result-entry (if (seq pending)
                                 (merge-chunk-entries (cons entry pending))
                                 entry)]
              (recur (drop (inc (count pending)) remaining)
                     (conj merged-window result-entry)))
            ;; Splice merged window back into entries
            (into (subvec entries 0 lo)
                  (into merged-window
                        (subvec entries (inc hi))))))))))

(defn- split-chunk-entry
  "Split a ChunkEntry with oversized chunk into multiple entries.
   Each split entry will have chunk-size or fewer elements.
   next-key is the chunk-id of the entry after this one (or nil if last).
   Uses chunk-id-between for unbounded key generation."
  [^ChunkEntry entry next-key ^long chunk-size]
  (let [chunk (.chunk entry)
        len (chunk/chunk-length chunk)
        num-chunks (quot (+ len chunk-size -1) chunk-size)]
    (loop [i 0
           prev-key nil
           result []]
      (if (>= i num-chunks)
        result
        (let [start (* i chunk-size)
              end (min len (* (inc i) chunk-size))
              sub-chunk (chunk/chunk-slice chunk start end)
              sub-len (- end start)
              sub-stats (stats/compute-stats (chunk/chunk-data sub-chunk) sub-len)
              key (if (zero? i)
                    (.chunk-id entry)
                    (index/chunk-id-between prev-key next-key))]
          (recur (inc i)
                 key
                 (conj result (index/->ChunkEntry key sub-chunk sub-stats))))))))

(defn- split-oversized-chunks
  "Split ChunkEntries with length > chunk-size into multiple entries.
   Returns list with oversized chunks split."
  [entries ^long chunk-size]
  (let [v (vec entries)
        n (count v)]
    (loop [i 0
           result []]
      (if (>= i n)
        result
        (let [^ChunkEntry entry (nth v i)
              next-key (when (< (inc i) n) (.chunk-id ^ChunkEntry (nth v (inc i))))]
          (if (> (chunk/chunk-length (.chunk entry)) chunk-size)
            (recur (inc i) (into result (split-chunk-entry entry next-key chunk-size)))
            (recur (inc i) (conj result entry))))))))

;; ============================================================================
;; Processor Implementation
;; ============================================================================

(deftype ChunkCompactionProcessor [^long chunk-size]
  ILeafProcessor

  (shouldProcess [_ leaf-size settings]
    ;; Only process if leaf has multiple entries (potential for merging)
    ;; OR if leaf might contain oversized chunks (need splitting)
    (>= leaf-size 2))

  (processLeaf [this entries storage settings]
    ;; Fallback: full scan (called when modifiedIndex is not available)
    (let [compacted (compact-adjacent-chunks (vec entries) chunk-size)
          split (split-oversized-chunks compacted chunk-size)]
      (java.util.ArrayList. ^java.util.Collection split)))

  (processLeaf [_ entries modified-index storage settings]
    ;; Targeted: only check neighbors of the modification point — O(1)
    (let [v (vec entries)
          ;; Only check the entry at modifiedIndex for oversized split
          ^ChunkEntry entry (nth v modified-index nil)
          needs-split? (and entry (> (chunk/chunk-length (.chunk entry)) chunk-size))
          ;; Split first if needed, then compact near the modification point
          v (if needs-split?
              (let [next-key (when (< (inc modified-index) (count v))
                               (.chunk-id ^ChunkEntry (nth v (inc modified-index))))
                    splits (split-chunk-entry entry next-key chunk-size)]
                (into (subvec v 0 modified-index)
                      (into (vec splits)
                            (subvec v (inc modified-index)))))
              v)
          ;; Compact near the modification point (handles merge of small neighbors)
          compacted (compact-near v (min modified-index (dec (count v))) chunk-size)]
      (java.util.ArrayList. ^java.util.Collection compacted))))

;; ============================================================================
;; Constructor
;; ============================================================================

(defn make-chunk-compaction-processor
  "Create a ChunkCompactionProcessor with the given chunk size.

   The processor will:
   - Merge adjacent chunks if combined length < chunk-size
   - Split chunks if length > chunk-size

   Example:
     (make-chunk-compaction-processor 8192)"
  [chunk-size]
  (ChunkCompactionProcessor. (long chunk-size)))
