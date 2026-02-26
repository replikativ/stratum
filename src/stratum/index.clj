(ns stratum.index
  "PersistentColumnIndex - A tree of ColChunks for O(log N) lookup with columnar efficiency.

   Structure:
   - PSS tree indexed by monotonic chunk-id for ordering
   - Each leaf contains a ColChunk of CHUNK_SIZE elements
   - Uses PSS stats-based weighted navigation (getNth) for O(log chunks) point access
   - Provides both O(log N) point lookup and efficient range scans

   Inspired by:
   - Arrow CSR format (indptr + indices for efficient access)
   - Proximum's chunked edge store
   - LSM-tree segment organization"
  (:require [stratum.chunk :as chunk]
            [stratum.stats :as stats]
            [stratum.storage :as storage]
            [stratum.cached-storage :as cstorage]
            [org.replikativ.persistent-sorted-set :as pss]
            [konserve.core :as k])
  (:import [stratum.chunk PersistentColChunk]
           [stratum.stats ChunkStats]
           [org.replikativ.persistent_sorted_set IMeasure PersistentSortedSet]
           [java.util Arrays]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:const DEFAULT_CHUNK_SIZE
  "Default number of elements per chunk.
   8192 is a good balance for:
   - L2 cache efficiency (~64KB for doubles)
   - GPU transfer granularity
   - CoW overhead amortization"
  8192)

;; ============================================================================
;; Chunk Entry - what we store in the PSS tree
;; ============================================================================

(defrecord ChunkEntry [chunk-id          ; Vector key for ordering in PSS, e.g. [0], [1], [2 1]
                       chunk             ; The PersistentColChunk
                       ^ChunkStats stats]) ; Statistics for O(chunks) aggregations

(defn- lex-compare
  "Lexicographic comparison for vectors of longs.
   Compares element-by-element; if all shared elements are equal,
   shorter vector is less (prefix ordering)."
  [a b]
  (let [alen (count a)
        blen (count b)
        minlen (min alen blen)]
    (loop [i 0]
      (if (< i minlen)
        (let [c (Long/compare (long (nth a i)) (long (nth b i)))]
          (if (zero? c)
            (recur (inc i))
            c))
        ;; All shared elements equal — shorter is less
        (Long/compare alen blen)))))

(defn chunk-entry-comparator
  "Comparator for ChunkEntry - sorts by chunk-id (vector, lexicographic with prefix ordering)"
  [^ChunkEntry a ^ChunkEntry b]
  (lex-compare (.chunk-id a) (.chunk-id b)))

(defn chunk-id-between
  "Generate a chunk-id vector that sorts between a and b.
   If b is nil, generates a key after a.
   Uses lexicographic vector comparison with level extension
   for unbounded insertion capacity."
  [a b]
  (let [a (vec a)]
    (loop [i 0
           prefix []]
      (let [ai (long (get a i 0))
            bi (when b (get (vec b) i nil))]
        (cond
          ;; No upper bound at this level — go one above ai
          (nil? bi)
          (conj prefix (inc ai))

          ;; Room to bisect at this level
          (> (long (- (long bi) ai)) 1)
          (conj prefix (+ ai (quot (- (long bi) ai) 2)))

          ;; Same or adjacent — go deeper
          :else
          (recur (inc i) (conj prefix ai)))))))

;; ============================================================================
;; PSS Stats Integration
;; ============================================================================

(deftype ChunkEntryMeasureOps []
  IMeasure
  (identity [_]
    stats/empty-stats)

  (extract [_ chunk-entry]
    (:stats chunk-entry))

  (merge [_ s1 s2]
    (stats/merge-stats s1 s2))

  (remove [_ current chunk-entry recompute]
    (let [to-remove (:stats chunk-entry)
          result (stats/->ChunkStats
                  (- (:count current) (:count to-remove))
                  (- (:sum current) (:sum to-remove))
                  (- (:sum-sq current) (:sum-sq to-remove))
                  Double/NaN
                  Double/NaN
                  (- (long (:null-count current)) (long (:null-count to-remove))))
          recomputed (when (and (pos? (:count result))
                                (or (Double/isNaN (:min-val result))
                                    (Double/isNaN (:max-val result))))
                       (when recompute
                         (.get recompute)))]
      (if recomputed
        (stats/->ChunkStats
         (:count result)
         (:sum result)
         (:sum-sq result)
         (:min-val recomputed)
         (:max-val recomputed)
         (:null-count result))
        result)))

  (weight [_ stats]
    (:count stats)))

(def chunk-entry-measure-ops
  (ChunkEntryMeasureOps.))

;; ============================================================================
;; Helper Functions for Chunk Operations
;; ============================================================================

(defn- copy-chunk-with-modification
  "Create a new chunk by copying an old chunk with a modification at idx."
  [old-chunk ^long local-idx val]
  (let [chunk-len (chunk/chunk-length old-chunk)
        datatype (chunk/chunk-datatype old-chunk)
        new-chunk (-> (chunk/make-chunk datatype chunk-len)
                      chunk/col-transient)]
    (dotimes [i chunk-len]
      (let [v (if (= i local-idx) val (chunk/read-value old-chunk i))]
        (chunk/write-value! new-chunk i v)))
    (chunk/col-persistent! new-chunk)))

(defn- copy-chunk-with-insertion
  "Create a new chunk by copying an old chunk and inserting a value at idx.
   Uses System.arraycopy (JVM intrinsic) instead of element-by-element copy."
  [old-chunk ^long local-idx val]
  (let [chunk-len (long (chunk/chunk-length old-chunk))
        datatype (chunk/chunk-datatype old-chunk)
        old-arr (chunk/chunk-data old-chunk)
        new-len (inc chunk-len)
        new-arr (case datatype
                  :int64 (long-array new-len)
                  :float64 (double-array new-len))]
    ;; Copy elements before insert point
    (when (pos? local-idx)
      (System/arraycopy old-arr 0 new-arr 0 local-idx))
    ;; Insert new value
    (case datatype
      :int64 (aset ^longs new-arr (int local-idx) (long val))
      :float64 (aset ^doubles new-arr (int local-idx) (double val)))
    ;; Copy elements after insert point
    (let [after (- chunk-len local-idx)]
      (when (pos? after)
        (System/arraycopy old-arr local-idx new-arr (inc local-idx) after)))
    (chunk/chunk-from-array new-arr)))

(defn- copy-chunk-with-deletion
  "Create a new chunk by copying an old chunk with a deletion at idx.
   Uses System.arraycopy (JVM intrinsic) instead of element-by-element copy."
  [old-chunk ^long local-idx]
  (let [chunk-len (long (chunk/chunk-length old-chunk))
        datatype (chunk/chunk-datatype old-chunk)
        old-arr (chunk/chunk-data old-chunk)
        new-len (dec chunk-len)
        new-arr (case datatype
                  :int64 (long-array new-len)
                  :float64 (double-array new-len))]
    ;; Copy elements before delete point
    (when (pos? local-idx)
      (System/arraycopy old-arr 0 new-arr 0 local-idx))
    ;; Copy elements after delete point (shifted down)
    (let [after (- chunk-len local-idx 1)]
      (when (pos? after)
        (System/arraycopy old-arr (inc local-idx) new-arr local-idx after)))
    (chunk/chunk-from-array new-arr)))

;; ============================================================================
;; PersistentColumnIndex
;; ============================================================================

(defprotocol IColumnIndex
  "Protocol for indexed columnar storage"
  (idx-length [this] "Total number of elements")
  (idx-datatype [this] "Element datatype")
  (idx-chunk-size [this] "Elements per chunk")

  ;; Point access
  (idx-get [this idx] "Get value at index (boxed)")
  (idx-get-double [this idx] "Get double at index")
  (idx-get-long [this idx] "Get long at index")

  ;; Range access
  (idx-slice [this start end] "Get a range as a new index (structural sharing)")
  (idx-scan [this f] "Scan all values, calling (f chunk-array) for each chunk")
  (idx-scan-with-stats [this f] "Scan with stats: (f chunk-array stats start-idx)")

  ;; Statistics
  (idx-stats [this] "Get merged stats for entire index. O(chunks).")
  (idx-all-chunk-stats [this] "Get seq of all chunk stats. O(chunks).")
  (idx-tree [this] "Get underlying PSS tree (for PSS stats integration)")

  ;; Persistent operations (thread-safe, return new index)
  (idx-set [this idx val] "Set value at index. Returns NEW index, original unchanged.")
  (idx-insert [this idx val] "Insert value at index, shifting subsequent values. Returns NEW index.")
  (idx-delete [this idx] "Delete value at index, shifting subsequent values. Returns NEW index.")
  (idx-append [this val] "Append value to end. Returns NEW index, original unchanged.")

  ;; Transient operations (fast, unsafe, mutate in place)
  (idx-set! [this idx val] "Set value at index. MUTATES index, returns same object. Must be transient.")
  (idx-insert! [this idx val] "Insert value at index. MUTATES index, returns same object. Must be transient.")
  (idx-delete! [this idx] "Delete value at index. MUTATES index, returns same object. Must be transient.")
  (idx-append! [this val] "Append value. MUTATES index, returns same object. Must be transient.")
  (idx-append-chunk! [this chunk] "Append chunk. MUTATES index, returns same object. Must be transient."))

(defprotocol IColumnIndexPersistence
  "Persistence protocol for column index"
  (idx-fork [this] "Create a structural copy")
  (idx-transient [this] "Switch to transient mode for batch updates")
  (idx-persistent! [this] "Seal and return persistent version")
  (idx-dirty-chunks [this] "Get set of modified chunk positions (for storage tracking)")
  (idx-clear-dirty! [this] "Clear the dirty chunks set after successful sync")
  (idx-sync! [this storage] "Persist index to storage. Returns new index with :commit-id in metadata.")
  (idx-storage [this] "Get the CachedStorage (IStorage) or nil for in-memory indices."))

(deftype PersistentColumnIndex
         [^:volatile-mutable tree           ; PSS of ChunkEntry (mutable for transient mode)
          ^:volatile-mutable ^long total-length  ; Total elements across all chunks (mutable for transient mode)
          datatype                          ; Element type
          ^long chunk-size                  ; Elements per chunk
          cmp                               ; Comparator for PSS operations
          ^:volatile-mutable edit           ; Transient mode flag (Object or nil)
          dirty-chunks                      ; atom - set of modified chunk ids (for storage tracking)
          metadata
          ^:volatile-mutable ^long next-chunk-id ; Monotonic counter for chunk ordering
          storage]                          ; CachedStorage (IStorage) or nil for in-memory

  ;; ========================================================================
  ;; Clojure Standard Protocols
  ;; ========================================================================

  clojure.lang.Counted
  (count [_]
    total-length)

  clojure.lang.Indexed
  (nth [this i]
    (idx-get this i))
  (nth [this i not-found]
    (if (and (>= i 0) (< i total-length))
      (idx-get this i)
      not-found))

  clojure.lang.ILookup
  (valAt [this k]
    (when (and (integer? k) (>= k 0) (< k total-length))
      (idx-get this k)))
  (valAt [this k not-found]
    (if (and (integer? k) (>= k 0) (< k total-length))
      (idx-get this k)
      not-found))

  clojure.lang.IReduce
  (reduce [this f]
    ;; Reduce over chunks without materialization
    ;; First element is used as init, start reducing from second element
    (let [result (volatile! ::none)
          first-done (volatile! false)]
      (try
        (idx-scan this
                  (fn [arr]
                    (let [len (case datatype
                                :int64 (alength ^longs arr)
                                :float64 (alength ^doubles arr))]
                      (loop [i (if (and (not @first-done) (= @result ::none)) 1 0)
                             acc (if (= @result ::none)
                                   (do
                                     (vreset! first-done true)
                                     (case datatype
                                       :int64 (aget ^longs arr 0)
                                       :float64 (aget ^doubles arr 0)))
                                   @result)]
                        (if (< i len)
                          (let [v (case datatype
                                    :int64 (aget ^longs arr (int i))
                                    :float64 (aget ^doubles arr (int i)))
                                new-acc (f acc v)]
                            (if (reduced? new-acc)
                              (do (vreset! result @new-acc)
                                  (throw (ex-info "reduced" {})))
                              (recur (inc i) new-acc)))
                          (vreset! result acc))))))
        @result
        (catch clojure.lang.ExceptionInfo e
          (if (= (.getMessage e) "reduced")
            @result
            (throw e))))))
  (reduce [this f init]
    ;; Reduce over chunks with init value
    (let [result (volatile! init)]
      (try
        (idx-scan this
                  (fn [arr]
                    (let [len (case datatype
                                :int64 (alength ^longs arr)
                                :float64 (alength ^doubles arr))]
                      (loop [i 0
                             acc @result]
                        (if (< i len)
                          (let [v (case datatype
                                    :int64 (aget ^longs arr (int i))
                                    :float64 (aget ^doubles arr (int i)))
                                new-acc (f acc v)]
                            (if (reduced? new-acc)
                              (do (vreset! result @new-acc)
                                  (throw (ex-info "reduced" {})))
                              (recur (inc i) new-acc)))
                          (vreset! result acc))))))
        @result
        (catch clojure.lang.ExceptionInfo e
          (if (= (.getMessage e) "reduced")
            @result
            (throw e))))))

  clojure.lang.Reversible
  (rseq [this]
    ;; Return lazy reversed seq via chunked iteration
    ;; Note: This materializes to a vector internally for simplicity
    ;; A more sophisticated impl could iterate chunks in reverse order
    (let [result (volatile! [])]
      (idx-scan this
                (fn [arr]
                  (let [len (case datatype
                              :int64 (alength ^longs arr)
                              :float64 (alength ^doubles arr))
                        chunk-vals (loop [i 0 acc []]
                                     (if (< i len)
                                       (recur (inc i)
                                              (conj acc (case datatype
                                                          :int64 (aget ^longs arr (int i))
                                                          :float64 (aget ^doubles arr (int i)))))
                                       acc))]
                    (vswap! result into chunk-vals))))
      (rseq @result)))

  IColumnIndex
  (idx-length [_] total-length)
  (idx-datatype [_] datatype)
  (idx-chunk-size [_] chunk-size)

  (idx-get [this idx]
    (when (or (neg? idx) (>= idx total-length))
      (throw (IndexOutOfBoundsException. (str "Index " idx " out of bounds [0, " total-length ")"))))
    (when-let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree idx)]
      (chunk/read-value (.chunk entry) local-offset)))

  (idx-get-double [this idx]
    (when (or (neg? idx) (>= idx total-length))
      (throw (IndexOutOfBoundsException. (str "Index " idx " out of bounds [0, " total-length ")"))))
    (when-let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree idx)]
      (chunk/read-double (.chunk entry) local-offset)))

  (idx-get-long [this idx]
    (when (or (neg? idx) (>= idx total-length))
      (throw (IndexOutOfBoundsException. (str "Index " idx " out of bounds [0, " total-length ")"))))
    (when-let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree idx)]
      (chunk/read-long (.chunk entry) local-offset)))

  (idx-slice [this start end]
    (throw (UnsupportedOperationException. "idx-slice not yet implemented")))

  (idx-scan [this f]
    ;; Iterate through all chunks in order, passing chunk arrays
    (doseq [^ChunkEntry entry (pss/slice tree nil nil)]
      (f (chunk/chunk-data (.chunk entry)))))

  (idx-scan-with-stats [this f]
    ;; Iterate through all chunks with stats, computing running offset
    (let [offset (volatile! (long 0))]
      (doseq [^ChunkEntry entry (pss/slice tree nil nil)]
        (let [start @offset
              chunk-len (long (chunk/chunk-length (.chunk entry)))]
          (f (chunk/chunk-data (.chunk entry))
             (.stats entry)
             start)
          (vswap! offset + chunk-len)))))

  (idx-stats [this]
    (if-let [fast-stats (pss/measure tree)]
      fast-stats
      (stats/merge-all-stats
       (map (fn [^ChunkEntry e] (.stats e))
            (pss/slice tree nil nil)))))

  (idx-all-chunk-stats [this]
    (map (fn [^ChunkEntry e] (.stats e))
         (pss/slice tree nil nil)))

  (idx-tree [_]
    tree)

  ;; ========================================================================
  ;; Persistent Operations - Thread-safe, return new index
  ;; ========================================================================

  (idx-set [this idx val]
    (when (or (neg? idx) (>= idx total-length))
      (throw (IndexOutOfBoundsException. (str "Index " idx " out of bounds [0, " total-length ")"))))

    (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree idx)]
      (if entry
        (let [new-chunk (copy-chunk-with-modification (.chunk entry) local-offset val)
              chunk-len (chunk/chunk-length new-chunk)
              new-stats (stats/compute-stats (chunk/chunk-data new-chunk) chunk-len)
              new-entry (->ChunkEntry (.chunk-id entry) new-chunk new-stats)
              new-tree (pss/replace tree entry new-entry)]
          (PersistentColumnIndex. new-tree total-length datatype chunk-size cmp
                                  nil (atom #{}) metadata next-chunk-id storage))
        (throw (IllegalStateException. (str "Entry not found for index " idx))))))

  (idx-insert [this insert-idx val]
    (when (or (neg? insert-idx) (> insert-idx total-length))
      (throw (IndexOutOfBoundsException. (str "Insert index " insert-idx " out of bounds [0, " total-length "]"))))

    (if (= insert-idx total-length)
      (let [new-chunk (-> (chunk/make-chunk datatype 1)
                          chunk/col-transient
                          (chunk/write-value! 0 val)
                          chunk/col-persistent!)
            new-stats (stats/compute-stats (chunk/chunk-data new-chunk) 1)
            new-entry (->ChunkEntry [next-chunk-id] new-chunk new-stats)
            new-tree (clojure.core/conj tree new-entry)]
        (PersistentColumnIndex. new-tree (inc total-length) datatype chunk-size cmp
                                nil (atom #{}) metadata (inc next-chunk-id) storage))
      (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree insert-idx)]
        (if-not entry
          (throw (IllegalStateException. (str "Entry not found for index " insert-idx)))
          (let [new-chunk (copy-chunk-with-insertion (.chunk entry) local-offset val)
                new-len (long (chunk/chunk-length new-chunk))]
            (if (<= new-len chunk-size)
              ;; Fits — just replace
              (let [new-stats (stats/update-stats-append (.stats entry) (double val))
                    new-entry (->ChunkEntry (.chunk-id entry) new-chunk new-stats)
                    new-tree (pss/replace tree entry new-entry)]
                (PersistentColumnIndex. new-tree (inc total-length) datatype chunk-size cmp
                                        nil (atom #{}) metadata next-chunk-id storage))
              ;; Oversized — split into two chunks
              (let [half (quot new-len 2)
                    chunk1 (chunk/chunk-slice new-chunk 0 half)
                    chunk2 (chunk/chunk-slice new-chunk half new-len)
                    stats1 (stats/compute-stats (chunk/chunk-data chunk1) half)
                    stats2 (stats/compute-stats (chunk/chunk-data chunk2) (- new-len half))
                    entry1 (->ChunkEntry (.chunk-id entry) chunk1 stats1)
                    ;; Find successor: second entry in slice from current to end
                    successor ^ChunkEntry (second (pss/slice tree entry nil))
                    new-key (chunk-id-between (.chunk-id entry)
                                              (when successor (.chunk-id successor)))
                    entry2 (->ChunkEntry new-key chunk2 stats2)
                    new-tree (-> (pss/replace tree entry entry1)
                                 (clojure.core/conj entry2))]
                (PersistentColumnIndex. new-tree (inc total-length) datatype chunk-size cmp
                                        nil (atom #{}) metadata next-chunk-id storage))))))))

  (idx-delete [this delete-idx]
    (when (or (neg? delete-idx) (>= delete-idx total-length))
      (throw (IndexOutOfBoundsException. (str "Delete index " delete-idx " out of bounds [0, " total-length ")"))))

    (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree delete-idx)]
      (if entry
        (let [chunk-len (chunk/chunk-length (.chunk entry))]
          (if (= chunk-len 1)
            (let [new-tree (clojure.core/disj tree entry)]
              (PersistentColumnIndex. new-tree (dec total-length) datatype chunk-size cmp
                                      nil (atom #{}) metadata next-chunk-id storage))
            (let [;; Read removed value before deletion
                  removed-val (double (chunk/read-value (.chunk entry) local-offset))
                  new-chunk (copy-chunk-with-deletion (.chunk entry) local-offset)
                  new-stats (stats/update-stats-remove (.stats entry) removed-val
                                                       (chunk/chunk-data new-chunk)
                                                       (chunk/chunk-length new-chunk))
                  new-entry (->ChunkEntry (.chunk-id entry) new-chunk new-stats)
                  new-tree (pss/replace tree entry new-entry)]
              (PersistentColumnIndex. new-tree (dec total-length) datatype chunk-size cmp
                                      nil (atom #{}) metadata next-chunk-id storage))))
        (throw (IllegalStateException. (str "Entry not found for index " delete-idx))))))

  (idx-append [this val]
    (idx-insert this total-length val))

  ;; ========================================================================
  ;; Transient Operations - Fast, unsafe, mutate in place
  ;; ========================================================================

  (idx-set! [this idx val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent index. Call idx-transient first.")))
    (when (or (neg? idx) (>= idx total-length))
      (throw (IndexOutOfBoundsException. (str "Index " idx " out of bounds [0, " total-length ")"))))

    (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree idx)]
      (if entry
        (let [chunk (.chunk entry)
              ;; Lazy fork: if chunk is still persistent, fork + make transient
              chunk (if (chunk/col-transient? chunk)
                      chunk
                      (-> (chunk/col-fork chunk) chunk/col-transient))]
          (chunk/write-value! chunk local-offset val)

          (let [chunk-len (chunk/chunk-length chunk)
                new-stats (stats/compute-stats (chunk/chunk-data chunk) chunk-len)
                new-entry (->ChunkEntry (.chunk-id entry) chunk new-stats)]

            (swap! dirty-chunks clojure.core/conj (.chunk-id entry))

            (set! tree (pss/replace tree entry new-entry))

            this))

        (throw (IllegalStateException. (str "Entry not found for index " idx))))))

  (idx-append! [this val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent index. Call idx-transient first.")))

    (let [last-entry (last (pss/slice tree nil nil))]
      (if (and last-entry
               (< (chunk/chunk-length (.chunk ^ChunkEntry last-entry))
                  chunk-size))
        (let [^ChunkEntry entry last-entry
              local-idx (chunk/chunk-length (.chunk entry))
              ;; Lazy fork: if chunk is still persistent, fork + make transient
              chunk (let [c (.chunk entry)]
                      (if (chunk/col-transient? c)
                        c
                        (-> (chunk/col-fork c) chunk/col-transient)))]

          (chunk/write-value! chunk local-idx val)

          (let [new-stats (stats/update-stats-append (.stats entry) (double val))
                new-entry (->ChunkEntry (.chunk-id entry) chunk new-stats)]

            (swap! dirty-chunks clojure.core/conj (.chunk-id entry))

            (set! tree (pss/replace tree entry new-entry))
            (set! total-length (unchecked-inc total-length))

            this))

        (let [new-id [next-chunk-id]
              _ (set! next-chunk-id (unchecked-inc next-chunk-id))
              new-chunk (-> (chunk/make-chunk datatype 1)
                            chunk/col-transient)
              _ (chunk/write-value! new-chunk 0 val)
              new-stats (stats/update-stats-append stats/empty-stats (double val))
              new-entry (->ChunkEntry new-id new-chunk new-stats)]

          (swap! dirty-chunks clojure.core/conj new-id)

          (set! tree (conj! tree new-entry))
          (set! total-length (unchecked-inc total-length))

          this))))

  (idx-append-chunk! [this new-chunk]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent index. Call idx-transient first.")))

    (let [new-id [next-chunk-id]
          _ (set! next-chunk-id (unchecked-inc next-chunk-id))
          chunk-len (chunk/chunk-length new-chunk)
          new-stats (stats/compute-stats (chunk/chunk-data new-chunk) chunk-len)
          new-entry (->ChunkEntry new-id new-chunk new-stats)]

      (swap! dirty-chunks clojure.core/conj new-id)

      (set! tree (conj! tree new-entry))
      (set! total-length (long (+ total-length chunk-len)))

      this))

  (idx-insert! [this insert-idx val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent index. Call idx-transient first.")))
    (when (or (< insert-idx 0) (> insert-idx total-length))
      (throw (IndexOutOfBoundsException. (str "Insert index " insert-idx " out of bounds [0, " total-length "]"))))

    (if (= insert-idx total-length)
      (idx-append! this val)

      (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree insert-idx)]
        (if-not entry
          (throw (ex-info "Could not find chunk for insert" {:idx insert-idx}))

          (let [old-chunk (.chunk entry)
                chunk-len (long (chunk/chunk-length old-chunk))
                old-arr (chunk/chunk-data old-chunk)
                new-len (inc chunk-len)
                new-arr (case datatype
                          :int64 (long-array new-len)
                          :float64 (double-array new-len))]

            ;; System.arraycopy: copy before insert point
            (when (pos? local-offset)
              (System/arraycopy old-arr 0 new-arr 0 local-offset))
            ;; Insert new value
            (case datatype
              :int64 (aset ^longs new-arr (int local-offset) (long val))
              :float64 (aset ^doubles new-arr (int local-offset) (double val)))
            ;; System.arraycopy: copy after insert point
            (let [after (- chunk-len local-offset)]
              (when (pos? after)
                (System/arraycopy old-arr local-offset new-arr (inc local-offset) after)))

            (if (<= new-len chunk-size)
              ;; Fits — just replace
              (let [new-chunk (chunk/chunk-from-array new-arr)
                    new-stats (stats/update-stats-append (.stats entry) (double val))
                    new-entry (->ChunkEntry (.chunk-id entry) new-chunk new-stats)]

                (swap! dirty-chunks clojure.core/conj (.chunk-id entry))
                (set! tree (pss/replace tree entry new-entry))

                (set! total-length (unchecked-inc total-length))
                this)

              ;; Oversized — split into two chunks
              (let [new-chunk (chunk/chunk-from-array new-arr)
                    half (quot new-len 2)
                    chunk1 (chunk/chunk-slice new-chunk 0 half)
                    chunk2 (chunk/chunk-slice new-chunk half new-len)
                    stats1 (stats/compute-stats (chunk/chunk-data chunk1) half)
                    stats2 (stats/compute-stats (chunk/chunk-data chunk2) (- new-len half))
                    entry1 (->ChunkEntry (.chunk-id entry) chunk1 stats1)
                    ;; Find successor: second entry in slice from current to end
                    successor ^ChunkEntry (second (pss/slice tree entry nil))
                    new-key (chunk-id-between (.chunk-id entry)
                                              (when successor (.chunk-id successor)))
                    entry2 (->ChunkEntry new-key chunk2 stats2)]

                (swap! dirty-chunks clojure.core/conj (.chunk-id entry))
                (swap! dirty-chunks clojure.core/conj new-key)
                (set! tree (-> (pss/replace tree entry entry1)
                               (conj! entry2)))

                (set! total-length (unchecked-inc total-length))
                this)))))))

  (idx-delete! [this delete-idx]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent index. Call idx-transient first.")))
    (when (or (< delete-idx 0) (>= delete-idx total-length))
      (throw (IndexOutOfBoundsException. (str "Delete index " delete-idx " out of bounds [0, " total-length ")"))))

    (let [[^ChunkEntry entry ^long local-offset] (pss/get-nth tree delete-idx)]
      (if-not entry
        (throw (ex-info "Could not find chunk for delete" {:idx delete-idx}))

        (let [old-chunk (.chunk entry)
              chunk-len (chunk/chunk-length old-chunk)]

          (if (= chunk-len 1)
            (do
              (set! tree (disj! tree entry))
              (set! total-length (unchecked-dec total-length))
              this)

            (let [old-arr (chunk/chunk-data old-chunk)
                  new-len (dec chunk-len)
                  new-arr (case datatype
                            :int64 (long-array new-len)
                            :float64 (double-array new-len))
                  ;; Read removed value before copying
                  removed-val (double (case datatype
                                        :int64 (aget ^longs old-arr (int local-offset))
                                        :float64 (aget ^doubles old-arr (int local-offset))))]

              ;; System.arraycopy: copy before delete point
              (when (pos? local-offset)
                (System/arraycopy old-arr 0 new-arr 0 local-offset))
              ;; System.arraycopy: copy after delete point (shifted down)
              (let [after (- chunk-len local-offset 1)]
                (when (pos? after)
                  (System/arraycopy old-arr (inc local-offset) new-arr local-offset after)))

              (let [new-chunk (chunk/chunk-from-array new-arr)
                    new-stats (stats/update-stats-remove (.stats entry) removed-val new-arr new-len)
                    new-entry (->ChunkEntry (.chunk-id entry) new-chunk new-stats)]

                (swap! dirty-chunks clojure.core/conj (.chunk-id entry))
                (set! tree (pss/replace tree entry new-entry))

                (set! total-length (unchecked-dec total-length))
                this)))))))

  IColumnIndexPersistence
  (idx-fork [_]
    (PersistentColumnIndex. tree total-length datatype chunk-size cmp nil (atom #{}) metadata next-chunk-id storage))

  (idx-transient [this]
    (if edit
      (throw (IllegalStateException. "Already transient"))
      ;; Lazy forking: don't fork chunks eagerly. Individual chunks
      ;; are forked lazily when first modified (idx-set!/idx-append! check
      ;; col-transient?, idx-insert!/idx-delete! create new arrays directly).
      (PersistentColumnIndex. (clojure.core/transient tree)
                              total-length
                              datatype
                              chunk-size
                              cmp
                              (Object.)
                              (atom #{})
                              metadata
                              next-chunk-id
                              storage)))

  (idx-persistent! [this]
    (if-not edit
      (throw (IllegalStateException. "Already persistent"))
      (do
        (doseq [^ChunkEntry entry (pss/slice tree nil nil)]
          (let [chunk (.chunk entry)]
            (when (chunk/col-transient? chunk)
              (chunk/col-persistent! chunk))))
        (set! tree (persistent! tree))
        (set! edit nil)
        this)))

  (idx-dirty-chunks [_]
    @dirty-chunks)

  (idx-clear-dirty! [_]
    (reset! dirty-chunks #{}))

  (idx-storage [_] storage)

  (idx-sync! [this store]
    (when edit
      (throw (IllegalStateException. "Cannot sync transient index. Call idx-persistent! first.")))

    ;; PSS-backed storage: store PSS tree nodes directly in konserve.
    ;; pss/store walks the tree bottom-up, only storing dirty/unstored nodes.
    (let [crypto-hash? (:crypto-hash? metadata)
          parent-commit-id (get-in metadata [:commit :id])

          ;; Create or reuse CachedStorage
          cached-storage (or storage
                             (cstorage/create-storage
                              store {:crypto-hash? crypto-hash?}))

          ;; Migrate in-memory PSS to storage-backed PSS if needed
          ;; (first sync: tree has no storage attached)
          storage-tree
          (if (.-_storage ^PersistentSortedSet tree)
            ;; Already storage-backed — use as is
            tree
            ;; First sync: create new PSS with storage, conj all entries
            (let [new-tree (pss/sorted-set*
                            {:cmp cmp
                             :measure chunk-entry-measure-ops
                             :storage cached-storage
                             :branching-factor stratum.cached-storage/BRANCHING_FACTOR
                             :ref-type :weak})]
              (reduce conj new-tree (pss/slice tree nil nil))))

          ;; Store tree — incremental, only dirty nodes written
          root-addr (pss/store storage-tree cached-storage)

          ;; Flush pending writes to konserve
          _ (cstorage/flush-writes! cached-storage)

          ;; Generate commit ID
          stats (idx-stats this)
          commit-id (if crypto-hash?
                      (storage/generate-commit-id
                       {:pss-root root-addr
                        :total-length total-length
                        :datatype datatype
                        :chunk-size chunk-size})
                      (storage/generate-commit-id))

          ;; Build snapshot with pss-root instead of chunk-addresses
          parents (if parent-commit-id #{parent-commit-id} #{})
          snapshot {:commit-id commit-id
                    :parents parents
                    :total-length total-length
                    :datatype datatype
                    :chunk-size chunk-size
                    :stats stats
                    :pss-root root-addr
                    :next-chunk-id next-chunk-id
                    :timestamp (System/currentTimeMillis)
                    :metadata metadata}]

      ;; Write index commit (no branch — indices don't own branches)
      (storage/write-index-commit! store commit-id snapshot)

      ;; Clear dirty chunks and return new index with storage attached
      (idx-clear-dirty! this)
      (PersistentColumnIndex. storage-tree total-length datatype chunk-size cmp
                              nil (atom #{})
                              (assoc-in metadata [:commit :id] commit-id)
                              next-chunk-id cached-storage)))

  clojure.lang.Counted
  (count [_] total-length)

  clojure.lang.IObj
  (meta [_] metadata)
  (withMeta [_ m]
    (PersistentColumnIndex. tree total-length datatype chunk-size cmp edit dirty-chunks m next-chunk-id storage))

  Object
  (toString [_]
    (let [num-chunks (count (pss/slice tree nil nil))]
      (format "#ColumnIndex[%s x %d in %d chunks, %s]"
              (name datatype) total-length num-chunks
              (if edit "transient" "persistent")))))

;; ============================================================================
;; Range Statistics with Boundary Corrections
;; ============================================================================

(defn- find-chunk-entry-at
  "Find the ChunkEntry and local offset for a given data index."
  [index ^long data-idx]
  (pss/get-nth (idx-tree index) data-idx))

(defn idx-stats-range
  "Get aggregate stats for data indices [start-idx, end-idx) using overhang approach."
  [index ^long start-idx ^long end-idx]
  (when (>= start-idx end-idx)
    (throw (ex-info "Invalid range: start >= end" {:start start-idx :end end-idx})))

  (let [tree (idx-tree index)

        [^ChunkEntry first-entry ^long first-local-offset] (find-chunk-entry-at index start-idx)
        [^ChunkEntry last-entry ^long last-local-offset] (find-chunk-entry-at index (dec end-idx))

        _ (when-not first-entry
            (throw (ex-info "start-idx out of bounds" {:start-idx start-idx})))
        _ (when-not last-entry
            (throw (ex-info "end-idx out of bounds" {:end-idx end-idx})))]

    (if (= (.chunk-id first-entry) (.chunk-id last-entry))
      ;; Single chunk: scan the requested slice directly
      (let [arr (chunk/chunk-data (.chunk first-entry))
            offset first-local-offset
            len (- end-idx start-idx)]
        (stats/compute-stats-range arr offset len))

      ;; Multiple chunks: overhang approach
      (let [first-chunk-len (chunk/chunk-length (.chunk first-entry))

            first-overhang-stats
            (let [arr (chunk/chunk-data (.chunk first-entry))
                  offset first-local-offset
                  len (- first-chunk-len first-local-offset)]
              (stats/compute-stats-range arr offset len))

            interior-stats
            ;; Get stats for all chunks strictly between first and last.
            ;; Slice [first, last] inclusive, then filter out first and last.
            (let [all-entries (pss/slice tree first-entry last-entry)
                  first-id (.chunk-id first-entry)
                  last-id (.chunk-id last-entry)
                  interior (remove #(let [cid (.chunk-id ^ChunkEntry %)]
                                      (or (= cid first-id) (= cid last-id)))
                                   all-entries)]
              (when (seq interior)
                (reduce stats/merge-stats
                        (map #(.stats ^ChunkEntry %) interior))))

            last-overhang-stats
            (let [arr (chunk/chunk-data (.chunk last-entry))
                  len (inc last-local-offset)]
              (stats/compute-stats-range arr 0 len))]

        (stats/merge-stats first-overhang-stats
                           (stats/merge-stats interior-stats last-overhang-stats))))))

;; ============================================================================
;; Constructors
;; ============================================================================

(defn make-index
  "Create an empty column index with PSS aggregate statistics enabled."
  ([datatype]
   (make-index datatype {}))
  ([datatype opts]
   (let [chunk-size (get opts :chunk-size DEFAULT_CHUNK_SIZE)
         processor (get opts :leaf-processor nil)
         idx-metadata (get opts :metadata nil)
         cmp chunk-entry-comparator
         tree (pss/sorted-set* {:cmp cmp
                                :measure chunk-entry-measure-ops
                                :leaf-processor processor})]
     (PersistentColumnIndex. tree 0 datatype chunk-size cmp nil (atom #{}) idx-metadata 0 nil))))

(defn index-from-seq
  "Create a column index from a sequence of values."
  ([datatype coll]
   (index-from-seq datatype coll {}))
  ([datatype coll opts]
   (let [chunk-size (long (get opts :chunk-size DEFAULT_CHUNK_SIZE))
         data (vec coll)
         total (long (count data))]
     (loop [idx (-> (make-index datatype opts)
                    idx-transient)
            offset (long 0)]
       (if (>= offset total)
         (idx-persistent! idx)
         (let [end (long (min (+ offset chunk-size) total))
               chunk-data (subvec data (int offset) (int end))
               chunk (chunk/chunk-from-seq datatype chunk-data)
               new-idx (idx-append-chunk! idx chunk)]
           (recur new-idx end)))))))

(defn index-from-array
  "Create a column index from a primitive array (long[] or double[]).
   Much faster than index-from-seq — no boxing, uses Arrays/copyOfRange."
  ([arr] (index-from-array arr {}))
  ([arr opts]
   (let [chunk-size (long (get opts :chunk-size DEFAULT_CHUNK_SIZE))
         is-long (instance? (Class/forName "[J") arr)
         is-double (instance? (Class/forName "[D") arr)
         _ (when-not (or is-long is-double)
             (throw (ex-info "index-from-array requires long[] or double[]"
                             {:type (type arr)})))
         datatype (if is-long :int64 :float64)
         total (long (if is-long
                       (alength ^longs arr)
                       (alength ^doubles arr)))]
     (loop [idx (-> (make-index datatype opts)
                    idx-transient)
            offset (long 0)]
       (if (>= offset total)
         (idx-persistent! idx)
         (let [end (long (min (+ offset chunk-size) total))
               sub-arr (if is-long
                         (Arrays/copyOfRange ^longs arr (int offset) (int end))
                         (Arrays/copyOfRange ^doubles arr (int offset) (int end)))
               chunk (chunk/chunk-from-array sub-arr)
               new-idx (idx-append-chunk! idx chunk)]
           (recur new-idx end)))))))

;; ============================================================================
;; SIMD / Vectorized Operations
;; ============================================================================

(defn idx-reduce-chunks
  "Reduce over chunks with a function (f acc chunk-array)."
  [index f init]
  (let [result (volatile! init)]
    (idx-scan index
              (fn [arr]
                (vswap! result f arr)))
    @result))

(defn idx-sum
  "Sum all values in the index."
  [index]
  (idx-reduce-chunks index
                     (fn [^double acc arr]
                       (let [is-doubles (instance? (Class/forName "[D") arr)]
                         (if is-doubles
                           (let [^doubles a arr
                                 n (alength a)]
                             (loop [i 0, s acc]
                               (if (>= i n) s
                                   (recur (inc i) (+ s (aget a i))))))
                           (let [^longs a arr
                                 n (alength a)]
                             (loop [i 0, s acc]
                               (if (>= i n) s
                                   (recur (inc i) (+ s (double (aget a i))))))))))
                     0.0))

(defn idx-mean
  "Calculate mean of all values."
  [index]
  (/ (idx-sum index) (idx-length index)))

;; ============================================================================
;; O(chunks) Aggregations Using Statistics
;; ============================================================================

(defn idx-sum-stats
  ^double [index]
  (stats/stats-sum (idx-stats index)))

(defn idx-mean-stats
  ^double [index]
  (stats/stats-mean (idx-stats index)))

(defn idx-min-stats
  ^double [index]
  (stats/stats-min (idx-stats index)))

(defn idx-max-stats
  ^double [index]
  (stats/stats-max (idx-stats index)))

(defn idx-variance-stats
  (^double [index]
   (stats/stats-variance (idx-stats index)))
  (^double [index opts]
   (stats/stats-variance (idx-stats index) opts)))

(defn idx-stddev-stats
  (^double [index]
   (stats/stats-stddev (idx-stats index)))
  (^double [index opts]
   (stats/stats-stddev (idx-stats index) opts)))

;; ============================================================================
;; Zone Map Filtering
;; ============================================================================

(defn idx-filter-zonemap
  "Filter index with zone map optimization."
  [index elem-pred zone-pred]
  (let [result (java.util.ArrayList.)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats start-idx]
                           (when (zone-pred chunk-stats)
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (if is-doubles
                                           (aget ^doubles chunk-arr i)
                                           (aget ^longs chunk-arr i))]
                                   (when (elem-pred v)
                                     (.add result (+ start-idx i)))))))))
    (vec result)))

(defn idx-count-zonemap
  "Count matching elements with zone map optimization."
  [index elem-pred zone-all-pred zone-none-pred]
  (let [result (volatile! 0)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (zone-none-pred chunk-stats)
                             nil

                             (zone-all-pred chunk-stats)
                             (vswap! result + (:count chunk-stats))

                             :else
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (if is-doubles
                                           (aget ^doubles chunk-arr i)
                                           (aget ^longs chunk-arr i))]
                                   (when (elem-pred v)
                                     (vswap! result inc))))))))
    @result))

(defn idx-filter-range
  [index ^double lo ^double hi]
  (idx-filter-zonemap index
                      (fn [v] (and (>= (double v) lo) (< (double v) hi)))
                      (fn [s] (stats/zone-may-contain-range? s lo hi))))

(defn idx-filter-gt
  [index ^double threshold]
  (idx-filter-zonemap index
                      (fn [v] (> (double v) threshold))
                      (fn [s] (stats/zone-may-contain-gt? s threshold))))

(defn idx-filter-lt
  [index ^double threshold]
  (idx-filter-zonemap index
                      (fn [v] (< (double v) threshold))
                      (fn [s] (stats/zone-may-contain-lt? s threshold))))

(defn idx-filter-gte
  [index ^double threshold]
  (idx-filter-zonemap index
                      (fn [v] (>= (double v) threshold))
                      (fn [s] (stats/zone-may-contain-gte? s threshold))))

(defn idx-filter-lte
  [index ^double threshold]
  (idx-filter-zonemap index
                      (fn [v] (<= (double v) threshold))
                      (fn [s] (stats/zone-may-contain-lte? s threshold))))

(defn idx-filter-eq
  [index ^double target]
  (idx-filter-zonemap index
                      (fn [v] (== (double v) target))
                      (fn [s] (stats/zone-may-contain-eq? s target))))

(defn idx-filter-neq
  [index ^double target]
  (idx-filter-zonemap index
                      (fn [v] (not= (double v) target))
                      (fn [s] (stats/zone-may-contain-neq? s target))))

;; ============================================================================
;; Range Aggregations with Zone Map Optimization
;; ============================================================================

(defn- chunk-fully-in-range?
  [^stratum.stats.ChunkStats stats ^double lo ^double hi]
  (and (>= (:min-val stats) lo)
       (< (:max-val stats) hi)))

(defn- chunk-fully-outside-range?
  [^stratum.stats.ChunkStats stats ^double lo ^double hi]
  (or (>= (:min-val stats) hi)
      (< (:max-val stats) lo)))

(defn- scan-range-sum
  "Helper: sum values in [lo,hi) from a chunk array."
  ^double [chunk-arr ^double lo ^double hi]
  (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
        n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
    (loop [i 0, s 0.0]
      (if (>= i n) s
          (let [v (double (if is-doubles (aget ^doubles chunk-arr i) (aget ^longs chunk-arr i)))]
            (recur (inc i) (if (and (>= v lo) (< v hi)) (+ s v) s)))))))

(defn idx-sum-range
  ^double [index ^double lo ^double hi]
  (let [result (volatile! 0.0)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (chunk-fully-outside-range? chunk-stats lo hi) nil
                             (chunk-fully-in-range? chunk-stats lo hi) (vswap! result + (:sum chunk-stats))
                             :else (vswap! result + (scan-range-sum chunk-arr lo hi)))))
    @result))

(defn idx-count-range
  ^long [index ^double lo ^double hi]
  (let [result (volatile! 0)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (chunk-fully-outside-range? chunk-stats lo hi) nil
                             (chunk-fully-in-range? chunk-stats lo hi) (vswap! result + (:count chunk-stats))
                             :else
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (double (if is-doubles (aget ^doubles chunk-arr i) (aget ^longs chunk-arr i)))]
                                   (when (and (>= v lo) (< v hi))
                                     (vswap! result inc))))))))
    @result))

(defn idx-mean-range
  ^double [index ^double lo ^double hi]
  (let [sum (volatile! 0.0)
        cnt (volatile! 0)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (chunk-fully-outside-range? chunk-stats lo hi) nil
                             (chunk-fully-in-range? chunk-stats lo hi)
                             (do (vswap! sum + (:sum chunk-stats))
                                 (vswap! cnt + (:count chunk-stats)))
                             :else
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (double (if is-doubles (aget ^doubles chunk-arr i) (aget ^longs chunk-arr i)))]
                                   (when (and (>= v lo) (< v hi))
                                     (vswap! sum + v)
                                     (vswap! cnt inc))))))))
    (if (zero? @cnt) Double/NaN (/ @sum @cnt))))

(defn idx-min-range
  ^double [index ^double lo ^double hi]
  (let [result (volatile! Double/POSITIVE_INFINITY)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (chunk-fully-outside-range? chunk-stats lo hi) nil
                             (chunk-fully-in-range? chunk-stats lo hi)
                             (vswap! result min (:min-val chunk-stats))
                             :else
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (double (if is-doubles (aget ^doubles chunk-arr i) (aget ^longs chunk-arr i)))]
                                   (when (and (>= v lo) (< v hi))
                                     (vswap! result min v))))))))
    @result))

(defn idx-max-range
  ^double [index ^double lo ^double hi]
  (let [result (volatile! Double/NEGATIVE_INFINITY)]
    (idx-scan-with-stats index
                         (fn [chunk-arr chunk-stats _start-idx]
                           (cond
                             (chunk-fully-outside-range? chunk-stats lo hi) nil
                             (chunk-fully-in-range? chunk-stats lo hi)
                             (vswap! result max (:max-val chunk-stats))
                             :else
                             (let [is-doubles (instance? (Class/forName "[D") chunk-arr)
                                   n (if is-doubles (alength ^doubles chunk-arr) (alength ^longs chunk-arr))]
                               (dotimes [i n]
                                 (let [v (double (if is-doubles (aget ^doubles chunk-arr i) (aget ^longs chunk-arr i)))]
                                   (when (and (>= v lo) (< v hi))
                                     (vswap! result max v))))))))
    @result))

(defn idx-map-chunks
  "Map a function over each chunk, returning a new index.
   The function receives and returns a primitive array."
  [index f]
  (let [new-idx (-> (make-index (idx-datatype index)
                                {:chunk-size (idx-chunk-size index)})
                    idx-transient)]
    (idx-scan index
              (fn [arr]
                (let [result-arr (f arr)
                      new-chunk (chunk/chunk-from-array result-arr)]
                  (idx-append-chunk! new-idx new-chunk))))
    (idx-persistent! new-idx)))

;; ============================================================================
;; Materialization - For Fast Analytics
;; ============================================================================

(defn idx-materialize-to-array
  "Materialize index directly to a Java heap array (long[] or double[]).
   Single-copy: iterates chunks and copies each directly into target array.
   Constant chunks use Arrays.fill instead of arraycopy (no decompression needed)."
  [index]
  (let [n (idx-length index)
        dt (idx-datatype index)
        result (case dt :int64 (long-array n) :float64 (double-array n))
        tree (idx-tree index)]
    (let [offset (volatile! 0)]
      (doseq [^ChunkEntry entry (pss/slice tree nil nil)]
        (let [chk (.chunk entry)
              off (long @offset)
              chunk-len (long (chunk/chunk-length chk))]
          (if (chunk/chunk-constant? chk)
            ;; Constant chunk: fill range without expanding the chunk's data
            (let [cv (chunk/chunk-constant-val chk)]
              (case dt
                :float64 (java.util.Arrays/fill ^doubles result (int off) (int (+ off chunk-len)) (double cv))
                :int64 (java.util.Arrays/fill ^longs result (int off) (int (+ off chunk-len)) (long cv))))
            ;; Regular chunk: arraycopy
            (let [chunk-arr (chunk/chunk-data chk)]
              (System/arraycopy chunk-arr 0 result off chunk-len)))
          (vreset! offset (+ off chunk-len)))))
    result))

(defn idx-materialize
  "Materialize index to a contiguous heap array.

   Returns a long[] or double[] that can be passed to Java SIMD operations."
  [index]
  (idx-materialize-to-array index))

(defn idx-materialize-to-array-pruned
  "Materialize only chunks at surviving indices into a shorter heap array.
   Constant chunks use Arrays.fill instead of arraycopy (no decompression needed)."
  [index surviving-indices]
  (let [dt (idx-datatype index)
        tree (idx-tree index)
        entries (vec (pss/slice tree nil nil))
        total-len (long (reduce (fn [^long acc ^long i]
                                  (let [^ChunkEntry e (nth entries i)]
                                    (+ acc (chunk/chunk-length (.chunk e)))))
                                0 surviving-indices))
        result (case dt :int64 (long-array total-len) :float64 (double-array total-len))]
    (let [offset (volatile! 0)]
      (doseq [^long i surviving-indices]
        (let [^ChunkEntry entry (nth entries i)
              chk (.chunk entry)
              off (long @offset)
              chunk-len (long (chunk/chunk-length chk))]
          (if (chunk/chunk-constant? chk)
            (let [cv (chunk/chunk-constant-val chk)]
              (case dt
                :float64 (java.util.Arrays/fill ^doubles result (int off) (int (+ off chunk-len)) (double cv))
                :int64 (java.util.Arrays/fill ^longs result (int off) (int (+ off chunk-len)) (long cv))))
            (let [arr (chunk/chunk-data chk)]
              (System/arraycopy arr 0 result off chunk-len)))
          (vreset! offset (+ off chunk-len)))))
    result))

;; ============================================================================
;; Snapshot Restoration
;; ============================================================================

(defn restore-index-from-snapshot
  "Restore a PersistentColumnIndex from a snapshot.
   Uses PSS-backed lazy loading: creates a lazy tree from the stored root address.
   No data is loaded until first query touches a chunk."
  [snapshot store]
  (let [{:keys [datatype chunk-size total-length pss-root
                next-chunk-id metadata]} snapshot]
    (if (nil? pss-root)
      ;; Empty index
      (let [cmp chunk-entry-comparator
            tree (pss/sorted-set* {:cmp cmp :measure chunk-entry-measure-ops})]
        (PersistentColumnIndex. tree 0 datatype chunk-size cmp
                                nil (atom #{}) metadata (or next-chunk-id 0) nil))
      ;; Lazy restore via PSS
      (let [cached-storage (cstorage/create-storage
                            store {:crypto-hash? (:crypto-hash? metadata)})
            tree (pss/restore-by chunk-entry-comparator pss-root cached-storage
                                 {:measure chunk-entry-measure-ops
                                  :branching-factor cstorage/BRANCHING_FACTOR
                                  :ref-type :weak})]
        (PersistentColumnIndex. tree total-length datatype chunk-size
                                chunk-entry-comparator nil (atom #{})
                                metadata next-chunk-id cached-storage)))))

;; ============================================================================
;; Chunk Distribution Diagnostics
;; ============================================================================

(defn idx-chunk-distribution
  "Compute distribution statistics over chunk sizes in the index.
   Returns {:chunk-count N, :total-elements N, :chunk-size CS,
            :min N, :p25 N, :p50 N, :p75 N, :p90 N, :max N,
            :avg F, :undersized N, :oversized N, :fill-ratio F}

   - :undersized = chunks with < chunk-size/2 elements
   - :oversized = chunks with > chunk-size elements
   - :fill-ratio = total-elements / (chunk-count * chunk-size)"
  [idx]
  (let [chunk-size (long (idx-chunk-size idx))
        all-stats (idx-all-chunk-stats idx)
        lengths (mapv :count all-stats)
        n (count lengths)]
    (if (zero? n)
      {:chunk-count 0 :total-elements 0 :chunk-size chunk-size
       :min 0 :p25 0 :p50 0 :p75 0 :p90 0 :max 0
       :avg 0.0 :undersized 0 :oversized 0 :fill-ratio 0.0}
      (let [sorted (vec (sort lengths))
            total (reduce + 0 lengths)
            at (fn [p] (nth sorted (min (int (* n p)) (dec n))))
            half-chunk (quot chunk-size 2)]
        {:chunk-count n
         :total-elements total
         :chunk-size chunk-size
         :min (first sorted)
         :p25 (at 0.25)
         :p50 (at 0.50)
         :p75 (at 0.75)
         :p90 (at 0.90)
         :max (peek sorted)
         :avg (/ (double total) n)
         :undersized (count (filter #(< % half-chunk) lengths))
         :oversized (count (filter #(> % chunk-size) lengths))
         :fill-ratio (/ (double total) (* n chunk-size))}))))

(comment
  ;; Quick REPL test
  (require '[criterium.core :as crit])

  ;; Create index with 1M doubles
  (def idx (index-from-seq :float64 (range 1000000)))
  (idx-length idx) ;; => 1000000

  ;; Point access
  (idx-get-double idx 0)       ;; => 0.0
  (idx-get-double idx 999999)  ;; => 999999.0

  ;; SIMD sum
  (idx-sum idx) ;; => 4.999995E11

  ;; Benchmark point access
  (crit/quick-bench (idx-get-double idx 500000))

  ;; Benchmark scan
  (crit/quick-bench (idx-sum idx)))
