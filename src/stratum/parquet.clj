(ns stratum.parquet
  "Parquet support for Stratum.

   Three paths, picked by the lifecycle of the data:

     (from-parquet \"data/orders.parquet\")
       In-memory: builds heap arrays and returns a non-persistent
       StratumDataset. Suitable for small-to-medium files used
       programmatically. Memory footprint scales with file size.

     (parquet-dataset \"data/orders.parquet\")
       Zero-copy: opens the file, reads only metadata, and returns a
       read-only StratumDataset whose columns lazy-decode their row
       groups on first SIMD-path or materialization. Open is
       constant-time. Stats from parquet's own row-group metadata feed
       directly into stratum's zone-map pruning. No konserve, no
       persistence — the parquet file is the storage. Best for ad-hoc
       analytics on parquet files.

     (index-parquet! \"data/orders.parquet\" store branch)
       Streaming: reads the file row-group-by-row-group, fills
       chunk-sized buffers per column, flushes chunks to konserve
       via idx-append-chunk!, and periodically syncs to free heap.
       Returns a synced, index-backed StratumDataset. Memory is
       bounded — independent of file size. Best when the data needs
       stratum's mutate/branch/sync lifecycle.

   No Hadoop runtime required — uses LocalInputFile for direct file access."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [stratum.dataset :as dataset]
            [stratum.index :as idx]
            [stratum.chunk :as chunk]
            [stratum.stats :as stats]
            [stratum.cached-storage :as cstorage]
            [stratum.storage :as storage]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.io LocalInputFile]
           [org.apache.parquet.column ColumnDescriptor ColumnReader Dictionary]
           [org.apache.parquet.column.impl ColumnReadStoreImpl]
           [org.apache.parquet.column.page PageReadStore DictionaryPageReadStore]
           [org.apache.parquet.column.statistics Statistics]
           [org.apache.parquet.hadoop.metadata BlockMetaData ColumnChunkMetaData]
           [org.apache.parquet.io ColumnIOFactory MessageColumnIO]
           [org.apache.parquet.io.api RecordMaterializer GroupConverter PrimitiveConverter Binary]
           [org.apache.parquet.schema MessageType Type GroupType]
           [org.apache.parquet.schema PrimitiveType PrimitiveType$PrimitiveTypeName]
           [org.apache.parquet.schema LogicalTypeAnnotation LogicalTypeAnnotation$StringLogicalTypeAnnotation
            LogicalTypeAnnotation$TimestampLogicalTypeAnnotation]
           [java.lang.ref Cleaner]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.file Path Paths]
           [java.util HashMap]
           [stratum.internal ColumnOpsString]))

(set! *warn-on-reflection* true)

(defn- primitive-type-name
  "Get the PrimitiveTypeName for a column."
  [^PrimitiveType ptype]
  (.getPrimitiveTypeName ptype))

(defn- string-logical-type?
  "Check if a type has STRING logical annotation."
  [^PrimitiveType ptype]
  (instance? LogicalTypeAnnotation$StringLogicalTypeAnnotation
             (.getLogicalTypeAnnotation ptype)))

(defn- timestamp-logical-type?
  "Check if a type has TIMESTAMP logical annotation."
  [^PrimitiveType ptype]
  (instance? LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
             (.getLogicalTypeAnnotation ptype)))

(defn- classify-column
  "Classify a Parquet column type to :long, :double, or :string."
  [^PrimitiveType ptype]
  (let [ptn (primitive-type-name ptype)]
    (cond
      ;; String types
      (and (= ptn PrimitiveType$PrimitiveTypeName/BINARY)
           (string-logical-type? ptype))
      :string

      ;; Fixed-len byte array with string annotation
      (and (= ptn PrimitiveType$PrimitiveTypeName/FIXED_LEN_BYTE_ARRAY)
           (string-logical-type? ptype))
      :string

      ;; Integer types → long
      (= ptn PrimitiveType$PrimitiveTypeName/INT64) :long
      (= ptn PrimitiveType$PrimitiveTypeName/INT32) :long

      ;; Float types → double
      (= ptn PrimitiveType$PrimitiveTypeName/DOUBLE) :double
      (= ptn PrimitiveType$PrimitiveTypeName/FLOAT) :double

      ;; Boolean → long (0/1)
      (= ptn PrimitiveType$PrimitiveTypeName/BOOLEAN) :long

      ;; INT96 (legacy timestamp) → long (epoch millis)
      (= ptn PrimitiveType$PrimitiveTypeName/INT96) :long

      ;; BINARY without string annotation → string (best effort)
      (= ptn PrimitiveType$PrimitiveTypeName/BINARY) :string

      :else :string)))

(defn- int96->epoch-millis
  "Convert INT96 Binary (12 bytes: 8 LE nanos-of-day + 4 LE julian-day) to epoch millis."
  ^long [^Binary v]
  (let [bytes (.getBytes v)
        buf   (-> (ByteBuffer/wrap bytes) (.order ByteOrder/LITTLE_ENDIAN))
        nanos-of-day (.getLong buf 0)
        julian-day   (.getInt buf 8)
        ;; Julian day epoch: Nov 24, 4714 BC. Unix epoch = Julian day 2440588
        epoch-day    (- (long julian-day) 2440588)
        millis-of-day (quot nanos-of-day 1000000)]
    (+ (* epoch-day 86400000) millis-of-day)))

;; ============================================================================
;; Row-by-row reader using ColumnIO + RecordReader
;; ============================================================================

;; ============================================================================
;; Streaming index-parquet! — bounded memory, persists chunks to konserve
;; ============================================================================

;; Match stratum's project-wide default (idx/DEFAULT_CHUNK_SIZE = 8192).
;; A larger value here (we previously used 65536) made each chunk 8× bigger
;; and, with PSS BRANCHING_FACTOR = 64, each PSS leaf 8× bigger. Leaf
;; serialization through Fressian's ByteArrayOutputStream then needed
;; hundreds of MB of contiguous heap during sync.
(def ^:private ^:const DEFAULT_CHUNK_SIZE 8192)

;; Chunks per column between syncs. Each sync re-walks the PSS, Fressian-
;; encodes leaves, and round-trips through konserve — so syncing too
;; aggressively dominates ingest time. Memory is already bounded by the
;; storage LRU (cstorage/DEFAULT_CACHE_SIZE = 16 nodes), so sync_every
;; controls only how often we flush, not steady-state heap.
;;
;; Measured 5M-row × 23-col ingest at -Xmx4g (Ryan-shape fixture):
;;   sync=4    269s   19k rows/s   peak 1.4 GB
;;   sync=16    45s  112k rows/s   peak 2.7 GB
;;   sync=64    25s  200k rows/s   peak 2.3 GB    ← matches from-parquet
;;   sync=256   14s  364k rows/s   peak 2.4 GB
;; All fit in 4 GB Xmx with breathing room. 64 is the conservative pick.
(def ^:private ^:const DEFAULT_SYNC_EVERY 64)

(defn- alloc-buf
  "Pre-allocated chunk buffer filled with the type's NULL sentinel."
  [col-type ^long chunk-size]
  (case col-type
    :long
    (let [a (long-array chunk-size)] (java.util.Arrays/fill a Long/MIN_VALUE) a)
    :double
    (let [a (double-array chunk-size)] (java.util.Arrays/fill a Double/NaN) a)
    :string
    (let [a (long-array chunk-size)] (java.util.Arrays/fill a Long/MIN_VALUE) a)))

(defn- make-chunk-from-buf
  "Copy buf[0..len) into a fresh stratum chunk. Returns a persistent ColChunk."
  [col-type ^long len buf]
  (case col-type
    :long   (chunk/chunk-from-array (java.util.Arrays/copyOf ^longs buf (int len)))
    :double (chunk/chunk-from-array (java.util.Arrays/copyOf ^doubles buf (int len)))
    :string (chunk/chunk-from-array (java.util.Arrays/copyOf ^longs buf (int len)))))

(defn- streaming-converter
  "Build a PrimitiveConverter that writes one cell per call into the column's
   chunk buffer at position cursor[0]. After GroupConverter.end() bumps
   cursor[0] by 1, the next row writes to the next slot.

   col-state is a map {:type :buf :dict-map :next-id ...}; cursor is a shared
   int[1] indexed by all columns in lockstep."
  ^PrimitiveConverter [col-state is-int96 ^ints cursor]
  (let [t (:type col-state)
        buf (:buf col-state)]
    (case t
      :long
      (let [^longs arr buf]
        (if is-int96
          (proxy [PrimitiveConverter] []
            (addBinary [^Binary v]
              (aset arr (aget cursor 0) (long (int96->epoch-millis v)))))
          (proxy [PrimitiveConverter] []
            (addLong    [v] (aset arr (aget cursor 0) (long v)))
            (addInt     [v] (aset arr (aget cursor 0) (long (int v))))
            (addBoolean [v] (aset arr (aget cursor 0) (if v 1 0)))
            (addBinary  [^Binary v] (aset arr (aget cursor 0) (long (int96->epoch-millis v))))
            (addFloat   [v] (aset arr (aget cursor 0) (long (float v))))
            (addDouble  [v] (aset arr (aget cursor 0) (long (double v)))))))

      :double
      (let [^doubles arr buf]
        (proxy [PrimitiveConverter] []
          (addDouble [v] (aset arr (aget cursor 0) (double v)))
          (addFloat  [v] (aset arr (aget cursor 0) (double (float v))))
          (addLong   [v] (aset arr (aget cursor 0) (double (long v))))
          (addInt    [v] (aset arr (aget cursor 0) (double (int v))))))

      :string
      (let [^longs arr buf
            ^HashMap dict-map (:dict-map col-state)
            ^longs next-id (:next-id col-state)]
        (proxy [PrimitiveConverter] []
          (addBinary [^Binary v]
            (let [s (.toStringUsingUTF8 v)
                  pos (aget cursor 0)
                  id (.get dict-map s)]
              (if id
                (aset arr pos (long id))
                (let [new-id (aget next-id 0)]
                  (.put dict-map s new-id)
                  (aset arr pos new-id)
                  (aset next-id 0 (inc new-id))))))
          (addLong [v]
            (let [s (str v)
                  pos (aget cursor 0)
                  id (.get dict-map s)]
              (if id
                (aset arr pos (long id))
                (let [new-id (aget next-id 0)]
                  (.put dict-map s new-id)
                  (aset arr pos new-id)
                  (aset next-id 0 (inc new-id))))))
          (addInt [v]
            (let [s (str v)
                  pos (aget cursor 0)
                  id (.get dict-map s)]
              (if id
                (aset arr pos (long id))
                (let [new-id (aget next-id 0)]
                  (.put dict-map s new-id)
                  (aset arr pos new-id)
                  (aset next-id 0 (inc new-id)))))))))))

(defn- finalize-string-col
  "Build the dict-encoded column map for a string col after ingest:
   {:type :int64 :source :index :index <pci> :dict <String[]>
    :dict-type :string :dict-alpha-masks ... :dict-bigram-masks ...}"
  [col-state]
  (let [^HashMap dict-map (:dict-map col-state)
        ^longs next-id (:next-id col-state)
        dict-size (aget next-id 0)
        reverse-dict (make-array String dict-size)]
    (doseq [^java.util.Map$Entry e (.entrySet dict-map)]
      (when-let [k (.getKey e)]
        (aset ^"[Ljava.lang.String;" reverse-dict
              (int (long (.getValue e))) k)))
    {:type :int64
     :source :index
     :index (:idx col-state)
     :dict reverse-dict
     :dict-type :string
     :dict-alpha-masks (ColumnOpsString/buildDictAlphaMasks reverse-dict)
     :dict-bigram-masks (ColumnOpsString/buildDictBigramMasks reverse-dict)}))

(defn- finalize-numeric-col
  [col-state]
  (let [t (:type col-state)
        idx (:idx col-state)
        idx-type (case t :long :int64 :double :float64)]
    {:type idx-type :source :index :index idx}))

(defn index-parquet!
  "Stream a Parquet file directly into a chunked, persistent StratumDataset.

   Reads row-by-row, accumulating chunk-sized buffers per column. When a chunk
   buffer fills, it's appended to that column's transient PersistentColumnIndex
   via idx-append-chunk!. Every :sync-every chunks the index is persisted and
   batch-flushed to konserve via a single bounded-parallel fan-out across all
   columns, then re-transient'd — chunks become weak-referenced and GC
   reclaims their heap. String columns are dict-encoded on the fly: a HashMap
   interns each distinct value once and the chunk stores only long indices.

   Intermediate index commits during the ingest loop are skipped — the final
   `dataset/sync!` re-syncs each column and writes the durable commits.

   Memory is bounded by chunk-size × num-columns × 8 bytes during reading,
   plus the LRU node cache in the konserve storage layer. Independent of
   total file size.

   Returns a synced StratumDataset with index-backed columns.

   Options:
     :columns     - vector of column names to read (default: all)
     :limit       - max rows to read (default: full file)
     :chunk-size  - rows per chunk (default DEFAULT_CHUNK_SIZE)
     :sync-every  - flush to konserve every N chunks (default DEFAULT_SYNC_EVERY)
     :name        - dataset name (default: derived from filename)"
  [^String path store ^String branch
   & {:keys [columns limit chunk-size sync-every name]
      :or {chunk-size DEFAULT_CHUNK_SIZE
           sync-every DEFAULT_SYNC_EVERY}}]
  (let [input-file (LocalInputFile. (Paths/get path (make-array String 0)))
        reader (ParquetFileReader/open input-file)]
    (try
      (let [footer (.getFooter reader)
            schema (.getSchema (.getFileMetaData footer))
            fields (.getFields ^MessageType schema)
            file-rows (.getRecordCount reader)
            target-rows (long (min file-rows (or limit Long/MAX_VALUE)))

            col-indices (if columns
                          (let [col-set (set columns)]
                            (vec (keep-indexed
                                  (fn [i ^Type f]
                                    (when (and (col-set (.getName f))
                                               (not (instance? GroupType f)))
                                      i))
                                  fields)))
                          (vec (keep-indexed
                                (fn [i ^Type f]
                                  (when (not (instance? GroupType f)) i))
                                fields)))
            col-names (mapv #(.getName ^Type (nth fields %)) col-indices)
            col-ptypes (mapv #(.asPrimitiveType ^Type (nth fields %)) col-indices)
            col-types (mapv classify-column col-ptypes)
            col-is-int96 (mapv #(= (primitive-type-name %)
                                   PrimitiveType$PrimitiveTypeName/INT96)
                               col-ptypes)
            ncols (count col-indices)

            ;; Per-column state. Each holds: :type, :buf, :dict-map (string only),
            ;; :next-id (string only), and an :idx-ref atom holding the current
            ;; transient PersistentColumnIndex (mutated by flush + sync).
            col-states
            (mapv (fn [i]
                    (let [t (nth col-types i)
                          idx-type (case t :long :int64 :double :float64 :string :int64)
                          idx0 (-> (idx/make-index idx-type {:chunk-size chunk-size})
                                   idx/idx-transient)]
                      (cond-> {:type t
                               :buf (alloc-buf t chunk-size)
                               :idx-ref (atom idx0)}
                        (= t :string) (assoc :dict-map (HashMap.)
                                             :next-id  (long-array 1)))))
                  (range ncols))

            cursor (int-array 1)                  ; rows in current chunk buffer
            chunks-since-sync (int-array 1)       ; chunks accumulated per col since last sync

            ;; Flush all column buffers as chunks into their respective indices.
            ;; Returns total rows flushed.
            flush-buffers!
            (fn [^long n]
              (when (pos? n)
                (dotimes [c ncols]
                  (let [cs (nth col-states c)
                        chunk (make-chunk-from-buf (:type cs) n (:buf cs))]
                    (swap! (:idx-ref cs) idx/idx-append-chunk! chunk)
                    ;; Reset buffer to NULL sentinels for next chunk.
                    (let [t (:type cs) buf (:buf cs)]
                      (case t
                        :long   (java.util.Arrays/fill ^longs buf Long/MIN_VALUE)
                        :double (java.util.Arrays/fill ^doubles buf Double/NaN)
                        :string (java.util.Arrays/fill ^longs buf Long/MIN_VALUE)))))
                (aset cursor 0 0)
                (aset chunks-since-sync 0 (inc (aget chunks-since-sync 0)))))

            ;; Sync all columns: per-column pss/store fills each storage's
            ;; pending-writes (no I/O), then a single bounded-parallel flush
            ;; drains them all together. Intermediate commits are skipped —
            ;; the final dataset/sync! re-runs idx-sync! per col and writes
            ;; the real commits there. PSS :ref-type :weak + small cache
            ;; (cstorage/DEFAULT_CACHE_SIZE) lets GC reclaim chunk arrays.
            sync-all!
            (fn []
              (when (pos? (aget chunks-since-sync 0))
                (let [synced
                      (binding [idx/*defer-flush?* true
                                storage/*skip-commit-write?* true]
                        (mapv (fn [cs]
                                (let [pers (idx/idx-persistent! @(:idx-ref cs))
                                      synced-idx (idx/idx-sync! pers store)]
                                  [cs synced-idx]))
                              col-states))
                      storages (->> synced
                                    (keep (fn [[_ s]] (idx/idx-storage s)))
                                    distinct)]
                  (async/<!! (cstorage/flush-storages-ch! storages))
                  (doseq [[cs synced-idx] synced]
                    (reset! (:idx-ref cs) (idx/idx-transient synced-idx))))
                (aset chunks-since-sync 0 0)))

            proj-fields (mapv #(nth fields %) col-indices)
            proj-schema (MessageType. (.getName ^MessageType schema)
                                      ^java.util.List (vec proj-fields))]

        ;; Read row groups, flushing on each chunk boundary and syncing every N chunks.
        (loop [pages (.readNextRowGroup reader)
               rows-read (long 0)]
          (cond
            (nil? pages) nil
            (>= rows-read target-rows) nil
            :else
            (let [^PageReadStore pg pages
                  rg-rows (.getRowCount pg)
                  remaining (- target-rows rows-read)
                  rg-to-read (min rg-rows remaining)
                  column-io (.getColumnIO (ColumnIOFactory.) proj-schema)
                  converters (mapv (fn [i]
                                     (streaming-converter (nth col-states i)
                                                          (nth col-is-int96 i)
                                                          cursor))
                                   (range ncols))
                  group-converter (proxy [GroupConverter] []
                                    (getConverter [i] (nth converters i))
                                    (start [])
                                    (end []
                                      ;; Advance cursor; flush + maybe sync when chunk full.
                                      (let [pos (inc (aget cursor 0))]
                                        (aset cursor 0 pos)
                                        (when (>= pos chunk-size)
                                          (flush-buffers! pos)
                                          (when (>= (aget chunks-since-sync 0) sync-every)
                                            (sync-all!))))))
                  record-materializer (proxy [RecordMaterializer] []
                                        (getRootConverter [] group-converter)
                                        (getCurrentRecord [] nil))
                  record-reader (.getRecordReader ^MessageColumnIO column-io
                                                  pg record-materializer)]
              (dotimes [_ rg-to-read]
                (.read record-reader))
              (recur (.readNextRowGroup reader)
                     (+ rows-read rg-to-read)))))

        ;; Flush any remaining partial chunk and do a final sync.
        (flush-buffers! (aget cursor 0))
        (sync-all!)

        ;; Build final column map and dataset; write the dataset commit.
        (let [col-map (into {}
                            (map (fn [i]
                                   (let [cs (nth col-states i)
                                         finalized (if (= :string (:type cs))
                                                     (finalize-string-col
                                                      (assoc cs :idx (idx/idx-persistent!
                                                                      @(:idx-ref cs))))
                                                     (finalize-numeric-col
                                                      (assoc cs :idx (idx/idx-persistent!
                                                                      @(:idx-ref cs)))))]
                                     [(keyword (nth col-names i)) finalized])))
                            (range ncols))
              ds-name (or name (str "parquet:" (.getName (java.io.File. path))))
              ds (dataset/make-dataset col-map
                                       {:name ds-name
                                        :metadata {:source-path path
                                                   :source-type :parquet}})]
          (dataset/sync! ds store branch)))
      (finally
        (.close reader)))))

(defn from-parquet
  "Read a Parquet file into a StratumDataset.

   Builds heap arrays in memory: pre-allocated long[]/double[] for numeric
   columns and dict-encoded long[] + HashMap<String,Long> for string columns.
   Memory scales with file size — for very large files prefer index-parquet!
   which streams into chunked indices with bounded memory.

   Returns StratumDataset suitable for queries and table registration.
   String columns are dict-encoded at ingest (Long.MIN_VALUE marks NULL).

   Options:
     :columns — vector of column names to read (default: all)
     :limit   — max rows to read
     :name    — dataset name (default: derived from filename)"
  [path & {:keys [columns limit name]}]
  (let [input-file (LocalInputFile. (Paths/get ^String path (make-array String 0)))
        reader (ParquetFileReader/open input-file)]
    (try
      (let [footer (.getFooter reader)
            schema (.getSchema (.getFileMetaData footer))
            fields (.getFields ^MessageType schema)
            file-rows (.getRecordCount reader)
            target-rows (long (min file-rows (or limit Long/MAX_VALUE)))
            col-indices (if columns
                          (let [col-set (set columns)]
                            (vec (keep-indexed
                                  (fn [i ^Type f]
                                    (when (and (col-set (.getName f))
                                               (not (instance? GroupType f)))
                                      i))
                                  fields)))
                          (vec (keep-indexed
                                (fn [i ^Type f]
                                  (when (not (instance? GroupType f)) i))
                                fields)))
            col-names (mapv #(.getName ^Type (nth fields %)) col-indices)
            col-ptypes (mapv #(.asPrimitiveType ^Type (nth fields %)) col-indices)
            col-types (mapv classify-column col-ptypes)
            col-is-int96 (mapv #(= (primitive-type-name %)
                                   PrimitiveType$PrimitiveTypeName/INT96)
                               col-ptypes)
            ncols (count col-indices)
            ;; Per-column state with primitive backing arrays. The shape mirrors
            ;; index-parquet!'s chunk buffer but sized for the whole dataset.
            col-states (mapv (fn [i]
                               (let [t (nth col-types i)]
                                 (cond-> {:type t
                                          :buf (alloc-buf t target-rows)}
                                   (= t :string) (assoc :dict-map (HashMap.)
                                                        :next-id  (long-array 1)))))
                             (range ncols))
            cursor (int-array 1)]
        (let [proj-fields (mapv #(nth fields %) col-indices)
              proj-schema (MessageType. (.getName ^MessageType schema)
                                        ^java.util.List (vec proj-fields))]
          (loop [pages (.readNextRowGroup reader)
                 rows-read (long 0)]
            (cond
              (nil? pages) nil
              (>= rows-read target-rows) nil
              :else
              (let [^PageReadStore pg pages
                    rg-rows (.getRowCount pg)
                    remaining (- target-rows rows-read)
                    rg-to-read (min rg-rows remaining)
                    column-io (.getColumnIO (ColumnIOFactory.) proj-schema)
                    converters (mapv (fn [i]
                                       (streaming-converter (nth col-states i)
                                                            (nth col-is-int96 i)
                                                            cursor))
                                     (range ncols))
                    group-converter (proxy [GroupConverter] []
                                      (getConverter [i] (nth converters i))
                                      (start [])
                                      (end []
                                        (aset cursor 0 (inc (aget cursor 0)))))
                    record-materializer (proxy [RecordMaterializer] []
                                          (getRootConverter [] group-converter)
                                          (getCurrentRecord [] nil))
                    record-reader (.getRecordReader ^MessageColumnIO column-io
                                                    pg record-materializer)]
                (dotimes [_ rg-to-read]
                  (.read record-reader))
                (recur (.readNextRowGroup reader)
                       (+ rows-read rg-to-read))))))
        (let [actual-rows (long (aget cursor 0))
              col-map (into {}
                            (map (fn [i]
                                   (let [cs (nth col-states i)
                                         col-name (nth col-names i)
                                         t (:type cs)
                                         buf (:buf cs)
                                         data (case t
                                                :long   (java.util.Arrays/copyOf ^longs buf actual-rows)
                                                :double (java.util.Arrays/copyOf ^doubles buf actual-rows)
                                                :string (java.util.Arrays/copyOf ^longs buf actual-rows))
                                         entry (case t
                                                 :long   {:type :int64   :data data}
                                                 :double {:type :float64 :data data}
                                                 :string (let [^HashMap dm (:dict-map cs)
                                                               ^longs nid (:next-id cs)
                                                               n (aget nid 0)
                                                               rd (make-array String n)]
                                                           (doseq [^java.util.Map$Entry e (.entrySet dm)]
                                                             (when-let [k (.getKey e)]
                                                               (aset ^"[Ljava.lang.String;" rd
                                                                     (int (long (.getValue e))) k)))
                                                           {:type :int64
                                                            :data data
                                                            :dict rd
                                                            :dict-type :string
                                                            :dict-alpha-masks (ColumnOpsString/buildDictAlphaMasks rd)
                                                            :dict-bigram-masks (ColumnOpsString/buildDictBigramMasks rd)}))]
                                     [(keyword col-name) entry])))
                            (range ncols))
              ds-name (or name (str "parquet:" (.getName (java.io.File. ^String path))))]
          (dataset/make-dataset col-map
                                {:name ds-name
                                 :metadata {:source-path path
                                            :source-type :parquet}})))
      (finally
        (.close reader)))))

;; ============================================================================
;; parquet-dataset — zero-copy / lazy-decode parquet reader
;; ============================================================================
;;
;; Open a parquet file in constant time, return a read-only StratumDataset
;; whose columns are parquet-backed PSS trees. Each row group becomes one
;; chunk; chunks decode their data lazily on first SIMD-path / materialize
;; access. Stats from parquet metadata feed our zone-map pruning directly.
;;
;; No konserve, no ingest. The parquet file is the storage.

(def ^:private cleaner
  "Shared Cleaner that closes ParquetFileReaders when their owning dataset
   is no longer reachable."
  (delay (Cleaner/create)))

(defn- parquet-type->stratum-type
  "Map a parquet column to stratum :int64 / :float64 / :string. Mirrors
   classify-column above, returning stratum's idx-datatype keywords."
  [^ColumnDescriptor cd]
  (let [pt (.getPrimitiveType cd)]
    (case (str (.getPrimitiveTypeName pt))
      "INT64"  :int64
      "INT32"  :int64
      "INT96"  :int64
      "BOOLEAN" :int64
      "DOUBLE" :float64
      "FLOAT"  :float64
      "BINARY" :string
      "FIXED_LEN_BYTE_ARRAY" :string)))

(defn- find-column-meta
  "Find the ColumnChunkMetaData for `cd` in `block`."
  ^ColumnChunkMetaData [^BlockMetaData block ^ColumnDescriptor cd]
  (let [path (str/join "." (.getPath cd))]
    (or (some (fn [^ColumnChunkMetaData c]
                (when (= path (.toDotString (.getPath c)))
                  c))
              (.getColumns block))
        (throw (ex-info "Column metadata not found in row group"
                        {:path path
                         :rg-cols (mapv #(.toDotString (.getPath ^ColumnChunkMetaData %))
                                        (.getColumns block))})))))

(defn- statistics->chunk-stats
  "Map a parquet `Statistics` (per-row-group/per-column) to stratum
   ChunkStats. parquet doesn't store sum/sum-sq, so those are 0.0 — the
   stats-only SUM/AVG fast path won't fire on parquet-backed columns,
   but min/max/count/null-count drive zone-map pruning fully.

   For string columns, we don't yet know the per-row-group dict-id range
   (the global dict is built later). Caller updates the stats with
   string-stats-from-dict-translate once the translation table exists."
  ^stratum.stats.ChunkStats [^Statistics s ^long n-values datatype]
  (let [null-count (.getNumNulls s)
        nominal-count (- n-values null-count)
        [mn mx] (cond
                  (not (.hasNonNullValue s))
                  [Double/NaN Double/NaN]

                  (= :int64 datatype)
                  [(double (.longValue ^Number (.genericGetMin s)))
                   (double (.longValue ^Number (.genericGetMax s)))]

                  (= :float64 datatype)
                  [(double (.doubleValue ^Number (.genericGetMin s)))
                   (double (.doubleValue ^Number (.genericGetMax s)))]

                  :else
                  [Double/NaN Double/NaN])]
    (stats/->ChunkStats nominal-count 0.0 0.0 mn mx null-count)))

(defn- string-rg-min-max
  "Compute min/max global-dict-id appearing in the row-group `rg-idx`
   from the per-row-group translation table — the smallest and largest
   global ids the row group can produce. Returns [min max] as doubles,
   or [NaN NaN] if the row group has no dict (plain encoding)."
  [translates rg-idx]
  (if-let [^longs t (when translates (nth translates rg-idx))]
    (let [n (alength t)]
      (if (zero? n)
        [Double/NaN Double/NaN]
        (loop [i 1
               mn (aget t 0)
               mx (aget t 0)]
          (if (>= i n)
            [(double mn) (double mx)]
            (let [v (aget t i)]
              (recur (inc i)
                     (if (< v mn) v mn)
                     (if (> v mx) v mx)))))))
    [Double/NaN Double/NaN]))

;; ----------------------------------------------------------------------------
;; String column support — global dict + per-row-group translation
;; ----------------------------------------------------------------------------
;;
;; Stratum's query engine expects string columns as long-encoded dict IDs
;; against a single global String[] per column. Parquet, by contrast,
;; stores a separate dictionary per row group. To bridge the two cleanly:
;;
;;   1. At parquet-dataset open time, walk every row group's dict page for
;;      this column (small; no row data read), merge entries into a global
;;      HashMap<String,Long>. Cost = sum of distinct values across row
;;      groups, typically tiny for low-cardinality columns.
;;
;;   2. Build a per-row-group `int[] localId → long globalId` translation
;;      table while merging.
;;
;;   3. At chunk decode time, the column reader emits local-dict IDs
;;      (parquet's `getCurrentValueDictionaryID()`) — we look them up in
;;      the row group's translation table to produce a long[] of global
;;      IDs. For pages encoded plainly (no dict), fall back to looking up
;;      the binary in the global dict.

(defn- read-rg-dict
  "Read the dictionary page for `cd` in row group `rg-idx` of `reader`,
   returning a vector of `String`s in local-dict ID order. Returns nil
   when the row group has no dict page (e.g. all-PLAIN encoding)."
  [^ParquetFileReader reader rg-idx ^ColumnDescriptor cd]
  (let [^DictionaryPageReadStore drs (.getDictionaryReader reader (int rg-idx))
        dp (when drs (.readDictionaryPage drs cd))]
    (when dp
      (let [^Dictionary dict (.initDictionary (.getEncoding dp) cd dp)
            n (inc (.getMaxId dict))
            out (object-array n)]
        (dotimes [i n]
          (aset out i (.toStringUsingUTF8 ^Binary (.decodeToBinary dict (int i)))))
        (vec out)))))

(defn- build-string-global-dict
  "Walk every row group's dict page for `cd`, merging into a single global
   `String → long` map. Returns:
     {:dict     ^objects globalIdx → String
      :translates [translate-table-per-row-group]
                 each translate-table is a long[] (local-dict-id → global-id)
                 or nil if the row group has no dict page (caller falls back).}

   `dict-cap` is the maximum allowed global dict size; throws when crossed
   (per the Ryan-style high-cardinality OOM guard)."
  [^ParquetFileReader reader ^ColumnDescriptor cd ^long n-row-groups dict-cap]
  (let [global ^HashMap (HashMap.)
        translates (object-array n-row-groups)]
    (dotimes [rg-idx n-row-groups]
      (when-let [rg-dict (read-rg-dict reader rg-idx cd)]
        (let [n (count rg-dict)
              tab (long-array n)]
          (dotimes [i n]
            (let [s ^String (nth rg-dict i)
                  existing (.get global s)
                  gid (if existing
                        (long existing)
                        (let [new-id (.size global)]
                          (when (and dict-cap (>= new-id (long dict-cap)))
                            (throw (ex-info
                                    "parquet-dataset string dict cap exceeded — column has too many distinct values"
                                    {:column (last (.getPath cd))
                                     :dict-cap dict-cap})))
                          (.put global s (long new-id))
                          new-id))]
              (aset ^longs tab i (long gid))))
          (aset translates rg-idx tab))))
    (let [n (.size global)
          arr (make-array String n)]
      (doseq [^java.util.Map$Entry e (.entrySet global)]
        (aset ^"[Ljava.lang.String;" arr
              (int (long (.getValue e))) ^String (.getKey e)))
      {:dict arr
       :global-map global             ; for plain-encoded fallbacks
       :translates (vec translates)})))

(defn- decode-row-group-string
  "Decode a single string column from a single row group into a long[]
   of global-dict IDs. When the row group is dict-encoded we use the
   translation table for zero-string-allocation; for plain pages we look
   up the binary in the global map. Long.MIN_VALUE marks NULL."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd
   translate ^HashMap global-map length]
  (let [rg-index (int rg-index)
        length (int length)
        full-schema (.getSchema (.getFileMetaData (.getFooter reader)))
        col-name (last (.getPath cd))
        proj-field (some (fn [^Type f] (when (= col-name (.getName f)) f))
                         (.getFields ^MessageType full-schema))
        proj-schema (MessageType. (.getName ^MessageType full-schema)
                                  ^"[Lorg.apache.parquet.schema.Type;"
                                  (into-array Type [proj-field]))
        ^longs out (long-array length)
        _ (java.util.Arrays/fill out Long/MIN_VALUE)
        cursor (int-array 1)
        pages (.readRowGroup reader rg-index)
        column-io (.getColumnIO (ColumnIOFactory.) proj-schema)
        ^longs translate-arr translate
        conv (proxy [PrimitiveConverter] []
               ;; Opt into dict-aware decoding. Parquet calls setDictionary
               ;; once per dict-encoded page; we ignore the per-page
               ;; Dictionary because our translation table maps local ids
               ;; to global ids precomputed at open time.
               (hasDictionarySupport [] (some? translate-arr))
               (setDictionary [_dict] nil)
               (addValueFromDictionary [local-id]
                 (aset out (aget cursor 0) (aget translate-arr (int local-id))))
               (addBinary [^Binary v]
                 (let [s (.toStringUsingUTF8 v)
                       gid (.get global-map s)]
                   (aset out (aget cursor 0)
                         (if gid (long gid) Long/MIN_VALUE)))))
        gc (proxy [GroupConverter] []
             (getConverter [_] conv)
             (start [])
             (end [] (aset cursor 0 (inc (aget cursor 0)))))
        rm (proxy [RecordMaterializer] []
             (getRootConverter [] gc)
             (getCurrentRecord [] nil))
        rr (.getRecordReader ^MessageColumnIO column-io pages rm)]
    (dotimes [_ length] (.read rr))
    out))

(defn- noop-primitive-converter
  "PrimitiveConverter required by ColumnReadStoreImpl's path-walk; we
   bypass it by reading values directly off the ColumnReader, so the
   converter is never invoked."
  ^PrimitiveConverter []
  (proxy [PrimitiveConverter] []))

(defn- noop-group-converter
  "Single-column root GroupConverter that returns a no-op primitive
   converter for any field index. Plumbing required by ColumnReadStoreImpl
   even though the decoder loop never calls back through it."
  ^GroupConverter []
  (let [pc (noop-primitive-converter)]
    (proxy [GroupConverter] []
      (getConverter [_] pc)
      (start [])
      (end []))))

(defn- decode-row-group-numeric
  "Decode a single column from a single row group into a long[] or
   double[] sized to the row group. One allocation, one decode pass.

   Uses parquet's lower-level ColumnReader directly, bypassing the
   RecordReader+proxy callback path (which dispatches a Clojure proxy
   per value plus a GroupConverter.end() per row). For flat primitive
   columns (no rep/def levels beyond NULL/non-NULL), the inner loop is
   `consume + getLong/getInteger/getDouble + aset` — no virtual call
   per value beyond the (already-required) ValuesReader."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd datatype length]
  (let [rg-index (int rg-index)
        length (int length)
        full-schema (.getSchema (.getFileMetaData (.getFooter reader)))
        col-name (last (.getPath cd))
        proj-field (some (fn [^Type f] (when (= col-name (.getName f)) f))
                         (.getFields ^MessageType full-schema))
        _ (when (nil? proj-field)
            (throw (ex-info "Projected field not found"
                            {:col col-name
                             :have (mapv #(.getName ^Type %) (.getFields ^MessageType full-schema))})))
        proj-schema (MessageType. (.getName ^MessageType full-schema)
                                  ^"[Lorg.apache.parquet.schema.Type;"
                                  (into-array Type [proj-field]))
        ptn (str (.getPrimitiveTypeName (.getPrimitiveType cd)))
        max-def (.getMaxDefinitionLevel cd)
        ^PageReadStore pages (.readRowGroup reader rg-index)
        created-by (.getCreatedBy (.getFileMetaData (.getFooter reader)))
        rs (ColumnReadStoreImpl. pages (noop-group-converter) proj-schema created-by)
        ^ColumnReader cr (.getColumnReader rs cd)
        n (long length)]
    (case datatype
      :int64
      (let [^longs out (long-array length)
            int96? (= "INT96" ptn)
            int? (= "INT32" ptn)
            bool? (= "BOOLEAN" ptn)]
        (cond
          int96?
          (do (java.util.Arrays/fill out Long/MIN_VALUE)
              (loop [i 0]
                (when (< i n)
                  (when (= max-def (.getCurrentDefinitionLevel cr))
                    (aset out i (long (int96->epoch-millis (.getBinary cr)))))
                  (.consume cr)
                  (recur (unchecked-inc i)))))
          int?
          (do (java.util.Arrays/fill out Long/MIN_VALUE)
              (loop [i 0]
                (when (< i n)
                  (when (= max-def (.getCurrentDefinitionLevel cr))
                    (aset out i (long (.getInteger cr))))
                  (.consume cr)
                  (recur (unchecked-inc i)))))
          bool?
          (do (java.util.Arrays/fill out Long/MIN_VALUE)
              (loop [i 0]
                (when (< i n)
                  (when (= max-def (.getCurrentDefinitionLevel cr))
                    (aset out i (if (.getBoolean cr) 1 0)))
                  (.consume cr)
                  (recur (unchecked-inc i)))))
          :else                              ; INT64
          (do (java.util.Arrays/fill out Long/MIN_VALUE)
              (loop [i 0]
                (when (< i n)
                  (when (= max-def (.getCurrentDefinitionLevel cr))
                    (aset out i (.getLong cr)))
                  (.consume cr)
                  (recur (unchecked-inc i))))))
        out)

      :float64
      (let [^doubles out (double-array length)
            float? (= "FLOAT" ptn)]
        (java.util.Arrays/fill out Double/NaN)
        (if float?
          (loop [i 0]
            (when (< i n)
              (when (= max-def (.getCurrentDefinitionLevel cr))
                (aset out i (double (.getFloat cr))))
              (.consume cr)
              (recur (unchecked-inc i))))
          (loop [i 0]
            (when (< i n)
              (when (= max-def (.getCurrentDefinitionLevel cr))
                (aset out i (.getDouble cr)))
              (.consume cr)
              (recur (unchecked-inc i)))))
        out))))

(deftype ParquetRowGroupChunk
         [;; lifeline keeps the underlying ParquetFileReader strongly
          ;; reachable. The Cleaner action that closes the reader is
          ;; registered on this same lifeline object — as long as any
          ;; chunk references it, the reader stays open. ds also
          ;; references it, so closing happens only when ds AND all
          ;; chunks are unreachable.
          lifeline
          ^ParquetFileReader reader
          ^long rg-index
          ^ColumnDescriptor col-desc
          datatype                      ; :int64 | :float64 | :string
          ^long length
          translate                     ; long[] (string cols) or nil
          global-map                    ; HashMap (string fallback) or nil
          ^:volatile-mutable data       ; nil until first decode; long[] | double[]
          ^:volatile-mutable lock]

  chunk/IColChunk
  (chunk-length [_] length)
  ;; Stratum's :string source-type is presented through the column map's
  ;; :dict; the underlying chunk is long-encoded, so we report :int64 to
  ;; satisfy long-vs-double dispatch in the SIMD path. Numeric types map
  ;; through unchanged.
  (chunk-datatype [_]
    (if (= :string datatype) :int64 datatype))

  (read-value [this idx]
    (if (= :float64 datatype)
      (aget ^doubles (chunk/chunk-as-doubles this) (int idx))
      (aget ^longs (chunk/chunk-as-longs this) (int idx))))
  (read-long [this idx]
    (aget ^longs (chunk/chunk-as-longs this) (int idx)))
  (read-double [this idx]
    (if (= :float64 datatype)
      (aget ^doubles (chunk/chunk-as-doubles this) (int idx))
      (double (aget ^longs (chunk/chunk-as-longs this) (int idx)))))

  (chunk-data [this]
    (if (= :float64 datatype)
      (chunk/chunk-as-doubles this)
      (chunk/chunk-as-longs this)))

  (chunk-as-longs [_]
    (or data
        (locking lock
          (or data
              (let [arr (case datatype
                          :string  (decode-row-group-string reader rg-index col-desc
                                                            translate global-map length)
                          (decode-row-group-numeric reader rg-index col-desc datatype length))]
                (set! data arr)
                arr)))))

  (chunk-as-doubles [_]
    (or data
        (locking lock
          (or data
              (let [arr (decode-row-group-numeric reader rg-index col-desc datatype length)]
                (set! data arr)
                arr)))))

  (chunk-constant? [_] false)
  (chunk-constant-val [_] nil)

  chunk/IColChunkMut
  (write-value! [_ _ _]
    (throw (UnsupportedOperationException. "ParquetRowGroupChunk is read-only")))
  (write-double! [_ _ _]
    (throw (UnsupportedOperationException. "ParquetRowGroupChunk is read-only")))
  (write-long! [_ _ _]
    (throw (UnsupportedOperationException. "ParquetRowGroupChunk is read-only")))

  chunk/IColChunkPersistence
  (col-fork [this] this)              ; immutable, sharing is safe
  (col-transient [_]
    (throw (UnsupportedOperationException. "ParquetRowGroupChunk is read-only")))
  (col-persistent! [this] this)
  (col-transient? [_] false))

(defn- build-parquet-column-index
  "Build a stratum PCI for a single parquet column. One ChunkEntry per
   row group, lazy-decoded. PSS tree is in-memory (no storage).

   `lifeline` is shared across all chunks of all columns from one file —
   the Cleaner action that closes the reader is bound to its
   reachability.

   `dict-info` is nil for numeric columns; for strings it is the map
   produced by `build-string-global-dict` (translates per row group +
   global fallback map)."
  [lifeline ^ParquetFileReader reader ^ColumnDescriptor cd dict-info]
  (let [datatype (parquet-type->stratum-type cd)
        ;; Stratum's downstream wiring expects string columns as
        ;; long-encoded indices into a dict; the chunk reports :int64
        ;; via `chunk-datatype`, but the PCI we build needs :int64 too
        ;; so the query engine treats it as a long column.
        idx-datatype (if (= :string datatype) :int64 datatype)
        footer (.getFooter reader)
        blocks (vec (.getBlocks footer))
        translates (:translates dict-info)
        global-map (:global-map dict-info)
        cmp idx/chunk-entry-comparator
        empty-tree (pss/sorted-set*
                    {:cmp cmp
                     :measure idx/chunk-entry-measure-ops})
        entries (mapv (fn [^long rg-idx ^BlockMetaData block]
                        (let [col-meta (find-column-meta block cd)
                              n (.getValueCount col-meta)
                              base-stats (statistics->chunk-stats
                                          (.getStatistics col-meta) n datatype)
                              translate (when translates (nth translates rg-idx))
                              ;; For strings: parquet has Binary min/max; we
                              ;; want dict-id min/max (so zone-map and dense
                              ;; group-by sizing both work). Compute from the
                              ;; per-row-group translation table.
                              cstats (if (= :string datatype)
                                       (let [[mn mx] (string-rg-min-max translates rg-idx)]
                                         (assoc base-stats :min-val mn :max-val mx))
                                       base-stats)
                              chunk (->ParquetRowGroupChunk
                                     lifeline reader rg-idx cd datatype n
                                     translate global-map nil (Object.))]
                          (idx/->ChunkEntry [rg-idx] chunk cstats)))
                      (range (count blocks))
                      blocks)
        tree (reduce conj empty-tree entries)
        total (long (reduce + (map #(.getRowCount ^BlockMetaData %) blocks)))]
    (idx/->PersistentColumnIndex
     tree total idx-datatype idx/DEFAULT_CHUNK_SIZE cmp
     nil                             ; edit (persistent)
     (atom #{})                      ; dirty-chunks
     {:source :parquet}              ; metadata
     (count blocks)                  ; next-chunk-id
     nil)))                          ; storage (no konserve)

(defn parquet-dataset
  "Open a parquet file as a read-only, zero-copy StratumDataset.

   Constant-time open: reads only file footer metadata (typically a few
   ms for a multi-GB file). Each parquet row group becomes one chunk in
   the stratum index; chunks lazy-decode on first SIMD-path or
   materialize. Per-row-group min/max/count/null-count from parquet
   metadata feed stratum's zone-map pruning, so chunks the planner can
   prove irrelevant are never decoded.

   No konserve, no persistence — the parquet file is the storage. The
   returned dataset is read-only: idx-set!/idx-append!/idx-sync! all
   throw.

   Lifecycle: a Cleaner action is registered to close the underlying
   ParquetFileReader when the dataset becomes unreachable. For long-lived
   processes you can invoke `close-parquet-dataset!` explicitly.

   Options:
     :columns - vector of column names to expose (default: all)
     :name    - dataset name (default: derived from filename)"
  [^String path & {:keys [columns name dict-cap]
                   :or {dict-cap (long 50000000)}}]
  (let [reader (ParquetFileReader/open
                (LocalInputFile. (Paths/get path (make-array String 0))))
        ;; lifeline is the cleanup-anchor object. Both the dataset and
        ;; every ParquetRowGroupChunk reference it strongly. The Cleaner
        ;; action runs only when ALL of those references go away — so
        ;; chunks that outlive the dataset (held via column refs) keep
        ;; the reader open until they are themselves unreachable.
        lifeline (Object.)
        footer (.getFooter reader)
        schema (.getSchema (.getFileMetaData footer))
        n-row-groups (count (.getBlocks footer))
        all-cols (vec (.getColumns schema))
        proj-cols (if columns
                    (let [col-set (set columns)]
                      (vec (filter #(col-set (last (.getPath ^ColumnDescriptor %))) all-cols)))
                    all-cols)
        col-map (into {}
                      (map (fn [^ColumnDescriptor cd]
                             (let [col-name (last (.getPath cd))
                                   datatype (parquet-type->stratum-type cd)
                                   dict-info (when (= :string datatype)
                                               (build-string-global-dict
                                                reader cd n-row-groups dict-cap))
                                   pci (build-parquet-column-index
                                        lifeline reader cd dict-info)
                                   col-info (if (= :string datatype)
                                              ;; String column wired as
                                              ;; long-encoded dict ID
                                              ;; against global :dict.
                                              {:type :int64 :source :index
                                               :dict (:dict dict-info)
                                               :dict-type :string
                                               :dict-alpha-masks
                                               (ColumnOpsString/buildDictAlphaMasks
                                                ^"[Ljava.lang.String;" (:dict dict-info))
                                               :dict-bigram-masks
                                               (ColumnOpsString/buildDictBigramMasks
                                                ^"[Ljava.lang.String;" (:dict dict-info))
                                               :stats-sum-incomplete? true
                                               :index pci}
                                              {:type datatype :source :index
                                               :stats-sum-incomplete? true
                                               :index pci})]
                               [(keyword col-name) col-info])))
                      proj-cols)
        ds-name (or name (str "parquet:" (.getName (java.io.File. path))))
        ds (dataset/make-dataset col-map
                                 {:name ds-name
                                  :metadata {:source-path path
                                             :source-type :parquet
                                             ::reader reader
                                             ::lifeline lifeline}})]
    ;; Cleaner watches `lifeline`. As long as any chunk or `ds` holds it,
    ;; the reader stays open. Closes only after the entire chain drops.
    (.register ^Cleaner @cleaner lifeline
               (reify Runnable
                 (run [_]
                   (try (.close reader) (catch Exception _ nil)))))
    ds))

(defn close-parquet-dataset!
  "Explicitly close the parquet file backing a parquet-dataset.
   After this call, accessing column data throws."
  [ds]
  (when-let [reader (some-> ds meta :metadata ::reader)]
    (.close ^ParquetFileReader reader)))
