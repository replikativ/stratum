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
           [org.apache.parquet.column.page PageReadStore PageReader DataPage DataPageV1 DataPageV2 DictionaryPageReadStore]
           [org.apache.parquet.column.statistics Statistics]
           [org.apache.parquet.hadoop.metadata BlockMetaData ColumnChunkMetaData]
           [org.apache.parquet.io ColumnIOFactory MessageColumnIO]
           [org.apache.parquet.io.api RecordMaterializer GroupConverter PrimitiveConverter Binary]
           [org.apache.parquet.schema MessageType Type GroupType]
           [org.apache.parquet.schema PrimitiveType PrimitiveType$PrimitiveTypeName]
           [org.apache.parquet.schema LogicalTypeAnnotation
            LogicalTypeAnnotation$StringLogicalTypeAnnotation
            LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
            LogicalTypeAnnotation$DateLogicalTypeAnnotation
            LogicalTypeAnnotation$TimeLogicalTypeAnnotation
            LogicalTypeAnnotation$IntLogicalTypeAnnotation
            LogicalTypeAnnotation$EnumLogicalTypeAnnotation
            LogicalTypeAnnotation$JsonLogicalTypeAnnotation
            LogicalTypeAnnotation$UUIDLogicalTypeAnnotation
            LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
            LogicalTypeAnnotation$TimeUnit]
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

;; ----------------------------------------------------------------------------
;; Column type classification — physical type + logical-type annotation
;; ----------------------------------------------------------------------------
;;
;; Stratum's column type model has three kinds: :int64, :float64, :string.
;; Parquet's primitive types times its logical-type annotations cover much
;; more ground (Decimal, Date, Time, Timestamp[unit], UUID, …). We support
;; the subset that maps cleanly onto our model — values are converted at
;; decode time and at stats-mapping time so that downstream queries
;; receive uniform values regardless of how the column was written.
;;
;; `classify-parquet-column` returns either:
;;   {:datatype :int64|:float64|:string
;;    :decoder-kind :int32-raw|:int64-raw|:int32-date|:ts-millis|:ts-micros|
;;                  :ts-nanos|:int96-millis|:boolean|:double|:float|
;;                  :binary-utf8|:flba-utf8|:flba-uuid
;;    :stats-scale  long-multiplier-applied-to-int-min/max  (1 for default)
;;    :stats-divide long-divisor-applied-to-int-min/max     (1 for default)
;;    :uuid?        true for UUID columns (FLBA[16] → 36-char hex string)}
;;
;; Or, for unsupported columns:
;;   {:reject "human-readable reason"}

(defn- classify-parquet-column
  [^ColumnDescriptor cd]
  (let [pt (.getPrimitiveType cd)
        ptn (str (.getPrimitiveTypeName pt))
        annotation (.getLogicalTypeAnnotation pt)
        col-name (last (.getPath cd))
        max-rep (.getMaxRepetitionLevel cd)]
    (cond
      ;; Repeated columns (LIST elements / MAP entries / nested arrays).
      ;; max-repetition-level > 0 means the column is inside a repeated
      ;; group; flat scalar reads of these would yield wrong row counts.
      (pos? max-rep)
      {:reject (str "Repeated/nested column '" col-name
                    "' (LIST/MAP/STRUCT) not supported by parquet-dataset. "
                    "Use index-parquet! or project to a flat schema.")}

      (instance? LogicalTypeAnnotation$DecimalLogicalTypeAnnotation annotation)
      {:reject (str "Decimal column '" col-name
                    "' not supported (stratum has no decimal type). "
                    "Project to DOUBLE in your writer or use index-parquet!.")}

      :else
      (case ptn
        "BOOLEAN" {:datatype :int64 :decoder-kind :boolean}
        "DOUBLE"  {:datatype :float64 :decoder-kind :double}
        "FLOAT"   {:datatype :float64 :decoder-kind :float}

        "INT32"
        (cond
          (or (nil? annotation)
              (instance? LogicalTypeAnnotation$IntLogicalTypeAnnotation annotation))
          {:datatype :int64 :decoder-kind :int32-raw}

          (instance? LogicalTypeAnnotation$DateLogicalTypeAnnotation annotation)
          ;; days-since-1970-01-01 → epoch millis
          {:datatype :int64 :decoder-kind :int32-date :stats-scale 86400000}

          (instance? LogicalTypeAnnotation$TimeLogicalTypeAnnotation annotation)
          ;; INT32 time is always TimeUnit.MILLIS — keep raw millis-of-day
          {:datatype :int64 :decoder-kind :int32-raw}

          :else
          {:reject (str "INT32 column '" col-name "' has unsupported "
                        "logical-type annotation: " annotation)})

        "INT64"
        (cond
          (or (nil? annotation)
              (instance? LogicalTypeAnnotation$IntLogicalTypeAnnotation annotation))
          {:datatype :int64 :decoder-kind :int64-raw}

          (instance? LogicalTypeAnnotation$TimestampLogicalTypeAnnotation annotation)
          (let [unit (.getUnit ^LogicalTypeAnnotation$TimestampLogicalTypeAnnotation annotation)]
            (condp = unit
              LogicalTypeAnnotation$TimeUnit/MILLIS
              {:datatype :int64 :decoder-kind :ts-millis}

              LogicalTypeAnnotation$TimeUnit/MICROS
              {:datatype :int64 :decoder-kind :ts-micros :stats-divide 1000}

              LogicalTypeAnnotation$TimeUnit/NANOS
              {:datatype :int64 :decoder-kind :ts-nanos :stats-divide 1000000}))

          (instance? LogicalTypeAnnotation$TimeLogicalTypeAnnotation annotation)
          ;; Time MICROS / NANOS — keep raw, user interprets
          {:datatype :int64 :decoder-kind :int64-raw}

          :else
          {:reject (str "INT64 column '" col-name "' has unsupported "
                        "logical-type annotation: " annotation)})

        "INT96"
        ;; Legacy timestamp. parquet stores it as 12-byte tuples; we
        ;; convert each value to epoch-millis on decode. parquet stats
        ;; for INT96 use BinaryStatistics with undefined sort order, so
        ;; the stats-mapper skips min/max for INT96 columns.
        {:datatype :int64 :decoder-kind :int96-millis :stats-skip-min-max? true}

        "BINARY"
        (cond
          (or (nil? annotation)
              (instance? LogicalTypeAnnotation$StringLogicalTypeAnnotation annotation)
              (instance? LogicalTypeAnnotation$EnumLogicalTypeAnnotation annotation)
              (instance? LogicalTypeAnnotation$JsonLogicalTypeAnnotation annotation))
          {:datatype :string :decoder-kind :binary-utf8}

          :else
          {:reject (str "BINARY column '" col-name "' has unsupported "
                        "logical-type annotation: " annotation)})

        "FIXED_LEN_BYTE_ARRAY"
        (cond
          (instance? LogicalTypeAnnotation$StringLogicalTypeAnnotation annotation)
          {:datatype :string :decoder-kind :flba-utf8}

          (instance? LogicalTypeAnnotation$UUIDLogicalTypeAnnotation annotation)
          {:datatype :string :decoder-kind :flba-uuid :uuid? true}

          :else
          {:reject (str "FIXED_LEN_BYTE_ARRAY column '" col-name
                        "' has unsupported logical-type annotation: "
                        annotation
                        " — Float16, Interval, Decimal, Geometry, and bare"
                        " FLBA are not yet supported.")})

        ;; Unknown physical type
        {:reject (str "Unknown parquet primitive type for column '"
                      col-name "': " ptn)}))))

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

   `spec` is the column spec from `classify-parquet-column`. We honor
   `:stats-skip-min-max?` (e.g. INT96 has BinaryStatistics + undefined
   sort order), `:stats-scale` (Date: ×86_400_000), and `:stats-divide`
   (TimestampMicros: ÷1000, TimestampNanos: ÷1_000_000).

   For string columns the per-row-group dict-id range isn't known yet
   (global dict built later). Caller overrides min/max via
   `string-rg-min-max` once the translation table exists."
  ^stratum.stats.ChunkStats [^Statistics s ^long n-values spec]
  (let [datatype (:datatype spec)
        null-count (.getNumNulls s)
        nominal-count (- n-values null-count)
        scale (long (or (:stats-scale spec) 1))
        divide (long (or (:stats-divide spec) 1))
        ;; INT96 stats: BinaryStatistics whose .genericGetMin returns
        ;; Binary, not Number. Skip — INT96 sort order is undefined per
        ;; the parquet spec, so writers shouldn't rely on these stats
        ;; anyway. Values fall through to NaN, disabling zone-map
        ;; pruning for the column (correctness > pruning).
        [mn mx]
        (cond
          (not (.hasNonNullValue s))
          [Double/NaN Double/NaN]

          (:stats-skip-min-max? spec)
          [Double/NaN Double/NaN]

          (= :int64 datatype)
          (try
            (let [raw-mn (long (.longValue ^Number (.genericGetMin s)))
                  raw-mx (long (.longValue ^Number (.genericGetMax s)))]
              [(double (quot (* raw-mn scale) divide))
               (double (quot (* raw-mx scale) divide))])
            (catch ClassCastException _
              ;; Defensive — some writers emit Binary stats for INT96
              ;; despite the spec, and other surprises are possible.
              [Double/NaN Double/NaN]))

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

(defn- binary->hex
  "Render a 16-byte FLBA UUID into the canonical 36-char dashed form."
  ^String [^Binary b]
  (let [bytes (.getBytes b)
        sb (StringBuilder. 36)
        hex "0123456789abcdef"]
    (dotimes [i 16]
      (when (or (= i 4) (= i 6) (= i 8) (= i 10))
        (.append sb \-))
      (let [v (bit-and (aget ^bytes bytes (int i)) 0xff)]
        (.append sb (.charAt ^String hex (bit-shift-right v 4)))
        (.append sb (.charAt ^String hex (bit-and v 0x0f)))))
    (.toString sb)))

(defn- decode-binary-by-spec
  "Convert a parquet Binary value to the canonical String for the column,
   per its decoder-kind: UTF-8 for normal binary/FLBA; 36-char hex for
   UUID FLBA[16]."
  ^String [^Binary b decoder-kind]
  (case decoder-kind
    :flba-uuid (binary->hex b)
    (.toStringUsingUTF8 b)))

(defn- read-rg-dict
  "Read the dictionary page for `cd` in row group `rg-idx` of `reader`,
   returning a vector of `String`s in local-dict ID order. Returns nil
   when the row group has no dict page (e.g. all-PLAIN encoding).
   `decoder-kind` selects the binary→string conversion (UTF-8 vs UUID)."
  [^ParquetFileReader reader rg-idx ^ColumnDescriptor cd decoder-kind]
  (let [^DictionaryPageReadStore drs (.getDictionaryReader reader (int rg-idx))
        dp (when drs (.readDictionaryPage drs cd))]
    (when dp
      (let [^Dictionary dict (.initDictionary (.getEncoding dp) cd dp)
            n (inc (.getMaxId dict))
            out (object-array n)]
        (dotimes [i n]
          (aset out i (decode-binary-by-spec
                       ^Binary (.decodeToBinary dict (int i))
                       decoder-kind)))
        (vec out)))))

(defn- intern-string!
  "Intern `s` into the global `String → long` map. Returns the global
   id. Throws when the cap would be exceeded."
  ^long [^HashMap global ^long cap col-name s]
  (let [existing (.get global s)]
    (if existing
      (long existing)
      (let [new-id (.size global)]
        (when (>= new-id cap)
          (throw (ex-info
                  "parquet-dataset string dict cap exceeded — column has too many distinct values"
                  {:column col-name
                   :dict-cap cap})))
        (.put global s (long new-id))
        new-id))))

(declare read-rg-projected decode-row-group-numeric-fallback)

(defn- scan-rg-plain-strings!
  "For a row group whose data pages aren't dict-encoded, eagerly walk
   every value via parquet's RecordReader and intern each Binary
   (decoded by `decoder-kind`) into the global map. Returns nil — the
   row group has no translation table; later `decode-row-group-string`
   takes the addBinary hot path against the same global map.

   This keeps strings correct for columns parquet wrote PLAIN (random
   UUIDs, high-cardinality text) at the cost of one full column scan
   at open time. Bounded by `dict-cap`; throws on overflow."
  [^ParquetFileReader reader rg-idx ^ColumnDescriptor cd
   ^HashMap global cap col-name decoder-kind]
  (let [full-schema (.getSchema (.getFileMetaData (.getFooter reader)))
        proj-field (some (fn [^Type f] (when (= col-name (.getName f)) f))
                         (.getFields ^MessageType full-schema))
        proj-schema (MessageType. (.getName ^MessageType full-schema)
                                  ^"[Lorg.apache.parquet.schema.Type;"
                                  (into-array Type [proj-field]))
        pages (.readRowGroup reader (int rg-idx))
        column-io (.getColumnIO (ColumnIOFactory.) proj-schema)
        n-values (long (.getRowCount ^PageReadStore pages))
        conv (proxy [PrimitiveConverter] []
               (addBinary [^Binary v]
                 (intern-string! global cap col-name
                                 (decode-binary-by-spec v decoder-kind))))
        gc (proxy [GroupConverter] []
             (getConverter [_] conv)
             (start [])
             (end []))
        rm (proxy [RecordMaterializer] []
             (getRootConverter [] gc)
             (getCurrentRecord [] nil))
        rr (.getRecordReader ^MessageColumnIO column-io pages rm)]
    (dotimes [_ n-values] (.read rr))
    nil))

(defn- build-string-global-dict
  "Walk every row group's dict page for `cd`, merging into a single
   global `String → long` map. For row groups whose data pages aren't
   dict-encoded (parquet writes UUIDs and other random/high-cardinality
   strings as PLAIN), eagerly scan that row group's values so the
   global dict is correct.

   Returns:
     {:global-map  HashMap<String,Long>  (live)
      :translates  vec of long[] (local→global) or nil per row group
      :dict-cap    long (capacity ceiling; respected throughout)}

   `dict-cap` is the maximum allowed global dict size; throws when
   crossed (avoids high-cardinality OOM)."
  [^ParquetFileReader reader ^ColumnDescriptor cd n-row-groups
   decoder-kind dict-cap]
  (let [global ^HashMap (HashMap.)
        translates (object-array n-row-groups)
        cap (long (or dict-cap Long/MAX_VALUE))
        col-name (last (.getPath cd))]
    (dotimes [rg-idx n-row-groups]
      (if-let [rg-dict (read-rg-dict reader rg-idx cd decoder-kind)]
        (let [n (count rg-dict)
              tab (long-array n)]
          (dotimes [i n]
            (let [gid (intern-string! global cap col-name ^String (nth rg-dict i))]
              (aset ^longs tab i gid)))
          (aset translates rg-idx tab))
        ;; No dict page for this row group — eagerly scan values.
        (scan-rg-plain-strings! reader rg-idx cd global cap col-name
                                decoder-kind)))
    {:dict-cap cap
     :global-map global
     :translates (vec translates)}))

(defn- finalize-global-dict
  "Snapshot the live `global-map` HashMap into a String[] sized to the
   final dict size. Called after all decode-row-group-string fallbacks
   have had a chance to add plain-encoded values."
  ^"[Ljava.lang.String;" [^HashMap global]
  (let [n (.size global)
        arr (make-array String n)]
    (doseq [^java.util.Map$Entry e (.entrySet global)]
      (aset ^"[Ljava.lang.String;" arr
            (int (long (.getValue e))) ^String (.getKey e)))
    arr))

(defn- decode-row-group-string-impl
  "Inner body of `decode-row-group-string`; runs under the caller's
   lifeline lock so concurrent decodes on the same reader serialize.
   Projects parquet's row-group read down to just `cd` first so we
   only fetch this column's bytes from disk."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd
   translate ^HashMap global-map dict-cap decoder-kind length]
  (let [rg-index (int rg-index)
        length (int length)
        col-name (last (.getPath cd))
        [^PageReadStore pages proj-schema] (read-rg-projected reader cd rg-index)
        ^longs out (long-array length)
        _ (java.util.Arrays/fill out Long/MIN_VALUE)
        cursor (int-array 1)
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
                 ;; Plain-encoded fallback. The string may be present
                 ;; in the global dict already (also seen in another
                 ;; row group's dict page) or new — insert if new,
                 ;; honoring the dict cap.
                 (let [s (decode-binary-by-spec v decoder-kind)
                       existing (.get global-map s)
                       gid (if existing
                             (long existing)
                             (let [new-id (.size global-map)
                                   cap (long dict-cap)]
                               (when (>= new-id cap)
                                 (throw (ex-info
                                         "parquet-dataset string dict cap exceeded during plain-page decode — column has too many distinct values"
                                         {:column col-name
                                          :dict-cap cap})))
                               (.put global-map s (long new-id))
                               new-id))]
                   (aset out (aget cursor 0) gid))))
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

(defn- decode-row-group-string
  "Public wrapper: lock on the shared dataset's `lifeline` so concurrent
   chunks decoding different columns from the same reader serialize
   their `setRequestedSchema` + `readRowGroup` calls."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd
   translate ^HashMap global-map dict-cap decoder-kind length lifeline]
  (locking lifeline
    (decode-row-group-string-impl
     reader rg-index cd translate global-map dict-cap decoder-kind length)))

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

(defn- read-rg-projected
  "Read a single row group via parquet-mr's `readRowGroup`, but project
   the requested schema down to just `cd` first so parquet only loads
   that column's bytes from disk. The `setRequestedSchema` call mutates
   the reader's state, so callers must serialize concurrent decodes
   against the same reader (we use `(locking lifeline ...)`).

   Returns [PageReadStore proj-schema]."
  [^ParquetFileReader reader ^ColumnDescriptor cd rg-index]
  (let [full-schema (.getSchema (.getFileMetaData (.getFooter reader)))
        col-name (last (.getPath cd))
        proj-field (some (fn [^Type f] (when (= col-name (.getName f)) f))
                         (.getFields ^MessageType full-schema))
        _ (when (nil? proj-field)
            (throw (ex-info "Projected field not found"
                            {:col col-name
                             :have (mapv #(.getName ^Type %) (.getFields ^MessageType full-schema))})))
        proj-schema (MessageType. (.getName ^MessageType full-schema)
                                  ^"[Lorg.apache.parquet.schema.Type;"
                                  (into-array Type [proj-field]))]
    (.setRequestedSchema reader proj-schema)
    [(.readRowGroup reader (int rg-index)) proj-schema]))

(defn- bulk-decode-pages!
  "Bulk-decode every PLAIN page in `pr` into `out[off..off+pr-rows)`.
   `kind` selects how to copy each page's bytes:
     :long   — `ByteBuffer.asLongBuffer().get(out, off, n)`
     :double — `asDoubleBuffer().get(out, off, n)`
     :int    — read as INT32 into a scratch int[], widen to long[]
   Returns the next offset (long). Returns -1 if any page uses an
   encoding we can't bulk-decode — caller falls back to ColumnReader."
  [^PageReader pr out kind off]
  (let [v-count (.getTotalValueCount pr)
        cursor (long-array 1)
        _ (aset cursor 0 (long off))
        end (+ (long off) v-count)
        scratch (atom nil)
        bail? (atom false)]
    (loop []
      (when (and (not @bail?) (< (aget cursor 0) end))
        (let [page (.readPage pr)]
          (when page
            (let [n (.getValueCount page)
                  enc (str (.getValueEncoding ^DataPageV1 page))]
              (if (= "PLAIN" enc)
                (let [bb (.toByteBuffer (.getBytes ^DataPageV1 page))
                      o (int (aget cursor 0))]
                  (.order bb ByteOrder/LITTLE_ENDIAN)
                  (case kind
                    :long   (.get (.asLongBuffer bb) ^longs out o (int n))
                    :double (.get (.asDoubleBuffer bb) ^doubles out o (int n))
                    :int    (let [buf (or @scratch
                                          (let [a (int-array n)] (reset! scratch a) a))
                                  buf (if (>= (alength ^ints buf) n)
                                        buf
                                        (let [a (int-array n)] (reset! scratch a) a))]
                              (.get (.asIntBuffer bb) ^ints buf 0 (int n))
                              (dotimes [i n]
                                (aset ^longs out (int (+ o i))
                                      (long (aget ^ints buf i))))))
                  (aset cursor 0 (+ (aget cursor 0) (long n))))
                (reset! bail? true))))
          (recur))))
    (if @bail? -1 (aget cursor 0))))

(defn- decode-row-group-numeric
  "Decode a single column from a single row group into a long[] or
   double[] sized to the row group. One allocation, one decode pass.

   Two paths:
   - **Bulk** for required (max-def=0, max-rep=0) columns whose pages
     are all PLAIN-encoded INT32 / INT64 / DOUBLE — the page bytes are
     contiguous fixed-width values, so we slice each page's
     `ByteBuffer` into a `LongBuffer` / `DoubleBuffer` and call
     `.get(out, off, n)` for one JNI memcpy per page. ~10× the
     ColumnReader path on the Ryan-shape fixture.
   - **Fallback** via parquet's lower-level `ColumnReader` for pages
     using DICT, DELTA, BYTE_STREAM_SPLIT, or for nullable columns.
     Inner loop is `consume + get* + aset` — no proxy callback.

   `setRequestedSchema` projects parquet's row-group read down to just
   the chunk's column, so reading one column doesn't pull in the other
   22. This is the largest win on the Ryan-shape fixture (3 s → 50 ms
   per row group). The reader is shared across chunks, so we serialize
   concurrent decodes via `locking` on `lifeline`."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd spec length lifeline]
  (let [rg-index (int rg-index)
        length (int length)
        datatype (:datatype spec)
        decoder-kind (:decoder-kind spec)
        max-def (.getMaxDefinitionLevel cd)
        max-rep (.getMaxRepetitionLevel cd)
        bulk-eligible? (and (zero? max-def) (zero? max-rep)
                            (#{:int64-raw :ts-millis :double :int32-raw} decoder-kind))]
    (if bulk-eligible?
      ;; Bulk path: one synchronized read of the projected row group,
      ;; bulk-decode each page directly from its ByteBuffer.
      (locking lifeline
        (let [[^PageReadStore pages _] (read-rg-projected reader cd rg-index)
              ^PageReader pr (.getPageReader pages cd)]
          (case decoder-kind
            (:int64-raw :ts-millis)
            (let [out (long-array length)
                  end (bulk-decode-pages! pr out :long 0)]
              (if (neg? (long end))
                (decode-row-group-numeric-fallback
                 reader rg-index cd spec length lifeline)
                out))
            :int32-raw
            (let [out (long-array length)
                  end (bulk-decode-pages! pr out :int 0)]
              (if (neg? (long end))
                (decode-row-group-numeric-fallback
                 reader rg-index cd spec length lifeline)
                out))
            :double
            (let [out (double-array length)
                  end (bulk-decode-pages! pr out :double 0)]
              (if (neg? (long end))
                (decode-row-group-numeric-fallback
                 reader rg-index cd spec length lifeline)
                out)))))
      (decode-row-group-numeric-fallback
       reader rg-index cd spec length lifeline))))

(defn- decode-row-group-numeric-fallback
  "ColumnReader-based decoder used for pages that aren't bulk-eligible
   (DICT-encoded numerics, nullable columns, INT96 timestamps, Date,
   Timestamp[Micros|Nanos], boolean, float). Same per-value loop as
   before — `consume + get* + aset` — but now also takes the lifeline
   lock and projects the row-group read to just the chunk's column."
  [^ParquetFileReader reader rg-index ^ColumnDescriptor cd spec length lifeline]
  (locking lifeline
    (let [rg-index (int rg-index)
          length (int length)
          datatype (:datatype spec)
          decoder-kind (:decoder-kind spec)
          [^PageReadStore pages proj-schema] (read-rg-projected reader cd rg-index)
          max-def (.getMaxDefinitionLevel cd)
          created-by (.getCreatedBy (.getFileMetaData (.getFooter reader)))
          rs (ColumnReadStoreImpl. pages (noop-group-converter) proj-schema created-by)
          ^ColumnReader cr (.getColumnReader rs cd)
          n (long length)]
      (case datatype
        :int64
        (let [^longs out (long-array length)]
          (java.util.Arrays/fill out Long/MIN_VALUE)
          (case decoder-kind
            :int96-millis
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (int96->epoch-millis (.getBinary cr))))
                (.consume cr)
                (recur (unchecked-inc i))))

            :int32-raw
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (long (.getInteger cr))))
                (.consume cr)
                (recur (unchecked-inc i))))

            :int32-date
          ;; days-since-epoch → epoch-millis
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (* (long (.getInteger cr)) 86400000)))
                (.consume cr)
                (recur (unchecked-inc i))))

            :boolean
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (if (.getBoolean cr) 1 0)))
                (.consume cr)
                (recur (unchecked-inc i))))

            :int64-raw
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (.getLong cr)))
                (.consume cr)
                (recur (unchecked-inc i))))

            :ts-millis
          ;; INT64 timestamp, already millis — direct copy
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (.getLong cr)))
                (.consume cr)
                (recur (unchecked-inc i))))

            :ts-micros
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (quot (.getLong cr) 1000)))
                (.consume cr)
                (recur (unchecked-inc i))))

            :ts-nanos
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (quot (.getLong cr) 1000000)))
                (.consume cr)
                (recur (unchecked-inc i)))))
          out)

        :float64
        (let [^doubles out (double-array length)]
          (java.util.Arrays/fill out Double/NaN)
          (case decoder-kind
            :float
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (double (.getFloat cr))))
                (.consume cr)
                (recur (unchecked-inc i))))
            :double
            (loop [i 0]
              (when (< i n)
                (when (= max-def (.getCurrentDefinitionLevel cr))
                  (aset out i (.getDouble cr)))
                (.consume cr)
                (recur (unchecked-inc i)))))
          out)))))

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
          spec                          ; classify-parquet-column result
          ^long length
          translate                     ; long[] (string cols) or nil
          global-map                    ; HashMap (string fallback) or nil
          ^long dict-cap                ; long; cap on global-map size during decode
          ^:volatile-mutable data       ; nil until first decode; long[] | double[]
          ^:volatile-mutable lock]

  chunk/IColChunk
  (chunk-length [_] length)
  ;; Stratum's :string columns are long-encoded dict IDs at the chunk
  ;; level (the column map carries the global :dict). Report :int64 so
  ;; long-vs-double dispatch in the SIMD path picks the long path.
  (chunk-datatype [_]
    (if (= :string (:datatype spec)) :int64 (:datatype spec)))

  (read-value [this idx]
    (if (= :float64 (:datatype spec))
      (aget ^doubles (chunk/chunk-as-doubles this) (int idx))
      (aget ^longs (chunk/chunk-as-longs this) (int idx))))
  (read-long [this idx]
    (aget ^longs (chunk/chunk-as-longs this) (int idx)))
  (read-double [this idx]
    (if (= :float64 (:datatype spec))
      (aget ^doubles (chunk/chunk-as-doubles this) (int idx))
      (double (aget ^longs (chunk/chunk-as-longs this) (int idx)))))

  (chunk-data [this]
    (if (= :float64 (:datatype spec))
      (chunk/chunk-as-doubles this)
      (chunk/chunk-as-longs this)))

  (chunk-as-longs [_]
    (or data
        (locking lock
          (or data
              (let [arr (case (:datatype spec)
                          :string  (decode-row-group-string
                                    reader rg-index col-desc translate global-map
                                    dict-cap (:decoder-kind spec) length lifeline)
                          (decode-row-group-numeric
                           reader rg-index col-desc spec length lifeline))]
                (set! data arr)
                arr)))))

  (chunk-as-doubles [_]
    (or data
        (locking lock
          (or data
              (let [arr (decode-row-group-numeric reader rg-index col-desc spec length lifeline)]
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

   `spec` is the column spec from `classify-parquet-column`. `dict-info`
   is nil for numeric columns; for strings it is the map from
   `build-string-global-dict`."
  [lifeline ^ParquetFileReader reader ^ColumnDescriptor cd spec dict-info]
  (let [datatype (:datatype spec)
        idx-datatype (if (= :string datatype) :int64 datatype)
        footer (.getFooter reader)
        blocks (vec (.getBlocks footer))
        translates (:translates dict-info)
        global-map (:global-map dict-info)
        dict-cap (long (or (:dict-cap dict-info) Long/MAX_VALUE))
        cmp idx/chunk-entry-comparator
        empty-tree (pss/sorted-set*
                    {:cmp cmp
                     :measure idx/chunk-entry-measure-ops})
        entries (mapv (fn [^long rg-idx ^BlockMetaData block]
                        (let [col-meta (find-column-meta block cd)
                              n (.getValueCount col-meta)
                              base-stats (statistics->chunk-stats
                                          (.getStatistics col-meta) n spec)
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
                                     lifeline reader rg-idx cd spec n
                                     translate global-map dict-cap nil (Object.))]
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
        success? (volatile! false)]
    (try
      (let [;; lifeline is the cleanup-anchor object. Both the dataset
            ;; and every ParquetRowGroupChunk reference it strongly. The
            ;; Cleaner action runs only when ALL of those references go
            ;; away — so chunks that outlive the dataset (held via
            ;; column refs) keep the reader open until they are
            ;; themselves unreachable.
            lifeline (Object.)
            footer (.getFooter reader)
            schema (.getSchema (.getFileMetaData footer))
            n-row-groups (count (.getBlocks footer))
            all-cols (vec (.getColumns schema))
            proj-cols (if columns
                        (let [col-set (set columns)]
                          (vec (filter #(col-set (last (.getPath ^ColumnDescriptor %))) all-cols)))
                        all-cols)
            ;; Step 1: classify every column. If any is unsupported,
            ;; throw before we allocate any chunk state — the
            ;; surrounding try/finally closes the reader.
            specs (mapv (fn [^ColumnDescriptor cd]
                          [cd (classify-parquet-column cd)])
                        proj-cols)
            _ (when-let [bad (some (fn [[^ColumnDescriptor cd s]]
                                     (when (:reject s)
                                       [(last (.getPath cd)) (:reject s)]))
                                   specs)]
                (throw (ex-info (str "parquet-dataset: " (second bad))
                                {:column (first bad)
                                 :reason (second bad)})))
            col-map (into {}
                          (map (fn [[^ColumnDescriptor cd spec]]
                                 (let [col-name (last (.getPath cd))
                                       datatype (:datatype spec)
                                       dict-info (when (= :string datatype)
                                                   (build-string-global-dict
                                                    reader cd n-row-groups
                                                    (:decoder-kind spec) dict-cap))
                                       pci (build-parquet-column-index
                                            lifeline reader cd spec dict-info)
                                       col-info
                                       (if (= :string datatype)
                                         ;; String column: dict is built
                                         ;; from row-group dict pages now;
                                         ;; on first plain-page decode it
                                         ;; can grow further. We snapshot
                                         ;; the dict eagerly for the
                                         ;; alpha/bigram masks; if a
                                         ;; later plain-page fallback adds
                                         ;; entries, queries against
                                         ;; LIKE-fast-paths will see only
                                         ;; the eager snapshot. (LIKE
                                         ;; correctness is preserved by
                                         ;; the SIMD fallback in the
                                         ;; query engine; only the
                                         ;; mask-based pruning is
                                         ;; affected.)
                                         (let [dict (finalize-global-dict
                                                     ^HashMap (:global-map dict-info))]
                                           {:type :int64 :source :index
                                            :dict dict
                                            :dict-type :string
                                            :dict-alpha-masks
                                            (ColumnOpsString/buildDictAlphaMasks dict)
                                            :dict-bigram-masks
                                            (ColumnOpsString/buildDictBigramMasks dict)
                                            :stats-sum-incomplete? true
                                            :index pci})
                                         {:type datatype :source :index
                                          :stats-sum-incomplete? true
                                          :index pci})]
                                   [(keyword col-name) col-info])))
                          specs)
            ds-name (or name (str "parquet:" (.getName (java.io.File. path))))
            ds (dataset/make-dataset col-map
                                     {:name ds-name
                                      :metadata {:source-path path
                                                 :source-type :parquet
                                                 ::reader reader
                                                 ::lifeline lifeline}})]
        ;; Cleaner watches `lifeline`. As long as any chunk or `ds`
        ;; holds it, the reader stays open. Closes only after the
        ;; entire chain drops.
        (.register ^Cleaner @cleaner lifeline
                   (reify Runnable
                     (run [_]
                       (try (.close reader) (catch Exception _ nil)))))
        (vreset! success? true)
        ds)
      (finally
        (when-not @success?
          ;; Open succeeded but classification or dict-build threw —
          ;; close the reader to avoid leaking an OS file handle.
          (try (.close reader) (catch Exception _ nil)))))))

(defn close-parquet-dataset!
  "Explicitly close the parquet file backing a parquet-dataset.
   After this call, any subsequent decode attempt throws IOException
   (\"Stream Closed\").

   Lifecycle is otherwise managed by a Cleaner registered on a shared
   lifeline; closing happens automatically when the dataset and all
   chunks become unreachable. Use this only when you need deterministic
   release (e.g. tests, file rotation)."
  [ds]
  (when-let [reader (some-> ds dataset/metadata ::reader)]
    (.close ^ParquetFileReader reader)))
