(ns stratum.parquet
  "Parquet import for Stratum.

   Two ingest paths:

     (from-parquet \"data/orders.parquet\")
       In-memory: builds heap arrays and returns a non-persistent
       StratumDataset. Suitable for small-to-medium files used
       programmatically. Memory footprint scales with file size.

     (index-parquet! \"data/orders.parquet\" store branch)
       Streaming: reads the file row-group-by-row-group, fills
       chunk-sized buffers per column, flushes chunks to konserve
       via idx-append-chunk!, and periodically syncs to free heap.
       Returns a synced, index-backed StratumDataset. Memory is
       bounded — independent of file size.

   No Hadoop runtime required — uses LocalInputFile for direct file access."
  (:require [clojure.core.async :as async]
            [stratum.dataset :as dataset]
            [stratum.index :as idx]
            [stratum.chunk :as chunk]
            [stratum.cached-storage :as cstorage]
            [stratum.storage :as storage])
  (:import [org.apache.parquet.hadoop ParquetFileReader]
           [org.apache.parquet.io LocalInputFile]
           [org.apache.parquet.column.page PageReadStore]
           [org.apache.parquet.io ColumnIOFactory MessageColumnIO]
           [org.apache.parquet.io.api RecordMaterializer GroupConverter PrimitiveConverter Binary]
           [org.apache.parquet.schema MessageType Type GroupType]
           [org.apache.parquet.schema PrimitiveType PrimitiveType$PrimitiveTypeName]
           [org.apache.parquet.schema LogicalTypeAnnotation LogicalTypeAnnotation$StringLogicalTypeAnnotation
            LogicalTypeAnnotation$TimestampLogicalTypeAnnotation]
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
