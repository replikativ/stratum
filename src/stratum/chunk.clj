(ns stratum.chunk
  "PersistentColChunk - A copy-on-write columnar chunk backed by Java heap arrays.

   Design inspired by:
   - Arrow Buffer: parent-child relationships, device awareness
   - Arrow SparseTensor CSR: separate index + data buffers
   - Proximum PersistentEdgeStore: chunk-based CoW, transient/persistent modes

   A ColChunk stores a contiguous array of primitive values with:
   - O(1) random access
   - O(1) fork (structural sharing)
   - O(chunk) copy-on-write on first mutation after fork

   Supports constant-value compression: when all values in a chunk are identical
   (detected via min==max in ChunkStats), the chunk stores a single value and
   lazily expands to a full array on first data access. This saves disk I/O
   (8 bytes vs 64KB per chunk) and memory for never-accessed constant chunks."
  (:import [clojure.lang IDeref IObj IMeta]
           [java.util Arrays]
           [java.nio ByteBuffer ByteOrder]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocols
;; ============================================================================

(defprotocol IColChunk
  "Protocol for columnar chunks with CoW semantics"
  (chunk-length [this] "Return the number of elements in this chunk")
  (chunk-datatype [this] "Return the element datatype (:float64, :int64, etc.)")
  (read-value [this idx] "Read value at index (boxed)")
  (read-double [this idx] "Read as double (unboxed)")
  (read-long [this idx] "Read as long (unboxed)")
  (chunk-data [this] "Return the underlying array (long[] or double[])")
  (chunk-as-longs [this] "Return the underlying array as ^longs")
  (chunk-as-doubles [this] "Return the underlying array as ^doubles")
  (chunk-constant? [this] "Return true if this is a constant-value chunk (may not have expanded data)")
  (chunk-constant-val [this] "Return the constant value (Number) if constant, nil otherwise"))

(defprotocol IColChunkMut
  "Mutation protocol - only available in transient mode"
  (write-value! [this idx val] "Write value at index")
  (write-double! [this idx val] "Write double at index")
  (write-long! [this idx val] "Write long at index"))

(defprotocol IColChunkPersistence
  "Persistence/versioning protocol"
  (col-fork [this] "Create a structural copy that shares data until modified")
  (col-transient [this] "Switch to transient (mutable) mode")
  (col-persistent! [this] "Switch to persistent (immutable) mode")
  (col-transient? [this] "Check if in transient mode"))

;; ============================================================================
;; Implementation
;; ============================================================================

(defn- expand-constant!
  "Expand a constant chunk's data lazily. Called when data is nil and constant-val is set.
   Thread-safe: worst case two threads both expand, no corruption."
  [datatype ^long length constant-val]
  (case datatype
    :float64 (let [a (double-array length)]
               (Arrays/fill a (double constant-val))
               a)
    :int64 (let [a (long-array length)]
             (Arrays/fill a (long constant-val))
             a)))

(deftype PersistentColChunk
         [^:volatile-mutable data            ; The primitive array (long[] or double[]), nil for unexpanded constant
          ^:volatile-mutable ^long length    ; Logical number of valid elements (mutable for growth)
          ^:volatile-mutable ^long capacity  ; Array capacity (>= length, mutable for growth)
          datatype                           ; Element type (:float64, :int64, etc.)
          ^:volatile-mutable edit            ; nil = persistent, non-nil = transient
          ^:volatile-mutable dirty           ; true if copied (no longer sharing)
          ^:volatile-mutable parent          ; Parent array we forked from (for CoW tracking), cleared after CoW
          ^:volatile-mutable constant-val    ; nil = regular chunk, Number = constant value (lazy expand)
          metadata]

  IColChunk
  (chunk-length [_] length)
  (chunk-datatype [_] datatype)

  (read-value [this idx]
    (if (and (nil? data) constant-val)
      constant-val
      (case datatype
        :float64 (aget ^doubles data idx)
        :int64 (aget ^longs data idx))))

  (read-double [this idx]
    (if (and (nil? data) constant-val)
      (double constant-val)
      (case datatype
        :float64 (aget ^doubles data idx)
        :int64 (double (aget ^longs data idx)))))

  (read-long [this idx]
    (if (and (nil? data) constant-val)
      (long constant-val)
      (case datatype
        :int64 (aget ^longs data idx)
        :float64 (long (aget ^doubles data idx)))))

  (chunk-data [_]
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype length constant-val)]
        (set! data arr)
        (set! capacity length)))
    data)

  (chunk-as-longs [_]
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype length constant-val)]
        (set! data arr)
        (set! capacity length)))
    data)

  (chunk-as-doubles [_]
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype length constant-val)]
        (set! data arr)
        (set! capacity length)))
    data)

  (chunk-constant? [_]
    (some? constant-val))

  (chunk-constant-val [_]
    constant-val)

  IColChunkMut
  (write-value! [this idx val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent chunk. Call col-transient first.")))

    ;; Step 0: Expand constant chunk if needed
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype (max length capacity) constant-val)]
        (set! data arr)
        (set! dirty true)
        (set! constant-val nil)))

    ;; Step 1: CoW - copy from parent if needed (before any mutation)
    (when (and parent (not dirty))
      (let [new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int capacity))
                       :int64 (Arrays/copyOf ^longs data (int capacity)))]
        (set! data new-data)
        (set! dirty true)
        (set! parent nil)))

    ;; Step 2: Growth - expand array if writing beyond capacity (transient mode)
    (when (>= idx capacity)
      (let [new-capacity (long (max (inc idx) (* 2 capacity)))
            new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int new-capacity))
                       :int64 (Arrays/copyOf ^longs data (int new-capacity)))]
        (set! data new-data)
        (set! capacity new-capacity)
        (set! dirty true)))

    (case datatype
      :float64 (aset ^doubles data (int idx) (double val))
      :int64 (aset ^longs data (int idx) (long val)))
    ;; Update length if we wrote beyond current length
    (when (>= idx length)
      (set! length (long (inc idx))))
    this)

  (write-double! [this idx val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent chunk. Call col-transient first.")))

    ;; Step 0: Expand constant chunk if needed
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype (max length capacity) constant-val)]
        (set! data arr)
        (set! dirty true)
        (set! constant-val nil)))

    ;; Step 1: CoW - copy from parent if needed (before any mutation)
    (when (and parent (not dirty))
      (let [new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int capacity))
                       :int64 (Arrays/copyOf ^longs data (int capacity)))]
        (set! data new-data)
        (set! dirty true)
        (set! parent nil)))

    ;; Step 2: Growth - expand array if writing beyond capacity (transient mode)
    (when (>= idx capacity)
      (let [new-capacity (long (max (inc idx) (* 2 capacity)))
            new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int new-capacity))
                       :int64 (Arrays/copyOf ^longs data (int new-capacity)))]
        (set! data new-data)
        (set! capacity new-capacity)
        (set! dirty true)))

    (case datatype
      :float64 (aset ^doubles data (int idx) (double val))
      :int64 (aset ^longs data (int idx) (long (double val))))
    ;; Update length if we wrote beyond current length
    (when (>= idx length)
      (set! length (long (inc idx))))
    this)

  (write-long! [this idx val]
    (when-not edit
      (throw (IllegalStateException. "Cannot mutate persistent chunk. Call col-transient first.")))

    ;; Step 0: Expand constant chunk if needed
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype (max length capacity) constant-val)]
        (set! data arr)
        (set! dirty true)
        (set! constant-val nil)))

    ;; Step 1: CoW - copy from parent if needed (before any mutation)
    (when (and parent (not dirty))
      (let [new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int capacity))
                       :int64 (Arrays/copyOf ^longs data (int capacity)))]
        (set! data new-data)
        (set! dirty true)
        (set! parent nil)))

    ;; Step 2: Growth - expand array if writing beyond capacity (transient mode)
    (when (>= idx capacity)
      (let [new-capacity (long (max (inc idx) (* 2 capacity)))
            new-data (case datatype
                       :float64 (Arrays/copyOf ^doubles data (int new-capacity))
                       :int64 (Arrays/copyOf ^longs data (int new-capacity)))]
        (set! data new-data)
        (set! capacity new-capacity)
        (set! dirty true)))

    (case datatype
      :int64 (aset ^longs data (int idx) (long val))
      :float64 (aset ^doubles data (int idx) (double (long val))))
    ;; Update length if we wrote beyond current length
    (when (>= idx length)
      (set! length (long (inc idx))))
    this)

  IColChunkPersistence
  (col-fork [_]
    ;; Create a new chunk with a defensive copy of the data array.
    ;; This guarantees isolation: mutating the original (via transient) cannot
    ;; affect the fork, and vice versa. Cost: O(chunk-size) = ~64KB copy.
    (if constant-val
      ;; Constant chunk: share constant-val, no data to copy
      (PersistentColChunk. data length (if data capacity length) datatype nil false
                           (when data data) constant-val metadata)
      ;; Regular chunk: copy data array for full isolation
      (let [copied (case datatype
                     :float64 (java.util.Arrays/copyOf ^doubles data (int capacity))
                     :int64 (java.util.Arrays/copyOf ^longs data (int capacity)))]
        (PersistentColChunk. copied length capacity datatype nil false
                             nil nil metadata))))

  (col-transient [this]
    (if edit
      (throw (IllegalStateException. "Already transient"))
      (do
        (set! edit (Object.))
        this)))

  (col-persistent! [this]
    (if-not edit
      (throw (IllegalStateException. "Already persistent"))
      (do
        ;; Compact: shrink array to exact size if we have extra capacity
        ;; This ensures persistent chunks are space-efficient for columnar ops
        (when (and data (< length capacity))
          (let [new-data (case datatype
                           :float64 (Arrays/copyOf ^doubles data (int length))
                           :int64 (Arrays/copyOf ^longs data (int length)))]
            (set! data new-data)
            (set! capacity length)))
        (set! edit nil)
        (set! parent nil)
        this)))

  (col-transient? [_]
    (boolean edit))

  ;; Standard Clojure protocols
  IDeref
  (deref [_]
    (when (and (nil? data) constant-val)
      (let [arr (expand-constant! datatype length constant-val)]
        (set! data arr)
        (set! capacity length)))
    data)

  IMeta
  (meta [_] metadata)

  IObj
  (withMeta [_ m]
    (PersistentColChunk. data length capacity datatype edit dirty parent constant-val m))

  Object
  (toString [_]
    (if constant-val
      (format "#ColChunk[%s x %d, constant=%s]"
              (name datatype) length constant-val)
      (format "#ColChunk[%s x %d, %s]"
              (name datatype) length
              (if edit "transient" "persistent")))))

;; ============================================================================
;; Constructors
;; ============================================================================

(defn make-chunk
  "Create a new ColChunk with the given datatype and length.

   Example:
     (make-chunk :float64 1000)
     (make-chunk :int64 1000)"
  ([datatype ^long length]
   (make-chunk datatype length {}))
  ([datatype ^long length _opts]
   (let [arr (case datatype
               :float64 (double-array length)
               :int64 (long-array length))]
     ;; Initial capacity = length (exact size)
     (PersistentColChunk. arr length length datatype nil false nil nil nil))))

(defn make-constant-chunk
  "Create a constant-value ColChunk. Data is lazily expanded on first access.
   Saves memory for chunks where all values are identical."
  [datatype ^long length constant-value]
  (PersistentColChunk. nil length length datatype nil false nil constant-value nil))

(defn chunk-from-seq
  "Create a ColChunk from a sequence of values.

   Example:
     (chunk-from-seq :float64 [1.0 2.0 3.0 4.0])"
  [datatype coll]
  (let [data (vec coll)
        length (count data)
        chunk (-> (make-chunk datatype length)
                  (col-transient))]
    (dotimes [i length]
      (write-value! chunk i (nth data i)))
    (col-persistent! chunk)))

(defn chunk-from-array
  "Create a ColChunk from a primitive array (copies the array).

   Example:
     (chunk-from-array (double-array [1.0 2.0 3.0]))"
  [arr]
  (let [is-long (instance? (Class/forName "[J") arr)
        is-double (instance? (Class/forName "[D") arr)
        datatype (cond is-long :int64
                       is-double :float64
                       :else (throw (ex-info "Unsupported array type" {:type (type arr)})))
        length (int (if is-long (alength ^longs arr) (alength ^doubles arr)))
        new-arr (if is-long
                  (Arrays/copyOf ^longs arr length)
                  (Arrays/copyOf ^doubles arr length))]
    (PersistentColChunk. new-arr length length datatype nil false nil nil nil)))

;; ============================================================================
;; Utility functions
;; ============================================================================

(defn chunk->array
  "Copy chunk contents to a primitive array."
  [chunk]
  (let [arr (chunk-data chunk)
        len (chunk-length chunk)
        dt (chunk-datatype chunk)]
    (case dt
      :float64 (Arrays/copyOf ^doubles arr (int len))
      :int64 (Arrays/copyOf ^longs arr (int len)))))

(defn chunk->vec
  "Copy chunk contents to a Clojure vector."
  [chunk]
  (let [arr (chunk-data chunk)
        len (chunk-length chunk)]
    (case (chunk-datatype chunk)
      :float64 (let [^doubles a arr] (into [] (map #(aget a (int %))) (range len)))
      :int64 (let [^longs a arr] (into [] (map #(aget a (int %))) (range len))))))

(defn chunk-slice
  "Create a copy of a portion of the chunk.
   Returns a new chunk with copied data."
  [chunk ^long start ^long end]
  (let [dt (chunk-datatype chunk)
        len (- end start)
        new-arr (case dt
                  :float64 (Arrays/copyOfRange ^doubles (chunk-data chunk) (int start) (int end))
                  :int64 (Arrays/copyOfRange ^longs (chunk-data chunk) (int start) (int end)))]
    (PersistentColChunk. new-arr len len dt nil false nil nil nil)))

;; ============================================================================
;; Serialization (for storage persistence)
;; ============================================================================

(defn chunk-to-bytes
  "Serialize a chunk to bytes for storage.
   Returns a map {:datatype :float64/:int64, :length N, :encoding :raw/:constant, :data byte-array}.

   When stats are provided and min-val == max-val, uses constant encoding (8 bytes
   instead of 8*N bytes). This is the same detection as DuckDB's COMPRESSION_CONSTANT."
  ([chunk] (chunk-to-bytes chunk nil))
  ([chunk stats]
   (let [dt (chunk-datatype chunk)
         len (chunk-length chunk)
         ;; Detect constant from stats (min==max) or from chunk's own constant-val
         cv (chunk-constant-val chunk)
         constant? (or (some? cv)
                       (and stats
                            (> len 0)
                            (case dt
                              ;; For int64, compare as longs to avoid precision loss near 2^53
                              :int64 (= (long (:min-val stats)) (long (:max-val stats)))
                              ;; For float64, use double comparison (exact for same-value floats)
                              :float64 (and (not (Double/isNaN (:min-val stats)))
                                            (== (double (:min-val stats)) (double (:max-val stats)))))))]
     (if constant?
       ;; Constant encoding: just 8 bytes for the single value
       (let [val (or cv
                     (case dt
                       :float64 (:min-val stats)
                       :int64 (long (:min-val stats))))
             bb (ByteBuffer/allocate 8)]
         (.order bb ByteOrder/LITTLE_ENDIAN)
         (case dt
           :float64 (.putDouble bb (double val))
           :int64 (.putLong bb (long val)))
         {:datatype dt :length len :encoding :constant :data (.array bb)})

       ;; Raw encoding: 8 bytes per element
       (let [arr (chunk-data chunk)]
         (case dt
           :float64
           (let [^doubles darr arr
                 bb (ByteBuffer/allocate (* len 8))]
             (.order bb ByteOrder/LITTLE_ENDIAN)
             (dotimes [i len]
               (.putDouble bb (aget darr i)))
             {:datatype dt :length len :encoding :raw :data (.array bb)})

           :int64
           (let [^longs larr arr
                 bb (ByteBuffer/allocate (* len 8))]
             (.order bb ByteOrder/LITTLE_ENDIAN)
             (dotimes [i len]
               (.putLong bb (aget larr i)))
             {:datatype dt :length len :encoding :raw :data (.array bb)})))))))

(defn chunk-from-bytes
  "Deserialize a chunk from bytes.
   Input: {:datatype :float64/:int64, :length N, :encoding :raw/:constant, :data byte-array}.
   Constant chunks are created with lazy expansion (data=nil until first access)."
  [chunk-bytes]
  (when-not (map? chunk-bytes)
    (throw (IllegalArgumentException.
            (str "chunk-from-bytes expects a map, got: " (type chunk-bytes)))))
  (let [{:keys [datatype ^long length ^bytes data]} chunk-bytes
        encoding (or (:encoding chunk-bytes) :raw)]
    (case encoding
      :constant
      (let [bb (ByteBuffer/wrap data)
            _ (.order bb ByteOrder/LITTLE_ENDIAN)
            val (case datatype
                  :float64 (.getDouble bb)
                  :int64 (.getLong bb))]
        ;; Lazy: data=nil, constant-val set, expands on first chunk-data call
        (make-constant-chunk datatype length val))

      :raw
      (case datatype
        :float64
        (let [bb (ByteBuffer/wrap data)
              _ (.order bb ByteOrder/LITTLE_ENDIAN)
              arr (double-array length)]
          (dotimes [i length]
            (aset arr i (.getDouble bb)))
          (PersistentColChunk. arr length length datatype nil false nil nil nil))

        :int64
        (let [bb (ByteBuffer/wrap data)
              _ (.order bb ByteOrder/LITTLE_ENDIAN)
              arr (long-array length)]
          (dotimes [i length]
            (aset arr i (.getLong bb)))
          (PersistentColChunk. arr length length datatype nil false nil nil nil))

        ;; Default case with helpful error
        (throw (IllegalArgumentException.
                (str "Unknown datatype in chunk-from-bytes: " datatype
                     " (expected :float64 or :int64)"))))

      ;; Unknown encoding
      (throw (IllegalArgumentException.
              (str "Unknown encoding in chunk-from-bytes: " encoding
                   " (expected :raw or :constant)"))))))

(comment
  ;; Quick REPL test
  (def c (make-chunk :float64 10))
  (-> c col-transient)
  (write-double! c 0 42.0)
  (write-double! c 1 3.14)
  (-> c col-persistent!)
  (read-double c 0) ;; => 42.0
  (read-double c 1) ;; => 3.14

  ;; Fork and modify
  (def c2 (col-fork c))
  (-> c2 col-transient)
  (write-double! c2 0 99.0)
  (-> c2 col-persistent!)
  (read-double c 0)  ;; => 42.0 (original unchanged)
  (read-double c2 0) ;; => 99.0 (fork modified)

  ;; Constant chunk
  (def cc (make-constant-chunk :int64 8192 42))
  (chunk-constant? cc) ;; => true
  (chunk-constant-val cc) ;; => 42
  (read-long cc 0) ;; => 42 (no expansion)
  (chunk-data cc) ;; expands lazily
  )
