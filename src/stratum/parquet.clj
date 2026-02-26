(ns stratum.parquet
  "Parquet import for Stratum.

   Reads Parquet files into StratumDataset using parquet-java.
   No Hadoop runtime required — uses LocalInputFile for direct file access.

   Usage:
     (from-parquet \"data/orders.parquet\")
     (from-parquet \"data/orders.parquet\" :columns [\"price\" \"qty\"] :limit 10000)"
  (:require [stratum.query :as q]
            [stratum.dataset :as dataset])
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
           [java.util ArrayList]))

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

(defn- make-converter
  "Create a PrimitiveConverter that appends values to an ArrayList.
   Sets seen[col-idx] = true when a value is added (for NULL tracking)."
  ^PrimitiveConverter [^ArrayList list col-type ^booleans seen col-idx is-int96]
  (let [c (int col-idx)
        s seen]
    (case col-type
      :long
      (if is-int96
        (proxy [PrimitiveConverter] []
          (addBinary [^Binary v]
            (aset s c true)
            (.add list (Long/valueOf (int96->epoch-millis v)))))
        (proxy [PrimitiveConverter] []
          (addLong [v] (aset s c true) (.add list (Long/valueOf (long v))))
          (addInt [v] (aset s c true) (.add list (Long/valueOf (long (int v)))))
          (addBoolean [v] (aset s c true) (.add list (Long/valueOf (if (boolean v) 1 0))))
          (addBinary [^Binary v] (aset s c true) (.add list (Long/valueOf (int96->epoch-millis v))))
          (addFloat [v] (aset s c true) (.add list (Long/valueOf (long (float v)))))
          (addDouble [v] (aset s c true) (.add list (Long/valueOf (long (double v)))))))
      :double
      (proxy [PrimitiveConverter] []
        (addDouble [v] (aset s c true) (.add list (Double/valueOf (double v))))
        (addFloat [v] (aset s c true) (.add list (Double/valueOf (double (float v)))))
        (addLong [v] (aset s c true) (.add list (Double/valueOf (double (long v)))))
        (addInt [v] (aset s c true) (.add list (Double/valueOf (double (int v))))))
      :string
      (proxy [PrimitiveConverter] []
        (addBinary [^Binary v] (aset s c true) (.add list (.toStringUsingUTF8 v)))
        (addLong [v] (aset s c true) (.add list (str v)))
        (addInt [v] (aset s c true) (.add list (str v)))))))

(defn from-parquet
  "Read a Parquet file into a StratumDataset.

   Returns StratumDataset suitable for queries and table registration.
   String columns are automatically dictionary-encoded.

   Options:
     :columns — vector of column names to read (default: all)
     :limit   — max rows to read
     :name    — dataset name (default: derived from filename)"
  [path & {:keys [columns limit name]}]
  (let [input-file (LocalInputFile. (Paths/get ^String path (make-array String 0)))
        reader (ParquetFileReader/open input-file)]
    (try
      (let [schema (.getSchema (.getFileMetaData (.getFooter reader)))
            fields (.getFields ^MessageType schema)
            ;; Filter to requested columns, skipping non-primitive (nested) types
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
            ;; NULL sentinels per type
            null-sentinel {:long Long/MIN_VALUE :double Double/NaN :string nil}
            ;; Accumulate values per column across row groups
            col-lists (mapv (fn [_] (ArrayList.)) (range ncols))
            ;; Per-row NULL tracking: which columns received a value
            seen (boolean-array ncols)
            total-rows (atom 0)]
        ;; Build projected schema once (same for all row groups)
        (let [proj-fields (mapv #(nth fields %) col-indices)
              proj-schema (MessageType. (.getName ^MessageType schema)
                                        ^java.util.List (vec proj-fields))]
          ;; Read row groups
          (loop [pages (.readNextRowGroup reader)]
            (when pages
              (let [^PageReadStore pg pages
                    row-count (.getRowCount pg)
                    remaining (if limit (- (long limit) (long @total-rows)) Long/MAX_VALUE)]
                (when (pos? remaining)
                  (let [rows-to-read (min row-count remaining)
                        column-io (.getColumnIO (ColumnIOFactory.) proj-schema)
                        converters (mapv (fn [i]
                                           (make-converter (nth col-lists i) (nth col-types i)
                                                           seen (long i) (nth col-is-int96 i)))
                                         (range ncols))
                        group-converter (proxy [GroupConverter] []
                                          (getConverter [i] (nth converters i))
                                          (start []
                                            ;; Reset seen flags at start of each row
                                            (java.util.Arrays/fill seen false))
                                          (end []
                                            ;; Inject NULL sentinels for columns not seen
                                            (dotimes [c ncols]
                                              (when-not (aget seen c)
                                                (.add ^ArrayList (nth col-lists c)
                                                      (get null-sentinel (nth col-types c)))))))
                        record-materializer (proxy [RecordMaterializer] []
                                              (getRootConverter [] group-converter)
                                              (getCurrentRecord [] nil))
                        record-reader (.getRecordReader ^MessageColumnIO column-io
                                                        pg record-materializer)]
                    (dotimes [_ rows-to-read]
                      (.read record-reader))
                    (swap! total-rows + rows-to-read)))
                (when (or (nil? limit) (< (long @total-rows) (long limit)))
                  (recur (.readNextRowGroup reader)))))))
        ;; Convert ArrayLists to typed arrays
        (let [col-map (into {}
                            (map (fn [i]
                                   (let [col-name (nth col-names i)
                                         col-type (nth col-types i)
                                         ^ArrayList values (nth col-lists i)
                                         n (.size values)
                                         arr (case col-type
                                               :long (let [a (long-array n)]
                                                       (dotimes [j n]
                                                         (let [v (.get values j)]
                                                           (aset a j (if (nil? v) Long/MIN_VALUE (long v)))))
                                                       a)
                                               :double (let [a (double-array n)]
                                                         (dotimes [j n]
                                                           (let [v (.get values j)]
                                                             (aset a j (if (nil? v) Double/NaN (double v)))))
                                                         a)
                                               :string (let [a (make-array String n)]
                                                         (dotimes [j n]
                                                           (let [v (.get values j)]
                                                             (when v
                                                               (aset ^"[Ljava.lang.String;" a j
                                                                     ^String v))))
                                                         a))]
                                     [(keyword col-name) arr])))
                            (range ncols))
              ds-name (or name (str "parquet:" (.getName (java.io.File. ^String path))))]
          (dataset/make-dataset col-map
                                {:name ds-name
                                 :metadata {:source-path path
                                            :source-type :parquet}})))
      (finally
        (.close reader)))))
