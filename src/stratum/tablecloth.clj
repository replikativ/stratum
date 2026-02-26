(ns stratum.tablecloth
  "Tablecloth integration for Stratum - protocol extensions for seamless interop.

   This namespace extends StratumDataset with tech.ml.dataset protocols, enabling
   direct use with tablecloth without conversion at callsites.

   Protocol implementations:
   - PDataset: is-dataset?, column, rows, rowvecs
   - PRowCount, PColumnCount: counting
   - PMissing: missing value tracking (NaN/Long.MIN_VALUE sentinels)
   - PSelectColumns, PSelectRows: selection operations

   Usage:
     (require '[stratum.dataset :as dataset])
     (require '[tablecloth.api :as tc])

     (def ds (dataset/make-dataset {:x (double-array [1 2 3])}))
     (tc/info ds)                    ;; Works directly!
     (tc/select-columns ds [:x])     ;; No conversion needed!

   All selection operations return StratumDataset (type consistency).

   NOTE: The old make-dataset/arrays->dataset wrappers have been removed.
   Use stratum.dataset/make-dataset or stratum.csv/from-csv instead."
  (:require [stratum.index :as idx]
            [stratum.chunk :as chunk]
            [stratum.stats :as stats]
            [stratum.dataset :as dataset]
            [stratum.column :as column]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [org.roaringbitmap RoaringBitmap]
           [stratum.index PersistentColumnIndex]
           [stratum.chunk PersistentColChunk]
           [stratum.dataset StratumDataset]
           [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [clojure.lang IPersistentMap IObj]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Protocol: Polymorphic Dataset Interface for Query Engine
;; ============================================================================

(defprotocol IStratumDataset
  "Protocol for dataset sources that can be used in Stratum queries.
   Enables polymorphic dispatch in query.execute without explicit conversion."

  (ds-column-names [ds]
    "Return sequence of column names (keywords).")

  (ds-column-data [ds col-name]
    "Return column data for query execution.
     Should return PersistentColumnIndex (zero-copy) or array (long[]/double[]).
     Query engine will call encode-column on the result.")

  (ds-row-count [ds]
    "Return total number of rows in dataset.")

  (ds-column-type [ds col-name]
    "Return column datatype: :int64 or :float64.")

  (ds-has-index? [ds col-name]
    "Return true if column is backed by PersistentColumnIndex (zero-copy available).")

  (ds-chunk-stats [ds col-name]
    "Return sequence of ChunkStats for zone map optimization.
     Returns nil if column is not indexed (no zone maps available)."))

;; ============================================================================
;; Bidirectional Support: tech.ml.Dataset → Stratum Query Engine
;; ============================================================================

(defn- tech-column->stratum
  "Convert tech.ml.dataset Column to Stratum column format.

   Zero-copy when Column is backed by primitive array (most common case).
   Falls back to materialization for native buffers or computed columns.

   Returns: {:type :int64|:float64 :data array}"
  [^Column col]
  (let [data (.data col)
        dtype (dtype-proto/elemwise-datatype col)]
    (cond
      ;; Zero-copy path: Column backed by double array
      (instance? (Class/forName "[D") data)
      {:type :float64 :data data}

      ;; Zero-copy path: Column backed by long array
      (instance? (Class/forName "[J") data)
      {:type :int64 :data data}

      ;; String column: Convert to String array
      ;; Return raw String[] which encode-column will dict-encode
      (or (= dtype :string) (= dtype :text))
      (let [n (long (count col))
            ^"[Ljava.lang.String;" str-array (make-array String n)]
        (dotimes [i n]
          (aset str-array i (str (nth col i))))
        ;; Return raw array - will be passed through encode-column
        str-array)

      ;; Fallback: Materialize to array (native buffers, computed columns, etc.)
      :else
      (let [arr (dtype/make-container :java-array dtype col)]
        {:type (if (= dtype :float64) :float64 :int64)
         :data arr}))))

;; Extend IDataset protocol to tech.ml.Dataset for bidirectional support
(extend-protocol dataset/IDataset
  Dataset

  (ds-name [ds]
    (or (:name (meta ds)) "tech.ml.dataset"))

  (metadata [ds]
    (meta ds))

  (row-count [ds]
    (ds/row-count ds))

  (column-names [ds]
    (vec (ds/column-names ds)))

  (schema [_ds]
    ;; tech.ml.dataset doesn't have explicit schema
    ;; Could infer from column types if needed
    {})

  (columns [ds]
    "Convert tech.ml.dataset columns to Stratum format.
     Zero-copy when columns are array-backed."
    (into {}
          (map (fn [col-name]
                 (let [col (ds/column ds col-name)
                       col-data (tech-column->stratum col)]
                   ;; Pass through encode-column to handle String[] dict-encoding
                   [col-name (column/encode-column col-data)])))
          (ds/column-names ds)))

  (column [ds col-name]
    (when-let [col (ds/column ds col-name)]
      (column/encode-column (tech-column->stratum col))))

  (column-type [ds col-name]
    (when-let [col (ds/column ds col-name)]
      (let [dtype (dtype-proto/elemwise-datatype col)]
        (if (= dtype :float64) :float64 :int64))))

  (has-index? [_ds _col-name]
    ;; tech.ml.dataset doesn't use PersistentColumnIndex
    false)

  (chunk-stats [_ds _col-name]
    ;; tech.ml.dataset doesn't have zone maps
    nil))

;; ============================================================================
;; Chunked Reader (zero-copy, lazy access to PersistentColumnIndex)
;; ============================================================================

(defn- index->reader
  "Convert a PersistentColumnIndex to a dtype-next Reader (zero-copy, lazy).

   Uses reify to create LongReader or DoubleReader that delegates to index."
  ^tech.v3.datatype.Buffer [^PersistentColumnIndex idx]
  (let [n (idx/idx-length idx)]
    (case (idx/idx-datatype idx)
      :float64
      (reify tech.v3.datatype.DoubleReader
        (lsize [_] n)
        (readDouble [_ i]
          (idx/idx-get-double idx (long i)))
        (readObject [_ i]
          (let [v (idx/idx-get-double idx (long i))]
            (if (Double/isNaN v) nil v))))

      :int64
      (reify tech.v3.datatype.LongReader
        (lsize [_] n)
        (readLong [_ i]
          (idx/idx-get-long idx (long i)))
        (readObject [_ i]
          (let [v (idx/idx-get-long idx (long i))]
            (if (= v Long/MIN_VALUE) nil v)))))))

;; ============================================================================
;; Missing Value Support (Lazy Scan)
;; ============================================================================

(defn- scan-index-for-nulls
  "Scan PersistentColumnIndex and build RoaringBitmap of NULL rows.
   Only called when tablecloth explicitly asks for missing data.

   For float64: checks Double/isNaN
   For int64: checks Long.MIN_VALUE sentinel"
  [^PersistentColumnIndex idx]
  (let [bm (RoaringBitmap.)
        n (idx/idx-length idx)]
    (case (idx/idx-datatype idx)
      :float64
      (dotimes [i n]
        (when (Double/isNaN (idx/idx-get-double idx i))
          (.add bm (int i))))

      :int64
      (dotimes [i n]
        (when (= Long/MIN_VALUE (idx/idx-get-long idx i))
          (.add bm (int i)))))
    bm))

;; ============================================================================
;; Column Wrapper (uses tech.ml.dataset's Column type)
;; ============================================================================

(defn- make-column
  "Create a tech.ml.dataset Column from a PersistentColumnIndex or array.

   Zero-copy: PersistentColumnIndex wrapped in lazy Reader.
   Stores index reference in metadata for query integration."
  [col-name idx-or-array metadata]
  (let [data (if (instance? PersistentColumnIndex idx-or-array)
               ;; Zero-copy lazy reader
               (index->reader idx-or-array)
               ;; Array - use directly
               idx-or-array)
        ;; Store index reference in metadata if this is an index
        meta-with-idx (if (instance? PersistentColumnIndex idx-or-array)
                        (assoc metadata
                               :name col-name
                               :stratum/index idx-or-array)
                        (assoc metadata :name col-name))]
    (col-impl/new-column col-name data meta-with-idx)))

;; ============================================================================
;; Protocol Extensions: StratumDataset → tech.ml.dataset Protocols
;; ============================================================================
;;
;; These extensions make StratumDataset a first-class citizen in the
;; tech.ml.dataset ecosystem, enabling direct use with tablecloth.
;;
;; Key design decisions:
;; 1. Column objects wrap our data (array or index) in tech.ml.dataset.Column
;; 2. Selection operations return StratumDataset (type consistency)
;; 3. Missing values tracked via NaN/Long.MIN_VALUE sentinels
;; 4. Zero-copy preserved for index-backed columns via lazy Readers

(extend-protocol ds-proto/PDataset
  StratumDataset

  (is-dataset? [_] true)

  (column [ds colname]
    "Return tech.ml.dataset Column for the given column name.
     Wraps array or index in proper Column object with metadata."
    (let [col-kw (if (keyword? colname) colname (keyword colname))
          col-data (dataset/column ds col-kw)]
      (when-not col-data
        (throw (RuntimeException. (str "Column not found: " colname
                                       ". Available: " (dataset/column-names ds)))))
      ;; Create Column wrapping our data (array or index)
      (if (:index col-data)
        ;; Index-backed: wrap in lazy Reader (zero-copy)
        (make-column col-kw (:index col-data) {:name col-kw})
        ;; Array-backed: use array directly
        (col-impl/new-column col-kw (:data col-data) {:name col-kw}))))

  (rows [ds options]
    "Return ObjectReader of row maps.
     Each row is a map {col-name value}."
    (let [cols (dataset/columns ds)
          col-names (dataset/column-names ds)
          n (dataset/row-count ds)]
      (reify tech.v3.datatype.ObjectReader
        (lsize [_] n)
        (readObject [_ idx]
          (into {}
                (map (fn [col-name]
                       (let [col-data (get cols col-name)]
                         (if (= :index (:source col-data))
                           ;; Index-backed column
                           (let [index (:index col-data)
                                 val (idx/idx-get index idx)]
                             [col-name (if (= :float64 (:type col-data))
                                         (if (Double/isNaN val) nil val)
                                         (if (= val Long/MIN_VALUE) nil val))])
                           ;; Array-backed column
                           (let [arr (:data col-data)
                                 val (if (instance? (Class/forName "[D") arr)
                                       (aget ^doubles arr idx)
                                       (aget ^longs arr idx))]
                             ;; Convert sentinels to nil
                             [col-name (if (instance? (Class/forName "[D") arr)
                                         (if (Double/isNaN val) nil val)
                                         (if (= val Long/MIN_VALUE) nil val))]))))
                     col-names))))))

  (rowvecs [ds options]
    "Return ObjectReader of row vectors.
     Each row is a vector [val1 val2 ...]."
    (let [cols (dataset/columns ds)
          col-names (vec (dataset/column-names ds))
          n (dataset/row-count ds)]
      (reify tech.v3.datatype.ObjectReader
        (lsize [_] n)
        (readObject [_ idx]
          (mapv (fn [col-name]
                  (let [col-data (get cols col-name)]
                    (if (= :index (:source col-data))
                      ;; Index-backed column
                      (let [index (:index col-data)
                            val (idx/idx-get index idx)]
                        (if (= :float64 (:type col-data))
                          (if (Double/isNaN val) nil val)
                          (if (= val Long/MIN_VALUE) nil val)))
                      ;; Array-backed column
                      (let [arr (:data col-data)
                            val (if (instance? (Class/forName "[D") arr)
                                  (aget ^doubles arr idx)
                                  (aget ^longs arr idx))]
                        ;; Convert sentinels to nil
                        (if (instance? (Class/forName "[D") arr)
                          (if (Double/isNaN val) nil val)
                          (if (= val Long/MIN_VALUE) nil val))))))
                col-names))))))

(extend-protocol ds-proto/PRowCount
  StratumDataset
  (row-count [ds] (dataset/row-count ds)))

(extend-protocol ds-proto/PColumnCount
  StratumDataset
  (column-count [ds] (count (dataset/column-names ds))))

(extend-protocol ds-proto/PMissing
  StratumDataset

  (missing [ds]
    "Return RoaringBitmap of row indices with missing values (NaN or Long.MIN_VALUE)."
    (let [cols (dataset/columns ds)
          col-names (dataset/column-names ds)
          bitmaps (keep (fn [col-name]
                          (let [col-data (get cols col-name)
                                arr (:data col-data)
                                bm (RoaringBitmap.)]
                            (if (instance? (Class/forName "[D") arr)
                              ;; Double array: check for NaN
                              (dotimes [i (alength ^doubles arr)]
                                (when (Double/isNaN (aget ^doubles arr i))
                                  (.add bm (int i))))
                              ;; Long array: check for Long.MIN_VALUE
                              (dotimes [i (alength ^longs arr)]
                                (when (= Long/MIN_VALUE (aget ^longs arr i))
                                  (.add bm (int i)))))
                            (when-not (.isEmpty bm) bm)))
                        col-names)]
      (if (empty? bitmaps)
        (RoaringBitmap.)
        (reduce #(RoaringBitmap/or %1 %2) bitmaps))))

  (num-missing [ds]
    "Return count of missing values across all columns."
    (.getCardinality ^RoaringBitmap (ds-proto/missing ds)))

  (any-missing? [ds]
    "Return true if any column has missing values."
    (not (.isEmpty ^RoaringBitmap (ds-proto/missing ds)))))

(extend-protocol ds-proto/PSelectColumns
  StratumDataset

  (select-columns [ds colnames]
    "Return new StratumDataset with selected columns.

     Args:
       colnames - :all, sequence of column names, or map {old-name new-name}

     Returns: StratumDataset (maintains type consistency)"
    (let [;; Normalize column selection
          selected-cols (cond
                          (= :all colnames) (dataset/column-names ds)
                          (map? colnames) (keys colnames)  ;; TODO: handle renaming
                          :else colnames)
          ;; Build new column map
          cols (dataset/columns ds)
          new-cols (into {}
                         (keep (fn [col-name]
                                 (when-let [col-data (get cols col-name)]
                                   ;; Extract data or index depending on source
                                   (let [col-value (if (= :index (:source col-data))
                                                     (:index col-data)
                                                     (:data col-data))]
                                     [col-name col-value]))))
                         selected-cols)]
      ;; Return new StratumDataset (not tech.ml.dataset!)
      (dataset/make-dataset new-cols
                            {:name (dataset/ds-name ds)
                             :metadata (assoc (dataset/metadata ds)
                                              :derived-from :select-columns)}))))

(extend-protocol ds-proto/PSelectRows
  StratumDataset

  (select-rows [ds rowidxs]
    "Return new StratumDataset with selected rows.

     Args:
       rowidxs - Row indices (array, bitmap, range, iterable)

     Returns: StratumDataset with selected rows"
    (let [;; Normalize row indices to sequence
          idxs (vec (col-impl/simplify-row-indexes (dataset/row-count ds) rowidxs))
          n (count idxs)
          cols (dataset/columns ds)
          ;; Select rows from each column
          new-cols (into {}
                         (map (fn [[col-name col-data]]
                                (if (= :index (:source col-data))
                                  ;; Index-backed: use index slicing
                                  (let [index (:index col-data)
                                        new-arr (if (= :float64 (:type col-data))
                                                  (let [out (double-array n)]
                                                    (dotimes [i n]
                                                      (aset out i (double (idx/idx-get index (long (nth idxs i))))))
                                                    out)
                                                  (let [out (long-array n)]
                                                    (dotimes [i n]
                                                      (aset out i (long (idx/idx-get index (long (nth idxs i))))))
                                                    out))]
                                    [col-name new-arr])
                                  ;; Array-backed: direct array slicing
                                  (let [arr (:data col-data)
                                        new-arr (if (instance? (Class/forName "[D") arr)
                                                  ;; Double array
                                                  (let [out (double-array n)]
                                                    (dotimes [i n]
                                                      (aset out i (aget ^doubles arr (long (nth idxs i)))))
                                                    out)
                                                  ;; Long array
                                                  (let [out (long-array n)]
                                                    (dotimes [i n]
                                                      (aset out i (aget ^longs arr (long (nth idxs i)))))
                                                    out))]
                                    [col-name new-arr]))))
                         cols)]
      ;; Return new StratumDataset
      (dataset/make-dataset new-cols
                            {:name (dataset/ds-name ds)
                             :metadata (assoc (dataset/metadata ds)
                                              :derived-from :select-rows)}))))


