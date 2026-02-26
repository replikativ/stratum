(ns stratum.column
  "Column normalization and encoding for Stratum.

   Provides column type detection and normalization to canonical format
   used by query engine and datasets."
  (:require [stratum.index :as index])
  (:import [stratum.index PersistentColumnIndex]))

(set! *warn-on-reflection* true)

(defn encode-column
  "Detect column type and extract data array from various inputs.
   Pre-encoding columns avoids repeated dictionary encoding on every query.

   Accepts:
     long[]                      → {:type :int64 :data array}
     double[]                    → {:type :float64 :data array}
     String[]                    → {:type :int64 :data long[] :dict String[] :dict-type :string}
     {:type T :data arr}         → passthrough (already normalized)
     PersistentColumnIndex       → {:type T :source :index :index idx}
     Sequential[String]          → converted to String[] then dict-encoded

   Returns: Normalized column map with keys:
     :type       - :int64 or :float64
     :data       - typed array (optional if :source is :index)
     :source     - :index (optional, indicates index-backed column)
     :index      - PersistentColumnIndex (optional, if :source is :index)
     :dict       - String[] reverse dictionary (optional, for string columns)
     :dict-type  - :string (required if :dict present)"
  [col-val]
  (cond
    ;; Already normalized
    (and (map? col-val) (:type col-val) (or (:data col-val) (:index col-val)))
    col-val

    ;; Raw long array
    (instance? (Class/forName "[J") col-val)
    {:type :int64 :data col-val}

    ;; Raw double array
    (instance? (Class/forName "[D") col-val)
    {:type :float64 :data col-val}

    ;; String array — dictionary-encode to long[] for SIMD group-by
    ;; NULL strings (nil) are encoded as Long.MIN_VALUE sentinel (same as int64 NULL)
    (instance? (Class/forName "[Ljava.lang.String;") col-val)
    (let [^"[Ljava.lang.String;" strings col-val
          n (alength strings)
          dict-map (java.util.HashMap.)
          encoded (long-array n)
          next-id (long-array 1)] ;; mutable counter
      (dotimes [i n]
        (let [s (aget strings i)]
          (if (nil? s)
            ;; NULL string → Long.MIN_VALUE sentinel
            (aset encoded i Long/MIN_VALUE)
            (let [id (.get dict-map s)]
              (if id
                (aset encoded i (long id))
                (let [new-id (aget next-id 0)]
                  (.put dict-map s new-id)
                  (aset encoded i new-id)
                  (aset next-id 0 (inc new-id))))))))
      ;; Build reverse dict: int → String
      (let [dict-size (aget next-id 0)
            reverse-dict (make-array String dict-size)]
        (doseq [^java.util.Map$Entry e (.entrySet dict-map)]
          (when-let [k (.getKey e)]
            (aset ^"[Ljava.lang.String;" reverse-dict (int (long (.getValue e))) k)))
        {:type :int64 :data encoded :dict reverse-dict :dict-type :string}))

    ;; Stratum index - preserve as index source for chunk-streaming
    (satisfies? index/IColumnIndex col-val)
    (let [dt (index/idx-datatype col-val)]
      {:type dt :source :index :index col-val})

    ;; Collection of strings (e.g., vector) — convert to String[] then dict-encode
    (and (sequential? col-val)
         (string? (first col-val)))
    (encode-column (into-array String col-val))

    ;; Collection of numbers — infer type and convert to array
    (and (sequential? col-val)
         (number? (first col-val)))
    (let [first-val (first col-val)]
      (if (or (instance? Double first-val)
              (instance? Float first-val))
        ;; Floating point - convert to double[]
        {:type :float64 :data (double-array col-val)}
        ;; Integer - convert to long[]
        {:type :int64 :data (long-array col-val)}))

    :else
    (throw (ex-info (str "Cannot detect column type for: " (type col-val))
                    {:col-type (type col-val)}))))
