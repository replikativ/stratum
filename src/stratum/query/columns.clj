(ns stratum.query.columns
  "Shared column preparation and materialization utilities.
   Used by query.clj and join.clj to avoid duplication."
  (:require [stratum.query.expression :as expr]
            [stratum.index :as index]
            [stratum.column :as column]))

(set! *warn-on-reflection* true)

(defn get-column-length
  "Get the length of a column (array or index-backed)."
  ^long [col-info]
  (if-let [d (:data col-info)]
    (cond
      (expr/long-array? d) (alength ^longs d)
      (expr/double-array? d) (alength ^doubles d)
      :else (count d))
    (index/idx-length (:index col-info))))

(defn prepare-columns
  "Normalize all columns in :from map. Preserves index sources."
  [from-map]
  (into {} (map (fn [[k v]] [k (column/encode-column v)])) from-map))

(defn materialize-column
  "Ensure a column has array :data, materializing from index if needed.
   Uses direct single-copy path (idx-materialize-to-array) to avoid
   the intermediate native buffer allocation."
  [col-info]
  (if (:data col-info)
    col-info
    (assoc col-info :data (index/idx-materialize-to-array (:index col-info)))))

(defn check-memory-budget!
  "Throw if materializing index columns would likely OOM.
   Estimates required bytes from index column lengths and checks against available heap."
  [columns]
  (let [index-cols (filter (fn [[_ v]] (and (not (:data v)) (:index v))) columns)]
    (when (seq index-cols)
      (let [est-bytes (reduce (fn [^long acc [_ v]]
                                (+ acc (* (long (index/idx-length (:index v))) 8)))
                              0 index-cols)
            rt (Runtime/getRuntime)
            max-heap (.maxMemory rt)]
        (when (> est-bytes (* max-heap 0.7))
          (throw (ex-info (str "Query would materialize ~" (quot est-bytes 1073741824) " GB "
                               "but only ~" (quot max-heap 1073741824) " GB heap available. "
                               "Consider using chunked index inputs or reducing data size.")
                          {:estimated-bytes est-bytes :available-bytes max-heap})))))))

(defn materialize-columns
  "Materialize all index-sourced columns to arrays."
  [columns]
  (check-memory-budget! columns)
  (into {} (map (fn [[k v]] [k (materialize-column v)])) columns))
