(ns stratum.query.columns
  "Shared column preparation and materialization utilities.
   Used by query.clj and join.clj to avoid duplication."
  (:require [clojure.walk :as walk]
            [stratum.query.expression :as expr]
            [stratum.index :as index]
            [stratum.column :as column]))

(set! *warn-on-reflection* true)

(def ^:dynamic *query-column-refs*
  "Set of column keywords actually referenced by the current query, or
   nil for 'all columns' (e.g. SELECT * with no other restrictions).
   Bound by `stratum.query/q` via `query-references`. When non-nil,
   `materialize-columns` and `check-memory-budget!` only consider these
   columns — index-backed columns outside the set are left as-is and
   never decoded.

   Default nil so callers outside the main query engine (tests, REPL,
   train-iforest, etc.) get the original behaviour of materializing
   the whole map."
  nil)

(def ^:private query-ref-slots
  "Slots in a query map that can contain column references. Walked by
   `query-references`. Excludes `:from` (the data) and
   `:limit`/`:offset` (numeric).
   HAVING/ORDER columns that aren't projected stay alive via
   `:agg` injections in the legacy path, so we don't need a separate
   `:select-other-cols` slot."
  [:where :having :agg :group :order :select :join :window])

(defn query-references
  "Return the set of column keywords actually referenced anywhere in
   `query`, or nil to mean 'all columns of `columns`' for shapes where
   pruning is unsafe (currently: SELECT * without aggregation).

   Conservative: any keyword inside the query slots that matches a key
   of `columns` is counted as referenced. This catches column refs in
   any position (predicate args, expression args, aggregate cols,
   GROUP BY, ORDER BY, window specs, join on-pairs, SELECT items).

   Returns nil rather than the full set so callers can explicitly
   distinguish 'no pruning' from 'every column happens to be referenced'."
  [query columns]
  (let [select-clause (:select query)
        select-star? (or (nil? select-clause)
                         (= [:*] select-clause)
                         (some #(or (= % :*) (= % '*)) select-clause))]
    (if (and select-star?
             (empty? (:agg query))
             (empty? (:group query)))
      ;; SELECT * with no aggregation/grouping — every column may end
      ;; up in the output, so don't prune.
      nil
      (let [col-keys (set (keys columns))
            slots (select-keys query query-ref-slots)
            refs (volatile! (transient #{}))]
        (walk/postwalk
         (fn [x]
           (when (and (keyword? x) (contains? col-keys x))
             (vswap! refs conj! x))
           x)
         slots)
        (persistent! @refs)))))

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
   Estimates required bytes from the lengths of *referenced* index
   columns and checks against available heap. When
   `*query-column-refs*` is bound to a set, only those columns are
   counted; otherwise the whole map is."
  [columns]
  (let [refs *query-column-refs*
        active (if refs (select-keys columns refs) columns)
        index-cols (filter (fn [[_ v]] (and (not (:data v)) (:index v))) active)]
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
  "Materialize index-sourced columns to arrays. When
   `*query-column-refs*` is bound to a set, only the referenced
   index columns are decoded; unreferenced columns are passed through
   untouched (still index-backed) so their memory cost is avoided."
  [columns]
  (check-memory-budget! columns)
  (let [refs *query-column-refs*]
    (into {}
          (map (fn [[k v]]
                 (if (or (nil? refs) (contains? refs k))
                   [k (materialize-column v)]
                   [k v])))
          columns)))
