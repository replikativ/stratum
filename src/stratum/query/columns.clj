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
   Uses direct single-copy path that ALSO concatenates per-chunk
   validity bitmaps into a flat `:validity` field (nil when every
   chunk is all-valid — the common case for dense data). Downstream
   kernels dispatch on `:validity` being non-nil to select the
   bitmap-aware sibling."
  [col-info]
  (if (:data col-info)
    col-info
    (let [{:keys [data validity]}
          (index/idx-materialize-to-array-with-validity (:index col-info))]
      (assoc col-info :data data :validity validity))))

(def ^:private long-array-class    (Class/forName "[J"))
(def ^:private double-array-class  (Class/forName "[D"))
(def ^:private string-array-class  (Class/forName "[Ljava.lang.String;"))
;; Step 7b: generic Object[] backing for passthrough types (INTERVAL,
;; future struct columns) — see alength-any / copy-prefix.
(def ^:private object-array-class  (Class/forName "[Ljava.lang.Object;"))

(defn- alength-any
  "Length of a typed Java array. Throws on unsupported types so the
   caller can't silently get nil."
  ^long [arr]
  (cond
    (instance? long-array-class arr)   (alength ^longs arr)
    (instance? double-array-class arr) (alength ^doubles arr)
    (instance? string-array-class arr) (alength ^"[Ljava.lang.String;" arr)
    ;; Object[] / any non-primitive reference array (e.g., Interval[]):
    ;; `(class arr).isArray()` plus a non-primitive component type.
    (and (some-> arr class .isArray)
         (not (.isPrimitive (.getComponentType (class arr)))))
    (alength ^objects arr)
    :else (throw (ex-info "alength-any: unsupported array type"
                          {:class (class arr)}))))

(defn- copy-prefix
  "Return a fresh array of the same component type containing the
   first `n` elements of `arr` (n is clamped to `arr`'s length by
   the caller via `take-prefix-column`)."
  [arr ^long n]
  (cond
    (instance? long-array-class arr)
    (java.util.Arrays/copyOfRange ^longs arr 0 (int n))
    (instance? double-array-class arr)
    (java.util.Arrays/copyOfRange ^doubles arr 0 (int n))
    (instance? string-array-class arr)
    (java.util.Arrays/copyOfRange ^"[Ljava.lang.String;" arr 0 (int n))
    ;; Object[] / Interval[] / any reference array — Arrays/copyOfRange
    ;; preserves the component type via Object[] form.
    (and (some-> arr class .isArray)
         (not (.isPrimitive (.getComponentType (class arr)))))
    (java.util.Arrays/copyOfRange ^objects arr 0 (int n))
    :else (throw (ex-info "copy-prefix: unsupported array type"
                          {:class (class arr)}))))

(defn take-prefix-column
  "Materialize the first `n` rows of a column entry into a heap-array
   `:data` slot, preserving everything else (`:type`, `:dict`,
   `:dict-type`). Used by the `LHead` LIMIT-without-ORDER-BY
   pushdown so a `SELECT * LIMIT N` on a large index touches only the
   first chunk(s).

   For arrays, copies the prefix via `Arrays/copyOfRange`. For
   indices, uses `index/idx-materialize-to-array-prefix`. Dict
   metadata (`:dict`, `:dict-type`) is carried through verbatim — the
   prefix codes still index into the same dict."
  [col-info ^long n]
  (cond
    ;; Already a raw array (long[] / double[] / String[]).
    (:data col-info)
    (let [data (:data col-info)
          take-n (Math/min n (long (alength-any data)))]
      (assoc col-info :data (copy-prefix data take-n)))

    ;; Index-backed — fetch only the first N rows.
    (:index col-info)
    (assoc col-info :data (index/idx-materialize-to-array-prefix (:index col-info) n))

    :else
    col-info))

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
