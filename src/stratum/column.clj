(ns stratum.column
  "Column normalization and encoding for Stratum.

   Provides column type detection and normalization to canonical format
   used by query engine and datasets."
  (:require [stratum.chunk :as chunk]
            [stratum.index :as index])
  (:import [stratum.index PersistentColumnIndex]
           [stratum.internal ColumnOpsString]))

(set! *warn-on-reflection* true)

;; ----------------------------------------------------------------------------
;; Validity-bitmap identity cache
;;
;; `encode-column` derives a validity bitmap from raw long[]/double[] inputs
;; by scanning for the per-type NULL sentinel. The scan is O(n) per column,
;; and ad-hoc query shapes that pass the same raw array into `q/q` repeatedly
;; (e.g. the OLAP bench's `bench-1t` running 5+10 iterations over h2o's 6M-row
;; vectors) re-paid the scan on every iteration — bisect traced H2O-J1 going
;; from 27.7ms to 42.7ms exactly to the encode-column scan added in 88bfaca.
;;
;; Identity cache: keyed on the array reference itself (Java arrays use
;; identity equality / identity hashCode, so a plain WeakHashMap is an
;; identity cache without any extra wrapping). WeakHashMap lets GC reclaim
;; entries once no caller still holds the array. `Collections.synchronizedMap`
;; makes get/put atomic across threads — the wrapper is enough because we
;; do at most one get + one put per call.
;;
;; The cached value is either a `long[]` bitmap or the `::no-nulls` sentinel
;; (Clojure keyword) standing in for "scanned, no nulls present" — needed
;; because nil already means "miss" inside a HashMap.get call.
;; ----------------------------------------------------------------------------

(def ^:private ^java.util.Map validity-cache
  (java.util.Collections/synchronizedMap (java.util.WeakHashMap.)))

(defn- cached-scan-validity
  "Like `chunk/scan-validity` but memoised by array identity. Returns the
   bitmap (or nil for all-valid) and caches the result so subsequent calls
   on the same array reference skip the O(n) sentinel scan."
  ^longs [arr datatype ^long length]
  (let [hit (.get validity-cache arr)]
    (cond
      (nil? hit)
      (let [v (chunk/scan-validity arr datatype length)]
        (.put validity-cache arr (or v ::no-nulls))
        v)

      (identical? hit ::no-nulls) nil
      :else hit)))

;; ----------------------------------------------------------------------------
;; Dictionary order
;;
;; A dict-encoded string column stores `long[]` codes indexing a `String[]`
;; reverse dictionary. Codes are assigned in FIRST-SEEN order, so the code
;; space carries no ordering information: `MIN(code)` is the first value
;; encountered, not the lexicographically smallest.
;;
;; Encoding deliberately does not sort. Sorting at encode time would tax
;; every ingest (O(d log d) plus a full O(n) rewrite of the codes) to serve
;; only the queries that want order, and it could never be a global
;; invariant anyway: `dataset/append!`, `parquet` streaming ingest and the
;; lazily-extended `parquet-dataset` global dict all hand out codes before
;; the dictionary is complete, and for persisted columns those codes are
;; already durable. A per-column "is it sorted?" flag then has to be
;; carried by every producer and consulted by every consumer — which is
;; how a dropped flag becomes a wrong answer. DuckDB is first-seen
;; everywhere for the same reason, keeping ordering in the statistics
;; instead of the codes.
;;
;; Ordering is therefore derived where it is needed, not where data is
;; written: `stratum.query.string-order` ranks a dictionary at QUERY time,
;; only for the columns a query actually orders by, and only when the
;; dictionary is not already sorted. `dict-sorted?` below is the cheap
;; test that lets that step no-op — it is a PREDICATE on the array, never
;; a stored property, so it cannot go stale or be dropped.
;; ----------------------------------------------------------------------------

;; Dicts above this size use Arrays/parallelSort. Below it the ForkJoin
;; split costs more than the sort saves. Measured on an 8-core box: a
;; 200k-entry String[] sorts in ~147ms serial vs ~34ms parallel, while a
;; 10-entry dict is free either way.
(def ^:private ^:const PARALLEL_SORT_THRESHOLD 8192)

(def ^:private ^java.util.Map dict-sorted-cache
  (java.util.Collections/synchronizedMap (java.util.WeakHashMap.)))

(defn dict-sorted?
  "True when `dict` (a `String[]` reverse dictionary) is in ascending
   lexicographic order, i.e. its codes are ranks and comparing codes
   numerically is equivalent to comparing the strings they denote.

   O(d) on the first call per dict array, then memoised on array
   identity (dict arrays are treated as immutable once attached to a
   column; the paths that grow one build a fresh array).

   nil / empty / single-entry dicts are trivially sorted.

   Anything that is not a `String[]` answers `false` rather than
   throwing. A dict in mid-growth is a `java.util.ArrayList` (see
   `dataset/transient!`), and that shape reaches here through
   `encode-column`'s already-normalized branch; \"not order-preserving\"
   is both the safe answer and the true one, since an ArrayList dict is
   by definition still being appended to."
  [dict]
  (if-not (instance? (Class/forName "[Ljava.lang.String;") dict)
    false
    (let [hit (.get dict-sorted-cache dict)]
      (if (some? hit)
        (identical? hit ::sorted)
        (let [^"[Ljava.lang.String;" d dict
              n (alength d)
              sorted? (loop [i 1]
                        (cond
                          (>= i n) true
                          ;; A nil entry means the dict has a hole — treat
                          ;; as unsorted rather than NPE-ing on compareTo.
                          (or (nil? (aget d i)) (nil? (aget d (dec i)))) false
                          (pos? (.compareTo ^String (aget d (dec i)) ^String (aget d i)))
                          false
                          :else (recur (unchecked-inc i))))]
          (.put dict-sorted-cache d (if sorted? ::sorted ::unsorted))
          sorted?)))))

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
     :type           - :int64 or :float64
     :data           - typed array (optional if :source is :index)
     :source         - :index (optional, indicates index-backed column)
     :index          - PersistentColumnIndex (optional, if :source is :index)
     :dict           - String[] reverse dictionary (optional, for string columns)
     :dict-type      - :string (required if :dict present)
     :validity       - long[] packed bitmap, present only when the data
                       contains NULL sentinels; absent maps to the
                       all-valid fast path
     :temporal-unit  - :days/:seconds/:millis/:micros (optional; tags long[]
                       columns as DATE or TIMESTAMP and selects the matching
                       date kernels)

  NULL opt-out: callers that know a column is non-nullable can pass
  `:nullable? false` via the 2-arity form OR pre-normalise to
  `{:type T :data arr}` (which already bypasses the sentinel scan,
  because the passthrough branch trusts caller-supplied metadata).
  Skipping the scan avoids an O(n) sweep at column registration;
  downstream kernels then take the all-valid fast path."
  ([col-val] (encode-column col-val nil))
  ([col-val {:keys [nullable? no-sentinel-null? validity]
             :or {nullable? true no-sentinel-null? false}}]
   (cond
    ;; Already normalized — passed through untouched. Dictionary order is
    ;; never assumed here; ask `dict-sorted?` at the point of use.
     (and (map? col-val) (:type col-val) (or (:data col-val) (:index col-val)))
     col-val

    ;; Raw long array — scan once for Long.MIN_VALUE sentinels so the
    ;; downstream kernels can dispatch to their Nullable siblings.
    ;; Returns nil bitmap when no NULLs present (the common case),
    ;; preserving the all-valid fast path.
    ;;
    ;; Step 8 sentinel opt-out: callers that need to STORE
    ;; `Long.MIN_VALUE` as a genuine value (e.g., a UBIGINT column
    ;; holding 2^63) pass `:no-sentinel-null? true`. The scan is
    ;; skipped — NULL must then be tracked via an explicit `:validity`
    ;; bitmap supplied by the caller (or assumed all-valid).
    ;; Kernels still see the column as int64; they're safe as long as
    ;; the caller's promise (no implicit sentinel NULLs) holds.
     (instance? (Class/forName "[J") col-val)
     (let [v (cond
               no-sentinel-null? validity                ;; trust caller
               nullable? (cached-scan-validity col-val :int64
                                               (alength ^longs col-val))
               :else nil)]
       (cond-> {:type :int64 :data col-val}
         (false? nullable?)         (assoc :nullable? false)
         no-sentinel-null?          (assoc :no-sentinel-null? true)
         v                          (assoc :validity v)))

    ;; Raw double array — same lazy validity derivation.
     (instance? (Class/forName "[D") col-val)
     (let [v (when nullable?
               (cached-scan-validity col-val :float64 (alength ^doubles col-val)))]
       (cond-> {:type :float64 :data col-val}
         (false? nullable?) (assoc :nullable? false)
         v (assoc :validity v)))

    ;; String array — dictionary-encode to long[] for SIMD group-by
    ;; NULL strings (nil) are encoded as Long.MIN_VALUE sentinel (same as int64 NULL)
     (instance? (Class/forName "[Ljava.lang.String;") col-val)
     (let [^"[Ljava.lang.String;" strings col-val
           n (alength strings)]
       (if (zero? n)
        ;; Empty string array — preserve dict metadata so schema stays correct
         {:type :int64 :data (long-array 0) :dict (make-array String 0) :dict-type :string}
         (let [dict-map (java.util.HashMap.)
               encoded (long-array n)
               next-id (long-array 1) ;; mutable counter
               ;; The encoding loop already knows whether any NULL was
               ;; written, and `Long/MIN_VALUE` lands in `encoded` only
               ;; where the source string was nil. Remembering that lets
               ;; the all-valid case skip the separate O(n) sentinel scan
               ;; below entirely.
               any-null? (loop [i 0 any-null? false]
                           (if (>= i n)
                             any-null?
                             (let [s (aget strings i)]
                               (if (nil? s)
                ;; NULL string → Long.MIN_VALUE sentinel
                                 (do (aset encoded i Long/MIN_VALUE)
                                     (recur (unchecked-inc i) true))
                                 (let [id (.get dict-map s)]
                                   (if id
                                     (aset encoded i (long id))
                                     (let [new-id (aget next-id 0)]
                                       (.put dict-map s new-id)
                                       (aset encoded i new-id)
                                       (aset next-id 0 (inc new-id))))
                                   (recur (unchecked-inc i) any-null?))))))]
          ;; Build reverse dict: int → String
           (let [dict-size (aget next-id 0)
                 reverse-dict (make-array String dict-size)]
             (doseq [^java.util.Map$Entry e (.entrySet dict-map)]
               (when-let [k (.getKey e)]
                 (aset ^"[Ljava.lang.String;" reverse-dict (int (long (.getValue e))) k)))
            ;; Codes stay in FIRST-SEEN order. Encoding does not sort: a
            ;; column's dictionary order is not a correctness property of
            ;; the column — `MIN`/`MAX` gets its ordering from
            ;; `stratum.query.string-order`, which ranks the dictionary at
            ;; query time and only when a query actually asks for it.
            ;; Sorting here instead would tax every ingest (O(d log d) plus
            ;; a full O(n) rewrite of the codes) to serve the queries that
            ;; do, and — because `append!`, streaming Parquet ingest and
            ;; the lazily-extended `parquet-dataset` dict all hand out
            ;; codes before the dictionary is complete — it could only ever
            ;; hold for SOME columns, leaving every consumer to ask whether
            ;; this one is ordered. DuckDB is first-seen everywhere for the
            ;; same reason; it keeps ordering in the statistics instead.
             (let [v (when any-null? (chunk/scan-validity encoded :int64 n))]
               (cond-> {:type :int64 :data encoded :dict reverse-dict :dict-type :string
                        :dict-alpha-masks (ColumnOpsString/buildDictAlphaMasks reverse-dict)
                        :dict-bigram-masks (ColumnOpsString/buildDictBigramMasks reverse-dict)}
                 v (assoc :validity v)))))))

    ;; Step 7 / 8b / 8c / UUID: typed reference arrays carrying values
    ;; the engine doesn't operate on directly (Interval, BigInteger,
    ;; BigDecimal, UUID, etc.). They flow through SELECT via the
    ;; Object[] passthrough path (query/execution.clj) and render via
    ;; their value class's toString in format-results. Filters,
    ;; aggregates, and group-by over these columns aren't supported —
    ;; this branch only enables the SELECT-passthrough capability.
     (and (some-> col-val class .isArray)
          (not (.isPrimitive (.getComponentType (class col-val))))
          (let [ct (.getComponentType (class col-val))]
            (or (= ct stratum.internal.Interval)
                (= ct java.math.BigInteger)
                (= ct java.math.BigDecimal)
                (= ct java.util.UUID)
               ;; Object[] whose first element is one of the above —
               ;; covers (object-array [(BigInteger. "…")]) form.
                (and (= ct Object)
                     (pos? (alength ^objects col-val))
                     (let [first-non-nil (some identity (seq col-val))]
                       (or (instance? stratum.internal.Interval first-non-nil)
                           (instance? java.math.BigInteger first-non-nil)
                           (instance? java.math.BigDecimal first-non-nil)
                           (instance? java.util.UUID first-non-nil)))))))
     (let [ct (.getComponentType (class col-val))
           probe (when (and (= ct Object) (pos? (alength ^objects col-val)))
                   (some identity (seq col-val)))
           kind (cond
                  (or (= ct stratum.internal.Interval)
                      (instance? stratum.internal.Interval probe))   :interval
                  (or (= ct java.math.BigInteger)
                      (instance? java.math.BigInteger probe))        :hugeint
                  (or (= ct java.math.BigDecimal)
                      (instance? java.math.BigDecimal probe))        :decimal128
                  (or (= ct java.util.UUID)
                      (instance? java.util.UUID probe))              :uuid)]
       {:type kind :data col-val})

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
                     {:col-type (type col-val)})))))
