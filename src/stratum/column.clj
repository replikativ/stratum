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
    ;; Already normalized
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
            ;; nil strings became Long.MIN_VALUE sentinels above; derive
            ;; validity so downstream Nullable kernels see the NULL set.
             (let [v (chunk/scan-validity encoded :int64 n)]
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
