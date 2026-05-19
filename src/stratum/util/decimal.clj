(ns stratum.util.decimal
  "Step 5: DECIMAL(p,s) support helpers — typmod encode/decode,
   precision/scale parsing, BigDecimal ↔ unscaled-long conversion.

   Stratum stores DECIMAL(p,s) values with p ≤ 18 as int64 unscaled
   longs in a `long[]` chunk; per-column metadata carries
   `:decimal? true :precision p :scale s`. This namespace keeps the
   pure bookkeeping in one place so the SQL parser, INSERT path,
   wire-output, planner, and aggregate decoders all read the same
   conventions.

   References

   - Postgres NUMERIC typmod encoding: `numeric.c:875-925`
     `((precision << 16) | (scale & 0x7ff)) + VARHDRSZ(4)`.
   - pg-datahike's `datahike.pg.types/encode-numeric-typmod` /
     `decode-numeric-typmod` / `parse-numeric-args` at
     `src/datahike/pg/types.clj:591-625`. The implementations here
     are a clean-room port of the same algorithm.
   - DuckDB's int64-tier upper bound: `DecimalWidth<int64_t>::max = 18`
     (`decimal.hpp:28-31`)."
  (:require [clojure.string :as str])
  (:import [java.math BigDecimal MathContext RoundingMode]))

;; ---------------------------------------------------------------------------
;; Precision/scale bounds

(def MAX-PRECISION-INT64
  "Largest precision representable as an int64-backed DECIMAL.
   DECIMAL(18,*) fits in `Math.pow(10,18) < Long.MAX_VALUE`; DECIMAL(19,*)
   does not (would need int128, step 8). Matches DuckDB."
  18)

;; Powers of ten for scale arithmetic.
(def ^"[J" POWERS-OF-10
  "POWERS-OF-10[s] = 10^s as long. Valid for 0 ≤ s ≤ 18; index 0 = 1."
  (let [a (long-array 19)]
    (aset a 0 1)
    (loop [i 1 acc 1]
      (when (< i (alength a))
        (let [v (unchecked-multiply acc 10)]
          (aset a i v)
          (recur (inc i) v))))
    a))

(defn pow10
  "10^s as a long. Throws IndexOutOfBounds for s > 18."
  ^long [^long s]
  (aget POWERS-OF-10 (int s)))

;; ---------------------------------------------------------------------------
;; PG NUMERIC typmod (RFC: numeric.c:875-925)

(def ^:private VARHDRSZ 4)

(defn encode-typmod
  "Build the Postgres `atttypmod` int for DECIMAL(p,s)."
  ^long [^long precision ^long scale]
  (+ VARHDRSZ
     (bit-or (bit-shift-left precision 16)
             (bit-and scale 0x7ff))))

(defn decode-typmod
  "Decode an `atttypmod` int back to `[precision scale]`.
   Returns `[nil nil]` for unconstrained (-1)."
  [typmod]
  (when (and typmod (>= typmod VARHDRSZ))
    (let [t (- typmod VARHDRSZ)]
      [(bit-and (bit-shift-right t 16) 0xffff)
       (bit-and t 0x7ff)])))

;; ---------------------------------------------------------------------------
;; Type-name parsing — accepts `NUMERIC`, `NUMERIC(10)`, `NUMERIC(10, 2)`,
;; `DECIMAL(10,2)`, with arbitrary whitespace. Returns:
;;   {:precision Long :scale Long}    on explicit (p,s)
;;   {:precision Long :scale 0}       on explicit (p)
;;   :unconstrained                   on bare NUMERIC / DECIMAL
;;   nil                              if the string isn't a decimal type
;; Mirrors the lenient shape pg-datahike's parse-numeric-args uses.

(def ^:private DECIMAL-RE
  #"(?i)^\s*(?:NUMERIC|DECIMAL)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$")

(defn parse-decimal-type
  "Recognise SQL DECIMAL / NUMERIC type strings."
  [s]
  (when-let [^java.util.regex.Matcher m (re-matcher DECIMAL-RE (or s ""))]
    (when (.matches m)
      (let [p (.group m 1)
            scale-str (.group m 2)]
        (cond
          (nil? p)        :unconstrained
          (some? scale-str)
          {:precision (Long/parseLong p)
           :scale (Long/parseLong scale-str)}
          :else
          {:precision (Long/parseLong p) :scale 0})))))

;; ---------------------------------------------------------------------------
;; BigDecimal ↔ unscaled long

(defn ^:private clamp-to-int64-range
  "Round-trip safety net: throw if `unscaled` exceeds int64 range. The
   precision check at INSERT/CAST time should prevent this, but we
   verify at the boundary for defence-in-depth."
  ^long [^java.math.BigInteger unscaled context-msg]
  (when (or (< (.compareTo unscaled (BigInteger/valueOf Long/MIN_VALUE)) 0)
            (> (.compareTo unscaled (BigInteger/valueOf Long/MAX_VALUE)) 0))
    (throw (ex-info (str "DECIMAL value exceeds int64 unscaled range "
                         "(context: " context-msg "). For precision > 18 "
                         "use DECIMAL128 (planned step 8).")
                    {:unscaled (.toString unscaled)})))
  (.longValueExact unscaled))

(defn bigdec->unscaled-long
  "Convert a BigDecimal to its int64 unscaled representation at the
   given target scale, using HALF_EVEN rounding (matches PG NUMERIC
   default). Returns the unscaled long. Throws ex-info on overflow.

   Examples:
     (bigdec->unscaled-long (BigDecimal. \"1.23\") 2 ctx) → 123
     (bigdec->unscaled-long (BigDecimal. \"1\")    2 ctx) → 100
     (bigdec->unscaled-long (BigDecimal. \"1.234\") 2 ctx) → 123 (HALF_EVEN)"
  ^long [^BigDecimal bd ^long target-scale context-msg]
  (let [rescaled (.setScale bd (int target-scale) RoundingMode/HALF_EVEN)]
    (clamp-to-int64-range (.unscaledValue rescaled) context-msg)))

(defn coerce->bigdec
  "Promote a value (Long/Double/BigDecimal/String) to BigDecimal,
   preserving textual precision for `String` and `Double` inputs.
   Used at INSERT / CAST time to normalise before scaling.

   For Double, we go through `Double/toString` so a literal like
   `1.10` (parsed by JSqlParser as `DoubleValue 1.1`) gives
   `BigDecimal \"1.1\"` — losing the trailing zero. Callers that want
   to preserve trailing zeros should pass the original token text
   as a String instead."
  ^BigDecimal [v]
  (cond
    (nil? v)                 nil
    (instance? BigDecimal v) v
    (instance? Long v)       (BigDecimal/valueOf (long v))
    (instance? Integer v)    (BigDecimal/valueOf (long v))
    (instance? Double v)     (BigDecimal. (Double/toString (double v)))
    (instance? Float v)      (BigDecimal. (Float/toString (float v)))
    (string? v)              (BigDecimal. ^String v)
    :else
    (throw (ex-info (str "Cannot coerce to BigDecimal: " (type v))
                    {:value v}))))

(defn unscaled-long->bigdec
  "The inverse of `bigdec->unscaled-long`: rebuild a BigDecimal from
   its int64 unscaled representation. NULL sentinel (`Long/MIN_VALUE`)
   maps to nil."
  ^BigDecimal [^long unscaled ^long scale]
  (when-not (= unscaled Long/MIN_VALUE)
    (BigDecimal/valueOf unscaled (int scale))))

(defn unscaled-long->plain-string
  "Render an unscaled-long DECIMAL as its `toPlainString` textual form
   (no scientific notation). The pgwire text-mode path uses this."
  [^long unscaled ^long scale]
  (when-not (= unscaled Long/MIN_VALUE)
    (.toPlainString (BigDecimal/valueOf unscaled (int scale)))))

;; ---------------------------------------------------------------------------
;; Validation

(defn validate-precision!
  "Throw if `p` is outside the int64-backed DECIMAL range."
  [^long p ^long s]
  (when (or (< p 1) (> p MAX-PRECISION-INT64))
    (throw (ex-info (str "DECIMAL precision must be 1..18 (got " p ")."
                         " DECIMAL(p > 18) requires int128 storage"
                         " (planned step 8).")
                    {:precision p :scale s})))
  (when (or (< s 0) (> s p))
    (throw (ex-info (str "DECIMAL scale must be 0..precision (got "
                         "scale=" s " precision=" p ").")
                    {:precision p :scale s}))))
