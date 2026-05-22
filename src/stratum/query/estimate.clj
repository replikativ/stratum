(ns stratum.query.estimate
  "Selectivity estimation for predicate-aware strategy selection.

   Three tiers of estimation, tried in order:
     1. Zone-map (chunk stats) — O(chunks), no materialization.
        Works for range predicates (:gt :lt :gte :lte :range :not-range :eq :neq).
        Classifies chunks as full-pass / full-fail / partial.
        Partial chunks interpolated assuming uniform distribution.

     2. Sample-based — O(sample-size), reads ~128 strided values.
        Fallback for equality on unsorted cols, IN-lists, string ops,
        complex expressions, or when zone maps give wide uncertainty.

     3. Heuristic constants — last resort for computed expressions or
        predicates on virtual columns with no data to sample.

   Entry point:
     (estimate-selectivity pred columns)
       → double in [0.0, 1.0]

     (estimate-all-selectivities preds columns)
       → vec of doubles, one per predicate

     (estimate-combined-selectivity preds columns)
       → double, product of individual selectivities (independence assumption)

     (estimate-output-rows preds columns length)
       → long, estimated rows surviving all predicates"
  (:require [stratum.query.columns :as cols]
            [stratum.query.expression :as expr]
            [stratum.index :as index]
            [stratum.chunk :as chunk]
            [stratum.query.group-by :as gb])
  (:import [stratum.index ChunkEntry]
           [stratum.stats ChunkStats]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Constants
;; ============================================================================

(def ^:const ^long SAMPLE_SIZE
  "Number of values to sample for sample-based estimation."
  128)

(def ^:const ^long MAX_SAMPLE_CHUNKS
  "Maximum number of chunks to sample from."
  4)

;; Heuristic selectivity constants (last resort)
(def heuristic-selectivity
  {:eq        0.02    ; equality on unknown column — assume ~2% match
   :neq       0.98
   :gt        0.33
   :lt        0.33
   :gte       0.34
   :lte       0.34
   :range     0.15
   :not-range 0.85
   :in        0.10    ; IN-list
   :not-in    0.90
   :like      0.10
   :ilike     0.10
   :contains  0.05
   :starts-with 0.10
   :ends-with   0.10
   :fn        0.50    ; arbitrary function — no information
   :is-null   0.05
   :is-not-null 0.95
   :or        0.50
   :default   0.50})

;; ============================================================================
;; Tier 1: Zone-map estimation (chunk stats)
;; ============================================================================

(defn- zone-map-estimate-gt
  "Estimate selectivity of (col > threshold) from chunk stats."
  ^double [chunk-entries ^double threshold]
  (loop [es (seq chunk-entries)
         full-pass (long 0)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ (+ (double full-pass) partial-est) (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          (> mn threshold)
          (recur (next es) (+ full-pass cnt) partial-est (+ total cnt))

          (<= mx threshold)
          (recur (next es) full-pass partial-est (+ total cnt))

          :else
          (let [range-size (- mx mn)
                frac (if (zero? range-size) 0.5 (/ (- mx threshold) range-size))]
            (recur (next es) full-pass (+ partial-est (* (double cnt) frac)) (+ total cnt))))))))

(defn- zone-map-estimate-gte
  "Estimate selectivity of (col >= threshold) from chunk stats.
   Differs from `zone-map-estimate-gt` only at the chunk-boundary
   tests, which use inclusive bounds. Required for floats — the
   `:gt (threshold-1)` reduction the planner used previously
   over-estimated when `mn` fell strictly between `threshold-1`
   and `threshold` (e.g. `mn=4.5, threshold=5.0`)."
  ^double [chunk-entries ^double threshold]
  (loop [es (seq chunk-entries)
         full-pass (long 0)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ (+ (double full-pass) partial-est) (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          (>= mn threshold)
          (recur (next es) (+ full-pass cnt) partial-est (+ total cnt))

          (< mx threshold)
          (recur (next es) full-pass partial-est (+ total cnt))

          :else
          (let [range-size (- mx mn)
                frac (if (zero? range-size) 0.5 (/ (- mx threshold) range-size))]
            (recur (next es) full-pass (+ partial-est (* (double cnt) frac)) (+ total cnt))))))))

(defn- zone-map-estimate-lt
  "Estimate selectivity of (col < threshold) from chunk stats."
  ^double [chunk-entries ^double threshold]
  (loop [es (seq chunk-entries)
         full-pass (long 0)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ (+ (double full-pass) partial-est) (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          (< mx threshold)
          (recur (next es) (+ full-pass cnt) partial-est (+ total cnt))

          (>= mn threshold)
          (recur (next es) full-pass partial-est (+ total cnt))

          :else
          (let [range-size (- mx mn)
                frac (if (zero? range-size) 0.5 (/ (- threshold mn) range-size))]
            (recur (next es) full-pass (+ partial-est (* (double cnt) frac)) (+ total cnt))))))))

(defn- zone-map-estimate-lte
  "Estimate selectivity of (col <= threshold) from chunk stats —
   inclusive-upper variant of `zone-map-estimate-lt`."
  ^double [chunk-entries ^double threshold]
  (loop [es (seq chunk-entries)
         full-pass (long 0)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ (+ (double full-pass) partial-est) (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          (<= mx threshold)
          (recur (next es) (+ full-pass cnt) partial-est (+ total cnt))

          (> mn threshold)
          (recur (next es) full-pass partial-est (+ total cnt))

          :else
          (let [range-size (- mx mn)
                frac (if (zero? range-size) 0.5 (/ (- threshold mn) range-size))]
            (recur (next es) full-pass (+ partial-est (* (double cnt) frac)) (+ total cnt))))))))

(defn- zone-map-estimate-eq
  "Estimate selectivity of (col == value) from chunk stats.
   Zone maps can only prune chunks where value is outside [min, max].
   For partial chunks, estimate 1/range_size (uniform assumption)."
  ^double [chunk-entries ^double value]
  (loop [es (seq chunk-entries)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ partial-est (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          ;; Exact match: all values equal
          (and (== mn value) (== mx value))
          (recur (next es) (+ partial-est (double cnt)) (+ total cnt))

          ;; Outside range: no matches
          (or (< value mn) (> value mx))
          (recur (next es) partial-est (+ total cnt))

          ;; Inside range: estimate 1/(distinct values) ≈ cnt/(mx-mn+1)
          :else
          (let [range-size (- mx mn)
                ;; For integer columns, distinct count ≈ range+1
                ;; For float columns, use a heuristic fraction
                est-matches (if (zero? range-size) (double cnt)
                                (/ (double cnt) (inc range-size)))]
            (recur (next es) (+ partial-est est-matches) (+ total cnt))))))))

(defn- zone-map-estimate-range
  "Estimate selectivity of (lo <= col <= hi) from chunk stats."
  ^double [chunk-entries ^double lo ^double hi]
  (loop [es (seq chunk-entries)
         full-pass (long 0)
         partial-est 0.0
         total (long 0)]
    (if-not es
      (if (zero? total) 0.5
          (/ (+ (double full-pass) partial-est) (double total)))
      (let [^ChunkEntry e (first es)
            ^ChunkStats s (.stats e)
            cnt (long (:count s))
            mn (double (:min-val s))
            mx (double (:max-val s))]
        (cond
          ;; Chunk fully inside [lo, hi]
          (and (>= mn lo) (<= mx hi))
          (recur (next es) (+ full-pass cnt) partial-est (+ total cnt))

          ;; Chunk fully outside
          (or (> mn hi) (< mx lo))
          (recur (next es) full-pass partial-est (+ total cnt))

          ;; Partial overlap
          :else
          (let [range-size (- mx mn)
                overlap-lo (max mn lo)
                overlap-hi (min mx hi)
                frac (if (zero? range-size) 0.5 (/ (- overlap-hi overlap-lo) range-size))]
            (recur (next es) full-pass (+ partial-est (* (double cnt) (max 0.0 frac))) (+ total cnt))))))))

(defn- zone-map-estimate
  "Try zone-map estimation for a predicate. Returns selectivity or nil if not applicable."
  [pred col-info]
  (when-let [idx (:index col-info)]
    (let [entries (gb/collect-chunk-entries idx)]
      (when (seq entries)
        (let [op (second pred)
              args (subvec pred 2)]
          (case op
            :gt  (zone-map-estimate-gt  entries (double (first args)))
            :gte (zone-map-estimate-gte entries (double (first args)))
            :lt  (zone-map-estimate-lt  entries (double (first args)))
            :lte (zone-map-estimate-lte entries (double (first args)))
            :eq  (zone-map-estimate-eq entries (double (first args)))
            :neq (- 1.0 (zone-map-estimate-eq entries (double (first args))))
            :range (zone-map-estimate-range entries
                                            (double (first args))
                                            (double (second args)))
            :not-range (- 1.0 (zone-map-estimate-range entries
                                                       (double (first args))
                                                       (double (second args))))
            nil))))))

;; ============================================================================
;; Tier 2: Sample-based estimation
;; ============================================================================

(defn- sample-from-index
  "Sample SAMPLE_SIZE values from an index by striding through random chunks.
   Returns a long[] or double[] of sampled values."
  [idx]
  (let [entries (vec (gb/collect-chunk-entries idx))
        n-chunks (count entries)
        chunks-to-use (min (long MAX_SAMPLE_CHUNKS) n-chunks)
        ;; Spread chunks evenly across the index
        chunk-stride (max 1 (quot n-chunks chunks-to-use))
        per-chunk (max 1 (quot (long SAMPLE_SIZE) chunks-to-use))
        result (java.util.ArrayList. (int SAMPLE_SIZE))]
    (loop [ci 0]
      (when (and (< ci n-chunks) (< (.size result) (int SAMPLE_SIZE)))
        (let [^ChunkEntry e (nth entries ci)
              arr (chunk/chunk-data (.chunk e))
              arr-len (long (:count (.stats e)))
              stride (max 1 (quot arr-len per-chunk))]
          (loop [i 0]
            (when (and (< i arr-len) (< (.size result) (int SAMPLE_SIZE)))
              (if (instance? (Class/forName "[J") arr)
                (.add result (Long/valueOf (aget ^longs arr (int i))))
                (.add result (Double/valueOf (aget ^doubles arr (int i)))))
              (recur (+ i stride)))))
        (recur (+ ci chunk-stride))))
    result))

(defn- sample-from-array
  "Sample SAMPLE_SIZE values from a materialized array by striding."
  [arr]
  (let [result (java.util.ArrayList. (int SAMPLE_SIZE))
        is-long? (expr/long-array? arr)
        len (long (if is-long? (alength ^longs arr) (alength ^doubles arr)))
        stride (max 1 (quot len (long SAMPLE_SIZE)))]
    (loop [i 0]
      (when (and (< i len) (< (.size result) (int SAMPLE_SIZE)))
        (if is-long?
          (.add result (Long/valueOf (aget ^longs arr (int i))))
          (.add result (Double/valueOf (aget ^doubles arr (int i)))))
        (recur (+ i stride))))
    result))

(defn- eval-pred-on-sample
  "Evaluate a predicate against a sample and return the pass rate."
  ^double [pred ^java.util.ArrayList sample]
  (if (.isEmpty sample)
    0.5
    (let [op (second pred)
          args (subvec pred 2)
          n (.size sample)
          hits (long
                (case op
                  :eq  (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (== (double (.get sample i)) v) (inc h) h)))))
                  :neq (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (not= (double (.get sample i)) v) (inc h) h)))))
                  :gt  (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (> (double (.get sample i)) v) (inc h) h)))))
                  :lt  (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (< (double (.get sample i)) v) (inc h) h)))))
                  :gte (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (>= (double (.get sample i)) v) (inc h) h)))))
                  :lte (let [v (double (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (<= (double (.get sample i)) v) (inc h) h)))))
                  :range (let [lo (double (first args)) hi (double (second args))]
                           (loop [i 0 h 0] (if (>= i n) h
                                               (let [v (double (.get sample i))]
                                                 (recur (inc i)
                                                        (if (and (>= v lo) (<= v hi)) (inc h) h))))))
                  :in  (let [vs (into #{} (map double) (first args))]
                         (loop [i 0 h 0] (if (>= i n) h
                                             (recur (inc i)
                                                    (if (contains? vs (double (.get sample i))) (inc h) h)))))
                  :not-in (let [vs (into #{} (map double) (first args))]
                            (loop [i 0 h 0] (if (>= i n) h
                                                (recur (inc i)
                                                       (if (not (contains? vs (double (.get sample i)))) (inc h) h)))))
                  ;; Unsupported op for sampling
                  (long (* n 0.5))))]
      (/ (double hits) (double n)))))

(defn- sample-estimate
  "Estimate selectivity by sampling from the column. Returns selectivity or nil."
  [pred col-info]
  (let [op (second pred)
        args (when (sequential? pred) (subvec (vec pred) 2))
        ;; The numeric `eval-pred-on-sample` casts every arg to
        ;; `double`. Skip sampling whenever an arg is a string (or
        ;; the column is dict-encoded) — dict resolution on
        ;; equality predicates happens later in `prepare-query`/
        ;; executor's `prepare-preds` and the planner can fall back
        ;; to a heuristic selectivity until then.
        all-numeric-args? (every? number? args)
        dict-string?      (= :string (:dict-type col-info))]
    ;; Can't sample :fn, :or, string ops on dict columns, or unknown ops
    (when (and (#{:eq :neq :gt :lt :gte :lte :range :not-range :in :not-in} op)
               all-numeric-args?
               (not dict-string?))
      (let [sample (cond
                     (:index col-info) (sample-from-index (:index col-info))
                     (:data col-info)  (sample-from-array (:data col-info))
                     :else nil)]
        (when (and sample (pos? (.size ^java.util.ArrayList sample)))
          (eval-pred-on-sample pred sample))))))

;; ============================================================================
;; Tier 2b: String-predicate sampling
;; ============================================================================

(def ^:private ^:const MAX_DICT_SAMPLE 256)

(def ^:private string-array-class
  (Class/forName "[Ljava.lang.String;"))

(defn- like->regex
  "Compile a SQL `LIKE` pattern to a `java.util.regex.Pattern`, honoring
   an optional escape code point (-1 = no ESCAPE clause). Delegates to the
   shared `ColumnOpsString/likeToRegex` so selectivity estimation can never
   disagree with the SIMD matcher about which `%`/`_` is a wildcard.
   `Pattern/DOTALL` so `.` covers embedded newlines."
  ^java.util.regex.Pattern [^String pat escape]
  (java.util.regex.Pattern/compile
   (stratum.internal.ColumnOpsString/likeToRegex pat (int escape))
   java.util.regex.Pattern/DOTALL))

(defn- compile-string-matcher
  "Return a `(fn [^String s])` that returns true if the row passes
   the string predicate. Returns nil for unsupported ops or arg
   shapes — the caller falls back to heuristic selectivity.

   `args` is the predicate's argument slots: `[pattern]`, or
   `[pattern escape]` for a `LIKE ... ESCAPE` clause."
  [op args]
  (when (and (>= (count args) 1) (string? (first args)))
    (let [pat (first args)
          esc-str (when (and (>= (count args) 2) (string? (second args))
                             (= 1 (count ^String (second args))))
                    (second args))
          esc    (if esc-str (int (.charAt ^String esc-str 0)) -1)
          esc-ci (if esc-str
                   (int (Character/toLowerCase (.charAt ^String esc-str 0)))
                   -1)]
      (case op
        :contains    (fn [^String s] (and s (.contains    s ^String pat)))
        :starts-with (fn [^String s] (and s (.startsWith  s ^String pat)))
        :ends-with   (fn [^String s] (and s (.endsWith    s ^String pat)))
        :like        (let [re (like->regex pat esc)]
                       (fn [^String s] (and s (.matches (.matcher re s)))))
        :not-like    (let [re (like->regex pat esc)]
                       (fn [^String s] (or (nil? s)
                                           (not (.matches (.matcher re s))))))
        :ilike       (let [re (like->regex (.toLowerCase ^String pat) esc-ci)]
                       (fn [^String s] (and s (.matches (.matcher re (.toLowerCase s))))))
        :not-ilike   (let [re (like->regex (.toLowerCase ^String pat) esc-ci)]
                       (fn [^String s] (or (nil? s)
                                           (not (.matches (.matcher re (.toLowerCase s)))))))
        nil))))

(defn- string-sample
  "Return up to `MAX_DICT_SAMPLE` strings from a column. Prefers the
   dict (one entry per distinct value, faster + more representative
   for high-cardinality columns) and falls back to the raw `String[]`
   data otherwise."
  ^java.util.ArrayList [col-info]
  (let [^"[Ljava.lang.String;" src
        (cond
          (instance? string-array-class (:dict col-info)) (:dict col-info)
          (instance? string-array-class (:data col-info)) (:data col-info)
          :else nil)]
    (when src
      (let [n      (alength src)
            k      (min n (long MAX_DICT_SAMPLE))
            stride (max 1 (quot n k))
            result (java.util.ArrayList. (int k))]
        (loop [i 0]
          (when (and (< i n) (< (.size result) (int k)))
            (.add result (aget src i))
            (recur (+ i stride))))
        result))))

(defn- string-sample-estimate
  "Estimate selectivity for a string predicate (`:like`, `:ilike`,
   `:contains`, `:starts-with`, `:ends-with`, plus their negations)
   by applying the matcher to a sample of the column's distinct
   string values. For a dict-encoded column the dict IS the set of
   distinct values, so the rate maps directly to row selectivity
   under a uniform-frequency assumption — a much better default
   than the 0.05 / 0.10 heuristic. Returns nil for unsupported
   shapes."
  [pred col-info]
  (let [op   (second pred)
        args (subvec (vec pred) 2)]
    (when-let [matcher (compile-string-matcher op args)]
      (when-let [sample (string-sample col-info)]
        (let [n (.size sample)]
          (when (pos? n)
            (let [hits
                  (loop [i 0, h 0]
                    (if (>= i n)
                      h
                      (recur (inc i)
                             (if (matcher (.get sample i)) (inc h) h))))]
              ;; Clamp to keep DP cost-comparisons from collapsing
              ;; on "no hits in sample" — rare events still cost
              ;; *something*.
              (max 0.001 (min 1.0 (/ (double hits) (double n)))))))))))

;; ============================================================================
;; Tier 3: Heuristic fallback
;; ============================================================================

(defn- heuristic-estimate
  "Return a heuristic selectivity based on the predicate op."
  ^double [pred]
  (let [op (second pred)]
    (double (get heuristic-selectivity op
                 (get heuristic-selectivity :default)))))

;; ============================================================================
;; Public API
;; ============================================================================

(defn estimate-selectivity
  "Estimate the selectivity of a single normalized predicate.
   Returns a double in [0.0, 1.0].

   Tries zone-map estimation first (O(chunks)), then sampling (O(128)),
   then heuristic constants."
  ^double [pred columns]
  (let [op (second pred)]
    ;; OR: estimate as 1 - product(1 - sub_selectivity) — check before column lookup
    (if (= :or op)
      (let [sub-preds (subvec pred 2)
            sub-sels (mapv #(estimate-selectivity % columns) sub-preds)]
        (- 1.0 (reduce (fn [^double acc ^double s] (* acc (- 1.0 s))) 1.0 sub-sels)))
      ;; Regular pred
      (let [col-key (first pred)
            col-info (when (keyword? col-key) (get columns col-key))]
        (if-not col-info
          ;; Unknown column (expression pred, cross-side, etc.) — heuristic
          (heuristic-estimate pred)
          ;; Try zone-map → numeric sample → string sample → heuristic
          (or (zone-map-estimate pred col-info)
              (sample-estimate pred col-info)
              (string-sample-estimate pred col-info)
              (heuristic-estimate pred)))))))

(defn estimate-all-selectivities
  "Estimate selectivity for each predicate. Returns a vec of doubles."
  [preds columns]
  (mapv #(estimate-selectivity % columns) preds))

(defn estimate-combined-selectivity
  "Estimate combined selectivity of all predicates (independence assumption).
   Returns a double in [0.0, 1.0]."
  ^double [preds columns]
  (if (empty? preds)
    1.0
    (reduce (fn [^double acc ^double s] (* acc s))
            1.0
            (estimate-all-selectivities preds columns))))

(defn estimate-output-rows
  "Estimate how many rows survive all predicates.
   Returns a long >= 1 (clamped to at least 1 to avoid division by zero)."
  ^long [preds columns ^long length]
  (max 1 (long (* (estimate-combined-selectivity preds columns) length))))

;; ============================================================================
;; Cardinality / NDV
;; ============================================================================

(defn- ndv-from-chunk-stats
  "Approximate distinct count for an int-typed indexed column from
   `min`/`max` across all chunks. Uses `min(length, max-min+1)`,
   the same identity zone-map-estimate-eq relies on for inclusive
   integer ranges. Returns nil when no chunk entries are available
   (e.g. the index hasn't been built yet)."
  [idx ^long length]
  (let [entries (seq (gb/collect-chunk-entries idx))]
    (when entries
      (loop [es entries, mn Long/MAX_VALUE, mx Long/MIN_VALUE]
        (if-not es
          (when (<= mn mx)
            (max 1 (min length (inc (- mx mn)))))
          (let [^ChunkEntry e (first es)
                ^ChunkStats s (.stats e)
                cmn (long (:min-val s))
                cmx (long (:max-val s))]
            (recur (next es) (min mn cmn) (max mx cmx))))))))

(defn estimate-ndv
  "Approximate the number of distinct values in a column. Used by
   join cardinality and other planner cost estimates that need
   `count_distinct(col)`. Returns a positive long.

   Heuristics, in order:
   - Dict-encoded string column: dict length (exact upper bound on
     distinct row values; may exceed actual NDV when not every
     dict entry is referenced).
   - Indexed int64 column: `min(length, max-min+1)` from chunk
     stats — exact for densely-populated ranges, an upper bound
     elsewhere.
   - Otherwise: `max(1, length/10)` — the legacy heuristic, kept
     as a backstop so unknown columns don't collapse cardinality
     to zero."
  ^long [col-info ^long length]
  (cond
    (instance? string-array-class (:dict col-info))
    (max 1 (alength ^"[Ljava.lang.String;" (:dict col-info)))

    (and (:index col-info)
         (or (= :int64 (:type col-info)) (nil? (:type col-info))))
    (or (ndv-from-chunk-stats (:index col-info) length)
        (max 1 (quot length 10)))

    :else
    (max 1 (quot length 10))))
