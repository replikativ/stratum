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
            :gt  (zone-map-estimate-gt entries (double (first args)))
            :gte (zone-map-estimate-gt entries (dec (double (first args))))
            :lt  (zone-map-estimate-lt entries (double (first args)))
            :lte (zone-map-estimate-lt entries (inc (double (first args))))
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
  (let [col-key (first pred)
        op (second pred)]
    ;; Can't sample :fn, :or, string ops on dict columns, or unknown ops
    (when (#{:eq :neq :gt :lt :gte :lte :range :not-range :in :not-in} op)
      (let [sample (cond
                     (:index col-info) (sample-from-index (:index col-info))
                     (:data col-info)  (sample-from-array (:data col-info))
                     :else nil)]
        (when (and sample (pos? (.size ^java.util.ArrayList sample)))
          (eval-pred-on-sample pred sample))))))

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
          ;; Try zone-map ��� sample → heuristic
          (or (zone-map-estimate pred col-info)
              (sample-estimate pred col-info)
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
