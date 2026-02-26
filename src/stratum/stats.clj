(ns stratum.stats
  "Chunk statistics for O(chunks) aggregations and zone map filtering.

   For numerical values in ordered metric spaces, we store:
   - count: element count
   - sum: Σx for mean
   - sum-sq: Σx² for variance/stddev
   - min-val/max-val: for min/max and zone maps

   This enables:
   - O(chunks) sum, mean, min, max, variance, stddev
   - Zone map filtering (skip chunks that can't match predicates)")

(set! *warn-on-reflection* true)

;; ============================================================================
;; ChunkStats Record
;; ============================================================================

(defrecord ChunkStats
           [^long count       ;; n - element count (total, including NULLs)
            ^double sum       ;; Σx - for sum, mean
            ^double sum-sq    ;; Σx² - for variance, stddev
            ^double min-val   ;; min(x) - for min, zone maps
            ^double max-val   ;; max(x) - for max, zone maps
            ^long null-count]) ;; number of NULL sentinels (NaN / Long.MIN_VALUE)

(def empty-stats
  "Stats for an empty chunk."
  (->ChunkStats 0 0.0 0.0 Double/NaN Double/NaN 0))

(defn compute-stats
  "Compute statistics for an array. O(n) single pass.
   Called once during chunk creation.

   arr must be a long[] or double[].
   If length is provided, only computes stats for the first length elements."
  ([arr]
   (let [is-doubles (instance? (Class/forName "[D") arr)]
     (compute-stats arr (if is-doubles
                          (alength ^doubles arr)
                          (alength ^longs arr)))))
  ([arr ^long length]
   (if (zero? length)
     empty-stats
     (let [is-doubles (instance? (Class/forName "[D") arr)]
       (loop [i (long 0)
              nc (long 0)
              sum (double 0.0)
              sum-sq (double 0.0)
              mn Double/MAX_VALUE
              mx (- Double/MAX_VALUE)]
         (if (>= i length)
           (->ChunkStats length sum sum-sq mn mx nc)
           (if is-doubles
             (let [v (aget ^doubles arr i)]
               (if (Double/isNaN v)
                 ;; Skip NaN (double NULL sentinel) — exclude from sum/sum-sq/min/max
                 (recur (unchecked-inc i) (unchecked-inc nc) sum sum-sq mn mx)
                 (recur (unchecked-inc i) nc (+ sum v) (+ sum-sq (* v v))
                        (Math/min mn v) (Math/max mx v))))
             (let [lv (aget ^longs arr i)]
               (if (= lv Long/MIN_VALUE)
                 ;; Skip Long.MIN_VALUE (long NULL sentinel) — exclude from sum/sum-sq/min/max
                 (recur (unchecked-inc i) (unchecked-inc nc) sum sum-sq mn mx)
                 (let [v (double lv)]
                   (recur (unchecked-inc i) nc (+ sum v) (+ sum-sq (* v v))
                          (Math/min mn v) (Math/max mx v))))))))))))

(defn compute-stats-range
  "Compute statistics for a sub-range [offset, offset+len) of an array.
   Skips NULL sentinels (NaN for doubles, Long.MIN_VALUE for longs)."
  [arr ^long offset ^long len]
  (if (zero? len)
    empty-stats
    (let [is-doubles (instance? (Class/forName "[D") arr)
          end (+ offset len)]
      (loop [i offset
             nc (long 0)
             sum (double 0.0)
             sum-sq (double 0.0)
             mn Double/MAX_VALUE
             mx (- Double/MAX_VALUE)]
        (if (>= i end)
          (->ChunkStats len sum sum-sq mn mx nc)
          (if is-doubles
            (let [v (aget ^doubles arr i)]
              (if (Double/isNaN v)
                (recur (unchecked-inc i) (unchecked-inc nc) sum sum-sq mn mx)
                (recur (unchecked-inc i) nc (+ sum v) (+ sum-sq (* v v))
                       (Math/min mn v) (Math/max mx v))))
            (let [lv (aget ^longs arr i)]
              (if (= lv Long/MIN_VALUE)
                (recur (unchecked-inc i) (unchecked-inc nc) sum sum-sq mn mx)
                (let [v (double lv)]
                  (recur (unchecked-inc i) nc (+ sum v) (+ sum-sq (* v v))
                         (Math/min mn v) (Math/max mx v)))))))))))

(defn update-stats-append
  "Incrementally update stats with a new appended value. O(1).
   Skips NULL sentinels (NaN, or double representation of Long.MIN_VALUE)."
  [^ChunkStats stats ^double val]
  (if (or (Double/isNaN val) (== val (double Long/MIN_VALUE)))
    ;; NULL sentinel — increment count + null-count but don't update sum/min/max
    (->ChunkStats (inc (:count stats))
                  (:sum stats) (:sum-sq stats)
                  (:min-val stats) (:max-val stats)
                  (inc (long (:null-count stats))))
    (->ChunkStats (inc (:count stats))
                  (+ (:sum stats) val)
                  (+ (:sum-sq stats) (* val val))
                  (if (Double/isNaN (:min-val stats)) val (Math/min ^double (:min-val stats) val))
                  (if (Double/isNaN (:max-val stats)) val (Math/max ^double (:max-val stats) val))
                  (:null-count stats))))

(defn update-stats-remove
  "Incrementally update stats after removing a value. O(1) for count/sum/sum-sq.
   For min/max: if the removed value equals current min or max, sets them to NaN
   to signal that a full rescan is needed. Otherwise preserves existing min/max.
   Skips NULL sentinels (NaN, Long.MIN_VALUE as double).

   arr — the chunk array AFTER removal (for rescan if needed)
   new-len — length of the array after removal"
  [^ChunkStats stats ^double val arr ^long new-len]
  (if (zero? new-len)
    empty-stats
    (let [is-null (or (Double/isNaN val) (== val (double Long/MIN_VALUE)))]
      (if is-null
        ;; Removing a NULL sentinel: decrement count + null-count, sum/min/max unchanged
        (->ChunkStats (dec (:count stats))
                      (:sum stats) (:sum-sq stats)
                      (:min-val stats) (:max-val stats)
                      (dec (long (:null-count stats))))
        (let [new-count (dec (:count stats))
              new-sum (- (:sum stats) val)
              new-sum-sq (- (:sum-sq stats) (* val val))
              old-min (double (:min-val stats))
              old-max (double (:max-val stats))
              nc (:null-count stats)
              need-rescan (or (== val old-min) (== val old-max))]
          (if need-rescan
            ;; Rescan the array for min/max, skipping NULL sentinels
            (let [is-doubles (instance? (Class/forName "[D") arr)]
              (loop [i (long 0)
                     mn Double/MAX_VALUE
                     mx (- Double/MAX_VALUE)]
                (if (>= i new-len)
                  (->ChunkStats new-count new-sum new-sum-sq mn mx nc)
                  (if is-doubles
                    (let [v (aget ^doubles arr i)]
                      (if (Double/isNaN v)
                        (recur (unchecked-inc i) mn mx)
                        (recur (unchecked-inc i) (Math/min mn v) (Math/max mx v))))
                    (let [lv (aget ^longs arr i)]
                      (if (= lv Long/MIN_VALUE)
                        (recur (unchecked-inc i) mn mx)
                        (let [v (double lv)]
                          (recur (unchecked-inc i) (Math/min mn v) (Math/max mx v)))))))))
            (->ChunkStats new-count new-sum new-sum-sq old-min old-max nc)))))))

;; ============================================================================
;; Stats Merging (for aggregating across chunks)
;; ============================================================================

(defn merge-stats
  "Merge two ChunkStats. O(1).
   Used to aggregate stats across multiple chunks."
  [^ChunkStats a ^ChunkStats b]
  (cond
    (nil? a) b
    (nil? b) a
    (zero? (:count a)) b
    (zero? (:count b)) a
    :else
    (->ChunkStats
     (+ (:count a) (:count b))
     (+ (:sum a) (:sum b))
     (+ (:sum-sq a) (:sum-sq b))
     (min (:min-val a) (:min-val b))
     (max (:max-val a) (:max-val b))
     (+ (long (:null-count a)) (long (:null-count b))))))

(defn merge-all-stats
  "Merge a sequence of ChunkStats. O(chunks)."
  [stats-seq]
  (reduce merge-stats nil stats-seq))

(defn subtract-stats
  "Subtract ChunkStats b from a. O(1).

   Used for boundary corrections when computing stats for partial ranges.
   Only invertible fields (count, sum, sum-sq) are computed exactly.
   Min/max are set to NaN since they're not invertible without full data."
  [^ChunkStats a ^ChunkStats b]
  (cond
    (nil? a) (throw (ex-info "Cannot subtract from nil stats" {:b b}))
    (nil? b) a
    (zero? (:count b)) a
    :else
    (->ChunkStats
     (- (:count a) (:count b))
     (- (:sum a) (:sum b))
     (- (:sum-sq a) (:sum-sq b))
     Double/NaN  ;; min is not invertible
     Double/NaN  ;; max is not invertible
     (- (long (:null-count a)) (long (:null-count b))))))

;; ============================================================================
;; Aggregations from Stats
;; ============================================================================

(defn stats-sum
  "Sum from merged stats. O(1)."
  ^double [^ChunkStats stats]
  (:sum stats))

(defn stats-count
  "Count from merged stats. O(1)."
  ^long [^ChunkStats stats]
  (:count stats))

(defn stats-mean
  "Mean from merged stats. O(1)."
  ^double [^ChunkStats stats]
  (/ (:sum stats) (:count stats)))

(defn stats-min
  "Min from merged stats. O(1)."
  ^double [^ChunkStats stats]
  (:min-val stats))

(defn stats-max
  "Max from merged stats. O(1)."
  ^double [^ChunkStats stats]
  (:max-val stats))

(defn stats-variance
  "Variance from merged stats. O(1).
   Uses: Var(X) = E[X²] - E[X]²

   Options:
   - :sample? true (default) - sample variance with Bessel's correction
   - :sample? false - population variance"
  (^double [^ChunkStats stats]
   (stats-variance stats {:sample? true}))
  (^double [^ChunkStats stats {:keys [sample?] :or {sample? true}}]
   (let [n (:count stats)
         sum (:sum stats)
         sum-sq (:sum-sq stats)
         mean (/ sum n)
         ;; Population variance: E[X²] - E[X]²
         pop-var (- (/ sum-sq n) (* mean mean))]
     (if sample?
       ;; Sample variance: multiply by n/(n-1) (Bessel's correction)
       (* pop-var (/ (double n) (dec n)))
       pop-var))))

(defn stats-stddev
  "Standard deviation from merged stats. O(1)."
  (^double [^ChunkStats stats]
   (Math/sqrt (stats-variance stats)))
  (^double [^ChunkStats stats opts]
   (Math/sqrt (stats-variance stats opts))))

;; ============================================================================
;; Zone Map Predicates
;; ============================================================================

(defn zone-may-contain-gt?
  "Can this chunk contain values > threshold?"
  [^ChunkStats stats ^double threshold]
  (> (:max-val stats) threshold))

(defn zone-may-contain-gte?
  "Can this chunk contain values >= threshold?"
  [^ChunkStats stats ^double threshold]
  (>= (:max-val stats) threshold))

(defn zone-may-contain-lt?
  "Can this chunk contain values < threshold?"
  [^ChunkStats stats ^double threshold]
  (< (:min-val stats) threshold))

(defn zone-may-contain-lte?
  "Can this chunk contain values <= threshold?"
  [^ChunkStats stats ^double threshold]
  (<= (:min-val stats) threshold))

(defn zone-may-contain-eq?
  "Can this chunk contain value equal to target?"
  [^ChunkStats stats ^double target]
  (and (<= (:min-val stats) target)
       (<= target (:max-val stats))))

(defn zone-may-contain-neq?
  "Can this chunk contain values != target?
   Only prunes if all elements are equal to target (min == max == target)."
  [^ChunkStats stats ^double target]
  (or (not= (:min-val stats) target)
      (not= (:max-val stats) target)))

(defn zone-may-contain-range?
  "Can this chunk contain values in [lo, hi]?"
  [^ChunkStats stats ^double lo ^double hi]
  (and (<= (:min-val stats) hi)
       (>= (:max-val stats) lo)))

(defn zone-predicate
  "Create a zone map predicate from comparison operator and value.
   Returns a function (fn [stats] -> bool) that returns true if
   the chunk might contain matching values.

   Supported ops: :gt :gte :lt :lte :eq :ne :between"
  [op value]
  (case op
    :gt     (fn [stats] (zone-may-contain-gt? stats value))
    :gte    (fn [stats] (zone-may-contain-gte? stats value))
    :lt     (fn [stats] (zone-may-contain-lt? stats value))
    :lte    (fn [stats] (zone-may-contain-lte? stats value))
    :eq     (fn [stats] (zone-may-contain-eq? stats value))
    :ne     (fn [_stats] true)  ;; Can't skip for inequality
    :between (let [[lo hi] value]
               (fn [stats] (zone-may-contain-range? stats lo hi)))
    ;; Default: can't skip
    (fn [_stats] true)))

;; ============================================================================
;; Zone Map "Fully Inside" Predicates
;; ============================================================================
;; These return true when ALL values in a chunk satisfy the predicate.
;; Used for stats-only aggregation (skip SIMD when chunk fully matches)
;; and for chunk pruning (skip materialization for fully excluded chunks).

(defn zone-fully-inside-gt?
  "Do ALL values in this chunk satisfy > threshold?"
  [^ChunkStats stats ^double threshold]
  (> (:min-val stats) threshold))

(defn zone-fully-inside-gte?
  "Do ALL values in this chunk satisfy >= threshold?"
  [^ChunkStats stats ^double threshold]
  (>= (:min-val stats) threshold))

(defn zone-fully-inside-lt?
  "Do ALL values in this chunk satisfy < threshold?"
  [^ChunkStats stats ^double threshold]
  (< (:max-val stats) threshold))

(defn zone-fully-inside-lte?
  "Do ALL values in this chunk satisfy <= threshold?"
  [^ChunkStats stats ^double threshold]
  (<= (:max-val stats) threshold))

(defn zone-fully-inside-eq?
  "Do ALL values in this chunk equal target?"
  [^ChunkStats stats ^double target]
  (and (== (:min-val stats) target) (== (:max-val stats) target)))

(defn zone-fully-inside-neq?
  "Do ALL values in this chunk satisfy != target?"
  [^ChunkStats stats ^double target]
  (or (> (:min-val stats) target) (< (:max-val stats) target)))

(defn zone-fully-inside-range?
  "Do ALL values in this chunk satisfy [lo, hi]?"
  [^ChunkStats stats ^double lo ^double hi]
  (and (>= (:min-val stats) lo) (<= (:max-val stats) hi)))

(defn zone-fully-inside-not-range?
  "Do ALL values in this chunk satisfy NOT [lo, hi]?"
  [^ChunkStats stats ^double lo ^double hi]
  (or (< (:max-val stats) lo) (> (:min-val stats) hi)))

(defn zone-fully-inside-predicate
  "Create a predicate that returns true if ALL values in chunk satisfy the condition.
   Returns a function (fn [stats] -> bool).
   Returns nil for ops that can't determine full containment."
  [op value]
  (case op
    :gt        (fn [stats] (zone-fully-inside-gt? stats (double value)))
    :gte       (fn [stats] (zone-fully-inside-gte? stats (double value)))
    :lt        (fn [stats] (zone-fully-inside-lt? stats (double value)))
    :lte       (fn [stats] (zone-fully-inside-lte? stats (double value)))
    :eq        (fn [stats] (zone-fully-inside-eq? stats (double value)))
    :neq       (fn [stats] (zone-fully-inside-neq? stats (double value)))
    :range     (let [lo (double (first value))
                     hi (double (second value))]
                 (fn [stats] (zone-fully-inside-range? stats lo hi)))
    :not-range (let [lo (double (first value))
                     hi (double (second value))]
                 (fn [stats] (zone-fully-inside-not-range? stats lo hi)))
    ;; Default: can't determine full containment
    nil))

(defn zone-may-contain-predicate
  "Create a zone map predicate from internal pred op and value(s).
   Returns a function (fn [stats] -> bool) that returns true if
   the chunk might contain matching values."
  [op value]
  (case op
    :gt        (fn [stats] (zone-may-contain-gt? stats (double value)))
    :gte       (fn [stats] (zone-may-contain-gte? stats (double value)))
    :lt        (fn [stats] (zone-may-contain-lt? stats (double value)))
    :lte       (fn [stats] (zone-may-contain-lte? stats (double value)))
    :eq        (fn [stats] (zone-may-contain-eq? stats (double value)))
    :neq       (fn [stats] (zone-may-contain-neq? stats (double value)))
    :range     (let [lo (double (first value))
                     hi (double (second value))]
                 (fn [stats] (zone-may-contain-range? stats lo hi)))
    :not-range (fn [_stats] true)  ;; can't easily prune
    ;; Default: can't skip
    (fn [_stats] true)))

(comment
  ;; Example usage
  (def s1 (->ChunkStats 100 4950.0 328350.0 0.0 99.0 0))
  (def s2 (->ChunkStats 100 14950.0 2328350.0 100.0 199.0 0))

  ;; Merge stats
  (def merged (merge-stats s1 s2))

  ;; Aggregations
  (stats-sum merged)      ;; => 19900.0
  (stats-mean merged)     ;; => 99.5
  (stats-min merged)      ;; => 0.0
  (stats-max merged)      ;; => 199.0
  (stats-variance merged) ;; sample variance

  ;; Zone maps
  (zone-may-contain-gt? s1 50.0)   ;; => true
  (zone-may-contain-gt? s1 100.0)  ;; => false (max is 99)
  ((zone-predicate :gt 100.0) s1)  ;; => false
  ((zone-predicate :gt 100.0) s2)  ;; => true
  )
