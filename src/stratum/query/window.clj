(ns stratum.query.window
  "Window function execution for analytical queries."
  (:require [stratum.query.expression :as expr]
            [stratum.query.columns :as cols])
  (:import [stratum.internal ColumnOpsAnalytics]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Partition Computation Helpers
;; ============================================================================

(defn- compute-partition-sizes
  "Compute partition sizes from sorted indices and partition keys.
   Returns a java.util.HashMap mapping partition key → size (long)."
  ^java.util.HashMap [^ints sorted-indices ^longs part-keys ^long length]
  (let [part-sizes (java.util.HashMap.)]
    (dotimes [i length]
      (let [idx (aget sorted-indices i)
            p (aget part-keys idx)
            cur (or (.get part-sizes p) 0)]
        (.put part-sizes p (inc (long cur)))))
    part-sizes))

(defn- compute-partition-boundaries
  "Compute partition start/end boundaries from sorted indices and partition keys.
   Returns [part-starts part-ends] as int-arrays indexed by sorted position."
  [^ints sorted-indices ^longs part-keys ^long length]
  (let [part-starts (int-array length)
        part-ends (int-array length)]
    ;; Find partition boundaries
    (let [p-start (int-array 1)]
      (aset p-start 0 0)
      (dotimes [i length]
        (when (and (> i 0)
                   (not= (aget part-keys (aget sorted-indices i))
                         (aget part-keys (aget sorted-indices (dec i)))))
          ;; Backfill end for previous partition
          (let [ps (aget p-start 0)]
            (dotimes [j (- i ps)]
              (aset part-ends (+ ps j) i)))
          (aset p-start 0 i)))
      ;; Backfill last partition
      (let [ps (aget p-start 0)]
        (dotimes [j (- length ps)]
          (aset part-ends (+ ps j) length)))
      ;; Fill starts
      (let [prev-p (long-array 1)]
        (aset prev-p 0 Long/MIN_VALUE)
        (let [cur-start (int-array 1)]
          (dotimes [i length]
            (let [idx (aget sorted-indices i)
                  p (aget part-keys idx)]
              (when (not= p (aget prev-p 0))
                (aset cur-start 0 i)
                (aset prev-p 0 p))
              (aset part-starts i (aget cur-start 0)))))))
    [part-starts part-ends]))

;; ============================================================================
;; Window Computation Helpers
;; ============================================================================

(defn- ^longs compute-window-partition-keys
  "Compute a composite partition key for each row given partition-by columns.
   Returns a long[] where equal values indicate same partition.
   Delegates to Java ColumnOpsAnalytics.computePartitionKeys for speed."
  [partition-by-cols col-arrays ^long length]
  (if (empty? partition-by-cols)
    (long-array length)
    (let [part-cols (into-array Object (mapv #(get col-arrays %) partition-by-cols))]
      (ColumnOpsAnalytics/computePartitionKeys part-cols length))))

(defn- get-sort-value
  "Get a comparable sort value from an array at index i."
  [arr ^long i]
  (cond
    (expr/long-array? arr) (aget ^longs arr (int i))
    (expr/double-array? arr) (aget ^doubles arr (int i))
    (expr/string-array? arr) (aget ^"[Ljava.lang.String;" arr (int i))
    :else nil))

(defn- ^"[Ljava.lang.Object;" window-order-keys
  "Extract order-by column arrays for Java sort.
   Returns [Object[] orderKeys, boolean[] orderDirs]."
  [order-by col-arrays]
  (let [oks (into-array Object (mapv (fn [[c _]] (get col-arrays c)) order-by))
        dirs (boolean-array (count order-by))]
    (dotimes [i (count order-by)]
      (aset dirs i (not= (second (nth order-by i)) :desc)))
    (into-array Object [oks dirs])))

(defn- frame-has-numeric-bounds?
  "Check if a frame has numeric ROWS bounds (not just unbounded/current)."
  [frame]
  (and frame
       (= :rows (:type frame))
       (or (vector? (:start frame))
           (vector? (:end frame)))))

(defn- compute-sliding-window-sum
  "Compute sliding window SUM using prefix sums.
   sorted-indices: row indices in partition-sorted order.
   part-keys: partition assignment for each row.
   val-arr: source values (long[] or double[]).
   start-bound/end-bound: frame bounds (keywords or [N :preceding/:following])."
  [sorted-indices part-keys val-arr length start-bound end-bound]
  (let [length (long length)
        ^ints sorted-indices sorted-indices
        ^longs part-keys part-keys
        result (double-array length)
        is-double (expr/double-array? val-arr)
        [^ints part-starts ^ints part-ends] (compute-partition-boundaries sorted-indices part-keys length)]
    ;; Build prefix sums per partition (within sorted order)
    (let [prefix (double-array (inc length))]
      (dotimes [i length]
        (let [idx (aget sorted-indices i)
              v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
              ps (aget part-starts i)]
          ;; Reset prefix at partition boundary
          (when (= i ps) (aset prefix i 0.0))
          (aset prefix (inc i) (+ (aget prefix i) v))))
      ;; Compute windowed sums
      (dotimes [i length]
        (let [idx (aget sorted-indices i)
              ps (aget part-starts i)
              pe (aget part-ends i)
              ;; Resolve frame bounds to sorted positions
              win-start (int (cond
                               (= start-bound :unbounded-preceding) ps
                               (= start-bound :current-row) i
                               (vector? start-bound)
                               (let [[n dir] start-bound]
                                 (case dir
                                   :preceding (max ps (- i (long n)))
                                   :following (min (dec pe) (+ i (long n)))))
                               :else ps))
              win-end (int (cond
                             (= end-bound :unbounded-following) pe
                             (= end-bound :current-row) (inc i)
                             (vector? end-bound)
                             (let [[n dir] end-bound]
                               (case dir
                                 :preceding (max ps (inc (- i (long n))))
                                 :following (min pe (inc (+ i (long n))))))
                             :else (inc i)))]
          (aset result idx (- (aget prefix win-end) (aget prefix win-start))))))
    result))

(defn- compute-sliding-window-count
  "Compute sliding window COUNT."
  [sorted-indices part-keys length start-bound end-bound]
  (let [^ints sorted-indices sorted-indices
        ^longs part-keys part-keys
        length (long length)
        result (double-array length)
        [^ints part-starts ^ints part-ends] (compute-partition-boundaries sorted-indices part-keys length)]
    ;; Compute counts
    (dotimes [i length]
      (let [idx (aget sorted-indices i)
            ps (aget part-starts i)
            pe (aget part-ends i)
            win-start (int (cond
                             (= start-bound :unbounded-preceding) ps
                             (= start-bound :current-row) i
                             (vector? start-bound)
                             (let [[n dir] start-bound]
                               (case dir :preceding (max ps (- i (long n))) :following (min (dec pe) (+ i (long n)))))
                             :else ps))
            win-end (int (cond
                           (= end-bound :unbounded-following) pe
                           (= end-bound :current-row) (inc i)
                           (vector? end-bound)
                           (let [[n dir] end-bound]
                             (case dir :preceding (max ps (inc (- i (long n)))) :following (min pe (inc (+ i (long n))))))
                           :else (inc i)))]
        (aset result idx (double (- win-end win-start)))))
    result))

;; ============================================================================
;; Public API
;; ============================================================================

(defn execute-window-functions
  "Execute window functions over row-level data.
   Takes the original columns map and window specs, returns updated column map
   with window result columns added.
   Uses Java ColumnOpsAnalytics for sort and hot-path ops (row-number, lag/lead, running-sum).

   window-specs: [{:op :row-number :partition-by [:cat] :order-by [[:price :asc]] :as :rn} ...]"
  [columns length window-specs]
  ;; Materialize only index columns referenced by window specs
  (let [referenced-cols (into #{}
                              (mapcat (fn [{:keys [col partition-by order-by]}]
                                        (concat (when col [col])
                                                partition-by
                                                (map first order-by))))
                              window-specs)
        columns (reduce (fn [cols k]
                          (let [v (get cols k)]
                            (if (and (map? v) (not (:data v)) (:index v))
                              (assoc cols k (cols/materialize-column v))
                              cols)))
                        columns referenced-cols)
        col-arrays (into {} (map (fn [[k v]]
                                   [k (if (map? v) (:data v) v)]))
                         columns)]
    (reduce
     (fn [cols win-spec]
       (let [{:keys [op col partition-by order-by as offset default frame]} win-spec
              ;; Compute partition keys via Java
             part-keys (compute-window-partition-keys
                        (or partition-by []) col-arrays length)
              ;; Sort via Java — returns int[] directly (no boxing)
             sorted-indices
             (let [[oks dirs] (if (seq order-by)
                                (let [arr (window-order-keys order-by col-arrays)]
                                  [(aget arr 0) (aget arr 1)])
                                [(into-array Object []) (boolean-array 0)])]
               (ColumnOpsAnalytics/windowArgSort part-keys
                                                 ^"[Ljava.lang.Object;" oks
                                                 ^booleans dirs
                                                 (int length)))
              ;; Determine if this is a full-partition frame
             full-partition-frame? (and frame
                                        (= :unbounded-preceding (:start frame))
                                        (= :unbounded-following (:end frame)))
              ;; Determine if this frame has numeric ROWS bounds (sliding window)
             sliding-frame? (frame-has-numeric-bounds? frame)
              ;; Execute window op — delegate to Java for hot paths
             result-arr
             (case op
               :row-number
               (ColumnOpsAnalytics/windowRowNumber part-keys sorted-indices (int length))

               (:lag :lead)
               (ColumnOpsAnalytics/windowLagLead part-keys sorted-indices
                                                 (get col-arrays col) (int length)
                                                 (int (or offset 1))
                                                 (double (or default Double/NaN))
                                                 (= op :lead))

               :sum
               (cond
                 sliding-frame?
                 (compute-sliding-window-sum sorted-indices part-keys
                                             (get col-arrays col) length (:start frame) (:end frame))
                 full-partition-frame?
                  ;; Full partition frame: partition totals broadcast
                 (let [val-arr (get col-arrays col)
                       part-totals (java.util.HashMap.)]
                   (dotimes [i length]
                     (let [idx (aget ^ints sorted-indices i)
                           p (aget ^longs part-keys idx)
                           v (if (expr/double-array? val-arr)
                               (aget ^doubles val-arr idx)
                               (double (aget ^longs val-arr idx)))
                           cur (or (.get part-totals p) 0.0)]
                       (.put part-totals p (+ (double cur) v))))
                   (let [result (double-array length)]
                     (dotimes [i length]
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)]
                         (aset result idx (double (.get part-totals p)))))
                     result))
                 :else
                  ;; Running sum — Java
                 (ColumnOpsAnalytics/windowRunningSum part-keys sorted-indices
                                                      (get col-arrays col) (int length)))

                ;; Remaining ops stay in Clojure (less frequent)
               :rank
               (let [result (double-array length)]
                 (loop [i (int 0), rk (long 1), prev-part Long/MIN_VALUE, prev-vals nil, same-count (long 0)]
                   (when (< i length)
                     (let [idx (aget ^ints sorted-indices i)
                           p (aget ^longs part-keys idx)
                           cur-vals (when (seq order-by)
                                      (mapv (fn [[c _]] (get-sort-value (get col-arrays c) idx)) order-by))
                           new-part? (not= p prev-part)
                           new-rk (long (cond
                                          new-part? 1
                                          (= cur-vals prev-vals) rk
                                          :else (+ rk same-count)))
                           new-same (long (if (or new-part? (not= cur-vals prev-vals)) 1 (inc same-count)))]
                       (aset result idx (double new-rk))
                       (recur (inc i) new-rk p cur-vals new-same))))
                 result)

               :dense-rank
               (let [result (double-array length)]
                 (loop [i (int 0), rk (long 1), prev-part Long/MIN_VALUE, prev-vals nil]
                   (when (< i length)
                     (let [idx (aget ^ints sorted-indices i)
                           p (aget ^longs part-keys idx)
                           cur-vals (when (seq order-by)
                                      (mapv (fn [[c _]] (get-sort-value (get col-arrays c) idx)) order-by))
                           new-part? (not= p prev-part)
                           new-rk (long (cond
                                          new-part? 1
                                          (= cur-vals prev-vals) rk
                                          :else (inc rk)))]
                       (aset result idx (double new-rk))
                       (recur (inc i) new-rk p cur-vals))))
                 result)

               :ntile
               (let [result (double-array length)
                     n-buckets (long (or col 4)) ;; col holds the NTILE argument
                     ^java.util.HashMap part-sizes (compute-partition-sizes sorted-indices part-keys length)]
                 ;; Second pass: compute NTILE
                 (loop [i (int 0), rank-in-part (long 0), prev-part Long/MIN_VALUE]
                   (when (< i length)
                     (let [idx (aget ^ints sorted-indices i)
                           p (aget ^longs part-keys idx)
                           new-part? (not= p prev-part)
                           rip (long (if new-part? 0 (inc rank-in-part)))
                           ps (long (.get part-sizes p))
                           bucket (inc (quot (* rip n-buckets) ps))]
                       (aset result idx (double bucket))
                       (recur (inc i) rip p))))
                 result)

               :percent-rank
               (let [result (double-array length)]
                 (let [^java.util.HashMap part-sizes (compute-partition-sizes sorted-indices part-keys length)]
                   (loop [i (int 0), rk (long 1), prev-part Long/MIN_VALUE, prev-vals nil, same-count (long 0)]
                     (when (< i length)
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             cur-vals (when (seq order-by)
                                        (mapv (fn [[c _]] (get-sort-value (get col-arrays c) idx)) order-by))
                             new-part? (not= p prev-part)
                             new-rk (long (cond
                                            new-part? 1
                                            (= cur-vals prev-vals) rk
                                            :else (+ rk same-count)))
                             new-same (long (if (or new-part? (not= cur-vals prev-vals)) 1 (inc same-count)))
                             ps (long (.get part-sizes p))
                             pr (if (<= ps 1) 0.0 (/ (double (dec new-rk)) (double (dec ps))))]
                         (aset result idx pr)
                         (recur (inc i) new-rk p cur-vals new-same)))))
                 result)

               :cume-dist
               (let [result (double-array length)]
                 (let [^java.util.HashMap part-sizes (compute-partition-sizes sorted-indices part-keys length)]
                    ;; Since rows are sorted, we need to find the last row with equal values
                    ;; and use its position as the count
                   (loop [i (int 0), prev-part Long/MIN_VALUE]
                     (when (< i length)
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             new-part? (not= p prev-part)
                             cur-vals (when (seq order-by)
                                        (mapv (fn [[c _]] (get-sort-value (get col-arrays c) idx)) order-by))
                              ;; Find last row with same values (look ahead)
                             last-pos (loop [j (inc i)]
                                        (if (and (< j length)
                                                 (= (aget ^longs part-keys (aget ^ints sorted-indices j)) p)
                                                 (= (when (seq order-by)
                                                      (mapv (fn [[c _]] (get-sort-value (get col-arrays c) (aget ^ints sorted-indices j))) order-by))
                                                    cur-vals))
                                          (recur (inc j))
                                          j))
                              ;; Find partition start
                             part-start (if new-part? i
                                            (loop [j (dec i)]
                                              (if (and (>= j 0)
                                                       (= (aget ^longs part-keys (aget ^ints sorted-indices j)) p))
                                                (recur (dec j))
                                                (inc j))))
                             pos-in-part (- last-pos part-start)
                             ps (long (.get part-sizes p))
                             cd (/ (double pos-in-part) (double ps))]
                         (aset result idx cd)
                         (recur (inc i) p)))))
                 result)

               :count
               (if sliding-frame?
                 (compute-sliding-window-count sorted-indices part-keys length
                                               (:start frame) (:end frame))
                 (let [result (double-array length)]
                   (if full-partition-frame?
                     (let [^java.util.HashMap part-counts (compute-partition-sizes sorted-indices part-keys length)]
                       (dotimes [i length]
                         (let [idx (aget ^ints sorted-indices i)
                               p (aget ^longs part-keys idx)]
                           (aset result idx (double (.get part-counts p))))))
                     (loop [i (int 0), cnt (long 0), prev-part Long/MIN_VALUE]
                       (when (< i length)
                         (let [idx (aget ^ints sorted-indices i)
                               p (aget ^longs part-keys idx)
                               new-cnt (long (if (= p prev-part) (inc cnt) 1))]
                           (aset result idx (double new-cnt))
                           (recur (inc i) new-cnt p)))))
                   result))

               :avg
               (if sliding-frame?
                  ;; AVG = SUM / COUNT over the sliding window
                 (let [sums (compute-sliding-window-sum sorted-indices part-keys
                                                        (get col-arrays col) length (:start frame) (:end frame))
                       cnts (compute-sliding-window-count sorted-indices part-keys length
                                                          (:start frame) (:end frame))
                       result (double-array length)]
                   (dotimes [i length]
                     (aset result i (/ (aget ^doubles sums i)
                                       (Math/max 1.0 (aget ^doubles cnts i)))))
                   result)
                 (let [result (double-array length)
                       val-arr (get col-arrays col)
                       is-double (expr/double-array? val-arr)]
                   (if full-partition-frame?
                     (let [part-totals (java.util.HashMap.)
                           part-counts (java.util.HashMap.)]
                       (dotimes [i length]
                         (let [idx (aget ^ints sorted-indices i)
                               p (aget ^longs part-keys idx)
                               v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))]
                           (.put part-totals p (+ (double (or (.get part-totals p) 0.0)) v))
                           (.put part-counts p (inc (long (or (.get part-counts p) 0))))))
                       (dotimes [i length]
                         (let [idx (aget ^ints sorted-indices i)
                               p (aget ^longs part-keys idx)]
                           (aset result idx (/ (double (.get part-totals p))
                                               (double (.get part-counts p)))))))
                     (loop [i (int 0), running-sum 0.0, cnt (long 0), prev-part Long/MIN_VALUE]
                       (when (< i length)
                         (let [idx (aget ^ints sorted-indices i)
                               p (aget ^longs part-keys idx)
                               v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
                               new-sum (if (= p prev-part) (+ running-sum v) v)
                               new-cnt (long (if (= p prev-part) (inc cnt) 1))]
                           (aset result idx (/ new-sum (double new-cnt)))
                           (recur (inc i) new-sum new-cnt p)))))
                   result))

               :min
               (let [result (double-array length)
                     val-arr (get col-arrays col)
                     is-double (expr/double-array? val-arr)]
                 (if full-partition-frame?
                   (let [part-mins (java.util.HashMap.)]
                     (dotimes [i length]
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
                             cur (or (.get part-mins p) Double/POSITIVE_INFINITY)]
                         (.put part-mins p (Math/min (double cur) v))))
                     (dotimes [i length]
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)]
                         (aset result idx (double (.get part-mins p))))))
                   (loop [i (int 0), running Double/POSITIVE_INFINITY, prev-part Long/MIN_VALUE]
                     (when (< i length)
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
                             new-running (if (= p prev-part) (Math/min running v) v)]
                         (aset result idx new-running)
                         (recur (inc i) new-running p)))))
                 result)

               :max
               (let [result (double-array length)
                     val-arr (get col-arrays col)
                     is-double (expr/double-array? val-arr)]
                 (if full-partition-frame?
                   (let [part-maxs (java.util.HashMap.)]
                     (dotimes [i length]
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
                             cur (or (.get part-maxs p) Double/NEGATIVE_INFINITY)]
                         (.put part-maxs p (Math/max (double cur) v))))
                     (dotimes [i length]
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)]
                         (aset result idx (double (.get part-maxs p))))))
                   (loop [i (int 0), running Double/NEGATIVE_INFINITY, prev-part Long/MIN_VALUE]
                     (when (< i length)
                       (let [idx (aget ^ints sorted-indices i)
                             p (aget ^longs part-keys idx)
                             v (if is-double (aget ^doubles val-arr idx) (double (aget ^longs val-arr idx)))
                             new-running (if (= p prev-part) (Math/max running v) v)]
                         (aset result idx new-running)
                         (recur (inc i) new-running p)))))
                 result))]
          ;; Add window result to columns
         (assoc cols as {:type :float64 :data result-arr})))
     columns
     window-specs)))
