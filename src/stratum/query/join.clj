(ns stratum.query.join
  "Hash join execution for Stratum queries: build/probe, fused join+group-by,
   fused join+global-agg, multi-column join keys."
  (:require [stratum.query.normalization :as norm]
            [stratum.query.expression :as expr]
            [stratum.query.columns :as cols]
            [stratum.query.group-by :as gb]
            [stratum.index :as index])
  (:import [stratum.internal ColumnOps ColumnOpsExt]))

(set! *warn-on-reflection* true)

;; ============================================================================

(defn normalize-join-on
  "Normalize :on spec to vector of [left-col right-col] pairs.
   Single: [:= :a :b] → [[:a :b]]
   Multi:  [[:= :a :x] [:= :b :y]] → [[:a :x] [:b :y]]"
  [on-spec]
  (if (and (vector? on-spec) (= := (first on-spec)))
    ;; Single: [:= :left :right]
    [[(norm/strip-ns (second on-spec)) (norm/strip-ns (nth on-spec 2))]]
    ;; Multi: [[:= :a :x] [:= :b :y]]
    (mapv (fn [clause]
            (let [c (vec clause)]
              (when-not (= := (first c))
                (throw (ex-info "Join :on clause must use := operator"
                                {:clause clause})))
              [(norm/strip-ns (second c)) (norm/strip-ns (nth c 2))]))
          on-spec)))

(defn normalize-join-spec
  "Normalize a single join spec to internal form."
  [join-spec]
  (let [{:keys [with on type]} join-spec
        join-type (or type :inner)]
    (when-not (#{:inner :left :right :full} join-type)
      (throw (ex-info (str "Unknown join type: " join-type)
                      {:type join-type})))
    (when-not (map? with)
      (throw (ex-info "Join :with must be a map of {col-name data}"
                      {:with with})))
    (when-not on
      (throw (ex-info "Join :on is required" {:join-spec join-spec})))
    {:with (cols/prepare-columns with)
     :on-pairs (normalize-join-on on)
     :type join-type}))

(defn column-length
  "Get length of a column (already prepared)."
  ^long [col-info]
  (let [d (:data col-info)]
    (cond
      (expr/long-array? d) (alength ^longs d)
      (expr/double-array? d) (alength ^doubles d)
      :else (index/idx-length (:index col-info)))))

(defn unify-dict-keys
  "Unify two dict-encoded columns with potentially different dictionaries.
   Returns [recoded-left-keys recoded-right-keys] as long arrays."
  [left-col left-len right-col right-len]
  (let [^"[Ljava.lang.String;" left-dict (:dict left-col)
        ^"[Ljava.lang.String;" right-dict (:dict right-col)
        ^longs left-data (:data left-col)
        ^longs right-data (:data right-col)
        ;; Build unified dictionary
        unified-map (java.util.HashMap.)
        next-id (long-array 1)]
    ;; Add left dict entries
    (dotimes [i (alength left-dict)]
      (let [s (aget left-dict i)]
        (when-not (.containsKey unified-map s)
          (let [id (aget next-id 0)]
            (.put unified-map s id)
            (aset next-id 0 (inc id))))))
    ;; Add right dict entries
    (dotimes [i (alength right-dict)]
      (let [s (aget right-dict i)]
        (when-not (.containsKey unified-map s)
          (let [id (aget next-id 0)]
            (.put unified-map s id)
            (aset next-id 0 (inc id))))))
    ;; Recode left
    (let [left-remap (long-array (alength left-dict))
          _ (dotimes [i (alength left-dict)]
              (aset left-remap i (long (.get unified-map (aget left-dict i)))))
          left-out (long-array left-len)
          _ (dotimes [i left-len]
              (let [v (aget left-data i)]
                (aset left-out i (if (= v Long/MIN_VALUE) Long/MIN_VALUE
                                     (aget left-remap (int v))))))
          ;; Recode right
          right-remap (long-array (alength right-dict))
          _ (dotimes [i (alength right-dict)]
              (aset right-remap i (long (.get unified-map (aget right-dict i)))))
          right-out (long-array right-len)
          _ (dotimes [i right-len]
              (let [v (aget right-data i)]
                (aset right-out i (if (= v Long/MIN_VALUE) Long/MIN_VALUE
                                      (aget right-remap (int v))))))]
      [left-out right-out])))

(defn unify-all-dict-keys
  "Unify dict-encoded columns across all join on-pairs (not just single-column).
   For each pair where both sides are dict-encoded, unify their dicts.
   Returns [updated-left-columns updated-right-columns]."
  [left-columns right-columns on-pairs left-length right-length]
  (let [left-length (long left-length)
        right-length (long right-length)]
    (reduce (fn [[lcols rcols] [lk rk]]
              (if (and (:dict (get lcols lk)) (:dict (get rcols rk)))
                (let [[recoded-l recoded-r]
                      (unify-dict-keys (get lcols lk) left-length
                                       (get rcols rk) right-length)]
                  [(assoc lcols lk (assoc (get lcols lk) :data recoded-l))
                   (assoc rcols rk (assoc (get rcols rk) :data recoded-r))])
                [lcols rcols]))
            [left-columns right-columns]
            (map vector
                 (mapv first on-pairs)
                 (mapv second on-pairs)))))

(defn extract-key-col-arrays
  "Extract long[] arrays for join key columns, converting doubles to long bits."
  [columns on-cols ^long length]
  (let [col-infos (mapv #(get columns %) on-cols)]
    (mapv (fn [ci]
            (if (= :float64 (:type ci))
              (let [^doubles dd (:data ci)
                    out (long-array length)]
                (dotimes [i length]
                  (let [dv (aget dd i)]
                    (aset out i (if (Double/isNaN dv)
                                  Long/MIN_VALUE
                                  (Double/doubleToRawLongBits dv)))))
                out)
              ^longs (:data ci)))
          col-infos)))

(defn array-max-positive
  "Find the max value in a long[], treating Long/MIN_VALUE as 0."
  ^long [^longs d ^long len ^long init-mx]
  (loop [i 0 mx init-mx]
    (if (>= i len) mx
        (let [v (aget d i)
              v (if (= v Long/MIN_VALUE) 0 v)]
          (recur (inc i) (long (max mx v)))))))

(defn compute-key-muls
  "Compute multipliers for composite key encoding from multiple key column arrays.
   Takes two vectors of long[] col-arrays (one per side) and computes
   multipliers based on the combined max across both sides."
  ^longs [left-col-arrays right-col-arrays ^long n-cols]
  (let [maxes (long-array n-cols)
        _ (dotimes [c n-cols]
            (let [^longs ld (nth left-col-arrays c)
                  ^longs rd (nth right-col-arrays c)
                  mx1 (array-max-positive ld (alength ld) 0)
                  mx2 (array-max-positive rd (alength rd) mx1)]
              (aset maxes c (inc mx2))))
        muls (long-array n-cols)
        _ (do (aset muls 0 1)
              (loop [c 1]
                (when (< c n-cols)
                  (aset muls c (* (aget muls (dec c)) (aget maxes (dec c))))
                  (recur (inc c)))))]
    muls))

(defn compute-join-key
  "Compute join key column(s) for one side.
   For single-column: returns the key array directly (long[]).
   For multi-column: computes composite key via multiplier encoding.
   For double[] keys: converts via Double.doubleToRawLongBits.
   For dict-encoded: uses the encoded long[] directly."
  (^longs [columns on-cols ^long length]
   (if (= 1 (count on-cols))
     ;; Single column key
     (let [col-name (first on-cols)
           col-info (get columns col-name)]
       (when-not col-info
         (throw (ex-info (str "Join key column not found: " col-name)
                         {:column col-name :available (keys columns)})))
       (if (= :float64 (:type col-info))
         (let [^doubles d (:data col-info)
               out (long-array length)]
           (dotimes [i length]
             (aset out i (Double/doubleToRawLongBits (aget d i))))
           out)
         (:data col-info)))
     ;; Multi-column: composite key via multiplier encoding
     (let [col-arrays (extract-key-col-arrays columns on-cols length)
           n-cols (count on-cols)
           muls (compute-key-muls col-arrays col-arrays n-cols)]
       (ColumnOpsExt/compositeKeyEncode
        (into-array expr/long-array-class col-arrays) muls (int length)))))
  (^longs [columns on-cols ^long length ^longs muls]
   ;; Multi-column with pre-computed multipliers (for consistent encoding across sides)
   (if (= 1 (count on-cols))
     (compute-join-key columns on-cols length)
     (let [col-arrays (extract-key-col-arrays columns on-cols length)]
       (ColumnOpsExt/compositeKeyEncode
        (into-array expr/long-array-class col-arrays) muls (int length))))))

(defn gather-column
  "Gather a single column using index array. Dispatches on type."
  [col-info ^ints indices ^long result-length]
  (let [d (:data col-info)]
    (cond
      (expr/long-array? d)
      (let [gathered (ColumnOps/gatherLong ^longs d indices (int result-length))]
        (assoc col-info :data gathered))

      (expr/double-array? d)
      (let [gathered (ColumnOps/gatherDouble ^doubles d indices (int result-length))]
        (assoc col-info :data gathered))

      :else
      (throw (ex-info "Unsupported column type for gather" {:col-info col-info})))))

(defn fused-join-group-agg-eligible?
  "Check if a join+group+agg query can use the fused Java path.
   Requires: single INNER join, group cols from dim side, agg cols from fact side,
   all group cols int64, dense-eligible key space, supported agg ops."
  [join-specs group aggs from-columns]
  (let [supported-agg-ops #{:sum :count :min :max :avg :sum-product}]
    (and (= 1 (count join-specs))
         (seq group)
         (seq aggs)
         (every? keyword? group)
         (every? #(supported-agg-ops (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs) ;; no expression aggs
         (let [spec (first join-specs)
               join-type (or (:type spec) :inner)]
           (= :inner join-type)))))

(defn perfect-hash-eligible?
  "Check if build-side keys qualify for perfect hash join (direct array indexing).
   Returns {:min-key long :key-range int} or nil."
  [^longs build-keys ^long build-length]
  (let [^longs mm (ColumnOpsExt/arrayMaxMinLong build-keys (int build-length))
        max-key (aget mm 0)
        min-key (aget mm 1)]
    (when (and (not= min-key Long/MIN_VALUE)  ;; no NULL-only keys
               (>= max-key min-key))
      (let [key-range (inc (- max-key min-key))]
        (when (and (> key-range 0)
                   (<= key-range (long ColumnOpsExt/PERFECT_HASH_MAX_RANGE)))
          {:min-key min-key :key-range (int key-range)})))))

(defn execute-fused-join-group-agg
  "Execute fused join+group+aggregate in Java — single pass, no intermediate arrays.
   Returns decoded results in same format as gb/execute-group-by-java."
  [from-columns fact-length join-spec group-cols aggs columnar?]
  (let [fact-length (long fact-length)
        {:keys [with on-pairs type]} (normalize-join-spec join-spec)
        right-columns (cols/materialize-columns with)
        right-length (column-length (val (first right-columns)))
        ;; left = fact (probe), right = dim (build)
        left-columns (cols/materialize-columns from-columns)
        left-key-cols (mapv first on-pairs)
        right-key-cols (mapv second on-pairs)
        ;; Handle dict-encoded string keys (all on-pairs, not just single-column)
        [left-columns right-columns]
        (unify-all-dict-keys left-columns right-columns on-pairs fact-length right-length)
        ;; Build-side keys
        right-keys (compute-join-key right-columns right-key-cols right-length)
        ;; Check perfect hash eligibility
        perfect-info (perfect-hash-eligible? right-keys right-length)
        ;; Probe keys from fact (left) side
        probe-keys (compute-join-key left-columns left-key-cols fact-length)
        ;; Prepare group columns — these come from dim (right) side
        ;; We need the raw dim arrays (not gathered), indexed by dimIdx in Java
        n-group (count group-cols)
        ;; Group cols must reference right-side columns
        group-col-data (mapv #(:data (get right-columns %)) group-cols)
        group-dicts (let [dicts (mapv #(:dict (get right-columns %)) group-cols)]
                      (when (some identity dicts) dicts))
        group-arrays (into-array expr/long-array-class group-col-data)
        ;; Compute max value per group column for tight strides (Java for speed)
        group-maxes (mapv (fn [^longs col]
                            (let [^longs mm (ColumnOpsExt/arrayMaxMinLong col (alength col))]
                              (aget mm 0)))
                          group-col-data)
        group-muls (try
                     (long-array
                      (let [muls (long-array n-group)]
                        (aset muls (dec n-group) 1)
                        (loop [i (- n-group 2)]
                          (when (>= i 0)
                            (aset muls i (Math/multiplyExact
                                          (inc (long (nth group-maxes (inc i))))
                                          (aget muls (inc i))))
                            (recur (dec i))))
                        (vec muls)))
                     (catch ArithmeticException _
                       (throw (ex-info "Join group-by key space overflows long"
                                       {:group-cols group-cols :group-maxes group-maxes}))))
        max-key (try
                  (long (reduce (fn [^long acc i]
                                  (Math/addExact acc
                                                 (Math/multiplyExact (long (nth group-maxes i))
                                                                     (aget ^longs group-muls (int i)))))
                                (long 0) (range n-group)))
                  (catch ArithmeticException _
                    (throw (ex-info "Join group-by key space overflows long"
                                    {:group-cols group-cols :group-maxes group-maxes}))))
        ;; Prepare aggregate types and source columns (from fact/left side)
        n-aggs (count aggs)
        agg-types (int-array (mapv (fn [agg]
                                     (case (:op agg)
                                       (:sum :sum-product) (int ColumnOps/AGG_SUM)
                                       :count (int ColumnOps/AGG_COUNT)
                                       :min   (int ColumnOps/AGG_MIN)
                                       :max   (int ColumnOps/AGG_MAX)
                                       :avg   (int ColumnOps/AGG_SUM)
                                       (int ColumnOps/AGG_SUM)))
                                   aggs))
        ;; Fact-side agg columns (pre-multiplied for SUM_PRODUCT — join path uses AGG_SUM)
        fact-col-arrays (into {} (map (fn [[k v]] [k (:data v)])) left-columns)
        conv-cache (java.util.HashMap.)
        agg-cols (into-array expr/double-array-class
                             (mapv #(gb/prepare-agg-source-col-premul % fact-col-arrays fact-length conv-cache) aggs))
        ;; Call fused Java method — returns flat double[] (same layout as dense group-by)
        ^doubles result-array
        (if perfect-info
          ;; Perfect hash: O(1) probe via direct array indexing
          (let [{:keys [min-key key-range]} perfect-info
                pht (ColumnOpsExt/perfectHashJoinBuild right-keys (int right-length) (long min-key) (int key-range))
                ^ints pf (aget ^objects pht 0)
                ^ints pn (aget ^objects pht 1)
                is-unique (boolean (aget ^objects pht 2))]
            (ColumnOpsExt/perfectJoinGroupAggregateDenseParallel
             pf pn is-unique (long min-key) (int key-range)
             ^longs probe-keys (int fact-length)
             (int n-group) group-arrays group-muls
             (int n-aggs) agg-types ^"[[D" agg-cols
             (int (inc max-key))))
          ;; Hash table fallback
          (let [ht (ColumnOps/hashJoinBuild right-keys (int right-length))
                ^longs ht-keys (aget ^objects ht 0)
                ^ints ht-first (aget ^objects ht 1)
                ^ints ht-next (aget ^objects ht 2)
                capacity (int (aget ^objects ht 3))]
            (ColumnOps/fusedJoinGroupAggregateDenseParallel
             ht-keys ht-first ht-next capacity
             ^longs probe-keys (int fact-length)
             (int n-group) group-arrays group-muls
             (int n-aggs) agg-types ^"[[D" agg-cols
             (int (inc max-key)))))
        max-key-inc (inc max-key)]
    ;; Decode results using flat decode functions (fused join now returns flat double[])
    (if columnar?
      (gb/decode-dense-group-results-columnar result-array max-key-inc group-cols group-muls aggs group-dicts)
      (gb/decode-dense-group-results result-array max-key-inc group-cols group-muls aggs group-dicts))))

(defn fused-join-global-agg-eligible?
  "Check if a join+global-agg (no GROUP BY) can use the fused Java path.
   Requires: single join (INNER or LEFT), supported agg ops, no expressions."
  [join-specs aggs]
  (let [supported-agg-ops #{:sum :count :min :max :avg :sum-product}]
    (and (= 1 (count join-specs))
         (seq aggs)
         (every? #(supported-agg-ops (:op %)) aggs)
         (every? #(nil? (:expr %)) aggs)
         (let [spec (first join-specs)
               join-type (or (:type spec) :inner)]
           (#{:inner :left} join-type)))))

(defn execute-fused-join-global-agg
  "Execute fused join+global-aggregate — single pass probe+accumulate, no gather.
   Returns a single-row result vector [{alias1 val1, alias2 val2, :_count N}]."
  [from-columns fact-length join-spec aggs]
  (let [fact-length (long fact-length)
        {:keys [with on-pairs type]} (normalize-join-spec join-spec)
        join-type (or type :inner)
        is-left (= :left join-type)
        right-columns (cols/materialize-columns with)
        right-length (column-length (val (first right-columns)))
        left-columns (cols/materialize-columns from-columns)
        left-key-cols (mapv first on-pairs)
        right-key-cols (mapv second on-pairs)
        ;; Handle dict-encoded string keys (all on-pairs, not just single-column)
        [left-columns right-columns]
        (unify-all-dict-keys left-columns right-columns on-pairs fact-length right-length)
        ;; Multi-col key setup: compute consistent multipliers from both sides
        multi-col? (> (count on-pairs) 1)
        left-key-col-arrays (when multi-col?
                              (extract-key-col-arrays left-columns left-key-cols fact-length))
        right-key-col-arrays (when multi-col?
                               (extract-key-col-arrays right-columns right-key-cols right-length))
        shared-muls (when multi-col?
                      (compute-key-muls left-key-col-arrays right-key-col-arrays
                                        (count on-pairs)))
        ;; Build-side keys
        right-keys (if multi-col?
                     (ColumnOpsExt/compositeKeyEncode
                      (into-array expr/long-array-class right-key-col-arrays)
                      shared-muls (int right-length))
                     (compute-join-key right-columns right-key-cols right-length))
        ;; Check perfect hash eligibility
        perfect-info (perfect-hash-eligible? right-keys right-length)
        ;; Determine which side each agg column comes from
        left-col-keys (set (keys left-columns))
        right-col-keys (set (keys right-columns))
        left-col-arrays (into {} (map (fn [[k v]] [k (:data v)])) left-columns)
        right-col-arrays (into {} (map (fn [[k v]] [k (:data v)])) right-columns)
        conv-cache-left (java.util.HashMap.)
        conv-cache-right (java.util.HashMap.)
        n-aggs (count aggs)
        agg-types (int-array (mapv (fn [agg]
                                     (case (:op agg)
                                       (:sum :sum-product) (int ColumnOps/AGG_SUM)
                                       :count (int ColumnOps/AGG_COUNT)
                                       :min   (int ColumnOps/AGG_MIN)
                                       :max   (int ColumnOps/AGG_MAX)
                                       :avg   (int ColumnOps/AGG_SUM)
                                       (int ColumnOps/AGG_SUM)))
                                   aggs))
        ;; Call fused Java method — perfect hash or hash table fallback
        ^doubles result-array
        (if perfect-info
          ;; Perfect hash: use mixed-type path (skip longToDouble conversion)
          (let [{:keys [min-key key-range]} perfect-info
                pht (ColumnOpsExt/perfectHashJoinBuild right-keys (int right-length) (long min-key) (int key-range))
                ^ints pf (aget ^objects pht 0)
                ^ints pn (aget ^objects pht 1)
                is-unique (boolean (aget ^objects pht 2))
                ;; Raw typed columns: long[] or double[] or nil — no conversion
                fact-agg-raw (into-array Object
                                         (mapv (fn [agg]
                                                 (let [col (:col agg)]
                                                   (if (or (nil? col) (contains? left-col-keys col))
                                                     (gb/prepare-agg-raw-col agg left-col-arrays fact-length conv-cache-left)
                                                     nil)))
                                               aggs))
                dim-agg-raw (into-array Object
                                        (mapv (fn [agg]
                                                (let [col (:col agg)]
                                                  (if (and col (contains? right-col-keys col))
                                                    (gb/prepare-agg-raw-col agg right-col-arrays right-length conv-cache-right)
                                                    nil)))
                                              aggs))]
            (if multi-col?
              (ColumnOpsExt/perfectJoinGlobalAggregateMixedInlineKeyParallel
               pf pn is-unique (long min-key) (int key-range)
               (into-array expr/long-array-class left-key-col-arrays)
               ^longs shared-muls (int (count on-pairs))
               (int fact-length)
               (int n-aggs) agg-types
               ^"[Ljava.lang.Object;" fact-agg-raw ^"[Ljava.lang.Object;" dim-agg-raw
               is-left)
              (let [probe-keys (compute-join-key left-columns left-key-cols fact-length)]
                (ColumnOpsExt/perfectJoinGlobalAggregateMixedParallel
                 pf pn is-unique (long min-key) (int key-range)
                 ^longs probe-keys (int fact-length)
                 (int n-aggs) agg-types
                 ^"[Ljava.lang.Object;" fact-agg-raw ^"[Ljava.lang.Object;" dim-agg-raw
                 is-left))))
          ;; Hash table fallback: convert to double[][] as before
          (let [fact-agg-cols (into-array expr/double-array-class
                                          (mapv (fn [agg]
                                                  (let [col (:col agg)]
                                                    (if (or (nil? col) (contains? left-col-keys col))
                                                      (gb/prepare-agg-source-col-premul agg left-col-arrays fact-length conv-cache-left)
                                                      nil)))
                                                aggs))
                dim-agg-cols (into-array expr/double-array-class
                                         (mapv (fn [agg]
                                                 (let [col (:col agg)]
                                                   (if (and col (contains? right-col-keys col))
                                                     (gb/prepare-agg-source-col-premul agg right-col-arrays right-length conv-cache-right)
                                                     nil)))
                                               aggs))
                ht (ColumnOps/hashJoinBuild right-keys (int right-length))
                ^longs ht-keys (aget ^objects ht 0)
                ^ints ht-first (aget ^objects ht 1)
                ^ints ht-next (aget ^objects ht 2)
                capacity (int (aget ^objects ht 3))]
            (if multi-col?
              (ColumnOpsExt/hashJoinGlobalAggregateInlineKeyParallel
               ht-keys ht-first ht-next capacity
               (into-array expr/long-array-class left-key-col-arrays)
               ^longs shared-muls (int (count on-pairs))
               (int fact-length)
               (int n-aggs) agg-types
               ^"[[D" fact-agg-cols ^"[[D" dim-agg-cols
               is-left)
              (let [probe-keys (compute-join-key left-columns left-key-cols fact-length)]
                (ColumnOpsExt/hashJoinGlobalAggregateParallel
                 ht-keys ht-first ht-next capacity
                 ^longs probe-keys (int fact-length)
                 (int n-aggs) agg-types
                 ^"[[D" fact-agg-cols ^"[[D" dim-agg-cols
                 is-left)))))
        ;; Decode single-row result
        total-count (long (aget result-array 1))]
    [(reduce
      (fn [row [idx agg]]
        (let [off (* (int idx) 2)
              sum-val (aget result-array off)
              cnt (long (aget result-array (inc off)))
              alias (keyword (or (:as agg) (:op agg)))
              value (case (:op agg)
                      (:count :count-non-null) cnt
                      :avg (if (zero? cnt) nil (/ sum-val (double cnt)))
                      (:sum :sum-product :min :max) (if (zero? cnt) nil sum-val)
                      sum-val)]
          (assoc row alias value)))
      {:_count total-count}
      (map-indexed vector aggs))]))

(defn execute-join
  "Execute a single join: left columns + join spec → merged columns + new length."
  [left-columns ^long left-length join-spec]
  (let [{:keys [with on-pairs type]} join-spec
        right-columns (cols/materialize-columns with)
        right-length (column-length (val (first right-columns)))
        ;; Get left/right key column names
        left-key-cols (mapv first on-pairs)
        right-key-cols (mapv second on-pairs)
        ;; Handle dict-encoded string keys: unify dicts if both sides have dicts (all on-pairs)
        [left-columns right-columns]
        (unify-all-dict-keys left-columns right-columns on-pairs left-length right-length)
        ;; Compute join keys
        left-keys (compute-join-key left-columns left-key-cols left-length)
        right-keys (compute-join-key right-columns right-key-cols right-length)
        ;; For INNER: build on smaller side. For LEFT: build on right. For RIGHT: swap to LEFT.
        [probe-keys build-keys probe-length build-length swap?]
        (case type
          :inner (if (<= right-length left-length)
                   [left-keys right-keys left-length right-length false]
                   [right-keys left-keys right-length left-length true])
          :left  [left-keys right-keys left-length right-length false]
          :right [right-keys left-keys right-length left-length true]
          :full  [left-keys right-keys left-length right-length false])
        ;; Check perfect hash eligibility on build side
        perfect-info (when (not= :full type)
                       (perfect-hash-eligible? build-keys build-length))
        ;; Build + Probe
        probe-result
        (if perfect-info
          ;; Perfect hash: O(1) probe
          (let [{:keys [min-key key-range]} perfect-info
                pht (ColumnOpsExt/perfectHashJoinBuild build-keys (int build-length) (long min-key) (int key-range))
                ^ints pf (aget ^objects pht 0)
                ^ints pn (aget ^objects pht 1)
                is-unique (boolean (aget ^objects pht 2))]
            (case type
              :inner (ColumnOpsExt/perfectJoinProbeInner pf pn is-unique
                                                         probe-keys (int probe-length)
                                                         (long min-key) (int key-range))
              (:left :right)
              (ColumnOpsExt/perfectJoinProbeLeft pf pn is-unique
                                                 probe-keys (int probe-length)
                                                 (long min-key) (int key-range)
                                                 (int build-length))))
          ;; Hash table fallback
          (let [ht (ColumnOps/hashJoinBuild build-keys (int build-length))
                ^longs ht-keys (aget ^objects ht 0)
                ^ints ht-first (aget ^objects ht 1)
                ^ints ht-next (aget ^objects ht 2)
                capacity (int (aget ^objects ht 3))]
            (case type
              :inner (ColumnOps/hashJoinProbeInner ht-keys ht-first ht-next capacity
                                                   probe-keys (int probe-length))
              (:left :right)
              (ColumnOps/hashJoinProbeLeft ht-keys ht-first ht-next capacity
                                           probe-keys (int probe-length) (int build-length))
              :full
              (ColumnOps/hashJoinProbeLeft ht-keys ht-first ht-next capacity
                                           probe-keys (int probe-length) (int build-length)))))
        ^ints probe-indices (aget ^objects probe-result 0)
        ^ints build-indices (aget ^objects probe-result 1)
        result-length (int (aget ^objects probe-result 2))
        ;; For FULL: append unmatched build rows
        [probe-indices build-indices result-length]
        (if (= :full type)
          (let [^booleans build-matched (aget ^objects probe-result 3)
                unmatched (loop [i 0 cnt 0]
                            (if (>= i build-length)
                              cnt
                              (recur (inc i) (if (aget build-matched i) cnt (inc cnt)))))
                total (+ result-length unmatched)]
            (if (zero? unmatched)
              [probe-indices build-indices result-length]
              (let [^ints new-probe (java.util.Arrays/copyOf probe-indices (int total))
                    ^ints new-build (java.util.Arrays/copyOf build-indices (int total))]
                (loop [i 0 pos (int result-length)]
                  (when (< i build-length)
                    (when-not (aget build-matched i)
                      (aset new-probe pos (int -1))
                      (aset new-build pos (int i)))
                    (recur (inc i)
                           (int (if (aget build-matched i) pos (inc pos))))))
                [new-probe new-build total])))
          [probe-indices build-indices result-length])
        ;; Swap back if we swapped probe/build for INNER or RIGHT
        [^ints left-indices ^ints right-indices]
        (if swap?
          [build-indices probe-indices]
          [probe-indices build-indices])
        ;; Determine right key columns to drop (redundant with left key)
        drop-right-keys (if (#{:inner :left} type)
                          (set right-key-cols)
                          #{})
        ;; Gather left columns
        merged (reduce-kv
                (fn [m k v]
                  (assoc m k (gather-column v left-indices result-length)))
                {} left-columns)
        ;; Gather right columns (skip join key cols for inner/left)
        merged (reduce-kv
                (fn [m k v]
                  (if (contains? drop-right-keys k)
                    m
                    (assoc m k (gather-column v right-indices result-length))))
                merged right-columns)]
    {:columns merged :length result-length}))

(defn execute-joins
  "Execute a chain of joins sequentially."
  [columns ^long length join-specs]
  (reduce (fn [{:keys [columns length]} join-spec]
            (let [spec (normalize-join-spec join-spec)]
              (execute-join (cols/materialize-columns columns) length spec)))
          {:columns columns :length length}
          join-specs))
