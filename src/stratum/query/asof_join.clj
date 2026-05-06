(ns stratum.query.asof-join
  "ASOF join execution: radix-partition both sides on equality keys,
   sort each partition by the inequality column, two-pointer merge
   with a monotonic build cursor and at-most-one match per probe row.

   Kept separate from stratum.query.join to avoid JIT cross-section
   interference with the equi-join hot path."
  (:require [stratum.query.expression :as expr]
            [stratum.query.join :as jn])
  (:import [stratum.internal ColumnOps ColumnOpsExt ColumnOpsAsof]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Spec normalization & validation
;; ============================================================================

(def ^:private supported-ops #{:>= :> :<= :<})

(def ^:private op->code
  {:>= ColumnOpsAsof/OP_GTE
   :>  ColumnOpsAsof/OP_GT
   :<= ColumnOpsAsof/OP_LTE
   :<  ColumnOpsAsof/OP_LT})

(defn normalize-asof-spec
  "Validate and normalize an ASOF join spec.
   Returns {:join-type :on-pairs :match-condition}."
  [{:keys [join-type on-pairs match-condition] :as spec}]
  (when-not (#{:inner :left} join-type)
    (throw (ex-info (str "ASOF join-type must be :inner or :left, got " join-type)
                    {:spec spec})))
  (when-not (and (vector? match-condition) (= 3 (count match-condition))
                 (contains? supported-ops (first match-condition)))
    (throw (ex-info "ASOF :match-condition must be [op left-col right-col] with op ∈ #{:>= :> :<= :<}"
                    {:match-condition match-condition})))
  {:join-type join-type
   :on-pairs (or on-pairs [])
   :match-condition match-condition})

;; ============================================================================
;; Inequality-column extraction
;; ============================================================================

(defn- get-long-column
  "Extract a long[] from a materialized column map. Doubles are converted to
   longs (rounded toward zero). Throws on missing or unsupported type."
  ^longs [columns col-key]
  (let [info (get columns col-key)]
    (when-not info
      (throw (ex-info "ASOF match-condition column not found"
                      {:column col-key :available (vec (keys columns))})))
    (let [d (:data info)]
      (cond
        (expr/long-array? d) d
        (expr/double-array? d)
        (let [^doubles src d
              n (alength src)
              out (long-array n)]
          (dotimes [i n] (aset out i (long (aget src i))))
          out)
        :else
        (throw (ex-info "ASOF match-condition column must be long[] or double[]"
                        {:column col-key :type (class d)}))))))

;; ============================================================================
;; Materialization
;; ============================================================================

(defn- gather-col
  [col-info ^ints indices ^long result-length]
  (let [d (:data col-info)]
    (cond
      (expr/long-array? d)
      (assoc col-info :data (ColumnOps/gatherLong ^longs d indices (int result-length)))

      (expr/double-array? d)
      (assoc col-info :data (ColumnOps/gatherDouble ^doubles d indices (int result-length)))

      :else
      (throw (ex-info "Unsupported column type for ASOF gather" {:col-info col-info})))))

(defn- gather-asof
  "Build merged output columns. Always drops right-side equality-key columns
   when they collide with left-side names — both INNER and LEFT take the
   probe-side value for the join key (the right side may be NULL on LEFT).
   Mirrors the equi-join INNER/LEFT convention in stratum.query.join."
  [probe-cols build-cols left-indices right-indices result-length
   _join-type on-pairs]
  (let [^ints li left-indices
        ^ints ri right-indices
        rl (long result-length)
        left-key-set (into #{} (map first) on-pairs)
        right-key-cols (mapv second on-pairs)
        drop-right-keys (into #{} (filter left-key-set) right-key-cols)
        merged (reduce-kv
                (fn [m k v]
                  (assoc m k (gather-col v li rl)))
                {} probe-cols)
        merged (reduce-kv
                (fn [m k v]
                  (if (contains? drop-right-keys k)
                    m
                    (assoc m k (gather-col v ri rl))))
                merged build-cols)]
    {:columns merged :length rl}))

;; ============================================================================
;; Composite-key computation for equality predicates
;; ============================================================================

(defn- compute-shared-eq-keys
  "Compute composite long[] equality keys for both sides using shared multipliers
   so that matching tuples encode to identical long values across sides.

   Returns [^longs build-key ^longs probe-key]."
  [probe-cols probe-length build-cols build-length on-pairs]
  (let [n (count on-pairs)
        probe-key-cols (mapv first on-pairs)
        build-key-cols (mapv second on-pairs)
        probe-arrs (jn/extract-key-col-arrays probe-cols probe-key-cols probe-length)
        build-arrs (jn/extract-key-col-arrays build-cols build-key-cols build-length)
        muls (jn/compute-key-muls probe-arrs build-arrs n)
        encode (fn [arrs ^long len]
                 (ColumnOpsExt/compositeKeyEncode
                  (into-array expr/long-array-class arrs)
                  muls (int len)))]
    [(encode build-arrs build-length)
     (encode probe-arrs probe-length)]))

;; ============================================================================
;; Execution
;; ============================================================================

(defn execute-asof-join
  "Execute an ASOF join.

   probe-cols   — materialized column map for the left (probe) side
   probe-length — long
   build-cols   — materialized column map for the right (build) side
   build-length — long
   spec         — {:join-type :on-pairs :match-condition}

   Returns {:columns merged-cols :length n}."
  [probe-cols probe-length build-cols build-length spec
   & {:keys [probe-mask build-mask]}]
  (let [{:keys [join-type on-pairs match-condition]} (normalize-asof-spec spec)
        is-left? (= :left join-type)
        op-code  (long (op->code (first match-condition)))
        left-ts-col  (nth match-condition 1)
        right-ts-col (nth match-condition 2)
        ^longs probe-ts (get-long-column probe-cols left-ts-col)
        ^longs build-ts (get-long-column build-cols right-ts-col)
        ^"[Ljava.lang.Object;" result
        (if (seq on-pairs)
          (let [[^longs build-key ^longs probe-key]
                (compute-shared-eq-keys probe-cols probe-length
                                        build-cols build-length on-pairs)]
            (ColumnOpsAsof/asofJoinPartitioned
             build-key build-ts (int build-length) build-mask
             probe-key probe-ts (int probe-length) probe-mask
             (int op-code) (boolean is-left?) (int ColumnOpsAsof/DEFAULT_PART_BITS)))
          (ColumnOpsAsof/asofJoinSinglePartition
           build-ts (int build-length) build-mask
           probe-ts (int probe-length) probe-mask
           (int op-code) (boolean is-left?)))
        ^ints left-idx  (aget result 0)
        ^ints right-idx (aget result 1)
        out-len (long (aget result 2))]
    (gather-asof probe-cols build-cols left-idx right-idx out-len
                 join-type on-pairs)))
