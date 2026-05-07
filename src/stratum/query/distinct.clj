(ns stratum.query.distinct
  "Streaming-style SELECT DISTINCT for single-column queries.

   For a query of the shape `SELECT DISTINCT col FROM t` (no aggs,
   groups, joins, predicates, ordering, etc.) the legacy path
   materializes the column, builds N row maps where N = row count,
   then dedupes the row maps with `apply-distinct`. On 50M rows that
   is dominated by ~4 GB of map allocation — most of the wall time is
   GC.

   This namespace bypasses row construction entirely:
     1. Materialize the one referenced column (Phase-1 column
        pruning ensures only this column is touched).
     2. Call `ColumnOpsExt/distinctLongValuesParallel` (or the
        `Double` variant) to get the distinct values directly via the
        same parallel BitSet/Hash path used by COUNT DISTINCT.
     3. Build at most N-distinct result rows, decoding dict-IDs back
        to strings for dict-encoded columns."
  (:require [stratum.query.columns :as cols]
            [stratum.query.normalization :as norm])
  (:import [stratum.internal ColumnOpsExt]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Eligibility
;; ============================================================================

(defn distinct-eligible?
  "True when `query` is a clean single-column DISTINCT shape:
     :distinct true
     :select is exactly one keyword (column ref, no expression, no alias)
     no :agg, :group, :having, :order, :limit, :where, :join, :window
     the selected column is :int64 or :float64."
  [query columns]
  (let [{:keys [distinct select agg group having order limit where join window]} query]
    (and distinct
         (= 1 (count select))
         (keyword? (first select))
         (empty? agg)
         (empty? group)
         (empty? having)
         (empty? order)
         (nil? limit)
         (empty? where)
         (empty? join)
         (empty? window)
         (let [c (get columns (first select))]
           (and c
                (or (:index c) (:data c))
                (#{:int64 :float64} (:type c)))))))

;; ============================================================================
;; Execution
;; ============================================================================

(defn- decode-string-value
  "Map a dict-ID (long) back to its string via the column's dict, or
   nil for the NULL sentinel. Returns the value unchanged for non-dict
   columns."
  [col-info v]
  (cond
    (and (= :int64 (:type col-info))
         (= :string (:dict-type col-info))
         (:dict col-info))
    (cond
      (= v Long/MIN_VALUE) nil
      :else (let [^"[Ljava.lang.String;" dict (:dict col-info)
                  i (long v)]
              (when (and (<= 0 i) (< i (alength dict)))
                (aget dict i))))

    ;; Plain int64: NULL sentinel → nil
    (= :int64 (:type col-info))
    (if (= v Long/MIN_VALUE) nil v)

    :else v))

(defn execute-distinct
  "Run the SELECT DISTINCT path. Caller has verified
   `distinct-eligible?`. Returns a vector of one-key result rows."
  [query columns]
  (let [^clojure.lang.Keyword col-kw (first (:select query))
        col-info (get columns col-kw)
        ;; Phase-1 column pruning makes this materialize exactly the
        ;; one referenced column.
        mat-cols (cols/materialize-columns columns)
        col-data (:data (get mat-cols col-kw))
        out-key (norm/strip-ns col-kw)]
    (case (:type col-info)
      :int64
      (let [vals (ColumnOpsExt/distinctLongValuesParallel
                  ^longs col-data (alength ^longs col-data))
            n (alength ^longs vals)]
        (mapv (fn [i]
                {out-key (decode-string-value col-info (aget ^longs vals i))})
              (range n)))

      :float64
      (let [vals (ColumnOpsExt/distinctDoubleValuesParallel
                  ^doubles col-data (alength ^doubles col-data))
            n (alength ^doubles vals)]
        (mapv (fn [i]
                (let [v (aget ^doubles vals i)]
                  {out-key (if (Double/isNaN v) nil v)}))
              (range n))))))
