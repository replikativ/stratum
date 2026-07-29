(ns stratum.query.string-order
  "Query-time ordering for dict-encoded string columns.

   A dict-encoded string column stores `long[]` codes indexing a
   `String[]` reverse dictionary, and codes are assigned in FIRST-SEEN
   order (see `stratum.column`). So `MIN(code)` is the first value
   encountered, not the lexicographically smallest — every kernel that
   compares codes numerically is comparing insertion positions.

   Rather than force the dictionary into lexicographic order when the
   column is written — which taxes every ingest, and which the paths that
   grow a dict incrementally (`append!`, streaming Parquet ingest, the
   lazily-extended `parquet-dataset` dict) cannot honour anyway — the
   ordering is derived HERE, at query time, for exactly the columns a
   query orders by.

   `rank-column` returns an equivalent column whose codes ARE ranks: same
   rows, same NULLs, dictionary sorted. Every downstream kernel then works
   unmodified and correctly, and the result decodes through the returned
   dictionary. Because it is derived per query from the column's own
   dictionary, it is correct for columns that no encode-time invariant
   could cover: joined columns, expression temporaries, streamed Parquet,
   and anything appended to after creation.

   Cost is O(d log d) to rank the dictionary plus O(n) to remap the codes,
   paid only by queries that actually ask for string ordering, and skipped
   entirely when the dictionary is already sorted (the common case for
   data that arrived sorted). Compare with sorting at encode time, which
   pays O(d log d + n) on EVERY ingest to serve the queries that do."
  (:require [stratum.column :as column]
            [stratum.query.columns :as cols]))

(set! *warn-on-reflection* true)

;; Dicts above this size use Arrays/parallelSort. Below it the ForkJoin
;; split costs more than the sort saves. Measured on an 8-core box: a
;; 200k-entry String[] sorts in ~147ms serial vs ~34ms parallel, while a
;; 10-entry dict is free either way.
(def ^:private ^:const PARALLEL_SORT_THRESHOLD 8192)

(defn dict-string-col?
  "True when `col-info` is a dict-encoded string column."
  [col-info]
  (and col-info
       (= :string (:dict-type col-info))
       (some? (:dict col-info))))

(defn- rank-table
  "`int[]` mapping old code → lexicographic rank, plus the sorted dict.

   Returns `[sorted-dict remap]`. Built by sorting a copy of the dict and
   walking it once against a string→old-code map, so it is O(d log d)."
  [^"[Ljava.lang.String;" dict]
  (let [d (alength dict)
        ^"[Ljava.lang.String;" sorted (java.util.Arrays/copyOf dict d)]
    (if (>= d (long PARALLEL_SORT_THRESHOLD))
      (java.util.Arrays/parallelSort ^"[Ljava.lang.Comparable;" sorted)
      (java.util.Arrays/sort ^"[Ljava.lang.Object;" sorted))
    ;; string -> old code. Built here rather than threaded in from the
    ;; encoder because by query time the encoder's forward map is long
    ;; gone (and for a loaded column never existed).
    (let [fwd (java.util.HashMap. (int (max 16 (* 2 d))))]
      (dotimes [c d]
        (when-let [s (aget dict c)]
          ;; A dict may repeat a string only if it was built by unifying
          ;; two dictionaries; first occurrence wins, which keeps the
          ;; mapping total and the ranks stable.
          (when-not (.containsKey fwd s)
            (.put fwd s (int c)))))
      (let [remap (int-array d)]
        (dotimes [rank d]
          (when-let [s (aget sorted rank)]
            (aset remap (int (long ^Integer (.get fwd s))) (int rank))))
        [sorted remap]))))

(defn rank-column
  "An equivalent column whose dict codes are lexicographic RANKS.

   Returns `col-info` unchanged when its dictionary is already sorted (so
   a pre-sorted source costs one memoised O(d) check and nothing else), or
   when it is not a dict string column. Otherwise returns a column with a
   fresh sorted `:dict` and a fresh remapped `:data`; the input arrays are
   never mutated, because they may be shared with a persisted dataset or
   with other columns in the same query.

   `Long/MIN_VALUE` (the NULL sentinel) and any code outside the
   dictionary are passed through untouched: a code that does not denote a
   string has no rank, and inventing one would turn an out-of-range read
   into a plausible-looking wrong answer.

   The dict-derived LIKE masks are dropped rather than rebuilt — they are
   indexed BY CODE, so they do not survive a renumbering, and this column
   exists only to be ordered. Rebuilding them is `ColumnOpsString`'s
   `buildDictAlphaMasks`/`buildDictBigramMasks` if a caller ever needs
   both on the same column."
  [col-info]
  (if-not (dict-string-col? col-info)
    col-info
    (let [dict (:dict col-info)]
      (if (column/dict-sorted? dict)
        col-info
        (let [^"[Ljava.lang.String;" dict dict
              ^longs data (:data col-info)]
          (if (nil? data)
            ;; Index-backed (chunked) column: no materialized code array to
            ;; remap here. Left to the caller, which falls back to a path
            ;; that decodes values rather than comparing codes.
            col-info
            (let [[sorted ^ints remap] (rank-table dict)
                  d (alength dict)
                  n (alength data)
                  out (long-array n)]
              (dotimes [i n]
                (let [c (aget data i)]
                  (aset out i (if (and (>= c 0) (< c d))
                                (long (aget remap (int c)))
                                c))))
              (-> col-info
                  (assoc :data out :dict sorted)
                  (dissoc :dict-alpha-masks :dict-bigram-masks)))))))))

(def order-agg-ops
  "Aggregates whose result is one of the input values, so it is defined
   for a string column and answered by comparing values."
  #{:min :max})

(defn- agg-source-cols
  "Column keywords an agg reads directly, including through an `:expr`
   (`MIN(UPPER(s))` still orders strings). Expression columns are
   materialized later than this, so a source column named inside an
   expression is ranked here and the temporary inherits the ordering."
  [agg]
  (cond
    (:col agg)  [(:col agg)]
    (:cols agg) (:cols agg)
    (:expr agg) (into [] (filter keyword?) (tree-seq sequential? seq (:expr agg)))
    :else       nil))

(defn rank-order-agg-columns
  "Rank every dict-encoded string column that `aggs` takes a MIN/MAX of.

   Returns `columns` unchanged (by identity, so callers can test with
   `identical?`) when nothing needs ranking — the overwhelmingly common
   case, costing one pass over the agg list plus a memoised O(d) check
   per candidate column.

   An index-backed column has no materialized `long[]` to remap, so it is
   materialized first. That is the same array the aggregate path would
   have materialized anyway, so the cost is a copy, not a new scan — and
   it is paid only by queries that MIN/MAX a string column whose
   dictionary is not already in order."
  [aggs columns]
  (let [targets (into #{}
                      (comp (filter #(contains? order-agg-ops (:op %)))
                            (mapcat agg-source-cols))
                      aggs)
        needs (into []
                    (filter (fn [c]
                              (let [ci (get columns c)]
                                (and (dict-string-col? ci)
                                     (not (column/dict-sorted? (:dict ci)))))))
                    targets)]
    (if (empty? needs)
      columns
      (reduce (fn [acc c]
                (let [ci (get acc c)
                      ci (if (nil? (:data ci))
                           ;; index-backed: materialize just this column
                           (get (cols/materialize-columns {c ci}) c)
                           ci)]
                  (assoc acc c (rank-column ci))))
              columns
              needs))))

(defn join-with-columns
  "Normalized column map contributed by a query's `:join` clauses.

   A join's `:with` is a column map in its own right, so the columns it
   brings are queryable — and aggregatable — even though they never
   appear in `:from`. Resolving aggregates against `:from` alone is what
   let `MIN` over a joined string column return a raw dictionary code and
   `SUM` over one escape the arithmetic check entirely."
  [join prepare-fn]
  (reduce (fn [acc j]
            (let [w (:with j)]
              (cond
                (nil? w) acc
                (map? w) (merge acc (prepare-fn w))
                :else acc)))
          {}
          (or join [])))

(defn rank-join-with-columns
  "Rank the MIN/MAX-target string columns inside each join's `:with`.

   Returns `[join' ranked?]`. The ranked columns have to go back into the
   join spec itself, because that is where the join path reads them from
   — substituting `:from` alone would leave the join reading the original
   first-seen codes."
  [join aggs prepare-fn]
  (let [ranked? (volatile! false)
        join' (mapv (fn [j]
                      (let [w (:with j)]
                        (if-not (map? w)
                          j
                          (let [prepared (prepare-fn w)
                                r (rank-order-agg-columns aggs prepared)]
                            (if (identical? r prepared)
                              j
                              (do (vreset! ranked? true)
                                  (assoc j :with r)))))))
                    (or join []))]
    [join' @ranked?]))

(defn rankable-column?
  "True when `rank-column` can actually deliver ranked codes for
   `col-info` — i.e. it is a dict string column that is either already
   sorted or has a materialized `long[]` to remap.

   Callers use this to decide between the code-comparing fast path and a
   value-decoding fallback, so that an index-backed column degrades to a
   slower CORRECT path instead of a fast wrong one."
  [col-info]
  (and (dict-string-col? col-info)
       (or (column/dict-sorted? (:dict col-info))
           (some? (:data col-info)))))
