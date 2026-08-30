(ns stratum.query.top-n
  "Streaming top-N pushdown for `ORDER BY col [DESC] LIMIT N`.

   Avoids materializing all columns of the source by:
     1. Streaming chunks of the order column to find the top-N
        (key, chunk-idx, local-idx) tuples via a fixed-size heap.
     2. Reading only those N rows from each output column via the
        chunk's `read-double` / `read-long` / `read-value` protocol
        method (each touched chunk decodes once and is cached).

   Eligibility gate intentionally narrow: small LIMIT, optional exact range
   conjunctions on a sole ORDER BY column, and no aggs / groups / joins /
   window. The
   legacy materialize-then-sort path remains the fallback for every
   other shape."
  (:require [stratum.chunk :as chunk]
            [stratum.column :as column]
            [stratum.dataset :as dataset]
            [stratum.index :as index]
            [stratum.query.expression :as expr]
            [stratum.query.normalization :as norm]
            [org.replikativ.persistent-sorted-set :as pss])
  (:import [java.util PriorityQueue Comparator]
           [stratum.index ChunkEntry]))

(set! *warn-on-reflection* true)

(def ^:dynamic *top-n-limit*
  "Maximum LIMIT value at which the top-N pushdown path takes over.
   Above this, the legacy materialize-then-sort path is used. 1024
   matches DuckDB's heap-vs-sort cutoff."
  1024)

(defn retained-count
  "Return the heap size needed for LIMIT/OFFSET, or nil for an invalid or
   overflowing pair. OFFSET is implemented by retaining `limit + offset`
   rows in the bounded heap and discarding the prefix after ordering."
  [limit offset]
  (when (some? limit)
    (try
      (let [limit (long limit)
            offset (long (or offset 0))
            retained (Math/addExact limit offset)]
        (when (and (not (neg? limit)) (not (neg? offset)))
          retained))
      (catch ArithmeticException _ nil))))

;; ============================================================================
;; Eligibility
;; ============================================================================

(defn- order-spec-col-and-dir
  "Decompose an `:order` entry into `[col dir]`. Accepts both
   `[:col :asc]` / `[:col :desc]` and bare `:col` (defaults to ASC)."
  [spec]
  (if (vector? spec) spec [spec :asc]))

(defn- order-col-eligible?
  "An order column is eligible for the streaming heap when it's
   index- or array-backed and primitive (`:int64`/`:float64`).

   Dict-encoded strings qualify only when the dictionary happens to be
   in lexicographic order: then codes are ranks and comparing them
   numerically is comparing the strings. Encoding never sorts, so this
   is opportunistic — a Parquet file already sorted on the column gets
   the heap for free. Otherwise the column falls back to the
   sort-decoded-rows path in `postprocess/apply-order`, which is correct
   either way. Asked of the dict array itself (memoised, O(d) once)
   rather than read from a stored flag, so it cannot go stale."
  [col-info]
  (and col-info
       (or (:index col-info) (:data col-info))
       (#{:int64 :float64} (:type col-info))
       (or (not= :string (:dict-type col-info))
           (column/dict-sorted? (:dict col-info)))))

(def ^:private range-ops #{:eq :gt :gte :lt :lte})

(defn- int64-literal?
  [value]
  (and (integer? value)
       (<= Long/MIN_VALUE value Long/MAX_VALUE)))

(defn range-predicates
  "Return a normalized exact range conjunction for a one-column ORDER BY.

   Nil means the WHERE clause is not safely representable by the streaming
   top-N path. An empty vector means there is no WHERE clause. Keeping this
   proof here lets the map executor and the IR rewrite share one eligibility
   boundary."
  [order where columns]
  (let [order (mapv order-spec-col-and-dir order)
        predicates (mapv norm/normalize-pred (or where []))]
    (cond
      (empty? predicates) []
      (not= 1 (count order)) nil
      :else
      (let [order-col (ffirst order)
            datatype (:type (get columns order-col))]
        (when (and (#{:int64 :float64} datatype)
                   (every? (fn [[col op value :as pred]]
                             (and (= 3 (count pred))
                                  (= order-col col)
                                  (contains? range-ops op)
                                  (if (= datatype :int64)
                                    (int64-literal? value)
                                    (number? value))))
                           predicates))
          predicates)))))

(defn top-n-eligible?
  "Returns true if `query` over `columns` is a clean top-N shape:
   1-or-more-column ORDER BY (each numeric, non-string), LIMIT ≤
   `*top-n-limit*` retained rows, no GROUP/AGG/HAVING/JOIN/WINDOW/DISTINCT.
   A non-negative OFFSET is eligible when `LIMIT + OFFSET` remains within the
   same bound.
   WHERE may be a conjunction of scalar ranges on the sole ORDER BY column.
   Multi-key ORDER BY keeps separate primitive long and double
   sort keys per row in the heap; comparison walks keys in declared order
   (matching DuckDB's `CreateSortKey` blob comparison)."
  [query columns]
  (let [{:keys [order limit group agg having join distinct where window
                offset]} query
        retained (retained-count limit offset)]
    (and order
         (>= (count order) 1)
         (some? retained)
         (<= retained (long *top-n-limit*))
         (empty? group)
         (empty? agg)
         (empty? having)
         (empty? join)
         (some? (range-predicates order where columns))
         (empty? window)
         (not distinct)
         ;; Every order column must be present, primitive-numeric,
         ;; and not a dict-string.
         (every? (fn [spec]
                   (let [[col _] (order-spec-col-and-dir spec)]
                     (and (keyword? col)
                          (order-col-eligible? (get columns col)))))
                 order))))

;; ============================================================================
;; Heap-based top-N over an index/array column
;; ============================================================================

;; chunk-id is the full ChunkEntry chunk-id vector (e.g. [3] or [3 1]
;; after a split). Storing only the first element collapses split
;; chunks together and makes downstream point-slice lookups miss.
;; Long and double keys stay in separate primitive arrays. In particular, an
;; int64 key must never pass through double: adjacent integers above 2^53 then
;; compare equal and an exact top-N can retain the wrong row. The unused slot
;; in the other array is deliberately cheaper than boxing every key.
(deftype ^:private TopNEntry
         [^longs long-keys ^doubles double-keys chunk-id ^long local-idx])

(defn- compare-long-asc
  "Compare int64 keys in ascending SQL order (NULL sentinel last)."
  [^long a ^long b]
  (cond
    (= a Long/MIN_VALUE) (if (= b Long/MIN_VALUE) 0 1)
    (= b Long/MIN_VALUE) -1
    :else (Long/compare a b)))

(defn- compare-double-asc
  "Compare float64 keys in ascending SQL order. Double/compare places the
   NaN NULL sentinel last, matching the existing top-N null convention."
  [^double a ^double b]
  (Double/compare a b))

(defn- prepare-range-predicates
  [predicates datatype]
  (mapv (fn [[_col op value]]
          [op (if (= datatype :float64) (double value) (long value))])
        predicates))

(defn- comparison-matches?
  [op comparison]
  (case op
    :eq (zero? comparison)
    :gt (pos? comparison)
    :gte (not (neg? comparison))
    :lt (neg? comparison)
    :lte (not (pos? comparison))))

(defn- range-key-matches?
  [predicates datatype long-key double-key]
  (if (empty? predicates)
    true
    ;; Long/MIN_VALUE and NaN are Stratum's NULL sentinels. Every ordinary
    ;; comparison with NULL is UNKNOWN and therefore excluded by WHERE.
    (when-not (if (= datatype :float64)
                (Double/isNaN (double double-key))
                (= Long/MIN_VALUE (long long-key)))
      (every? (fn [[op bound]]
                (comparison-matches?
                 op
                 (if (= datatype :float64)
                   (Double/compare (double double-key) (double bound))
                   (Long/compare (long long-key) (long bound)))))
              predicates))))

(declare compare-bound-asc)

(defn- chunk-has-non-null-values?
  [^ChunkEntry entry]
  (let [^ChunkStats stats (.stats entry)]
    (< (long (:null-count stats)) (long (:count stats)))))

(defn- chunk-may-match-range?
  [^ChunkEntry entry predicates datatype]
  (cond
    (empty? predicates)
    true

    ;; SQL comparisons never match NULL. An all-NULL chunk has sentinel
    ;; extrema (Double/MAX_VALUE and -Double/MAX_VALUE), not useful bounds.
    (not (chunk-has-non-null-values? entry))
    false

    :else
    (let [^ChunkStats stats (.stats entry)
          min-value (:min-val stats)
          max-value (:max-val stats)]
      (every? (fn [[op bound]]
                ;; Int64 extrema are rounded doubles. Monotonic conversion
                ;; still permits a conservative proof when the rounded values
                ;; are strictly separated, but equality is inconclusive near
                ;; 2^53 and must retain the chunk.
                (let [bound (if (= datatype :int64) (double bound) bound)
                      min-cmp (compare-double-asc min-value bound)
                      max-cmp (compare-double-asc max-value bound)]
                  (case op
                    :eq (and (not (pos? min-cmp))
                             (not (neg? max-cmp)))
                    :gt (not (neg? max-cmp))
                    :gte (not (neg? max-cmp))
                    :lt (not (pos? min-cmp))
                    :lte (not (pos? min-cmp)))))
              predicates))))

(defn- ^Comparator entry-cmp
  "Comparator for the heap. The PriorityQueue is a min-heap, so the
   comparator must return positive when `a` is *better* than `b`
   (where `a` evicts the worst-kept `b` at the heap's peek). For ASC
   that means smaller-is-better; for DESC larger-is-better. With
   multi-key ORDER BY we walk the keys in declared order, returning
   the first non-zero per-key result. Mixed direction (`ORDER BY x
   ASC, y DESC`) is supported — `dirs[i] ∈ {+1, -1}` flips the per-
   key sense."
  [^ints dirs ^booleans long-key?]
  (let [n (alength dirs)]
    (reify Comparator
      (compare [_ a b]
        (let [^longs la (.-long-keys ^TopNEntry a)
              ^longs lb (.-long-keys ^TopNEntry b)
              ^doubles da (.-double-keys ^TopNEntry a)
              ^doubles db (.-double-keys ^TopNEntry b)]
          (loop [i 0]
            (if (>= i n)
              0
              (let [asc-cmp (long
                             (if (aget long-key? i)
                               (compare-long-asc (aget la i) (aget lb i))
                               (compare-double-asc (aget da i) (aget db i))))
                    ;; The heap comparator is deliberately reversed: positive
                    ;; means a is better, while PriorityQueue.peek is worst.
                    c (if (= 1 (aget dirs i)) (- asc-cmp) asc-cmp)]
                (if (zero? c)
                  (recur (unchecked-inc i))
                  (int c))))))))))

(defn- ^ints dirs->int-array
  "Convert a vec of `:asc`/`:desc` keywords into a primitive int[]
   (`+1` for ASC, `-1` for DESC). Cheaper than re-checking keywords
   per row inside the heap comparator."
  [dirs]
  (int-array (map #(if (= % :desc) -1 1) dirs)))

(defn- set-key-from-chunk!
  [^longs long-keys ^doubles double-keys k chk i datatype]
  (let [k (int k)
        i (long i)]
    (case datatype
      :float64 (aset double-keys k (double (chunk/read-double chk i)))
      (:int64 :string) (aset long-keys k (long (chunk/read-long chk i))))))

(defn- set-key-from-array!
  [^longs long-keys ^doubles double-keys k arr i datatype]
  (let [k (int k)
        i (int i)]
    (case datatype
      :float64 (aset double-keys k (aget ^doubles arr i))
      (:int64 :string) (aset long-keys k (aget ^longs arr i)))))

(defn- maybe-evict-and-offer!
  "Heap-fill or evict-and-replace logic shared between the index and
   array paths. `scratch` carries this row's encoded keys; on insert
   we copy it (so the in-heap entry doesn't alias the next row).
   `n` and `local-idx` are passed boxed-long since Clojure's primitive
   fn rule caps `^long`/`^double` args at four."
  [^PriorityQueue pq ^Comparator cmp n ^longs scratch-longs
   ^doubles scratch-doubles chunk-id local-idx]
  (let [n-keys     (alength scratch-longs)
        n-long     (long n)
        local-long (long local-idx)]
    (if (< (.size pq) n-long)
      (.offer pq (TopNEntry. (java.util.Arrays/copyOf scratch-longs n-keys)
                             (java.util.Arrays/copyOf scratch-doubles n-keys)
                             chunk-id local-long))
      (let [^TopNEntry top (.peek pq)
            tmp-entry (TopNEntry. scratch-longs scratch-doubles
                                  chunk-id local-long)]
        (when (pos? (.compare cmp tmp-entry top))
          (.poll pq)
          (.offer pq (TopNEntry. (java.util.Arrays/copyOf scratch-longs n-keys)
                                 (java.util.Arrays/copyOf scratch-doubles n-keys)
                                 chunk-id local-long)))))))

(defn- find-top-n-on-arrays
  "Multi-column array path. Iterates row-by-row across `arrs`, fills a
   reused `scratch` keys array per row, and feeds the heap. Single-
   key callers pass a 1-element `arrs`/`datatypes` vec."
  [arrs n dirs datatypes range-preds]
  (let [n (long n)
        n-keys   (count arrs)
        long-key? (boolean-array (map #(not= :float64 %) datatypes))
        ^Comparator cmp (entry-cmp (dirs->int-array dirs) long-key?)
        pq       (PriorityQueue. (max 1 (int n)) cmp)
        scratch-longs (long-array n-keys)
        scratch-doubles (double-array n-keys)
        first-datatype (first datatypes)
        range-preds (prepare-range-predicates range-preds first-datatype)
        ;; All arrays share the same length (rows of a single table).
        first-arr (nth arrs 0)
        first-dt  (nth datatypes 0)
        len      (long (case first-dt
                         :float64 (alength ^doubles first-arr)
                         :int64   (alength ^longs first-arr)
                         :string  (alength ^longs first-arr)))]
    (loop [i 0]
      (when (< i len)
        ;; Fill scratch with this row's keys.
        (loop [k 0]
          (when (< k n-keys)
            (set-key-from-array! scratch-longs scratch-doubles k
                                 (nth arrs k) i (nth datatypes k))
            (recur (unchecked-inc k))))
        (when (range-key-matches? range-preds first-datatype
                                  (aget scratch-longs 0)
                                  (aget scratch-doubles 0))
          (maybe-evict-and-offer! pq cmp n scratch-longs scratch-doubles nil i))
        (recur (unchecked-inc i))))
    pq))

(defn- chunk-primary-bound
  "Lower (ASC) / upper (DESC) bound of `chunk-i`'s primary order
   column. Used by `find-top-n-on-indices` to (a) order chunk
   iteration and (b) decide when remaining chunks can no longer
   contribute. The primary order column is always the first key
   in `:order`; tiebreaker keys aren't used for chunk pruning."
  [first-entries chunk-i ^clojure.lang.Keyword first-dir datatype]
  (let [chunk-i (long chunk-i)
        ^ChunkEntry e (nth first-entries chunk-i)]
    ;; ChunkStats extrema are doubles for both datatypes. Int64 bounds are
    ;; therefore only conservative ordering hints: callers may prune on strict
    ;; separation, never on equality. All-NULL chunks return nil explicitly.
    (when (chunk-has-non-null-values? e)
      (let [^ChunkStats s (.stats e)]
        (double (if (= first-dir :desc) (:max-val s) (:min-val s)))))))

(defn- compare-bound-asc
  [a b datatype]
  (compare-double-asc (double a) (double b)))

(defn- chunk-iteration-order
  "Permutation `[0..n-chunks)` sorted so the most promising chunks
   come first — ascending min for ASC, descending max for DESC.
   Walking in this order lets `can-prune-rest?` cut off the loop
   the moment a chunk's primary bound can no longer beat the heap's
   worst kept value (DuckDB calls this `RowGroupPruner`'s
   set_scan_order; we apply the same idea but to streaming top-N
   instead of a separate scan-reorder pass)."
  [first-entries ^clojure.lang.Keyword first-dir datatype]
  (let [n (count first-entries)
        positions (vec (range n))]
    ;; Rounded int64 extrema remain monotonic, so they safely order chunks even
    ;; though equal rounded bounds cannot prove pruning. DESC ordering is safe
    ;; only when no chunk contains NULL/NaN (those sort first but are omitted
    ;; from extrema).
    (if (or (= first-dir :asc)
            (every? (fn [^ChunkEntry entry]
                      (zero? (long (:null-count (.stats entry)))))
                    first-entries))
      (let [cmp (fn [a b]
                  (let [a-bound (chunk-primary-bound first-entries (long a)
                                                     first-dir datatype)
                        b-bound (chunk-primary-bound first-entries (long b)
                                                     first-dir datatype)]
                    (cond
                      (nil? a-bound) (if (nil? b-bound) 0 1)
                      (nil? b-bound) -1
                      :else (let [c (int (compare-bound-asc a-bound b-bound
                                                            datatype))]
                              (if (= first-dir :desc) (- c) c)))))]
        (vec (sort cmp positions)))
      positions)))

(defn- can-prune-rest?
  "When the heap is full, no future chunk whose primary bound is
   provably worse than the heap's worst-kept primary key can
   contribute a winning row. Exact float64 single-key bounds may prune
   equality; rounded int64 or multi-key bounds require strict primary
   separation. DESC pruning additionally requires NULL-free chunks."
  [^PriorityQueue pq n n-keys ^clojure.lang.Keyword first-dir bound datatype
   desc-stats-safe?]
  (let [n (long n)]
    ;; Conservative proof boundary:
    ;; - rounded int64 bounds prove only strict separation;
    ;; - DESC extrema are safe only when every chunk is NULL/NaN-free;
    ;; - equal primary bounds cannot prune a multi-key ORDER BY because a later
    ;;   chunk may contain a better secondary key.
    (when (and (or (= first-dir :asc) desc-stats-safe?)
               (some? bound)
               (= n (.size pq)))
      (let [^TopNEntry top (.peek pq)
            worst (if (= datatype :float64)
                    (aget ^doubles (.-double-keys top) 0)
                    (aget ^longs (.-long-keys top) 0))
            c (long (compare-double-asc (double bound) (double worst)))
            strict-worse? (if (= first-dir :desc) (neg? c) (pos? c))
            equal-safe? (and (= datatype :float64)
                             (= 1 (long n-keys))
                             (zero? c))]
        (or strict-worse? equal-safe?)))))

(defn- find-top-n-on-indices
  "Multi-column index path. Walks chunks in primary-order (ASC: min
   ascending; DESC: max descending) so that once the heap is full,
   the next chunk's primary bound either beats every kept row
   (process it) or can't (stop). For sorted-on-disk inputs (Parquet
   time-series, append-only logs) this fires immediately after the
   first chunk; for random-order inputs it has no overhead beyond
   the upfront stats sort.

   All order indices must share chunk boundaries — true for
   stratum's column store (chunks are dataset-level)."
  [indices n dirs datatypes range-preds]
  (let [n (long n)
        n-keys (count indices)
        long-key? (boolean-array (map #(not= :float64 %) datatypes))
        ^Comparator cmp (entry-cmp (dirs->int-array dirs) long-key?)
        pq (PriorityQueue. (max 1 (int n)) cmp)
        scratch-longs (long-array n-keys)
        scratch-doubles (double-array n-keys)
        per-col-entries (mapv (fn [idx] (vec (pss/slice (index/idx-tree idx) nil nil)))
                              indices)
        first-entries (first per-col-entries)
        first-dir (first dirs)
        first-datatype (first datatypes)
        desc-stats-safe? (or (= first-dir :asc)
                             (every? (fn [^ChunkEntry entry]
                                       (zero? (long (:null-count (.stats entry)))))
                                     first-entries))
        range-preds (prepare-range-predicates range-preds first-datatype)
        ;; Visit chunks ordered by primary-key stats — promising chunks first.
        sorted-positions (->> (chunk-iteration-order first-entries first-dir
                                                     first-datatype)
                              (filterv (fn [position]
                                         (chunk-may-match-range?
                                          (nth first-entries position)
                                          range-preds first-datatype))))
        n-chunks (long (count sorted-positions))]
    (loop [pos-idx 0]
      (when (< pos-idx n-chunks)
        (let [chunk-i (long (nth sorted-positions pos-idx))
              bound (chunk-primary-bound first-entries chunk-i first-dir
                                         first-datatype)]
          (if (can-prune-rest? pq n n-keys first-dir bound first-datatype
                               desc-stats-safe?)
            ;; All remaining chunks (in primary order) are at least as
            ;; bad as `bound`; nothing they contain can dethrone the
            ;; heap. Stop iterating.
            nil
            (let [^"[Ljava.lang.Object;" chunks (object-array n-keys)
                  _ (loop [k 0]
                      (when (< k n-keys)
                        (let [^ChunkEntry e (nth (nth per-col-entries k) chunk-i)]
                          (aset chunks k (.chunk e)))
                        (recur (unchecked-inc k))))
                  chunk-id (.chunk-id ^ChunkEntry (nth first-entries chunk-i))
                  chunk-len (long (chunk/chunk-length (aget chunks 0)))]
              (loop [i 0]
                (when (< i chunk-len)
                  (loop [k 0]
                    (when (< k n-keys)
                      (set-key-from-chunk! scratch-longs scratch-doubles k
                                           (aget chunks k) i (nth datatypes k))
                      (recur (unchecked-inc k))))
                  (when (range-key-matches? range-preds first-datatype
                                            (aget scratch-longs 0)
                                            (aget scratch-doubles 0))
                    (maybe-evict-and-offer! pq cmp n scratch-longs
                                            scratch-doubles chunk-id i))
                  (recur (unchecked-inc i))))
              (recur (unchecked-inc pos-idx)))))))
    pq))

(defn- drain-heap-sorted
  "Drain `pq` into a vector ordered by the multi-key direction
   (best-first per the comparator, which inverts to largest-first
   for DESC and smallest-first for ASC on each key)."
  [^PriorityQueue pq]
  (let [out (java.util.ArrayList. (.size pq))]
    (while (pos? (.size pq))
      (.add out (.poll pq)))
    ;; pq drained in reverse of desired order (worst-first → best-first)
    (vec (reverse out))))

;; ============================================================================
;; Row fetch — given top-N entries, gather rows from the columns map
;; ============================================================================

(defn- chunks-by-id-for
  "Build {chunk-id-vec → chunk} for `idx`, restricted to the supplied
   `chunk-ids` collection (each is the full chunk-id vector from a
   ChunkEntry). Each id is fetched via a point-slice on the PSS tree,
   so the cost is O(N · log K) instead of O(K) — vital for
   konserve-backed indices where K can be 1000s of chunks per column."
  [idx chunk-ids]
  (let [tree (index/idx-tree idx)]
    (reduce (fn [m id-vec]
              ;; PSS slice bounds compare via chunk-entry-comparator
              ;; which calls `.chunk-id` on each side, so the bounds
              ;; must be ChunkEntry instances. Build a probe carrying
              ;; the full id vector — split chunks have multi-element
              ;; ids (e.g. [2 1]) which would collapse if we kept only
              ;; the first element.
              (let [probe (index/->ChunkEntry id-vec nil nil)
                    e (first (pss/slice tree probe probe))]
                (if e (assoc m id-vec (.chunk ^ChunkEntry e)) m)))
            {}
            chunk-ids)))

;; gather-row used to look up via the full chunk-by-id-map; the
;; current execute-top-n path inlines the gather using a
;; surviving-only chunk-by-id map (see `chunks-by-id-for` and
;; `get-chunk-map` in `execute-top-n`). Keeping this as a placeholder
;; in case future callers want the standalone helper.

(defn- decode-string-value
  "If `col-info` has a string dict, map a long dict-ID to its string."
  [col-info v]
  (if (and v (= :int64 (:type col-info)) (:dict col-info)
           (= :string (:dict-type col-info)))
    (let [^"[Ljava.lang.String;" dict (:dict col-info)
          i (long v)]
      (if (and (<= 0 i) (< i (alength dict)))
        (aget dict i)
        v))
    v))

;; ============================================================================
;; Public entry point
;; ============================================================================

(defn execute-top-n
  "Run the top-N pushdown path. Returns a vector of result rows.
   Caller has already verified `top-n-eligible?`.

   `select` may be nil (= all columns) or a vector of column kws.
   `:order` may carry one or more `[col dir]` specs (or bare `col`
   shorthand for ASC); the heap walks all keys in declared order
   for tie-breaking, mixed `:asc`/`:desc` is supported."
  [query columns]
  (let [{:keys [order limit offset select where]} query
        offset     (long (or offset 0))
        n          (or (retained-count limit offset)
                       (throw (ex-info "Invalid LIMIT/OFFSET in streaming top-N"
                                       {:limit limit :offset offset})))
        ;; Decompose every `:order` spec into [col dir] pairs.
        decomposed (mapv order-spec-col-and-dir order)
        order-cols (mapv first decomposed)
        dirs       (mapv second decomposed)
        order-infos (mapv (fn [k] (get columns k)) order-cols)
        datatypes   (mapv :type order-infos)
        range-preds (or (range-predicates order where columns)
                        (throw (ex-info "Unsupported WHERE in streaming top-N"
                                        {:where where :order order})))
        ;; Same-source assumption: either every order column has
        ;; `:data`, or every order column has `:index`. Mixed isn't
        ;; supported (it would imply the order columns came from
        ;; different sources, which the eligibility gate disallows
        ;; via the `order-col-eligible?` check).
        all-data?   (every? :data order-infos)
        all-index?  (every? :index order-infos)
        all-keys    (vec (keys columns))
        ;; Determine output projection. select may include :as
        ;; aliases or expressions; we restrict to plain keyword refs
        ;; in the eligibility gate so this is straightforward.
        out-cols (cond
                   (or (nil? select) (= [:*] select)) all-keys
                   (every? keyword? select)           (vec select)
                   :else                              all-keys)
        ;; Phase 1: streaming top-N
        pq (cond
             all-data?  (find-top-n-on-arrays
                         (mapv :data order-infos) n dirs datatypes range-preds)
             all-index? (find-top-n-on-indices
                         (mapv :index order-infos) n dirs datatypes range-preds)
             :else (throw (ex-info "top-N: order columns must all be array- or all index-backed"
                                   {:order-cols order-cols})))
        retained (drain-heap-sorted pq)
        sorted (if (zero? offset)
                 retained
                 (subvec retained (min (count retained) (int offset))))
        ;; Surviving rows live in only a small set of chunk-ids;
        ;; fetch a per-output-column map of {chunk-id → chunk} for
        ;; only those ids. On konserve-backed indices with thousands
        ;; of chunks per column this is the difference between an
        ;; O(N·K·C) walk (N rows × K chunks × C cols) and O(N·log K·C).
        surviving-chunk-ids (into #{} (map #(.chunk-id ^TopNEntry %)) sorted)
        chunk-maps (volatile! {})
        get-chunk-map (fn [^clojure.lang.Keyword k]
                        (or (get @chunk-maps k)
                            (let [m (when-let [idx (:index (get columns k))]
                                      (chunks-by-id-for idx surviving-chunk-ids))]
                              (vswap! chunk-maps assoc k m)
                              m)))]
    ;; Phase 2: fetch rows.
    ;; F-033: map sentinel values to nil at the API boundary so the
    ;; user-visible result doesn't expose Long.MIN_VALUE / NaN.
    ;; `decode-string-value` runs after the sentinel mapping so a
    ;; dict-encoded NULL string already passed through as nil.
    (mapv
     (fn [^TopNEntry e]
       (let [chunk-id (.chunk-id e)
             local-idx (.local-idx e)]
         (into {}
               (map (fn [k]
                      (let [col-info (get columns k)
                            v (cond
                                (:data col-info)
                                (let [d (:data col-info)]
                                  (cond
                                    (expr/long-array? d)
                                    (let [lv (aget ^longs d (int local-idx))]
                                      (when (not= lv Long/MIN_VALUE) lv))
                                    (expr/double-array? d)
                                    (let [dv (aget ^doubles d (int local-idx))]
                                      (when-not (Double/isNaN dv) dv))
                                    :else (nth d local-idx)))
                                (:index col-info)
                                (when-let [chk (get (get-chunk-map k) chunk-id)]
                                  (case (:type col-info)
                                    :float64 (let [dv (chunk/read-double chk local-idx)]
                                               (when-not (Double/isNaN dv) dv))
                                    (let [lv (chunk/read-long chk local-idx)]
                                      (when (not= lv Long/MIN_VALUE) lv))))
                                :else nil)
                            v (decode-string-value col-info v)]
                        [(norm/strip-ns k) v])))
               out-cols)))
     sorted)))
