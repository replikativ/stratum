(ns stratum.query.top-n
  "Streaming top-N pushdown for `ORDER BY col [DESC] LIMIT N`.

   Avoids materializing all columns of the source by:
     1. Streaming chunks of the order column to find the top-N
        (key, chunk-idx, local-idx) tuples via a fixed-size heap.
     2. Reading only those N rows from each output column via the
        chunk's `read-double` / `read-long` / `read-value` protocol
        method (each touched chunk decodes once and is cached).

   Eligibility gate intentionally narrow: single-column ORDER BY,
   small LIMIT, no predicates / aggs / groups / joins / window. The
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

(defn top-n-eligible?
  "Returns true if `query` over `columns` is a clean top-N shape:
   1-or-more-column ORDER BY (each numeric, non-string), LIMIT ≤
   `*top-n-limit*`, no GROUP/AGG/HAVING/JOIN/WHERE/WINDOW/DISTINCT,
   no OFFSET. Multi-key ORDER BY keeps separate primitive long and double
   sort keys per row in the heap; comparison walks keys in declared order
   (matching DuckDB's `CreateSortKey` blob comparison)."
  [query columns]
  (let [{:keys [order limit group agg having join distinct where window
                offset]} query]
    (and order
         (>= (count order) 1)
         limit
         (<= 0 (long limit))
         (<= (long limit) (long *top-n-limit*))
         (or (nil? offset) (zero? (long offset)))
         (empty? group)
         (empty? agg)
         (empty? having)
         (empty? join)
         (empty? where)
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
  [arrs ^long n dirs datatypes]
  (let [n-keys   (count arrs)
        long-key? (boolean-array (map #(not= :float64 %) datatypes))
        ^Comparator cmp (entry-cmp (dirs->int-array dirs) long-key?)
        pq       (PriorityQueue. (max 1 (int n)) cmp)
        scratch-longs (long-array n-keys)
        scratch-doubles (double-array n-keys)
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
        (maybe-evict-and-offer! pq cmp n scratch-longs scratch-doubles nil i)
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
        ^ChunkEntry e (nth first-entries chunk-i)
        ^ChunkStats s (.stats e)]
    (let [bound (if (= first-dir :desc) (:max-val s) (:min-val s))]
      (if (= datatype :float64) (double bound) (long bound)))))

(defn- compare-bound-asc
  [a b datatype]
  (if (= datatype :float64)
    (compare-double-asc (double a) (double b))
    (compare-long-asc (long a) (long b))))

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
        positions (range n)
        cmp (fn [a b]
              (let [c (long
                       (compare-bound-asc
                        (chunk-primary-bound first-entries (long a) first-dir datatype)
                        (chunk-primary-bound first-entries (long b) first-dir datatype)
                        datatype))]
                (if (= first-dir :desc) (int (- c)) (int c))))]
    (vec (sort cmp positions))))

(defn- can-prune-rest?
  "When the heap is full, no future chunk whose primary bound is
   already worse than the heap's worst-kept primary key can
   contribute a winning row. For ASC: skip when `chunk-min >=
   heap-worst-primary` (strict-positive comparator means equal
   values don't evict, so the `>=` is sound). For DESC: skip when
   `chunk-max <= heap-worst-primary`."
  [^PriorityQueue pq n ^clojure.lang.Keyword first-dir bound datatype]
  (let [n (long n)]
    (when (= n (.size pq))
      (let [^TopNEntry top (.peek pq)
            worst (if (= datatype :float64)
                    (aget ^doubles (.-double-keys top) 0)
                    (aget ^longs (.-long-keys top) 0))
            c (long (compare-bound-asc bound worst datatype))]
        (if (= first-dir :desc)
          (<= c 0)
          (>= c 0))))))

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
  [indices ^long n dirs datatypes]
  (let [n-keys (count indices)
        long-key? (boolean-array (map #(not= :float64 %) datatypes))
        ^Comparator cmp (entry-cmp (dirs->int-array dirs) long-key?)
        pq (PriorityQueue. (max 1 (int n)) cmp)
        scratch-longs (long-array n-keys)
        scratch-doubles (double-array n-keys)
        per-col-entries (mapv (fn [idx] (vec (pss/slice (index/idx-tree idx) nil nil)))
                              indices)
        n-chunks (long (count (first per-col-entries)))
        first-entries (first per-col-entries)
        first-dir (first dirs)
        first-datatype (first datatypes)
        ;; Visit chunks ordered by primary-key stats — promising chunks first.
        sorted-positions (chunk-iteration-order first-entries first-dir
                                                first-datatype)]
    (loop [pos-idx 0]
      (when (< pos-idx n-chunks)
        (let [chunk-i (long (nth sorted-positions pos-idx))
              bound (chunk-primary-bound first-entries chunk-i first-dir
                                         first-datatype)]
          (if (can-prune-rest? pq n first-dir bound first-datatype)
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
                  (maybe-evict-and-offer! pq cmp n scratch-longs
                                          scratch-doubles chunk-id i)
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
  (let [{:keys [order limit select]} query
        n          (long limit)
        ;; Decompose every `:order` spec into [col dir] pairs.
        decomposed (mapv order-spec-col-and-dir order)
        order-cols (mapv first decomposed)
        dirs       (mapv second decomposed)
        order-infos (mapv (fn [k] (get columns k)) order-cols)
        datatypes   (mapv :type order-infos)
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
                         (mapv :data order-infos) n dirs datatypes)
             all-index? (find-top-n-on-indices
                         (mapv :index order-infos) n dirs datatypes)
             :else (throw (ex-info "top-N: order columns must all be array- or all index-backed"
                                   {:order-cols order-cols})))
        sorted (drain-heap-sorted pq)
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
