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

(defn top-n-eligible?
  "Returns true if `query` over `columns` is a clean top-N shape:
   single-column ORDER BY, LIMIT ≤ *top-n-limit*, no
   GROUP/AGG/HAVING/JOIN/WHERE/WINDOW/DISTINCT, and the order column
   is index- or array-backed and primitive."
  [query columns]
  (let [{:keys [order limit group agg having join distinct where window
                offset]} query]
    (and order
         (= 1 (count order))
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
         (let [first-spec (first order)
               [col _dir] (if (vector? first-spec) first-spec [first-spec :asc])]
           (and (keyword? col)
                (let [c (get columns col)]
                  (and c
                       (or (:index c) (:data c))
                       ;; Primitive numeric / string only — top-N over
                       ;; expressions or non-comparable types takes the
                       ;; slow path.
                       (#{:int64 :float64 :string} (:type c)))))))))

;; ============================================================================
;; Heap-based top-N over an index/array column
;; ============================================================================

(deftype ^:private TopNEntry [^double key ^long chunk-id ^long local-idx])

(defn- ^Comparator entry-cmp
  "Comparator for the heap. For `:asc` (find smallest N) we want a
   max-heap so we evict the largest. For `:desc` (find largest N) we
   want a min-heap. Java's PriorityQueue is a min-heap, so for ASC we
   reverse-compare keys."
  [^clojure.lang.Keyword dir]
  (if (= :desc dir)
    (reify Comparator
      (compare [_ a b]
        (let [ka (.key ^TopNEntry a)
              kb (.key ^TopNEntry b)]
          (Double/compare ka kb))))
    (reify Comparator
      (compare [_ a b]
        (let [ka (.key ^TopNEntry a)
              kb (.key ^TopNEntry b)]
          (Double/compare kb ka))))))

(defn- key-double
  "Extract a double key from a chunk's value at index `i`. For string
   columns the chunk stores dict-IDs (longs); we cast to double for
   comparison ordering — dict-IDs are insertion-ordered and not
   directly meaningful, so string ordering returns is supported only
   when a sorted dict (or numeric dict) is in use. Callers gate on
   `:type` already; this is a guard."
  ^double [chk ^long i datatype]
  (case datatype
    :float64 (chunk/read-double chk i)
    :int64   (double (chunk/read-long chk i))
    :string  (double (chunk/read-long chk i))))

(defn- find-top-n-on-index
  "Stream chunks of `idx`, return up to `n` (key, chunk-id, local-idx)
   entries representing the top-N rows by `dir`."
  [idx ^long n dir datatype]
  (let [^Comparator cmp (entry-cmp dir)
        pq (PriorityQueue. (max 1 (int n)) cmp)
        tree (index/idx-tree idx)
        entries (vec (pss/slice tree nil nil))]
    (doseq [^ChunkEntry entry entries]
      (let [chk (.chunk entry)
            chunk-id (long (first (.chunk-id entry)))
            chunk-len (long (chunk/chunk-length chk))]
        (loop [i 0]
          (when (< i chunk-len)
            (let [k (key-double chk i datatype)
                  e (TopNEntry. k chunk-id i)]
              (if (< (.size pq) n)
                (.offer pq e)
                (let [^TopNEntry top (.peek pq)]
                  (when (pos? (.compare cmp e top))
                    ;; e is "better" than the worst kept → evict
                    (.poll pq)
                    (.offer pq e)))))
            (recur (unchecked-inc i))))))
    pq))

(defn- find-top-n-on-array
  "Stream a heap-array column to find top-N. Used when the source
   column already has `:data` (no index)."
  [arr ^long n dir datatype]
  (let [^Comparator cmp (entry-cmp dir)
        pq (PriorityQueue. (max 1 (int n)) cmp)
        len (long (case datatype
                    :float64 (alength ^doubles arr)
                    :int64   (alength ^longs arr)
                    :string  (alength ^longs arr)))]
    (loop [i 0]
      (when (< i len)
        (let [k (case datatype
                  :float64 (aget ^doubles arr i)
                  :int64   (double (aget ^longs arr i))
                  :string  (double (aget ^longs arr i)))
              ;; For arrays we encode the absolute row index in `local-idx`
              ;; and use chunk-id 0; row-fetch path treats 0 as "use array".
              e (TopNEntry. k 0 i)]
          (if (< (.size pq) n)
            (.offer pq e)
            (let [^TopNEntry top (.peek pq)]
              (when (pos? (.compare cmp e top))
                (.poll pq)
                (.offer pq e)))))
        (recur (unchecked-inc i))))
    pq))

(defn- drain-heap-sorted
  "Drain `pq` into a vector ordered by `dir` (largest-first for :desc,
   smallest-first for :asc)."
  [^PriorityQueue pq dir]
  (let [out (java.util.ArrayList. (.size pq))]
    (while (pos? (.size pq))
      (.add out (.poll pq)))
    ;; pq drained in reverse of desired order (worst-first → best-first)
    (vec (reverse out))))

;; ============================================================================
;; Row fetch — given top-N entries, gather rows from the columns map
;; ============================================================================

(defn- chunk-by-id-map
  "Build {chunk-id → chunk} for an index. Walks the PSS once."
  [idx]
  (let [tree (index/idx-tree idx)
        entries (vec (pss/slice tree nil nil))]
    (persistent!
     (reduce (fn [m ^ChunkEntry e]
               (assoc! m (long (first (.chunk-id ^ChunkEntry e))) (.chunk ^ChunkEntry e)))
             (transient {})
             entries))))

(defn- gather-row
  "Read column `col-name` at the given (chunk-id, local-idx)."
  [columns col-name chunk-id local-idx]
  (let [col-info (get columns col-name)]
    (cond
      ;; Already-materialized array
      (:data col-info)
      (let [d (:data col-info)]
        (cond
          (expr/long-array? d)   (aget ^longs d (int local-idx))
          (expr/double-array? d) (aget ^doubles d (int local-idx))
          :else                  (nth d local-idx)))
      ;; Index-backed: look up chunk and read
      (:index col-info)
      (let [chk (get (chunk-by-id-map (:index col-info)) chunk-id)]
        (when chk
          (case (:type col-info)
            :float64 (chunk/read-double chk local-idx)
            (chunk/read-long chk local-idx))))
      :else nil)))

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

   `select` may be nil (= all columns) or a vector of column kws."
  [query columns]
  (let [{:keys [order limit select]} query
        first-spec (first order)
        [^clojure.lang.Keyword col-kw dir] (if (vector? first-spec)
                                             first-spec
                                             [first-spec :asc])
        n (long limit)
        order-col (get columns col-kw)
        datatype (:type order-col)
        all-keys (vec (keys columns))
        ;; Determine output projection. select may include :as
        ;; aliases or expressions; we restrict to plain keyword refs
        ;; in the eligibility gate so this is straightforward.
        out-cols (cond
                   (or (nil? select) (= [:*] select)) all-keys
                   (every? keyword? select)           (vec select)
                   :else                              all-keys)
        ;; Build chunk-by-id maps lazily per output column on first
        ;; access to keep eligibility (which doesn't touch them) cheap.
        chunk-maps (volatile! {})
        get-chunk-map (fn [^clojure.lang.Keyword k]
                        (or (get @chunk-maps k)
                            (let [m (when-let [idx (:index (get columns k))]
                                      (chunk-by-id-map idx))]
                              (vswap! chunk-maps assoc k m)
                              m)))
        ;; Phase 1: streaming top-N
        pq (cond
             (:data order-col)  (find-top-n-on-array
                                 (:data order-col) n dir datatype)
             (:index order-col) (find-top-n-on-index
                                 (:index order-col) n dir datatype)
             :else              (throw (ex-info "top-N: order column has neither :data nor :index"
                                                {:col col-kw})))
        sorted (drain-heap-sorted pq dir)]
    ;; Phase 2: fetch rows
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
                                    (aget ^longs d (int local-idx))
                                    (expr/double-array? d)
                                    (aget ^doubles d (int local-idx))
                                    :else (nth d local-idx)))
                                (:index col-info)
                                (when-let [chk (get (get-chunk-map k) chunk-id)]
                                  (case (:type col-info)
                                    :float64 (chunk/read-double chk local-idx)
                                    (chunk/read-long chk local-idx)))
                                :else nil)
                            v (decode-string-value col-info v)]
                        [(norm/strip-ns k) v])))
               out-cols)))
     sorted)))
