;; # Columnar Internals
;;
;; Under the hood Stratum stores data in **chunks** — fixed-size typed
;; arrays grouped into a
;; [persistent sorted set](https://github.com/replikativ/persistent-sorted-set)
;; (PSS) index. This notebook walks through the internal building
;; blocks: chunks, indices, copy-on-write semantics, zone-map
;; statistics, and range-optimised aggregation.
;;
;; These are implementation details — most users only need
;; `stratum.api`. But understanding them clarifies why Stratum is fast.

(ns stratum-book.columnar-internals
  (:require
   [stratum.chunk :as chunk]
   [stratum.index :as index]
   [scicloj.kindly.v4.kind :as kind]))

;; ---
;;
;; ## Chunks
;;
;; A **chunk** is a flat, typed array (`:float64` or `:int64`) with a
;; fixed capacity. Chunks are the atomic unit of storage, zone-map
;; metadata, and [copy-on-write](https://en.wikipedia.org/wiki/Copy-on-write)
;; sharing.

;; ### Creation

(def c-f64 (chunk/make-chunk :float64 10))
(def c-i64 (chunk/make-chunk :int64 10))

(chunk/chunk-length c-f64)

(kind/test-last
 [(fn [n] (= 10 n))])

(chunk/chunk-datatype c-f64)

(kind/test-last
 [(fn [t] (= :float64 t))])

(chunk/chunk-datatype c-i64)

(kind/test-last
 [(fn [t] (= :int64 t))])

;; ### Read / Write
;;
;; Chunks use a
;; [transient / persistent protocol](https://clojure.org/reference/transients) —
;; writes require a `col-transient` call first.

(def c-rw
  (let [c (chunk/col-transient (chunk/make-chunk :float64 10))]
    (chunk/write-double! c 0 42.0)
    (chunk/write-double! c 5 3.14)
    (chunk/col-persistent! c)
    c))

[(chunk/read-double c-rw 0)
 (chunk/read-double c-rw 5)
 (chunk/read-double c-rw 1)]

(kind/test-last
 [(fn [[v0 v5 v1]]
    (and (= 42.0 v0)
         (= 3.14 v5)
         (= 0.0 v1)))])

;; Writing to a persistent chunk throws:

(try
  (chunk/write-double! (chunk/make-chunk :float64 10) 0 42.0)
  :no-error
  (catch IllegalStateException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ### Copy-on-Write
;;
;; `col-fork` creates a logical copy that shares the underlying array.
;; The first mutation triggers a physical copy — subsequent writes hit
;; the private copy, leaving the original unchanged.

(def c-original
  (let [c (chunk/col-transient (chunk/make-chunk :float64 10))]
    (chunk/write-double! c 0 42.0)
    (chunk/col-persistent! c)
    c))

(def c-forked
  (let [c (chunk/col-transient (chunk/col-fork c-original))]
    (chunk/write-double! c 0 99.0)
    (chunk/col-persistent! c)
    c))

{:original (chunk/read-double c-original 0)
 :forked   (chunk/read-double c-forked 0)}

(kind/test-last
 [(fn [{:keys [original forked]}]
    (and (= 42.0 original)
         (= 99.0 forked)))])

;; The reverse direction also holds — mutating the *original* after a
;; fork does not affect the fork:

(def c-base
  (let [c (chunk/col-transient (chunk/make-chunk :float64 10))]
    (chunk/write-double! c 0 42.0)
    (chunk/write-double! c 1 7.0)
    (chunk/col-persistent! c)
    c))

(def c-snapshot (chunk/col-fork c-base))

;; Mutate the original:
(let [c (chunk/col-transient c-base)]
  (chunk/write-double! c 0 999.0)
  (chunk/col-persistent! c))

{:base-mutated (chunk/read-double c-base 0)
 :snapshot-safe (chunk/read-double c-snapshot 0)
 :snapshot-v1   (chunk/read-double c-snapshot 1)}

(kind/test-last
 [(fn [{:keys [base-mutated snapshot-safe snapshot-v1]}]
    (and (= 999.0 base-mutated)
         (= 42.0 snapshot-safe)
         (= 7.0 snapshot-v1)))])

;; Int64 isolation works identically:

(let [c1 (let [c (chunk/col-transient (chunk/make-chunk :int64 10))]
           (chunk/write-long! c 0 42)
           (chunk/col-persistent! c)
           c)
      c2 (chunk/col-fork c1)
      _  (let [c (chunk/col-transient c1)]
           (chunk/write-long! c 0 999)
           (chunk/col-persistent! c))]
  {:original (chunk/read-long c1 0)
   :fork     (chunk/read-long c2 0)})

(kind/test-last
 [(fn [{:keys [original fork]}]
    (and (= 999 original)
         (= 42 fork)))])

;; ### Creating from a Sequence

(def c-seq (chunk/chunk-from-seq :float64 [1.0 2.0 3.0 4.0 5.0]))

[(chunk/chunk-length c-seq)
 (chunk/read-double c-seq 0)
 (chunk/read-double c-seq 2)
 (chunk/read-double c-seq 4)]

(kind/test-last
 [(fn [[len v0 v2 v4]]
    (and (= 5 len)
         (= 1.0 v0) (= 3.0 v2) (= 5.0 v4)))])

;; ---
;;
;; ## Indices
;;
;; A **PersistentColumnIndex** is a tree of chunks managed by a
;; [persistent sorted set](https://github.com/replikativ/persistent-sorted-set).
;; It supports O(1) fork, O(log n) point access, and O(chunks)
;; aggregate statistics via zone maps.

;; ### Creation

(def idx-empty (index/make-index :float64))

(index/idx-length idx-empty)

(kind/test-last
 [(fn [n] (= 0 n))])

(def idx-100 (index/index-from-seq :float64 (range 100)))

[(index/idx-length idx-100)
 (index/idx-get-double idx-100 0)
 (index/idx-get-double idx-100 50)
 (index/idx-get-double idx-100 99)]

(kind/test-last
 [(fn [[len v0 v50 v99]]
    (and (= 100 len)
         (= 0.0 v0) (= 50.0 v50) (= 99.0 v99)))])

;; ### Multi-Chunk Indices
;;
;; When data exceeds the chunk size (default 8192), the index
;; transparently spans multiple chunks.

(def idx-10k (index/index-from-seq :float64 (range 10000)))

[(index/idx-length idx-10k)
 (index/idx-get-double idx-10k 0)
 (index/idx-get-double idx-10k 5000)
 (index/idx-get-double idx-10k 9999)]

(kind/test-last
 [(fn [[len v0 v5k v9999]]
    (and (= 10000 len)
         (= 0.0 v0) (= 5000.0 v5k) (= 9999.0 v9999)))])

;; ### Bounds Checking

(try
  (index/idx-get-double idx-100 -1)
  :no-error
  (catch IndexOutOfBoundsException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try
  (index/idx-get-double idx-100 100)
  :no-error
  (catch IndexOutOfBoundsException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ### Fork + Copy-on-Write

(def idx-original (index/index-from-seq :float64 (range 100)))

(def idx-modified
  (-> (index/idx-fork idx-original)
      index/idx-transient
      (index/idx-set! 0 999.0)
      index/idx-persistent!))

{:original (index/idx-get-double idx-original 0)
 :modified (index/idx-get-double idx-modified 0)
 :shared   (index/idx-get-double idx-modified 50)}

(kind/test-last
 [(fn [{:keys [original modified shared]}]
    (and (= 0.0 original)
         (= 999.0 modified)
         (= 50.0 shared)))])

;; ### Append

(def idx-appended
  (-> (index/make-index :float64)
      index/idx-transient
      (index/idx-append! 1.0)
      (index/idx-append! 2.0)
      (index/idx-append! 3.0)
      index/idx-persistent!))

[(index/idx-length idx-appended)
 (index/idx-get-double idx-appended 0)
 (index/idx-get-double idx-appended 1)
 (index/idx-get-double idx-appended 2)]

(kind/test-last
 [(fn [[len v0 v1 v2]]
    (and (= 3 len)
         (= 1.0 v0) (= 2.0 v1) (= 3.0 v2)))])

;; Transient guard — mutation without `idx-transient` throws:

(try
  (index/idx-set! idx-100 0 42.0)
  :no-error
  (catch IllegalStateException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ### Dirty Chunk Tracking
;;
;; The storage layer only persists chunks that changed.
;; `idx-dirty-chunks` returns the set of modified chunk keys.

(let [idx (index/idx-transient (index/make-index :float64))]
  ;; Initially empty
  (assert (empty? (index/idx-dirty-chunks idx)))
  ;; Append marks chunk dirty
  (index/idx-append! idx 42.0)
  (let [dirty-after-append (index/idx-dirty-chunks idx)]
    ;; Clear and verify
    (index/idx-clear-dirty! idx)
    (let [dirty-after-clear (index/idx-dirty-chunks idx)]
      ;; Set marks dirty again
      (index/idx-set! idx 0 99.0)
      {:after-append (contains? dirty-after-append [0])
       :after-clear  (empty? dirty-after-clear)
       :after-set    (contains? (index/idx-dirty-chunks idx) [0])})))

(kind/test-last
 [(fn [{:keys [after-append after-clear after-set]}]
    (and after-append after-clear after-set))])

;; ---
;;
;; ## Zone-Map Statistics
;;
;; Each chunk maintains running statistics: count, sum, sum-of-squares,
;; min, max. Aggregations over an entire index can be answered in
;; **O(chunks)** without touching individual values.

(def stats-idx (index/index-from-seq :float64 (range 100)))

(index/idx-stats stats-idx)

(kind/test-last
 [(fn [{:keys [count sum min-val max-val sum-sq]}]
    (and (= 100 count)
         (= 4950.0 sum)     ;; 0+1+…+99
         (= 0.0 min-val)
         (= 99.0 max-val)
         (= 328350.0 sum-sq)))])  ;; Σi² = 99·100·199/6

;; Convenience functions on top of stats:

{:sum     (index/idx-sum-stats stats-idx)
 :mean    (index/idx-mean-stats stats-idx)
 :min     (index/idx-min-stats stats-idx)
 :max     (index/idx-max-stats stats-idx)}

(kind/test-last
 [(fn [{:keys [sum mean min max]}]
    (and (= 4950.0 sum)
         (= 49.5 mean)
         (= 0.0 min)
         (= 99.0 max)))])

;; Variance and standard deviation:

(let [v (index/idx-variance-stats stats-idx)
      s (index/idx-stddev-stats stats-idx)]
  {:variance (number? v)
   :stddev   (number? s)})

(kind/test-last
 [(fn [{:keys [variance stddev]}]
    (and variance stddev))])

;; ### Stats After Mutation
;;
;; Statistics are recomputed correctly after transient append/set.

;; Single-chunk append:

(let [idx (index/idx-transient (index/make-index :float64))]
  (dotimes [i 100] (index/idx-append! idx (double i)))
  (let [s (index/idx-stats (index/idx-persistent! idx))]
    {:count (:count s) :sum (:sum s)
     :min (:min-val s) :max (:max-val s)}))

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 100 count)
         (== 4950.0 sum)
         (== 0.0 min)
         (== 99.0 max)))])

;; Multi-chunk append (10 000 values):

(let [idx (index/idx-transient (index/make-index :float64))
      n   10000]
  (dotimes [i n] (index/idx-append! idx (double i)))
  (let [s (index/idx-stats (index/idx-persistent! idx))]
    {:count (:count s) :sum (:sum s)
     :min (:min-val s) :max (:max-val s)}))

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 10000 count)
         (== 4.9995E7 sum)    ;; 9999·10000/2
         (== 0.0 min)
         (== 9999.0 max)))])

;; Int64 column stats:

(let [idx (index/idx-transient (index/make-index :int64))]
  (dotimes [i 200] (index/idx-append! idx (long i)))
  (let [s (index/idx-stats (index/idx-persistent! idx))]
    {:count (:count s) :sum (:sum s)}))

(kind/test-last
 [(fn [{:keys [count sum]}]
    (and (= 200 count)
         (== 19900.0 sum)))])    ;; 199·200/2

;; ---
;;
;; ## Multi-Chunk Statistics
;;
;; With an explicit `:chunk-size`, we can force multiple chunks and
;; verify that aggregated statistics are correct.

(def mc-idx
  (index/index-from-seq :float64 (range 1000) {:chunk-size 100}))

(count (index/idx-all-chunk-stats mc-idx))

(kind/test-last
 [(fn [n] (= 10 n))])

(let [s (index/idx-stats mc-idx)]
  {:count (:count s) :sum (:sum s)
   :min (:min-val s) :max (:max-val s)})

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 1000 count)
         (= 499500.0 sum)    ;; 999·1000/2
         (= 0.0 min)
         (= 999.0 max)))])

;; ---
;;
;; ## Zone-Map Filtering
;;
;; Zone maps let the engine **skip entire chunks** whose `[min, max]`
;; range does not overlap the predicate. With sorted data each chunk
;; covers a distinct range, so filtering is highly selective.

;; Values > 850  — only last 2 of 10 chunks touched:

(def gt-result (index/idx-filter-gt mc-idx 850.0))

{:count (count gt-result)
 :first (first gt-result)
 :last  (last gt-result)}

(kind/test-last
 [(fn [{:keys [count first last]}]
    (and (= 149 count) (= 851 first) (= 999 last)))])

;; Values < 50  — only the first chunk:

(def lt-result (index/idx-filter-lt mc-idx 50.0))

{:count (count lt-result)
 :first (first lt-result)
 :last  (last lt-result)}

(kind/test-last
 [(fn [{:keys [count first last]}]
    (and (= 50 count) (= 0 first) (= 49 last)))])

;; Range [400, 600):

(def range-result (index/idx-filter-range mc-idx 400.0 600.0))

{:count (count range-result)
 :first (first range-result)
 :last  (last range-result)}

(kind/test-last
 [(fn [{:keys [count first last]}]
    (and (= 200 count) (= 400 first) (= 599 last)))])

;; ---
;;
;; ## Range Aggregations
;;
;; Combining zone-map pruning with per-chunk statistics, range
;; aggregations avoid scanning individual values for fully-covered
;; chunks. Only the "overhang" chunks at the boundaries are scanned.

;; Sum, count, mean, min, max of [200, 400):

{:sum   (index/idx-sum-range   mc-idx 200.0 400.0)
 :count (index/idx-count-range mc-idx 200.0 400.0)
 :mean  (index/idx-mean-range  mc-idx 200.0 400.0)
 :min   (index/idx-min-range   mc-idx 200.0 400.0)
 :max   (index/idx-max-range   mc-idx 200.0 400.0)}

(kind/test-last
 [(fn [{:keys [sum count mean min max]}]
    (and (= 59900.0 sum)      ;; (200+399)·200/2
         (= 200 count)
         (= 299.5 mean)
         (= 200.0 min)
         (= 399.0 max)))])

;; Partial overlap [150, 250) — overhang on chunks 1 and 2:

{:count (index/idx-count-range mc-idx 150.0 250.0)
 :sum   (index/idx-sum-range   mc-idx 150.0 250.0)}

(kind/test-last
 [(fn [{:keys [count sum]}]
    (and (= 100 count)
         (= 19950.0 sum)))])   ;; (150+249)·100/2

;; Range outside all data:

{:count (index/idx-count-range mc-idx 2000.0 3000.0)
 :sum   (index/idx-sum-range   mc-idx 2000.0 3000.0)}

(kind/test-last
 [(fn [{:keys [count sum]}]
    (and (= 0 count)
         (= 0.0 sum)))])

;; ---
;;
;; ## PSS Aggregate Statistics
;;
;; For large indices the PSS tree aggregates chunk-level stats in
;; O(log n) rather than O(chunks).

(def big-idx (index/index-from-seq :float64 (range 25000)))

(let [s (index/idx-stats big-idx)]
  {:count (:count s)
   :sum   (:sum s)
   :min   (:min-val s)
   :max   (:max-val s)})

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 25000 count)
         (= 312487500.0 sum)    ;; 24999·25000/2
         (= 0.0 min)
         (= 24999.0 max)))])

;; ### Range Stats — Exact Chunk Boundaries

(let [s (index/idx-stats-range big-idx 0 8192)]
  {:count (:count s)
   :sum   (:sum s)
   :min   (:min-val s)
   :max   (:max-val s)})

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 8192 count)
         (= 33550336.0 sum)    ;; 8191·8192/2
         (= 0.0 min)
         (= 8191.0 max)))])

;; ### Range Stats — Cross-Chunk With Overhangs

(let [s (index/idx-stats-range big-idx 4000 20000)]
  {:count (:count s)
   :sum   (:sum s)
   :min   (:min-val s)
   :max   (:max-val s)})

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 16000 count)
         (= 191992000.0 sum)    ;; (4000+19999)·16000/2
         (= 4000.0 min)
         (= 19999.0 max)))])

;; ### Range Stats — Full Index

(let [range-s (index/idx-stats-range big-idx 0 25000)
      full-s  (index/idx-stats big-idx)]
  {:count-match? (= (:count full-s) (:count range-s))
   :sum-match?   (= (:sum full-s) (:sum range-s))
   :min-match?   (= (:min-val full-s) (:min-val range-s))
   :max-match?   (= (:max-val full-s) (:max-val range-s))})

(kind/test-last
 [(fn [m] (every? true? (vals m)))])

;; ### Edge Cases — Single Element, Start, End

(let [s (index/idx-stats-range big-idx 100 101)]
  {:count (:count s) :sum (:sum s)})

(kind/test-last
 [(fn [{:keys [count sum]}]
    (and (= 1 count) (= 100.0 sum)))])

(let [s (index/idx-stats-range big-idx 0 10)]
  {:count (:count s) :sum (:sum s)})

(kind/test-last
 [(fn [{:keys [count sum]}]
    (and (= 10 count) (= 45.0 sum)))])   ;; 0+…+9

(let [s (index/idx-stats-range big-idx 24990 25000)]
  {:count (:count s) :sum (:sum s)
   :min (:min-val s) :max (:max-val s)})

(kind/test-last
 [(fn [{:keys [count sum min max]}]
    (and (= 10 count)
         (= 249945.0 sum)    ;; (24990+24999)·10/2
         (= 24990.0 min)
         (= 24999.0 max)))])

;; ### Error Handling

(try (index/idx-stats-range big-idx 100 100) :no-error
     (catch Exception _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try (index/idx-stats-range big-idx 100 99) :no-error
     (catch Exception _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try (index/idx-stats-range big-idx -1 100) :no-error
     (catch Exception _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])
