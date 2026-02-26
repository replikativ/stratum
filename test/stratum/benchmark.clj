(ns stratum.benchmark
  "Benchmarks comparing stratum vs dtype-next for update/append performance.

   Key insight:
   - dtype-next: contiguous arrays, O(n) for any mutation (must copy entire buffer)
   - stratum: chunked structure, O(chunk-size + log chunks) for mutations

   This makes stratum particularly advantageous for:
   - OLTP-style random updates
   - Batch updates (transient mode)
   - Incremental appends

   dtype-next is faster for:
   - Sequential reads (cache-friendly contiguous access)
   - Bulk analytics (SIMD on contiguous data)"
  (:require [stratum.index :as index]
            [tech.v3.datatype :as dtype]
            [criterium.core :as crit]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; dtype-next operations (baseline)
;; ============================================================================

(defn dtype-update-value
  "Update a value in a dtype-next buffer.
   For immutable semantics, requires copying entire buffer. O(n)"
  [buf idx value]
  (let [new-buf (dtype/clone buf)]
    (.writeDouble ^tech.v3.datatype.Buffer (dtype/->buffer new-buf) idx (double value))
    new-buf))

(defn dtype-append
  "Append a value to a dtype-next buffer.
   Requires copying entire buffer. O(n)"
  [buf value]
  (let [n (dtype/ecount buf)
        new-buf (dtype/make-container :native-heap :float64 (inc n))]
    (dtype/copy! buf (dtype/sub-buffer new-buf 0 n))
    (.writeDouble ^tech.v3.datatype.Buffer (dtype/->buffer new-buf) n (double value))
    new-buf))

;; ============================================================================
;; Benchmark Scenarios
;; ============================================================================

(defn bench-single-update
  "Compare updating a single value: dtype-next vs stratum"
  [n]
  (println "\n=== Single Update Benchmark (n=" n ") ===")
  (println "dtype-next: must clone entire buffer O(n)")
  (println "stratum: only CoW copy affected chunk O(chunk-size)")

  (let [data (vec (range n))
        dtype-buf (dtype/make-container :native-heap :float64 data)
        sag-idx (index/index-from-seq :float64 data {:chunk-size 8192})
        mid (long (/ n 2))]

    (println "\ndtype-next (clone + update):")
    (crit/quick-bench (dtype-update-value dtype-buf mid 999.0))

    (println "\nstratum (fork -> transient -> set -> persistent):")
    (crit/quick-bench
     (-> sag-idx
         index/idx-fork              ;; Fork first to get fresh persistent copy
         index/idx-transient
         (index/idx-set mid 999.0)
         index/idx-persistent!))))

(defn bench-batch-updates
  "Compare multiple random updates in a batch: dtype-next vs stratum"
  [n num-updates]
  (println "\n=== Batch Updates Benchmark (n=" n ", updates=" num-updates ") ===")
  (println "dtype-next: O(n) per update = O(n * num-updates) total")
  (println "stratum: transient mode batches all updates O(chunk-size * touched-chunks)")

  (let [data (vec (range n))
        dtype-buf (dtype/make-container :native-heap :float64 data)
        sag-idx (index/index-from-seq :float64 data {:chunk-size 8192})
        indices (vec (repeatedly num-updates #(rand-int n)))]

    (println "\ndtype-next (clone for each update):")
    (crit/quick-bench
     (reduce (fn [buf i] (dtype-update-value buf i 999.0))
             dtype-buf
             indices))

    (println "\nstratum (single transient batch):")
    (crit/quick-bench
     (let [idx' (-> sag-idx index/idx-fork index/idx-transient)]
       (doseq [i indices]
         (index/idx-set idx' i 999.0))
       (index/idx-persistent! idx')))))

(defn bench-sequential-appends
  "Compare appending values: dtype-next vs stratum"
  [n num-appends]
  (println "\n=== Sequential Appends Benchmark (initial=" n ", appends=" num-appends ") ===")
  (println "dtype-next: O(n) per append = O(n * num-appends + num-appends²/2) total")
  (println "stratum: only touches last chunk, O(chunk-size) amortized per append")

  (let [data (vec (range n))
        dtype-buf (dtype/make-container :native-heap :float64 data)
        sag-idx (index/index-from-seq :float64 data {:chunk-size 8192})]

    (println "\ndtype-next (copy + append each time):")
    (crit/quick-bench
     (reduce (fn [buf v] (dtype-append buf (double v)))
             dtype-buf
             (range num-appends)))

    (println "\nstratum (transient batch append):")
    (crit/quick-bench
     (let [idx' (-> sag-idx index/idx-fork index/idx-transient)]
       (doseq [v (range num-appends)]
         (index/idx-append idx' (double v)))
       (index/idx-persistent! idx')))))

(defn bench-read-performance
  "Compare random read performance (stratum has PSS lookup overhead)"
  [n num-reads]
  (println "\n=== Random Read Benchmark (n=" n ", reads=" num-reads ") ===")
  (println "dtype-next: O(1) direct array access")
  (println "stratum: O(log chunks) PSS lookup + O(1) chunk offset")

  (let [data (vec (range n))
        dtype-buf (dtype/make-container :native-heap :float64 data)
        sag-idx (index/index-from-seq :float64 data {:chunk-size 8192})
        indices (long-array (repeatedly num-reads #(rand-int n)))]

    (println "\ndtype-next (direct array access):")
    (crit/quick-bench
     (let [buf (dtype/->buffer dtype-buf)]
       (areduce indices i ret 0.0
                (+ ret (.readDouble ^tech.v3.datatype.Buffer buf (aget indices i))))))

    (println "\nstratum (chunk lookup + offset):")
    (crit/quick-bench
     (areduce indices i ret 0.0
              (+ ret (index/idx-get-double sag-idx (aget indices i)))))))

(defn bench-sequential-read
  "Compare sequential read performance (cache effects)"
  [n]
  (println "\n=== Sequential Read Benchmark (n=" n ") ===")
  (println "dtype-next: contiguous memory, excellent cache locality")
  (println "stratum: chunked but still reasonable locality within chunks")

  (let [data (vec (range n))
        dtype-buf (dtype/make-container :native-heap :float64 data)
        sag-idx (index/index-from-seq :float64 data {:chunk-size 8192})]

    (println "\ndtype-next (sum via reduce):")
    (crit/quick-bench
     (tech.v3.datatype.functional/sum dtype-buf))

    (println "\nstratum (scan chunks):")
    (crit/quick-bench
     (index/idx-sum sag-idx))

    (println "\nstratum (stats-based sum - O(chunks)):")
    (crit/quick-bench
     (index/idx-sum-stats sag-idx))))

(defn run-all-benchmarks
  "Run all benchmarks with various sizes"
  []
  (doseq [n [10000 100000 1000000]]
    (println "\n" (apply str (repeat 60 "=")))
    (println "SIZE:" n)
    (println (apply str (repeat 60 "=")))

    (bench-single-update n)
    (bench-batch-updates n 100)
    (bench-sequential-appends (min n 10000) 1000)
    (bench-read-performance n 10000)
    (bench-sequential-read n)))

(comment
  ;; Quick benchmarks
  (bench-single-update 100000)
  (bench-batch-updates 100000 100)
  (bench-sequential-appends 10000 1000)
  (bench-read-performance 100000 10000)
  (bench-sequential-read 100000)

  ;; Run all
  (run-all-benchmarks))
