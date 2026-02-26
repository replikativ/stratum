(ns insert-bench
  "Insert benchmark: simulates Datahike EAVT/AVET index insertion patterns.

   Measures throughput of PersistentColumnIndex for:
   1. Sorted insert — maintain sort order via binary search + idx-insert!
   2. Mixed (80/20) — 80% append, 20% sorted insert (realistic EAVT pattern)

   Each pattern tested with varying batch sizes (datoms per transient cycle):
   1, 10, 100, 1000

   Usage:
     clj -M:insert-bench              ;; default 1M elements
     clj -M:insert-bench 100000       ;; custom element count"
  (:require [stratum.index :as index]
            [stratum.index.processor :as processor]
            [org.replikativ.persistent-sorted-set.diagnostics :as pss-diag])
  (:import [stratum.index PersistentColumnIndex]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Binary Search for Sorted Insertion Position
;; ============================================================================

(defn- sorted-insert-pos
  "Find the position where `val` should be inserted to maintain sort order.
   Binary search over the index: O(log N) idx-get-long calls."
  ^long [^PersistentColumnIndex idx ^long val]
  (let [n (long (index/idx-length idx))]
    (if (zero? n)
      0
      (loop [lo (long 0)
             hi n]
        (if (>= lo hi)
          lo
          (let [mid (+ lo (unsigned-bit-shift-right (- hi lo) 1))
                mid-val (index/idx-get-long idx mid)]
            (if (< mid-val val)
              (recur (inc mid) hi)
              (recur lo mid))))))))

;; ============================================================================
;; Timing Helpers
;; ============================================================================

(defn- bench-insert
  "Time an insert workload. Returns {:total-ms :throughput :per-batch-us :result}.
   f takes no args and performs the full insert workload, returns the final index."
  [description f n-elements n-batches warmup-f]
  ;; Warmup (smaller scale)
  (when warmup-f
    (dotimes [_ 3] (warmup-f)))
  (System/gc)
  (Thread/sleep 200)
  ;; Timed run (3 iterations, take median)
  (let [last-result (atom nil)
        times (mapv (fn [_]
                      (System/gc)
                      (Thread/sleep 100)
                      (let [start (System/nanoTime)
                            result (f)
                            end (System/nanoTime)
                            ms (/ (- end start) 1e6)]
                        ;; Verify result
                        (assert (= n-elements (index/idx-length result))
                                (str "Expected " n-elements " elements, got " (index/idx-length result)))
                        (reset! last-result result)
                        ms))
                    (range 3))
        sorted-times (sort times)
        median (nth sorted-times 1)
        throughput (/ n-elements (/ median 1000.0))
        per-batch-us (/ (* median 1000.0) (double n-batches))]
    (println (format "  %-45s %8.1fms  %10.0f ops/sec  %7.1f µs/batch"
                     description median throughput per-batch-us))
    {:total-ms median :throughput throughput :per-batch-us per-batch-us
     :result @last-result}))

(defn- print-diagnostics
  "Print PSS tree stats and chunk distribution for an index."
  [^PersistentColumnIndex idx label]
  (let [tree (index/idx-tree idx)
        ts (pss-diag/tree-stats tree)
        cd (index/idx-chunk-distribution idx)]
    (println (format "    %s — tree: depth=%d, leaves=%d, fill=%.0f%%/%.0f%%/%.0f%% (p25/p50/p75)"
                     label
                     (:depth ts)
                     (:leaf-count ts)
                     (* 100.0 (get-in ts [:leaf-fill-histogram :p25]))
                     (* 100.0 (get-in ts [:leaf-fill-histogram :p50]))
                     (* 100.0 (get-in ts [:leaf-fill-histogram :p75]))))
    (println (format "    %s — chunks: n=%d, sizes=%d/%d/%d/%d (min/p50/p90/max), fill=%.0f%%, undersized=%d"
                     (apply str (repeat (count label) \space))
                     (:chunk-count cd)
                     (:min cd) (:p50 cd) (:p90 cd) (:max cd)
                     (* 100.0 (:fill-ratio cd))
                     (:undersized cd)))))

;; ============================================================================
;; Index Factory
;; ============================================================================

(def ^:private ^:const CHUNK_SIZE 8192)

(defn- make-bench-index
  "Create an index, optionally with the ChunkCompactionProcessor."
  ^PersistentColumnIndex [datatype processor?]
  (if processor?
    (index/make-index datatype {:chunk-size CHUNK_SIZE
                                :leaf-processor (processor/make-chunk-compaction-processor CHUNK_SIZE)})
    (index/make-index datatype)))

;; ============================================================================
;; Sorted Insert Workload
;; ============================================================================

(defn- run-sorted-insert
  "Insert N elements in sorted order using binary search + idx-insert!.
   Batch size = number of inserts per transient/persistent cycle."
  (^PersistentColumnIndex [^long n ^long batch-size]
   (run-sorted-insert n batch-size false))
  (^PersistentColumnIndex [^long n ^long batch-size processor?]
   (let [;; Pre-generate random values to insert (exclude from timing)
         values (long-array n)]
     (dotimes [i n]
       (aset values i (long (* (Math/random) Long/MAX_VALUE))))
     ;; Sort them so we know the correct insertion order
     (java.util.Arrays/sort values)
     ;; Shuffle to simulate random arrival order
     (let [shuffled (long-array n)
           indices (int-array n)]
       (dotimes [i n] (aset indices i (int i)))
       ;; Fisher-Yates shuffle
       (let [rng (java.util.Random. 42)]
         (loop [i (dec n)]
           (when (> i 0)
             (let [j (.nextInt rng (inc i))
                   tmp (aget indices i)]
               (aset indices i (aget indices j))
               (aset indices j tmp)
               (recur (dec i))))))
       (dotimes [i n]
         (aset shuffled i (aget values (aget indices i))))
       ;; Now insert shuffled values maintaining sort order
       (loop [idx (make-bench-index :int64 processor?)
              i 0]
         (if (>= i n)
           idx
           (let [batch-end (min (+ i batch-size) n)
                 idx (index/idx-transient idx)
                 idx (loop [idx idx j i]
                       (if (>= j batch-end)
                         idx
                         (let [val (aget shuffled j)
                               pos (sorted-insert-pos idx val)]
                           (recur (index/idx-insert! idx pos val) (inc j)))))
                 idx (index/idx-persistent! idx)]
             (recur idx batch-end))))))))

;; ============================================================================
;; Mixed Workload (80% Append, 20% Sorted Insert)
;; ============================================================================

(defn- run-mixed-insert
  "80% append (sequential increasing values), 20% sorted insert (random backfills).
   Simulates EAVT pattern: mostly new entities (increasing E), some backfill updates."
  (^PersistentColumnIndex [^long n ^long batch-size]
   (run-mixed-insert n batch-size false))
  (^PersistentColumnIndex [^long n ^long batch-size processor?]
  (let [rng (java.util.Random. 42)
        ;; Pre-generate values: 80% increasing, 20% random (within current range)
        append-fraction 0.8
        n-append (long (* n append-fraction))
        n-insert (- n n-append)]
    (loop [idx (make-bench-index :int64 processor?)
           next-seq-val (long 0)      ;; next sequential value for appends
           appends-remaining n-append
           inserts-remaining n-insert
           batch-count 0]
      (if (and (zero? appends-remaining) (zero? inserts-remaining))
        idx
        (let [batch-end (min batch-size (+ appends-remaining inserts-remaining))
              idx (index/idx-transient idx)
              ;; Decide how many appends vs inserts in this batch
              total-remaining (+ appends-remaining inserts-remaining)
              batch-appends (if (zero? total-remaining) 0
                                (min appends-remaining
                                     (long (Math/ceil (* batch-end (/ (double appends-remaining) total-remaining))))))
              batch-inserts (min inserts-remaining (- batch-end batch-appends))
              ;; Do appends first (fast)
              [idx next-seq-val] (loop [idx idx v next-seq-val i 0]
                                   (if (>= i batch-appends)
                                     [idx v]
                                     (recur (index/idx-append! idx v) (inc v) (inc i))))
              ;; Then sorted inserts (slower — binary search + shift)
              idx (loop [idx idx i 0]
                    (if (>= i batch-inserts)
                      idx
                      (let [len (index/idx-length idx)
                            ;; Insert a value that fits somewhere in the existing range
                            val (if (> len 0)
                                  (long (* (.nextDouble rng) (double next-seq-val)))
                                  0)
                            pos (sorted-insert-pos idx val)]
                        (recur (index/idx-insert! idx pos val) (inc i)))))
              idx (index/idx-persistent! idx)]
          (recur idx next-seq-val
                 (- appends-remaining batch-appends)
                 (- inserts-remaining batch-inserts)
                 (inc batch-count))))))))

;; ============================================================================
;; Append-Only Baseline
;; ============================================================================

(defn- run-append-only
  "Pure sequential append — baseline for comparison."
  (^PersistentColumnIndex [^long n ^long batch-size]
   (run-append-only n batch-size false))
  (^PersistentColumnIndex [^long n ^long batch-size processor?]
  (loop [idx (make-bench-index :int64 processor?)
         i 0]
    (if (>= i n)
      idx
      (let [batch-end (min (+ i batch-size) n)
            idx (index/idx-transient idx)
            idx (loop [idx idx j i]
                  (if (>= j batch-end)
                    idx
                    (recur (index/idx-append! idx (long j)) (inc j))))
            idx (index/idx-persistent! idx)]
        (recur idx batch-end))))))

;; ============================================================================
;; Main
;; ============================================================================

(defn -main [& args]
  (let [n (if (seq args) (Long/parseLong (first args)) 1000000)
        batch-sizes [1 10 100 1000]]

    (println "================================================================")
    (println (format "  Stratum Insert Benchmark — %,d elements" n))
    (println (format "  Chunk size: %d, JVM: %s" 8192 (System/getProperty "java.version")))
    (println "================================================================")

    ;; === Append-Only Baseline ===
    (println "\n--- Append-Only (baseline) ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [last-result (atom nil)]
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double n) bs)))
              r (bench-insert
                  (format "append-only, batch=%d (%d batches)" bs n-batches)
                  #(run-append-only n bs)
                  n n-batches
                  #(run-append-only (min 10000 n) bs))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "append-only"))

    ;; === Sorted Insert ===
    (println "\n--- Sorted Insert (AVET-like: random values, maintain sort order) ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [sorted-n (min n 100000)
          last-result (atom nil)]
      (when (< sorted-n n)
        (println (format "  (scaled to %,d elements — sorted insert is O(N) per insert)" sorted-n)))
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double sorted-n) bs)))
              r (bench-insert
                  (format "sorted-insert, batch=%d (%d batches)" bs n-batches)
                  #(run-sorted-insert sorted-n bs)
                  sorted-n n-batches
                  #(run-sorted-insert (min 5000 sorted-n) bs))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "sorted-insert"))

    ;; === Mixed (80/20) ===
    (println "\n--- Mixed 80/20 (EAVT-like: 80%% append, 20%% sorted insert) ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [mixed-n (min n 100000)
          last-result (atom nil)]
      (when (< mixed-n n)
        (println (format "  (scaled to %,d elements — sorted inserts are O(N))" mixed-n)))
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double mixed-n) bs)))
              r (bench-insert
                  (format "mixed-80/20, batch=%d (%d batches)" bs n-batches)
                  #(run-mixed-insert mixed-n bs)
                  mixed-n n-batches
                  #(run-mixed-insert (min 5000 mixed-n) bs))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "mixed-80/20"))

    ;; === With Processor: Append-Only ===
    (println "\n--- Append-Only + ChunkCompactionProcessor ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [last-result (atom nil)]
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double n) bs)))
              r (bench-insert
                  (format "append+proc, batch=%d (%d batches)" bs n-batches)
                  #(run-append-only n bs true)
                  n n-batches
                  #(run-append-only (min 10000 n) bs true))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "append+proc"))

    ;; === With Processor: Sorted Insert ===
    (println "\n--- Sorted Insert + ChunkCompactionProcessor ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [sorted-n (min n 100000)
          last-result (atom nil)]
      (when (< sorted-n n)
        (println (format "  (scaled to %,d elements)" sorted-n)))
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double sorted-n) bs)))
              r (bench-insert
                  (format "sorted+proc, batch=%d (%d batches)" bs n-batches)
                  #(run-sorted-insert sorted-n bs true)
                  sorted-n n-batches
                  #(run-sorted-insert (min 5000 sorted-n) bs true))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "sorted+proc"))

    ;; === With Processor: Mixed (80/20) ===
    (println "\n--- Mixed 80/20 + ChunkCompactionProcessor ---")
    (println (format "  %-45s %8s  %10s  %7s" "Description" "Time" "Throughput" "µs/batch"))
    (let [mixed-n (min n 100000)
          last-result (atom nil)]
      (when (< mixed-n n)
        (println (format "  (scaled to %,d elements)" mixed-n)))
      (doseq [bs batch-sizes]
        (let [n-batches (long (Math/ceil (/ (double mixed-n) bs)))
              r (bench-insert
                  (format "mixed+proc, batch=%d (%d batches)" bs n-batches)
                  #(run-mixed-insert mixed-n bs true)
                  mixed-n n-batches
                  #(run-mixed-insert (min 5000 mixed-n) bs true))]
          (reset! last-result (:result r))))
      (print-diagnostics @last-result "mixed+proc"))

    ;; === Verification ===
    (println "\n--- Verification ---")
    (let [idx (run-sorted-insert 1000 10)]
      (println (format "  Sorted insert 1000 elements: length=%d, sorted=%s"
                       (index/idx-length idx)
                       (let [vals (mapv #(index/idx-get-long idx %) (range (index/idx-length idx)))]
                         (= vals (sort vals))))))
    (let [idx (run-mixed-insert 1000 10)]
      (println (format "  Mixed insert 1000 elements: length=%d" (index/idx-length idx))))
    (let [idx (run-append-only 1000 100 true)]
      (println (format "  Append+proc 1000 elements: length=%d" (index/idx-length idx))))

    (println "\nInsert benchmark complete.")))
