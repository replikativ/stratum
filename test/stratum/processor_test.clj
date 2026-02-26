(ns stratum.processor-test
  "Tests for ChunkCompactionProcessor — automatic chunk merging/splitting.

   Uses small chunk-size (8-32) to force the processor to fire frequently.
   Verifies data integrity, count consistency, stats correctness, and
   chunk layout after compaction/splitting."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.index :as index]
            [stratum.index.processor :as processor]
            [stratum.chunk :as chunk]
            [org.replikativ.persistent-sorted-set.diagnostics :as pss-diag]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helpers
;; ============================================================================

(defn- make-processor-index
  "Create an index with ChunkCompactionProcessor enabled."
  ([datatype chunk-size]
   (make-processor-index datatype chunk-size []))
  ([datatype chunk-size data]
   (index/index-from-seq datatype data
                         {:chunk-size chunk-size
                          :leaf-processor (processor/make-chunk-compaction-processor chunk-size)})))

(defn- idx-all-values
  "Read all values from an index as a vector of doubles."
  [idx]
  (mapv #(index/idx-get-double idx %) (range (index/idx-length idx))))

(defn- idx-chunk-lengths
  "Get lengths of all chunks in the index."
  [idx]
  (mapv :count (index/idx-all-chunk-stats idx)))

(defn- verify-index
  "Verify index invariants: count matches actual elements, stats consistent."
  [idx expected-values msg]
  (let [actual (idx-all-values idx)
        stats (index/idx-stats idx)]
    (is (= (count expected-values) (index/idx-length idx))
        (str msg " — idx-length mismatch"))
    (is (= (count expected-values) (count actual))
        (str msg " — actual element count mismatch"))
    (is (= expected-values actual)
        (str msg " — values mismatch"))
    (when (pos? (count expected-values))
      (is (= (count expected-values) (:count stats))
          (str msg " — stats :count mismatch"))
      (is (= (apply min expected-values) (:min-val stats))
          (str msg " — stats :min-val mismatch"))
      (is (= (apply max expected-values) (:max-val stats))
          (str msg " — stats :max-val mismatch")))))

(defn- verify-tree-health
  "Verify PSS tree structural integrity and chunk distribution invariants."
  [idx chunk-size msg]
  (let [tree (index/idx-tree idx)]
    ;; PSS structural integrity
    (is (pss-diag/validate-full tree)
        (str msg " — PSS validate-full"))
    (is (pss-diag/validate-counts-known tree)
        (str msg " — PSS counts known"))
    (is (pss-diag/validate-measures-known tree)
        (str msg " — PSS measures known"))
    ;; Chunk distribution invariants
    (let [dist (index/idx-chunk-distribution idx)]
      (when (pos? (:chunk-count dist))
        (is (zero? (:oversized dist))
            (str msg " — no oversized chunks (got " (:oversized dist) ")"))
        (is (>= (:min dist) 1)
            (str msg " — no empty chunks"))))))

;; ============================================================================
;; Basic wiring — processor enabled, data intact
;; ============================================================================

(deftest processor-basic-append-test
  (testing "Index with processor: appended data reads back correctly"
    (let [data (mapv double (range 100))
          idx (make-processor-index :float64 16 (range 100))]
      (verify-index idx data "after construction"))))

(deftest processor-basic-small-test
  (testing "Index with processor: small data (fits in one chunk)"
    (let [data (mapv double (range 5))
          idx (make-processor-index :float64 16 (range 5))]
      (verify-index idx data "small index"))))

(deftest processor-empty-test
  (testing "Index with processor: empty index"
    (let [idx (make-processor-index :float64 16)]
      (is (= 0 (index/idx-length idx))))))

;; ============================================================================
;; Compaction on delete — persistent path
;; ============================================================================

(deftest persistent-delete-compaction-test
  (testing "Persistent delete triggers compaction of small adjacent chunks"
    (let [n 64
          chunk-size 16
          idx (make-processor-index :float64 chunk-size (range n))
          initial-chunks (count (index/idx-all-chunk-stats idx))]
      ;; Delete elements from middle chunks to make them small
      ;; Delete every other element from indices 16-47 (chunks 2-3)
      (let [idx2 (reduce (fn [idx i]
                           (let [pos (long (some (fn [p]
                                                   (when (== (double i) (index/idx-get-double idx p)) p))
                                                 (range (index/idx-length idx))))]
                             (index/idx-delete idx pos)))
                         idx
                         ;; Delete elements 16,18,20,...,46 (every other in range)
                         (range 16 48 2))
            final-chunks (count (index/idx-all-chunk-stats idx2))
            expected (mapv double (remove (set (range 16 48 2)) (range n)))]
        (verify-index idx2 expected "after persistent deletes")
        ;; Chunks should have been compacted (fewer or equal chunks)
        (is (<= final-chunks initial-chunks)
            (str "Expected compaction: " final-chunks " <= " initial-chunks))))))

;; ============================================================================
;; Compaction on delete — transient path (the key scenario)
;; ============================================================================

(deftest transient-delete-compaction-test
  (testing "Transient bulk delete triggers compaction via processor"
    (let [n 64
          chunk-size 16
          idx (make-processor-index :float64 chunk-size (range n))
          ;; Delete even-indexed positions (0,2,4,...62) in descending order.
          ;; Descending ensures lower positions are unaffected by shifts.
          even-positions (reverse (range 0 n 2)) ;; 62, 60, 58, ..., 0
          idx2 (-> idx
                   index/idx-transient
                   (as-> t
                         (reduce (fn [t pos] (index/idx-delete! t pos)) t even-positions))
                   index/idx-persistent!)
          ;; Deleting positions 0,2,4,... from [0..63] removes values 0,2,4,...
          expected (mapv double (range 1 n 2))]
      (verify-index idx2 expected "after transient bulk delete"))))

(deftest transient-delete-all-from-chunk-test
  (testing "Transient delete: remove entire chunk's worth of elements"
    (let [chunk-size 8
          idx (make-processor-index :float64 chunk-size (range 32))
          ;; Delete all elements from the second chunk (indices 8-15)
          idx2 (-> idx
                   index/idx-transient
                   (as-> t
                     ;; Delete position 8 repeatedly (elements shift down)
                         (reduce (fn [t _] (index/idx-delete! t 8)) t (range 8)))
                   index/idx-persistent!)
          expected (mapv double (concat (range 8) (range 16 32)))]
      (verify-index idx2 expected "after deleting entire chunk"))))

;; ============================================================================
;; Split on insert — persistent path
;; ============================================================================

(deftest persistent-insert-split-test
  (testing "Persistent insert into full chunk triggers split"
    (let [chunk-size 8
          ;; Create index with one full chunk
          idx (make-processor-index :float64 chunk-size (range 8))
          ;; Insert at position 0 repeatedly: each pushes previous right
          ;; -1 at 0 → [-1 0 1..7], then -2 at 0 → [-2 -1 0 1..7], etc.
          idx2 (reduce (fn [idx i]
                         (index/idx-insert idx 0 (double (- -1 i))))
                       idx
                       (range 8))
          expected (mapv double (range -8 8))]
      (verify-index idx2 expected "after persistent inserts"))))

;; ============================================================================
;; Split on insert — transient path
;; ============================================================================

(deftest transient-insert-split-test
  (testing "Transient insert causes chunk growth, data stays correct"
    (let [chunk-size 8
          idx (make-processor-index :float64 chunk-size (range 8))
          ;; Bulk insert via transient
          idx2 (-> idx
                   index/idx-transient
                   (as-> t
                         (reduce (fn [t i]
                                   (index/idx-append! t (double (+ 8 i))))
                                 t
                                 (range 24)))
                   index/idx-persistent!)
          expected (mapv double (range 32))]
      (verify-index idx2 expected "after transient appends"))))

;; ============================================================================
;; Mixed operations
;; ============================================================================

(deftest mixed-insert-delete-persistent-test
  (testing "Interleaved persistent insert and delete with processor"
    (let [chunk-size 8
          idx (make-processor-index :float64 chunk-size (range 32))
          ;; Delete 8 elements from position 4 (removes values 4-11)
          idx2 (reduce (fn [idx _] (index/idx-delete idx 4)) idx (range 8))
          ;; Insert -1..-8 at position 0 (each pushes right: result is -8..-1 at front)
          idx3 (reduce (fn [idx i] (index/idx-insert idx 0 (double (- -1 i)))) idx2 (range 8))
          remaining (vec (concat (take 4 (range 32)) (drop 12 (range 32))))
          expected (mapv double (concat (range -8 0) remaining))]
      (verify-index idx3 expected "after mixed persistent ops"))))

(deftest mixed-insert-delete-transient-test
  (testing "Interleaved transient insert and delete with processor"
    (let [chunk-size 8
          idx (make-processor-index :float64 chunk-size (range 32))
          idx2 (-> idx
                   index/idx-transient
                   (as-> t
                     ;; Delete 8 from position 4
                         (reduce (fn [t _] (index/idx-delete! t 4)) t (range 8))
                     ;; Append 8 new values
                     (reduce (fn [t i] (index/idx-append! t (double (+ 100 i)))) t (range 8)))
                   index/idx-persistent!)
          remaining (vec (concat (take 4 (range 32)) (drop 12 (range 32))))
          expected (mapv double (concat remaining (range 100 108)))]
      (verify-index idx2 expected "after mixed transient ops"))))

;; ============================================================================
;; Stats consistency after processor operations
;; ============================================================================

(deftest stats-consistent-after-compaction-test
  (testing "Stats match manual scan after processor compaction"
    (let [chunk-size 16
          idx (make-processor-index :float64 chunk-size (range 100))
          ;; Delete many elements to trigger compaction
          idx2 (reduce (fn [idx _] (index/idx-delete idx 0)) idx (range 50))
          stats (index/idx-stats idx2)
          actual (idx-all-values idx2)
          manual-sum (reduce + 0.0 actual)
          manual-count (count actual)]
      (is (= manual-count (:count stats))
          "Stats count matches actual element count")
      (is (== manual-sum (:sum stats))
          "Stats sum matches manual sum")
      (is (= (apply min actual) (:min-val stats))
          "Stats min matches")
      (is (= (apply max actual) (:max-val stats))
          "Stats max matches"))))

(deftest stats-consistent-after-transient-compaction-test
  (testing "Stats match manual scan after transient processor compaction"
    (let [chunk-size 16
          idx (make-processor-index :float64 chunk-size (range 100))
          ;; Bulk delete first 50 via transient
          idx2 (-> idx
                   index/idx-transient
                   (as-> t (reduce (fn [t _] (index/idx-delete! t 0)) t (range 50)))
                   index/idx-persistent!)
          stats (index/idx-stats idx2)
          actual (idx-all-values idx2)]
      (is (= (count actual) (:count stats))
          "Stats count matches after transient compaction")
      (is (== (reduce + 0.0 actual) (:sum stats))
          "Stats sum matches after transient compaction"))))

;; ============================================================================
;; Round-trip: persistent → transient → bulk delete → persistent
;; ============================================================================

(deftest roundtrip-bulk-delete-test
  (testing "Build large index, go transient, bulk delete, persist — the real use case"
    (let [chunk-size 16
          n 200
          idx (make-processor-index :float64 chunk-size (range n))
          ;; Go transient, delete first half
          idx2 (-> idx
                   index/idx-transient
                   (as-> t (reduce (fn [t _] (index/idx-delete! t 0)) t (range 100)))
                   index/idx-persistent!)
          expected (mapv double (range 100 200))]
      (verify-index idx2 expected "after roundtrip bulk delete")
      (verify-tree-health idx2 chunk-size "after roundtrip bulk delete")
      ;; Verify we can still do further operations
      (let [idx3 (index/idx-append idx2 999.0)]
        (is (= 101 (index/idx-length idx3)))
        (is (= 999.0 (index/idx-get-double idx3 100)))))))

(deftest roundtrip-bulk-append-test
  (testing "Build index, go transient, bulk append, persist"
    (let [chunk-size 16
          idx (make-processor-index :float64 chunk-size (range 50))
          idx2 (-> idx
                   index/idx-transient
                   (as-> t (reduce (fn [t i] (index/idx-append! t (double (+ 50 i)))) t (range 150)))
                   index/idx-persistent!)
          expected (mapv double (range 200))]
      (verify-index idx2 expected "after roundtrip bulk append")
      (verify-tree-health idx2 chunk-size "after roundtrip bulk append"))))

;; ============================================================================
;; No processor vs processor: same results
;; ============================================================================

(deftest processor-matches-no-processor-test
  (testing "Index with processor produces same values as without"
    (let [data (range 100)
          idx-plain (index/index-from-seq :float64 data {:chunk-size 16})
          idx-proc (make-processor-index :float64 16 data)]
      (is (= (index/idx-length idx-plain) (index/idx-length idx-proc))
          "Same length")
      (is (= (idx-all-values idx-plain) (idx-all-values idx-proc))
          "Same values"))))

;; ============================================================================
;; Chunk count verification (compaction actually reduces chunks)
;; ============================================================================

(deftest compaction-reduces-chunk-count-test
  (testing "Deleting from small chunks causes them to merge"
    (let [chunk-size 8
          ;; Create index with many small chunks: 4 elements each (half-full)
          ;; by creating full chunks then deleting half
          idx (make-processor-index :float64 chunk-size (range 32))
          initial-chunks (count (index/idx-all-chunk-stats idx))
          ;; Delete every other element — chunks become 4 elements each
          idx2 (reduce (fn [idx _]
                         ;; Always delete position 1 (second element)
                         ;; This shrinks each chunk one at a time
                         (index/idx-delete idx 1))
                       idx
                       (range 16))
          final-chunks (count (index/idx-all-chunk-stats idx2))]
      ;; After merging half-full adjacent chunks, should have fewer chunks
      (is (< final-chunks initial-chunks)
          (str "Compaction should reduce chunks: " final-chunks " < " initial-chunks))
      ;; All remaining data should be readable
      (is (= 16 (index/idx-length idx2))))))

;; ============================================================================
;; Edge cases
;; ============================================================================

(deftest single-element-with-processor-test
  (testing "Single element index with processor"
    (let [idx (make-processor-index :float64 8 [42.0])]
      (is (= 1 (index/idx-length idx)))
      (is (= 42.0 (index/idx-get-double idx 0)))
      ;; Delete it
      (let [idx2 (index/idx-delete idx 0)]
        (is (= 0 (index/idx-length idx2)))))))

(deftest delete-to-empty-with-processor-test
  (testing "Delete all elements with processor enabled"
    (let [idx (make-processor-index :float64 8 (range 16))
          idx2 (reduce (fn [idx _] (index/idx-delete idx 0)) idx (range 16))]
      (is (= 0 (index/idx-length idx2))))))

(deftest transient-delete-to-empty-test
  (testing "Transient delete all elements with processor"
    (let [idx (make-processor-index :float64 8 (range 16))
          idx2 (-> idx
                   index/idx-transient
                   (as-> t (reduce (fn [t _] (index/idx-delete! t 0)) t (range 16)))
                   index/idx-persistent!)]
      (is (= 0 (index/idx-length idx2))))))

;; ============================================================================
;; Stress: random operations
;; ============================================================================

(deftest stress-append-delete-with-processor-test
  (testing "Random append/delete ops with processor, verify at end"
    (let [chunk-size 8
          rng (java.util.Random. 42)
          idx-proc (make-processor-index :float64 chunk-size (range 20))
          idx-plain (index/index-from-seq :float64 (range 20) {:chunk-size chunk-size})
          ref (atom (vec (map double (range 20))))
          proc-atom (atom idx-proc)
          plain-atom (atom idx-plain)]
      ;; 200 random append/delete operations
      (dotimes [_ 200]
        (let [len (index/idx-length @proc-atom)]
          (if (or (zero? len) (< (.nextDouble rng) 0.5))
            ;; Append
            (let [v (double (.nextInt rng 10000))]
              (swap! ref conj v)
              (swap! proc-atom #(index/idx-append % v))
              (swap! plain-atom #(index/idx-append % v)))
            ;; Delete
            (let [pos (.nextInt rng len)]
              (swap! ref (fn [r] (into (subvec r 0 pos) (subvec r (inc pos)))))
              (swap! proc-atom #(index/idx-delete % pos))
              (swap! plain-atom #(index/idx-delete % pos))))))
      ;; Verify data
      (is (= (idx-all-values @plain-atom) (idx-all-values @proc-atom))
          "Processor index matches plain index")
      (verify-index @plain-atom @ref "plain after random ops")
      (verify-index @proc-atom @ref "processor after random ops")
      ;; Verify tree health
      (verify-tree-health @proc-atom chunk-size "processor after random ops"))))

(deftest stress-random-ops-with-processor-test
  (testing "Random interleaved ops with processor, verify at end"
    ;; idx-set uses pss/replace (not disj+conj), so processor doesn't fire
    ;; during chunk modifications. This allows all operation types.
    (let [chunk-size 8
          rng (java.util.Random. 42)
          idx-proc (make-processor-index :float64 chunk-size (range 20))
          idx-plain (index/index-from-seq :float64 (range 20) {:chunk-size chunk-size})
          ref (atom (vec (map double (range 20))))
          proc-atom (atom idx-proc)
          plain-atom (atom idx-plain)]
      ;; 200 random operations: append, delete, and set
      (dotimes [_ 200]
        (let [len (index/idx-length @proc-atom)
              r (.nextDouble rng)]
          (cond
            ;; Append (33% or forced when empty)
            (or (zero? len) (< r 0.33))
            (let [v (double (.nextInt rng 10000))]
              (swap! ref conj v)
              (swap! proc-atom #(index/idx-append % v))
              (swap! plain-atom #(index/idx-append % v)))

            ;; Delete (33%)
            (< r 0.66)
            (let [pos (.nextInt rng len)]
              (swap! ref (fn [r] (into (subvec r 0 pos) (subvec r (inc pos)))))
              (swap! proc-atom #(index/idx-delete % pos))
              (swap! plain-atom #(index/idx-delete % pos)))

            ;; Set (33%)
            :else
            (let [pos (.nextInt rng len)
                  v (double (.nextInt rng 10000))]
              (swap! ref assoc pos v)
              (swap! proc-atom #(index/idx-set % pos v))
              (swap! plain-atom #(index/idx-set % pos v))))))
      ;; Verify data
      (is (= (idx-all-values @plain-atom) (idx-all-values @proc-atom))
          "Processor index matches plain index")
      (verify-index @plain-atom @ref "plain after random ops")
      (verify-index @proc-atom @ref "processor after random ops")
      ;; Verify tree health
      (verify-tree-health @proc-atom chunk-size "processor after random ops"))))

(deftest stress-transient-append-delete-with-processor-test
  (testing "Random transient append/delete ops with processor, verify at end"
    (let [chunk-size 8
          rng (java.util.Random. 42)
          idx (make-processor-index :float64 chunk-size (range 20))
          ref (atom (vec (map double (range 20))))]
      ;; Go transient, do 200 random append/delete ops, persist
      (let [t (index/idx-transient idx)]
        (dotimes [_ 200]
          (let [len (index/idx-length t)]
            (if (or (zero? len) (< (.nextDouble rng) 0.5))
              ;; Append
              (let [v (double (.nextInt rng 10000))]
                (swap! ref conj v)
                (index/idx-append! t v))
              ;; Delete
              (let [pos (.nextInt rng len)]
                (swap! ref (fn [r] (into (subvec r 0 pos) (subvec r (inc pos)))))
                (index/idx-delete! t pos)))))
        (let [result (index/idx-persistent! t)]
          (verify-index result @ref "after transient random ops")
          (verify-tree-health result chunk-size "after transient random ops"))))))

;; ============================================================================
;; Persistent does not corrupt original (structural sharing)
;; ============================================================================

(deftest processor-structural-sharing-test
  (testing "Persistent ops with processor don't corrupt original"
    (let [chunk-size 8
          idx1 (make-processor-index :float64 chunk-size (range 32))
          vals1 (idx-all-values idx1)
          ;; Create derived index by deleting
          idx2 (reduce (fn [idx _] (index/idx-delete idx 0)) idx1 (range 8))]
      ;; Original must be unchanged
      (is (= vals1 (idx-all-values idx1))
          "Original unchanged after persistent delete")
      (is (= 32 (index/idx-length idx1)))
      (is (= 24 (index/idx-length idx2))))))
