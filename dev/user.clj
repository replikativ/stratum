(ns user
  "Development namespace for REPL exploration."
  (:require [stratum.chunk :as chunk]
            [stratum.index :as index]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [criterium.core :as crit]))

(comment
  ;; =========================================================================
  ;; Basic Chunk Operations
  ;; =========================================================================

  ;; Create a chunk
  (def c (chunk/make-chunk :float64 1000))
  (chunk/chunk-length c)    ;; => 1000
  (chunk/chunk-datatype c)  ;; => :float64

  ;; Write some values (must be transient)
  (-> c chunk/col-transient)
  (chunk/write-double! c 0 42.0)
  (chunk/write-double! c 1 3.14159)
  (chunk/write-double! c 999 123.456)
  (-> c chunk/col-persistent!)

  ;; Read values
  (chunk/read-double c 0)    ;; => 42.0
  (chunk/read-double c 1)    ;; => 3.14159
  (chunk/read-double c 999)  ;; => 123.456

  ;; =========================================================================
  ;; Copy-on-Write Semantics
  ;; =========================================================================

  ;; Fork the chunk (structural sharing)
  (def c2 (chunk/col-fork c))

  ;; Original and fork share data initially
  (chunk/read-double c 0)   ;; => 42.0
  (chunk/read-double c2 0)  ;; => 42.0

  ;; Modify the fork
  (-> c2 chunk/col-transient)
  (chunk/write-double! c2 0 99.0)
  (-> c2 chunk/col-persistent!)

  ;; Original is unchanged!
  (chunk/read-double c 0)   ;; => 42.0
  (chunk/read-double c2 0)  ;; => 99.0

  ;; =========================================================================
  ;; Index Operations
  ;; =========================================================================

  ;; Create an index from a sequence
  (def idx (index/index-from-seq :float64 (range 100000)))
  (index/idx-length idx)  ;; => 100000

  ;; Point access
  (index/idx-get-double idx 0)       ;; => 0.0
  (index/idx-get-double idx 50000)   ;; => 50000.0
  (index/idx-get-double idx 99999)   ;; => 99999.0

  ;; SIMD operations on chunks
  (index/idx-sum idx)   ;; => 4.99995E9
  (index/idx-mean idx)  ;; => 49999.5

  ;; =========================================================================
  ;; Performance Tests
  ;; =========================================================================

  ;; Create 1M element index
  (def big-idx (index/index-from-seq :float64 (range 1000000)))

  ;; Benchmark point access (should be O(log N) for tree + O(1) in chunk)
  (crit/quick-bench (index/idx-get-double big-idx 500000))
  ;; Expected: ~100-200ns

  ;; Benchmark full scan/sum (should be fast - SIMD on contiguous chunks)
  (crit/quick-bench (index/idx-sum big-idx))
  ;; Expected: ~1-2ms for 1M doubles

  ;; Compare to raw dtype-next sum
  (def raw-buf (dtype/make-container :native-heap :float64 (range 1000000)))
  (crit/quick-bench (dfn/sum raw-buf))
  ;; This should be faster (no tree overhead)

  ;; =========================================================================
  ;; Fork and Modify (Branching)
  ;; =========================================================================

  ;; Fork the index
  (def idx-v2 (index/idx-fork big-idx))

  ;; Modify the fork
  (def idx-v2-mut (-> idx-v2 index/idx-transient))
  (def idx-v2-final (-> idx-v2-mut
                        (index/idx-set 0 -999.0)
                        index/idx-persistent!))

  ;; Original unchanged
  (index/idx-get-double big-idx 0)     ;; => 0.0
  (index/idx-get-double idx-v2-final 0) ;; => -999.0

  ;; =========================================================================
  ;; Memory Analysis
  ;; =========================================================================

  ;; Check memory usage (requires clj-memory-meter)
  (require '[clj-memory-meter.core :as mm])

  (mm/measure big-idx)

  ;; =========================================================================
  ;; dtype-next Interop
  ;; =========================================================================

  ;; Get the underlying buffer for SIMD/GPU operations
  (def chunk-1 (chunk/make-chunk :float64 1000))
  (def native-buf (chunk/as-native-buffer chunk-1))

  ;; Can use with dtype-next operations
  (dfn/+ native-buf 10)  ;; Add 10 to all elements
  (dfn/* native-buf 2)   ;; Multiply all by 2

  ;; The native buffer address can be passed to GPU/FFI
  (dtype/->native-buffer native-buf)

  )
