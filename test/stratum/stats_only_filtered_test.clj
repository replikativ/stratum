(ns stratum.stats-only-filtered-test
  "Tests for F20 — filter-aware stats-only MIN/MAX/SUM/AVG/COUNT.

   The strategy picker promotes `LGlobalAgg` queries with predicates to
   `PStatsOnlyAgg` when zone maps prove every chunk classifies as
   `:stats-only` (all rows match) or `:skip` (no rows match). The
   executor accumulates surviving chunks' `ChunkStats` only — no SIMD
   scan, no materialization. Falls through to `PChunkedSIMDAgg` when
   any chunk is partial."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.query :as q]
            [stratum.query.plan :as plan]
            [stratum.index :as index])
  (:import [stratum.query.ir
            PStatsOnlyAgg PChunkedSIMDAgg PChunkedSIMDCount
            PFusedSIMDAgg]))

(set! *warn-on-reflection* true)

;; Each chunk in a column index is 8192 rows. Pick row counts that
;; produce predictable chunk boundaries.
(def chunk-rows 8192)

(defn- physical [query] (-> query plan/build-logical-plan plan/optimize))

;; ============================================================================
;; Strategy selection — promotes / falls through correctly
;; ============================================================================

(deftest fully-classifying-pred-promotes-to-stats-only
  (testing "Sorted column, pred falls on chunk boundary → all chunks :stats-only or :skip"
    (let [n (* 3 chunk-rows)
          data {:x (index/index-from-array (long-array (range n)))}
          ;; Pred [:< :x 8192] → chunk 0 fully-inside, chunks 1,2 skip.
          q {:from data :where [[:< :x chunk-rows]] :agg [[:sum :x]]}]
      (is (instance? PStatsOnlyAgg (physical q)))
      (is (= (reduce + 0 (range chunk-rows))
             (:sum (first (q/q q))))))))

(deftest partial-chunk-falls-through-to-chunked-simd
  (testing "Pred crosses a chunk boundary → cannot use stats-only"
    (let [n (* 3 chunk-rows)
          data {:x (index/index-from-array (long-array (range n)))}
          ;; Pred [:< :x 5000] → chunk 0 is partial (some match, some don't).
          q {:from data :where [[:< :x 5000]] :agg [[:sum :x]]}]
      (is (instance? PChunkedSIMDAgg (physical q)))
      (is (== (reduce + 0 (range 5000))
              (:sum (first (q/q q))))))))

(deftest no-preds-still-uses-stats-only
  (testing "F20 doesn't change the no-preds case — still PStatsOnlyAgg"
    (let [data {:x (index/index-from-array (long-array (range 1000)))}
          q {:from data :agg [[:sum :x] [:min :x] [:max :x]]}]
      (is (instance? PStatsOnlyAgg (physical q))))))

;; ============================================================================
;; Multi-agg with fully-classifying preds (F20 win — single-agg already had
;; per-chunk stats-only via PChunkedSIMDAgg, but multi-agg didn't)
;; ============================================================================

(deftest multi-agg-stats-only-with-pred
  (testing "MIN+MAX+SUM with fully-classifying pred routes via stats-only"
    (let [n (* 3 chunk-rows)
          data {:x (index/index-from-array (long-array (range n)))}
          ;; First two chunks fully inside, third skip.
          hi (* 2 chunk-rows)
          q {:from data :where [[:< :x hi]] :agg [[:sum :x] [:min :x] [:max :x]]}
          phys (physical q)
          result (first (q/q q))]
      (is (instance? PStatsOnlyAgg phys))
      (is (= (reduce + 0 (range hi)) (:sum result)))
      (is (= 0 (:min result)))
      (is (= (dec hi) (:max result))))))

;; ============================================================================
;; Skip-everything case — pred excludes all chunks
;; ============================================================================

(deftest pred-skips-all-chunks
  (testing "Pred where every chunk fails zone-may-contain → all :skip"
    (let [data {:x (index/index-from-array (long-array (range 2000)))}
          q-count {:from data :where [[:> :x 99999]] :agg [[:count]]}
          q-sum   {:from data :where [[:> :x 99999]] :agg [[:sum :x]]}
          q-min   {:from data :where [[:> :x 99999]] :agg [[:min :x]]}]
      (is (= 0 (long (:count (first (q/q q-count))))))
      ;; SUM/MIN/MAX over empty result set → SQL NULL
      (is (nil? (:sum (first (q/q q-sum)))))
      (is (nil? (:min (first (q/q q-min))))))))

;; ============================================================================
;; Non-zonemap predicates fall through (e.g. expression LHS)
;; ============================================================================

(deftest non-zonemap-pred-falls-through
  ;; A predicate with a computed LHS (`[:* :x 2]`) cannot be answered
  ;; from chunk stats — F20 must defer to the SIMD path. This is just
  ;; a strategy-selection check; we don't run the query because the
  ;; downstream path may have unrelated issues with computed preds.
  (let [data {:x (index/index-from-array (long-array (range 8192)))}
        q {:from data :where [[:> [:* :x 2] 100]] :agg [[:sum :x]]}
        phys (physical q)]
    (is (not (instance? PStatsOnlyAgg phys))
        "expr-LHS preds shouldn't reach stats-only")))

;; ============================================================================
;; Heap-array (non-indexed) columns don't promote
;; ============================================================================

(deftest heap-array-doesnt-promote
  (testing "Heap-array (non-indexed) data has no chunk stats → falls through"
    (let [data {:x (long-array (range 5000))}
          q {:from data :where [[:< :x 1000]] :agg [[:sum :x]]}
          phys (physical q)]
      (is (not (instance? PStatsOnlyAgg phys))))))

;; ============================================================================
;; Composes with F21 linear-agg-rewrite — `SUM(s·x + o)` over a
;; fully-classifying pred runs entirely from chunk stats.
;; ============================================================================

(deftest linear-rewrite-composes-with-filtered-stats-only
  (let [n (* 2 chunk-rows)
        data {:x (index/index-from-array (long-array (range n)))}
        ;; Chunk 0 fully-inside [:< :x chunk-rows], chunk 1 skip.
        q {:from data :where [[:< :x chunk-rows]] :agg [[:sum [:* :x 2]]]}
        phys (physical q)]
    (is (instance? PStatsOnlyAgg phys))
    (is (= (* 2.0 (reduce + 0 (range chunk-rows)))
           (:sum (first (q/q q)))))))
