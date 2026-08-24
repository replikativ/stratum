(ns stratum.warm-test
  "The dataset-level warm, with the storage's own read counter as the oracle.

   Stratum's CachedStorage counts `:reads` on cache MISSES only, and
   `dataset/load` itself touches some nodes on the way in — so the report's
   `:fetched` (which counts every restore the walk issues, hit or miss) bounds
   the miss delta from ABOVE, and the assertion that carries the weight is the
   one after it: the scan that follows a full warm performs ZERO further reads.

   SHAPE of these trees, measured rather than assumed: a stratum leaf is FAT —
   `ChunkEntry` carries its whole `PersistentColChunk`, so one leaf holds up to
   64 chunks of column data (~4MB) and restoring the leaves IS materializing
   the column. The restored branching factor is 64, so a tree only gains a
   level-2 spine past ~64 leaves; the depth fixtures below use `:chunk-size 1`
   to get there with a few thousand rows."
  (:require [clojure.test :refer [deftest testing is]]
            [konserve.memory :refer [new-mem-store]]
            [stratum.dataset :as dataset]
            [stratum.index :as index]
            [stratum.cached-storage :as cstorage]
            [stratum.warm :as warm]))

(defn- stored-dataset!
  [n chunk-size]
  (let [store (new-mem-store (atom {}) {:sync? true})
        ds    (dataset/make-dataset
               {:x (index/index-from-seq :float64 (map double (range n)) {:chunk-size chunk-size})
                :y (index/index-from-seq :int64 (range n) {:chunk-size chunk-size})}
               {:name "warm-test"})]
    (dataset/sync! ds store "main")
    store))

(defn- col-storage [ds col]
  (-> (dataset/column ds col) :index (#(.-storage ^stratum.index.PersistentColumnIndex %))))

(defn- reads [ds]
  ;; both columns share the load's CachedStorage; counting one counts all
  (:reads (cstorage/storage-stats (col-storage ds :x))))

(deftest a-full-warm-makes-the-following-scan-free
  (let [store (stored-dataset! 1000 8)
        ds    (dataset/load store "main")
        base  (reads ds)
        r     (warm/warm! ds {:depth :with-leaves :budget 100000})]
    (is (pos? (:fetched r)) "a cold dataset has something to fetch")
    (is (<= (- (reads ds) base) (:fetched r))
        "misses cannot exceed the restores the walk issued")
    (testing "the assertion that matters: the scan is free afterwards"
      (let [before (reads ds)]
        (is (= 999.0 (reduce max (-> (dataset/column ds :x) :index))))
        (is (= before (reads ds)) "zero further reads")))
    (testing "and a repeated warm walks the same tree"
      (is (= (:fetched r)
             (:fetched (warm/warm! ds {:depth :with-leaves :budget 100000})))))))

(deftest interior-warms-the-spine-not-the-column-data
  ;; chunk-size 1 over 3000 rows: ~70+ fat leaves under a level-2 root, so
  ;; there is a spine to warm and the leaves carry ALL the data.
  (let [store    (stored-dataset! 3000 1)
        ds1      (dataset/load store "main")
        interior (warm/warm! ds1 {:depth :interior :budget 100000})
        ds2      (dataset/load store "main")
        full     (warm/warm! ds2 {:depth :with-leaves :budget 100000})]
    (is (pos? (:fetched interior)) "a level-2 tree has a spine below the root")
    (is (< (:fetched interior) (/ (:fetched full) 10))
        (str "fat leaves mean the spine is a small fraction: interior="
             (:fetched interior) " full=" (:fetched full)))
    ;; NOT asserted: that a post-:interior scan still pays reads. Stratum's blob
    ;; granularity is coarser than its node structure (a whole small tree can be
    ;; one or two blobs), so which blob a scan touches is a storage-layout fact,
    ;; not part of the warm's contract. What IS contract: :interior stays a
    ;; small fraction of :with-leaves (above), and warming never makes a scan
    ;; MORE expensive (below).
    (testing "an :interior warm never makes the scan worse"
      (let [before (reads ds1)]
        (is (= 2999.0 (reduce max (-> (dataset/column ds1 :x) :index))))
        (let [cold-ds (dataset/load store "main")
              cold-b  (reads cold-ds)]
          (is (= 2999.0 (reduce max (-> (dataset/column cold-ds :x) :index))))
          (is (<= (- (reads ds1) before) (- (reads cold-ds) cold-b))))))))

(deftest the-budget-is-a-ceiling-and-splits-across-columns
  (let [store (stored-dataset! 3000 1)
        ds    (dataset/load store "main")
        r     (warm/warm! ds {:depth :with-leaves :budget 40})]
    (is (= 40 (:fetched r)) "exactly the budget, not one more")
    (is (true? (:budget-exhausted? r)))
    (let [{:keys [x y]} (:by-index r)]
      (is (and (pos? x) (pos? y)) "both columns were warmed")
      (is (<= (abs (- x y)) 2)
          (str "round-robin splits the budget evenly, got x=" x " y=" y)))))

(deftest warm-column-scopes-to-one-column
  (let [store (stored-dataset! 1000 8)
        ds    (dataset/load store "main")
        r     (warm/warm-column! ds :x {:depth :with-leaves :budget 100000})]
    (is (pos? (:fetched r)))
    (is (= [:x] (keys (:by-index r))) "only the asked-for column")))

(deftest an-unindexed-or-unknown-column-warms-nothing
  (let [store (stored-dataset! 100 8)
        ds    (dataset/load store "main")
        r     (warm/warm-column! ds :nope {})]
    (is (zero? (:fetched r)))
    (is (false? (:budget-exhausted? r)))))
