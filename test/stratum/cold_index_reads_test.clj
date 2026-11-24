(ns stratum.cold-index-reads-test
  "What a write, a statistics query and a point lookup READ on a column index that is on disk.

   A column index is a persistent-sorted-set of chunks with a statistics measure. Until
   persistent-sorted-set 0.5.149 a branch recomputed its measure from the children that were
   in memory and stored it as unknown when one was not, so a `idx-set` or `idx-delete` on a
   cold index erased the statistics from the chunk's parent to the root. `idx-set` and
   `idx-delete` also find their chunk by position, which added up the weights of every
   sibling to the left -- restoring them. Measured with 0.5.142, 6.4M values in 100 000
   chunks: 200 scattered `idx-set` read 3229 nodes of a 3226-node tree.

   From 0.5.149 every branch stores its children's measures, and the assertions below are
   the read counts that follow from it.

   A FILE store, deliberately: konserve's memory store hands back the live node objects, so
   nothing is ever cold there and every count below reads 1."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest testing is]]
            [konserve.filestore :as fs]
            [org.replikativ.persistent-sorted-set :as pss]
            [stratum.cached-storage :as cstorage]
            [stratum.index :as idx]
            [stratum.storage :as storage]))

(def ^:private n 1280000)
(def ^:private writes 20)

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "stratum-cold-reads" (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- delete-dir! [dir]
  (doseq [f (reverse (file-seq (io/file dir)))] (.delete ^java.io.File f)))

(defn- reopen
  "The index as a fresh process finds it: nothing in memory."
  [store i]
  (idx/restore-index-from-snapshot
   (storage/load-index-commit store (get-in (meta i) [:commit :id])) store))

(defn- reads [i] (:reads (cstorage/storage-stats (idx/idx-storage i))))

;; 20 000 chunks at fanout 64: leaves, their parents, a root.
(def ^:private h 3)

(deftest a-cold-index-reads-its-paths-and-keeps-its-statistics
  (let [dir   (temp-dir)
        store (fs/connect-fs-store dir :opts {:sync? true})]
    (try
      (let [arr  (double-array n)
            _    (dotimes [k n] (aset arr k (double (mod (* k 7919) 1000))))
            i0   (idx/idx-sync! (idx/index-from-array arr {:chunk-size 64}) store)
            ks   (mapv #(* 61337 (long %)) (range 1 (inc writes)))
            all  (let [c (reopen store i0)]
                   (count (pss/slice (idx/idx-tree c) nil nil))
                   (reads c))
            _    (is (> (long (:branches (pss/measure-census (idx/idx-tree (reopen store i0))))) 1)
                     "precondition: more than a root above the leaves")]
        (is (> (long all) (* 10 writes h))
            "precondition: the tree is much larger than the paths the writes touch")
        (testing "every stored branch has its children's statistics"
          (is (zero? (long (:unknown (pss/measure-census (idx/idx-tree (reopen store i0))))))))
        (doseq [[label write expect-count expect-sum]
                [["idx-set" #(idx/idx-set %1 %2 5.0) n
                  (+ (areduce arr k acc 0.0 (+ acc (aget arr k)))
                     (reduce + (map #(- 5.0 (aget arr (int %))) ks)))]
                 ["idx-delete" idx/idx-delete (- n writes)
                  ;; positions shift left as rows are deleted in ascending order
                  nil]]]
          (testing label
            (let [c       (reopen store i0)
                  written (reduce write c ks)
                  w-reads (reads c)
                  i1      (idx/idx-sync! written store)]
              (testing "the writes read their paths, not the tree"
                (is (<= (long w-reads) (* writes h)) (str w-reads " reads for " writes " writes, " all " nodes")))
              (testing "and leave every stored branch with its statistics"
                (is (zero? (long (:unknown (pss/measure-census (idx/idx-tree (reopen store i1))))))))
              (testing "so a statistics query on the reopened index reads the root"
                (let [c1 (reopen store i1)
                      st (idx/idx-stats c1)]
                  (is (<= (long (reads c1)) 1) (str (reads c1) " reads"))
                  (is (= expect-count (:count st)))
                  (when expect-sum (is (== expect-sum (:sum st))))))
              (testing "and a point lookup reads one node per level"
                (let [c2 (reopen store i1)]
                  (idx/idx-get-double c2 12345)
                  (is (<= (long (reads c2)) h) (str (reads c2) " reads"))))))))
      (finally
        (delete-dir! dir)))))
