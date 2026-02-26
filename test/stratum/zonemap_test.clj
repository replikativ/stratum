(ns stratum.zonemap-test
  "Tests for zone map optimization (Phase 3B)"
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.index :as idx]))

(deftest zone-map-filter-optimization-test
  (testing "Zone map skips irrelevant chunks"
    ;; Create index with 50K rows in known ranges
    (let [idx (loop [i 0
                     t (idx/idx-transient (idx/index-from-seq :float64 []))]
                (if (< i 50000)
                  (recur (inc i) (idx/idx-append! t (double i)))
                  (idx/idx-persistent! t)))]

      ;; Filter for values >= 45000 (only last 5000 rows)
      ;; Without zone maps: must scan all 50K
      ;; With zone maps: skips first ~44 chunks (8192 each)
      (let [result (idx/idx-filter-gte idx 45000.0)]
        (is (= 5000 (count result)))
        (is (= 45000 (first result)))
        (is (= 49999 (last result))))))

  (testing "Zone map count optimization"
    (let [idx (loop [i 0
                     t (idx/idx-transient (idx/index-from-seq :float64 []))]
                (if (< i 10000)
                  (recur (inc i) (idx/idx-append! t (double i)))
                  (idx/idx-persistent! t)))]

      ;; Count values in range [8000, 9000) - spans ~1 chunk
      (let [result (count (idx/idx-filter-range idx 8000.0 9000.0))]
        (is (= 1000 result)))))

  (testing "Zone map stats extraction via protocol"
    (let [price-idx (idx/index-from-seq :float64 [100.0 200.0 300.0 400.0 500.0])
          chunk-stats (idx/idx-all-chunk-stats price-idx)]

      ;; Should have 1 chunk with 5 values
      (is (= 1 (count chunk-stats)))
      (let [stats (first chunk-stats)]
        (is (= 5 (:count stats)))
        (is (= 100.0 (:min-val stats)))
        (is (= 500.0 (:max-val stats)))
        (is (= 1500.0 (:sum stats)))))))
