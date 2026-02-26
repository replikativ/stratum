;; # Stratum: High-Performance Columnar Analytics for Clojure

;; authors: Christian Weilbach

;; last change: 2026-02-17

;; ## Introduction

;; Stratum is a persistent columnar analytics engine built for the JVM,
;; combining Clojure's immutability with Java Vector API SIMD acceleration.
;; It provides:
;;
;; - **Blazing fast queries** — competitive with or faster than DuckDB on OLAP benchmarks
;; - **Clojure-native API** — query maps, SQL strings, or tablecloth datasets
;; - **Copy-on-write semantics** — fork datasets in O(1), structural sharing
;; - **Persistent storage** — Konserve-backed with snapshot isolation and time-travel
;; - **SQL interface** — PostgreSQL wire protocol (psql, JDBC, DBeaver, psycopg2)

;; ## Setup

(ns stratum-intro
  (:require [stratum.api :as st]
            [stratum.dataset :as dataset]    ;; transient mutations
            [stratum.storage :as storage]    ;; branch management
            [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]
            [scicloj.kindly.v4.kind :as kind]))

;; ## 1. Your Data, Your Way

;; Stratum works with column maps — maps from keywords to typed arrays.
;; The same shape tablecloth uses under the hood.

(def orders
  {:product (into-array String ["Apple" "Banana" "Apple" "Cherry" "Banana" "Apple"])
   :qty     (double-array [10.0 15.0 8.0 5.0 20.0 12.0])
   :price   (double-array [1.2  0.5  1.2  2.5  0.5  1.2])})

;; DSL query — Clojure map with keywords:

(st/q {:from   orders
       :group  [:product]
       :agg    [[:sum :qty]
                [:avg :price]
                [:count]]})

;; SQL string — same query, same result:

(st/q "SELECT product, SUM(qty), AVG(price), COUNT(*) FROM orders GROUP BY product"
      {"orders" orders})

;; Predicates in the DSL accept both keyword and symbol style:

(st/q {:from  orders
       :where [[:> :price 0.6]]
       :agg   [[:sum :qty] [:count]]})

;; ## 2. Tablecloth Integration

;; Pass a tablecloth (or tech.ml.dataset) dataset directly — Stratum reads
;; its columns with zero copy. Results come back as a sequence of maps by default.

(def tc-ds
  (tc/dataset {:category ["Electronics" "Clothing" "Electronics" "Clothing" "Electronics"]
               :revenue  (double-array [250.0 80.0 320.0 110.0 280.0])
               :qty      (long-array [1 2 1 3 2])}))

;; Query it like any Stratum source:

(st/q {:from  tc-ds
       :group [:category]
       :agg   [[:sum :revenue]
               [:sum :qty]
               [:count]]})

;; SQL also works directly with tablecloth datasets:

(st/q "SELECT category, SUM(revenue) AS total FROM t WHERE qty > 1 GROUP BY category"
      {"t" tc-ds})

;; ## 3. Why It's Fast: Fused SIMD Execution

;; Stratum compiles filter + aggregate into a single pass using the Java Vector API
;; (AVX-512 / AVX2 / NEON depending on CPU). No intermediate arrays, no row-by-row
;; iteration. At 1M rows you'll see sub-millisecond aggregations.

(def large-ds
  {:amount (double-array (repeatedly 1000000 #(* (rand) 1000.0)))
   :region (long-array   (repeatedly 1000000 #(rand-int 10)))
   :flag   (long-array   (repeatedly 1000000 #(rand-int 2)))})

;; Single-pass fused filter + aggregate:

(time
  (st/q {:from  large-ds
         :where [[:> :amount 500.0] [:= :flag 1]]
         :agg   [[:sum :amount] [:count]]}))

;; Dense group-by (direct array indexing, no hash for ≤200K groups):

(time
  (st/q {:from  large-ds
         :group [:region]
         :agg   [[:sum :amount] [:avg :amount] [:count]]}))

;; ## 4. Zone Map Pruning

;; For index-backed datasets, Stratum tracks min/max per 8192-row chunk.
;; Range queries classify each chunk as: skip / stats-only / SIMD — so
;; a predicate like ts >= 900000 only touches ~1% of chunks.

(def time-series
  (st/make-dataset
    {:ts    (st/index-from-seq :int64   (range 0 1000000))
     :value (st/index-from-seq :float64 (repeatedly 1000000 rand))}))

;; Only the last ~1% of chunks are scanned:

(time
  (st/q {:from  time-series
         :where [[:>= :ts 900000] [:< :ts 910000]]
         :agg   [[:sum :value] [:count]]}))

;; ## 5. Persistence: Version Control for Analytics

;; Datasets are immutable Clojure values. st/sync! durably persists to a
;; Konserve store (file, S3, memory …). st/fork is O(1) — structural sharing
;; with copy-on-write on mutation.

(require '[konserve.store :as kstore])

(def store-cfg
  {:backend :file
   :path    "/tmp/stratum-intro"
   :id      #uuid "550e8400-e29b-41d4-a716-446655440000"})

(when (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true}))

(def store (kstore/create-store store-cfg {:sync? true}))

;; Create a dataset with index-backed columns (required for persistence):

(def base
  (st/make-dataset
    {:product (st/index-from-seq :int64   [101 102 103])
     :qty     (st/index-from-seq :float64 [100.0 50.0 75.0])
     :revenue (st/index-from-seq :float64 [1000.0 750.0 1125.0])}
    {:name "sales-q1"}))

;; Persist to "main" branch — returns new dataset with commit metadata:

(def v1 (st/sync! base store "main"))
(:id (:commit-info v1))  ;; => #uuid "..."

;; Fork for a what-if scenario. Fork is O(1) — all chunks shared.
;; Mutations use the transient/persistent protocol (like Clojure's collections):

(def what-if
  (-> (st/fork base)
      transient
      (dataset/set-at!    :qty     0 150.0)      ;; optimistic product 101 sales
      (dataset/set-at!    :revenue 0 1500.0)
      (dataset/append! {:product 104 :qty 60.0 :revenue 900.0})
      persistent!))

;; Sync what-if to a separate branch:

(def v1-whatif (st/sync! what-if store "what-if"))

(storage/list-dataset-branches store)  ;; => #{"main" "what-if"}

;; Time-travel: load any version and query:

(let [main-ds   (st/load store "main")
      whatif-ds (st/load store "what-if")]
  {:main   (first (st/q {:from main-ds   :agg [[:sum :revenue]]}))
   :whatif (first (st/q {:from whatif-ds :agg [[:sum :revenue]]}))})
;; => {:main {:sum 2875.0} :whatif {:sum 4275.0}}

;; GC: mark-and-sweep from all branch HEADs — unreachable chunks deleted:

(st/gc! store)

;; ## 6. Advanced: Statistics and Joins

;; VARIANCE, STDDEV, CORR — Welford's online algorithm, single pass in Java:

(def sensors
  {:temp     (double-array [20.1 20.5 21.0 19.8 20.3 22.0])
   :humidity (double-array [65.0 68.0 70.0 62.0 66.0 72.0])
   :sensor   (into-array String ["A" "A" "A" "B" "B" "B"])})

(st/q {:from  sensors
       :group [:sensor]
       :agg   [[:avg :temp]
               [:stddev :temp]
               [:corr :temp :humidity]]})

;; Hash joins — INNER, LEFT, RIGHT, FULL:

(def fact
  {:order-id   (long-array [1 2 3 4 5])
   :product-id (long-array [101 102 101 103 102])
   :qty        (double-array [2 1 3 1 2])})

(def dim
  {:product-id (long-array [101 102 103])
   :name       (into-array String ["Widget" "Gadget" "Gizmo"])
   :price      (double-array [10.0 20.0 15.0])})

(st/q {:from fact
       :join [{:with dim
               :on   [:= :product-id :product-id]
               :type :inner}]
       :group [:name]
       :agg   [[:sum :qty]]})

;; ## 7. SQL Interface

;; Any PostgreSQL-compatible client works: psql, DBeaver, Python psycopg2, JDBC.

(comment
  (def srv (st/start-server {:port 5433}))
  (st/register-table! srv "orders" orders)

  ;; psql -h localhost -p 5433 -U stratum
  ;; SELECT product, SUM(qty) FROM orders GROUP BY product;

  (st/stop-server srv))

;; ## Performance Numbers (6M rows, 8-core Lunar Lake, vs DuckDB v1.5)
;;
;; | Query                  | Stratum 1T | Stratum 8T | DuckDB 1T | DuckDB 8T |
;; |------------------------|------------|------------|-----------|-----------|
;; | Filtered agg (TPC-H)   |    12ms    |    2.9ms   |   34ms    |   6.9ms   |
;; | TPC-H Q1               |    61ms    |    26ms    |  102ms    |   19ms    |
;; | SSB Q1.1               |    12ms    |    2.8ms   |   35ms    |   7.3ms   |
;; | Filtered count         |    2.1ms   |    1.3ms   |   14ms    |   3.4ms   |
;; | Group-by 10 keys       |    12ms    |    3.5ms   |   28ms    |   5.9ms   |
;; | STDDEV group-by        |    28ms    |    19ms    |   54ms    |   30ms    |
;; | CORR group-by          |    53ms    |    32ms    |   72ms    |   37ms    |
;;
;; All benchmarks: single JVM process, G1GC, JDK 21, no JVM tuning.
