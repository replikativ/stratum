;; # Quickstart
;;
;; A minimal introduction to **[Stratum](https://github.com/replikativ/stratum)** — a persistent
;; [columnar](https://en.wikipedia.org/wiki/Column-oriented_DBMS)
;; analytics engine for Clojure with
;; [SIMD](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data)-accelerated
;; queries, SQL support, and
;; [copy-on-write](https://en.wikipedia.org/wiki/Copy-on-write) semantics.

(ns stratum-book.quickstart
  (:require
   [stratum.api :as st]
   [stratum.dataset :as dataset]
   [stratum.storage :as storage]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [scicloj.kindly.v4.kind :as kind]))

;; ## Your Data, Your Way
;;
;; Stratum works with column maps — maps from keywords to typed arrays.
;; The same shape [tablecloth](https://github.com/scicloj/tablecloth) uses under the hood.

(def orders
  {:product (into-array String ["Apple" "Banana" "Apple" "Cherry" "Banana" "Apple"])
   :qty     (double-array [10.0 15.0 8.0 5.0 20.0 12.0])
   :price   (double-array [1.2  0.5  1.2  2.5  0.5  1.2])})

;; ### DSL query — Clojure map with keywords

(st/q {:from   orders
       :group  [:product]
       :agg    [[:sum :qty]
                [:avg :price]
                [:count]]})

(kind/test-last
 [(fn [result]
    (= 3 (count result)))])

;; ### SQL string — same query, same result

(st/q "SELECT product, SUM(qty), AVG(price), COUNT(*) FROM orders GROUP BY product"
      {"orders" orders})

(kind/test-last
 [(fn [result]
    (= 3 (count result)))])

;; ### Predicates

(st/q {:from  orders
       :where [[:> :price 0.6]]
       :agg   [[:sum :qty] [:count]]})

(kind/test-last
 [(fn [result]
    (= 1 (count result)))])

;; ## Tablecloth Integration
;;
;; Pass a tablecloth (or [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset))
;; dataset directly — Stratum reads its columns with zero copy.

(def tc-ds
  (tc/dataset {:category ["Electronics" "Clothing" "Electronics" "Clothing" "Electronics"]
               :revenue  (double-array [250.0 80.0 320.0 110.0 280.0])
               :qty      (long-array [1 2 1 3 2])}))

(st/q {:from  tc-ds
       :group [:category]
       :agg   [[:sum :revenue]
               [:sum :qty]
               [:count]]})

(kind/test-last
 [(fn [result]
    (= 2 (count result)))])

;; SQL also works directly with tablecloth datasets:

(st/q "SELECT category, SUM(revenue) AS total FROM t WHERE qty > 1 GROUP BY category"
      {"t" tc-ds})

(kind/test-last
 [(fn [result]
    (= 2 (count result)))])

;; ## Zone Map Pruning
;;
;; For index-backed datasets, Stratum tracks min/max per 8192-row chunk
;; using [zone maps](https://en.wikipedia.org/wiki/Block_Range_Index).
;; Range queries classify each chunk as: skip / stats-only / SIMD
;; (via the [Java Vector API](https://openjdk.org/jeps/460)) — so
;; a predicate like `ts >= 900000` only touches ~1% of chunks.

(def time-series
  (st/make-dataset
   {:ts    (st/index-from-seq :int64   (range 0 100000))
    :value (st/index-from-seq :float64 (repeatedly 100000 rand))}))

(st/q {:from  time-series
       :where [[:>= :ts 90000] [:< :ts 91000]]
       :agg   [[:sum :value] [:count]]})

(kind/test-last
 [(fn [result]
    (= 1000 (long (:count (first result)))))])

;; ## Statistics: STDDEV and CORR
;;
;; [Welford's online algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm),
;; single pass in Java.

(def sensors
  {:temp     (double-array [20.1 20.5 21.0 19.8 20.3 22.0])
   :humidity (double-array [65.0 68.0 70.0 62.0 66.0 72.0])
   :sensor   (into-array String ["A" "A" "A" "B" "B" "B"])})

(st/q {:from  sensors
       :group [:sensor]
       :agg   [[:avg :temp]
               [:stddev :temp]
               [:corr :temp :humidity]]})

(kind/test-last
 [(fn [result]
    (= 2 (count result)))])

;; ## Hash Joins
;;
;; [Hash join](https://en.wikipedia.org/wiki/Hash_join) support:
;; INNER, LEFT, RIGHT, FULL.

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

(kind/test-last
 [(fn [result]
    (= 3 (count result)))])

;; ## Persistence
;;
;; Datasets are immutable Clojure values. `st/sync!` durably persists to a
;; [Konserve](https://github.com/replikativ/konserve) store. `st/fork` is
;; O(1) — [structural sharing](https://en.wikipedia.org/wiki/Persistent_data_structure)
;; with copy-on-write.

(require '[konserve.store :as kstore])

(def store-cfg
  {:backend :file
   :path    "/tmp/stratum-quickstart"
   :id      #uuid "550e8400-e29b-41d4-a716-446655440099"})

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

;; Persist to "main" branch:

(def v1 (st/sync! base store "main"))

(:id (:commit-info v1))

(kind/test-last
 [(fn [id] (uuid? id))])

;; Fork for a what-if scenario — O(1), all chunks shared:

(def what-if
  (-> (st/fork base)
      transient
      (dataset/set-at!    :qty     0 150.0)
      (dataset/set-at!    :revenue 0 1500.0)
      (dataset/append! {:product 104 :qty 60.0 :revenue 900.0})
      persistent!))

(def v1-whatif (st/sync! what-if store "what-if"))

(storage/list-dataset-branches store)

(kind/test-last
 [(fn [branches]
    (= #{"main" "what-if"} branches))])

;; Time-travel: load any version and query:

(let [main-ds   (st/load store "main")
      whatif-ds (st/load store "what-if")]
  {:main   (first (st/q {:from main-ds   :agg [[:sum :revenue]]}))
   :whatif (first (st/q {:from whatif-ds :agg [[:sum :revenue]]}))})

(kind/test-last
 [(fn [{:keys [main whatif]}]
    (and (= 2875.0 (:sum main))
         (= 4275.0 (:sum whatif))))])

;; Clean up:

(kstore/delete-store store-cfg {:sync? true})
