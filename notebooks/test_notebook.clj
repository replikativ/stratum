;; Test script for verifying stratum_intro.clj examples work
;; Run with: clj -M:dev -i notebooks/test_notebook.clj

(require '[stratum.api :as st]
         '[stratum.dataset :as dataset]
         '[stratum.storage :as storage]
         '[stratum.tablecloth]   ;; Load protocol extensions
         '[tech.v3.dataset :as ds]
         '[konserve.store :as kstore])

(println "Testing Stratum Notebook Examples")
(println "==================================\n")

;; Test 1: Basic GROUP BY query via DSL
(println "Test 1: Basic GROUP BY via DSL")
(def orders
  {:product (into-array String ["Apple" "Banana" "Apple" "Cherry" "Banana" "Apple"])
   :qty     (double-array [10.0 15.0 8.0 5.0 20.0 12.0])
   :price   (double-array [1.2  0.5  1.2  2.5  0.5  1.2])})

(def result1
  (st/q {:from  orders
         :group [:product]
         :agg   [[:sum :qty] [:avg :price] [:count]]}))

(assert (= 3 (count result1)) "Should have 3 product groups")
(println "Result:" result1)
(println "✓ Test 1 passed\n")

;; Test 2: Same query via SQL string
(println "Test 2: GROUP BY via SQL string")
(def result2
  (st/q "SELECT product, SUM(qty), AVG(price), COUNT(*) FROM orders GROUP BY product"
        {"orders" orders}))

(assert (= 3 (count result2)) "Should have 3 product groups")
(println "Result:" result2)
(println "✓ Test 2 passed\n")

;; Test 3: Tablecloth (tech.ml.dataset) interop
(println "Test 3: Query tech.ml.dataset directly")
(def tc-ds
  (ds/->dataset {:category ["Electronics" "Clothing" "Electronics" "Clothing" "Electronics"]
                 :revenue  (double-array [250.0 80.0 320.0 110.0 280.0])
                 :qty      (long-array [1 2 1 3 2])}))

(def result3
  (st/q {:from  tc-ds
         :group [:category]
         :agg   [[:sum :revenue] [:sum :qty] [:count]]}))

(assert (= 2 (count result3)) "Should have 2 category groups")
(println "Result:" result3)
(println "✓ Test 3 passed\n")

;; Test 4: Index-backed dataset and zone map pruning
(println "Test 4: Zone map pruning on indexed dataset")
(def time-series
  (st/make-dataset
    {:ts    (st/index-from-seq :int64   (range 0 100000))
     :value (st/index-from-seq :float64 (repeatedly 100000 rand))}))

(def result4
  (st/q {:from  time-series
         :where [[:>= :ts 90000] [:< :ts 91000]]
         :agg   [[:sum :value] [:count]]}))

(assert (= 1 (count result4)) "Should return one row")
(assert (= 1000 (long (:count (first result4)))) "Should count 1000 rows in range")
(println "Result:" result4)
(println "✓ Test 4 passed\n")

;; Test 5: Statistical aggregations (STDDEV, CORR)
(println "Test 5: STDDEV and CORR aggregations")
(def sensors
  {:temp     (double-array [20.1 20.5 21.0 19.8 20.3 22.0])
   :humidity (double-array [65.0 68.0 70.0 62.0 66.0 72.0])
   :sensor   (into-array String ["A" "A" "A" "B" "B" "B"])})

(def result5
  (st/q {:from  sensors
         :group [:sensor]
         :agg   [[:avg :temp] [:stddev :temp] [:corr :temp :humidity]]}))

(assert (= 2 (count result5)) "Should have 2 sensor groups")
(println "Result:" result5)
(println "✓ Test 5 passed\n")

;; Test 6: Hash JOIN
(println "Test 6: Hash JOIN")
(def fact
  {:order-id   (long-array [1 2 3 4 5])
   :product-id (long-array [101 102 101 103 102])
   :qty        (double-array [2 1 3 1 2])})

(def dim
  {:product-id (long-array [101 102 103])
   :name       (into-array String ["Widget" "Gadget" "Gizmo"])
   :price      (double-array [10.0 20.0 15.0])})

(def result6
  (st/q {:from fact
         :join [{:with dim :on [:= :product-id :product-id] :type :inner}]
         :group [:name]
         :agg   [[:sum :qty]]}))

(assert (= 3 (count result6)) "Should have 3 product groups")
(println "Result:" result6)
(println "✓ Test 6 passed\n")

;; Test 7: Dataset persistence — sync, fork, load, GC
(println "Test 7: Dataset persistence with st/sync!, st/fork, st/load, st/gc!")
(def store-cfg
  {:backend :file
   :path    "/tmp/stratum-notebook-test"
   :id      #uuid "770e8400-e29b-41d4-a716-446655440002"})

(when (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true}))

(def store (kstore/create-store store-cfg {:sync? true}))

(def base-ds
  (st/make-dataset
    {:product (st/index-from-seq :int64   [101 102 103])
     :revenue (st/index-from-seq :float64 [1000.0 750.0 1125.0])}
    {:name "sales"}))

(def saved (st/sync! base-ds store "main"))
(assert (some? (:id (:commit-info saved))) "Should have commit ID after sync!")

;; Fork and mutate via transient protocol
(def forked
  (-> (st/fork base-ds)
      transient
      (dataset/append! {:product 104 :revenue 900.0})
      persistent!))

(st/sync! forked store "forecast")

(def loaded-main     (st/load store "main"))
(def loaded-forecast (st/load store "forecast"))

(assert (= 3 (dataset/row-count loaded-main))     "main should have 3 rows")
(assert (= 4 (dataset/row-count loaded-forecast)) "forecast should have 4 rows")
(println "main rows:" (dataset/row-count loaded-main)
         "| forecast rows:" (dataset/row-count loaded-forecast))

(dataset/delete-branch! store "forecast")
(def gc-result (st/gc! store))
(assert (pos? (:deleted-pss-nodes gc-result)) "GC should delete chunks after branch removal")
(println "GC deleted" (:deleted-pss-nodes gc-result) "PSS nodes")

(kstore/delete-store store-cfg {:sync? true})
(println "✓ Test 7 passed\n")

(println "All notebook examples passed! ✅")
