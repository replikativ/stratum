;; Test script for dataset persistence
;; Run with: clj -M:dev -i notebooks/test_persistence.clj

(require '[stratum.api :as st]
         '[stratum.dataset :as dataset]
         '[stratum.storage :as storage]
         '[konserve.store :as kstore])

(println "Testing Dataset Persistence")
(println "============================\n")

;; Test 1: Create filestore
(println "Test 1: Create filestore")
(def store-cfg
  {:backend :file
   :path    "/tmp/stratum-test-persistence"
   :id      #uuid "660e8400-e29b-41d4-a716-446655440001"})

(when (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true}))

(def store (kstore/create-store store-cfg {:sync? true}))
(println "✓ Store created at:" (:path store-cfg) "\n")

;; Test 2: Create and sync base dataset
(println "Test 2: Create and sync base dataset")
(def base-sales
  (st/make-dataset
    {:product (st/index-from-seq :int64   [101 102 103])
     :qty     (st/index-from-seq :float64 [100.0 50.0 75.0])
     :revenue (st/index-from-seq :float64 [1000.0 750.0 1125.0])}
    {:name "sales-q1"}))

(def saved-main (st/sync! base-sales store "main"))
(assert (= "main" (:branch (:commit-info saved-main))) "Branch should be 'main'")
(assert (some? (:id (:commit-info saved-main))) "Should have commit ID")
(println "✓ Base dataset synced to 'main'")
(println "  Commit ID:" (:id (:commit-info saved-main)) "\n")

;; Test 3: List branches
(println "Test 3: List branches")
(def branches (storage/list-dataset-branches store))
(assert (= #{"main"} branches) "Should have 'main' branch")
(println "✓ Branches:" branches "\n")

;; Test 4: Fork and modify via transient protocol
(println "Test 4: Fork and modify dataset")
(def forecast-sales
  (-> (st/fork base-sales)
      transient
      (dataset/set-at!    :qty     0 150.0)
      (dataset/set-at!    :revenue 0 1500.0)
      (dataset/append! {:product 104 :qty 60.0 :revenue 900.0})
      persistent!))

(assert (= 4 (dataset/row-count forecast-sales)) "Forecast should have 4 rows")
(assert (= 3 (dataset/row-count base-sales))     "Original should still have 3 rows")
(println "✓ Forked and modified — forecast:" (dataset/row-count forecast-sales)
         "rows, original:" (dataset/row-count base-sales) "rows\n")

;; Test 5: Sync forecast branch
(println "Test 5: Sync forecast branch")
(def saved-forecast (st/sync! forecast-sales store "forecast"))
(def branches2 (storage/list-dataset-branches store))
(assert (= #{"main" "forecast"} branches2) "Should have both branches")
(println "✓ Forecast synced, branches:" branches2 "\n")

;; Test 6: Load both branches and compare
(println "Test 6: Load both branches and compare")
(def reloaded-main     (st/load store "main"))
(def reloaded-forecast (st/load store "forecast"))

(assert (= 3 (dataset/row-count reloaded-main))     "Main should have 3 rows")
(assert (= 4 (dataset/row-count reloaded-forecast)) "Forecast should have 4 rows")

(def main-revenue     (:sum (first (st/q {:from reloaded-main     :agg [[:sum :revenue]]}))))
(def forecast-revenue (:sum (first (st/q {:from reloaded-forecast :agg [[:sum :revenue]]}))))

(assert (= 2875.0 main-revenue)     "Main revenue should be 2875.0")
(assert (= 4275.0 forecast-revenue) "Forecast revenue should be 4275.0")
(println "✓ Revenue — main:" main-revenue "| forecast:" forecast-revenue
         "| diff:" (- forecast-revenue main-revenue) "\n")

;; Test 7: GC with all branches alive — nothing deleted
(println "Test 7: GC with all branches alive")
(def gc-result1 (st/gc! store))
(assert (= 0 (:deleted-pss-nodes gc-result1)) "Should delete nothing while both branches live")
(assert (pos? (:kept-pss-nodes gc-result1))   "Should keep some chunks")
(println "✓ GC kept" (:kept-pss-nodes gc-result1) "nodes, deleted" (:deleted-pss-nodes gc-result1) "\n")

;; Test 8: Delete branch and GC — unreachable chunks reclaimed
(println "Test 8: Delete forecast branch and GC")
(dataset/delete-branch! store "forecast")
(assert (= #{"main"} (storage/list-dataset-branches store)) "Should only have 'main'")

(def gc-result2 (st/gc! store))
(assert (pos? (:deleted-pss-nodes gc-result2))      "Should delete some PSS nodes")
(assert (pos? (:deleted-index-commits gc-result2))  "Should delete some index commits")
(assert (pos? (:deleted-dataset-commits gc-result2)) "Should delete dataset commit")
(println "✓ After deleting forecast:")
(println "  Deleted PSS nodes:      " (:deleted-pss-nodes gc-result2))
(println "  Deleted index commits:  " (:deleted-index-commits gc-result2))
(println "  Deleted dataset commits:" (:deleted-dataset-commits gc-result2))
(println "  Kept PSS nodes:         " (:kept-pss-nodes gc-result2) "\n")

;; Test 9: Main branch still intact after GC
(println "Test 9: Main branch still accessible after GC")
(def reloaded-main2 (st/load store "main"))
(def main-revenue2  (:sum (first (st/q {:from reloaded-main2 :agg [[:sum :revenue]]}))))
(assert (= 2875.0 main-revenue2) "Main still has correct data after GC")
(println "✓ Main revenue after GC:" main-revenue2 "\n")

;; Test 10: Load by commit UUID (time-travel)
(println "Test 10: Time-travel by commit UUID")
(def commit-uuid (:id (:commit-info saved-main)))
(def at-commit   (st/resolve store "sales-q1" {:as-of commit-uuid}))
(assert (= 3 (dataset/row-count at-commit)) "Should load exactly 3 rows at commit")
(println "✓ Loaded dataset at commit" commit-uuid "—" (dataset/row-count at-commit) "rows\n")

;; Cleanup
(kstore/delete-store store-cfg {:sync? true})

(println "==========================================")
(println "All persistence tests passed! ✅")
