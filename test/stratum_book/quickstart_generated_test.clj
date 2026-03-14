(ns
 stratum-book.quickstart-generated-test
 (:require
  [stratum.api :as st]
  [stratum.dataset :as dataset]
  [stratum.storage :as storage]
  [tablecloth.api :as tc]
  [tech.v3.dataset :as ds]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def
 v3_l24
 (def
  orders
  {:product
   (into-array
    String
    ["Apple" "Banana" "Apple" "Cherry" "Banana" "Apple"]),
   :qty (double-array [10.0 15.0 8.0 5.0 20.0 12.0]),
   :price (double-array [1.2 0.5 1.2 2.5 0.5 1.2])}))


(def
 v5_l31
 (st/q
  {:from orders,
   :group [:product],
   :agg [[:sum :qty] [:avg :price] [:count]]}))


(deftest t6_l37 (is ((fn [result] (= 3 (count result))) v5_l31)))


(def
 v8_l43
 (st/q
  "SELECT product, SUM(qty), AVG(price), COUNT(*) FROM orders GROUP BY product"
  {"orders" orders}))


(deftest t9_l46 (is ((fn [result] (= 3 (count result))) v8_l43)))


(def
 v11_l52
 (st/q
  {:from orders,
   :where [[:> :price 0.6]],
   :agg [[:sum :qty] [:count]]}))


(deftest t12_l56 (is ((fn [result] (= 1 (count result))) v11_l52)))


(def
 v14_l65
 (def
  tc-ds
  (tc/dataset
   {:category
    ["Electronics" "Clothing" "Electronics" "Clothing" "Electronics"],
    :revenue (double-array [250.0 80.0 320.0 110.0 280.0]),
    :qty (long-array [1 2 1 3 2])})))


(def
 v15_l70
 (st/q
  {:from tc-ds,
   :group [:category],
   :agg [[:sum :revenue] [:sum :qty] [:count]]}))


(deftest t16_l76 (is ((fn [result] (= 2 (count result))) v15_l70)))


(def
 v18_l82
 (st/q
  "SELECT category, SUM(revenue) AS total FROM t WHERE qty > 1 GROUP BY category"
  {"t" tc-ds}))


(deftest t19_l85 (is ((fn [result] (= 2 (count result))) v18_l82)))


(def
 v21_l97
 (def
  time-series
  (st/make-dataset
   {:ts (st/index-from-seq :int64 (range 0 100000)),
    :value (st/index-from-seq :float64 (repeatedly 100000 rand))})))


(def
 v22_l102
 (st/q
  {:from time-series,
   :where [[:>= :ts 90000] [:< :ts 91000]],
   :agg [[:sum :value] [:count]]}))


(deftest
 t23_l106
 (is ((fn [result] (= 1000 (long (:count (first result))))) v22_l102)))


(def
 v25_l115
 (def
  sensors
  {:temp (double-array [20.1 20.5 21.0 19.8 20.3 22.0]),
   :humidity (double-array [65.0 68.0 70.0 62.0 66.0 72.0]),
   :sensor (into-array String ["A" "A" "A" "B" "B" "B"])}))


(def
 v26_l120
 (st/q
  {:from sensors,
   :group [:sensor],
   :agg [[:avg :temp] [:stddev :temp] [:corr :temp :humidity]]}))


(deftest t27_l126 (is ((fn [result] (= 2 (count result))) v26_l120)))


(def
 v29_l135
 (def
  fact
  {:order-id (long-array [1 2 3 4 5]),
   :product-id (long-array [101 102 101 103 102]),
   :qty (double-array [2 1 3 1 2])}))


(def
 v30_l140
 (def
  dim
  {:product-id (long-array [101 102 103]),
   :name (into-array String ["Widget" "Gadget" "Gizmo"]),
   :price (double-array [10.0 20.0 15.0])}))


(def
 v31_l145
 (st/q
  {:from fact,
   :join [{:with dim, :on [:= :product-id :product-id], :type :inner}],
   :group [:name],
   :agg [[:sum :qty]]}))


(deftest t32_l152 (is ((fn [result] (= 3 (count result))) v31_l145)))


(def v34_l163 (require '[konserve.store :as kstore]))


(def
 v35_l165
 (def
  store-cfg
  {:backend :file,
   :path "/tmp/stratum-quickstart",
   :id #uuid "550e8400-e29b-41d4-a716-446655440099"}))


(def
 v36_l170
 (when
  (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true})))


(def v37_l173 (def store (kstore/create-store store-cfg {:sync? true})))


(def
 v39_l177
 (def
  base
  (st/make-dataset
   {:product (st/index-from-seq :int64 [101 102 103]),
    :qty (st/index-from-seq :float64 [100.0 50.0 75.0]),
    :revenue (st/index-from-seq :float64 [1000.0 750.0 1125.0])}
   {:name "sales-q1"})))


(def v41_l186 (def v1 (st/sync! base store "main")))


(def v42_l188 (:id (:commit-info v1)))


(deftest t43_l190 (is ((fn [id] (uuid? id)) v42_l188)))


(def
 v45_l195
 (def
  what-if
  (->
   (st/fork base)
   transient
   (dataset/set-at! :qty 0 150.0)
   (dataset/set-at! :revenue 0 1500.0)
   (dataset/append! {:product 104, :qty 60.0, :revenue 900.0})
   persistent!)))


(def v46_l203 (def v1-whatif (st/sync! what-if store "what-if")))


(def v47_l205 (storage/list-dataset-branches store))


(deftest
 t48_l207
 (is ((fn [branches] (= #{"what-if" "main"} branches)) v47_l205)))


(def
 v50_l213
 (let
  [main-ds (st/load store "main") whatif-ds (st/load store "what-if")]
  {:main (first (st/q {:from main-ds, :agg [[:sum :revenue]]})),
   :whatif (first (st/q {:from whatif-ds, :agg [[:sum :revenue]]}))}))


(deftest
 t51_l218
 (is
  ((fn
    [{:keys [main whatif]}]
    (and (= 2875.0 (:sum main)) (= 4275.0 (:sum whatif))))
   v50_l213)))


(def v53_l225 (kstore/delete-store store-cfg {:sync? true}))
