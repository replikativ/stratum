(ns backward-test
  "Cross-release persistence fixture; loaded only by the compatibility script."
  (:require [konserve.filestore]
            [konserve.store :as kstore]
            [stratum.dataset :as dataset]
            [stratum.index :as index]))

(def ^:private store-id
  #uuid "39adfd29-c68c-42d4-a755-a4fc3ea7ef69")

(defn- store-config []
  {:backend :file
   :path (str (System/getenv "BACK_COMPAT_ROOT") "/store")
   :id store-id})

(defn- make-dataset [prices quantities name]
  (dataset/make-dataset
   {:price (index/index-from-seq :float64 prices)
    :quantity (index/index-from-seq :int64 quantities)}
   {:name name :metadata {:fixture-version 1}}))

(defn write [_]
  (let [store (kstore/create-store (store-config) {:sync? true})]
    (dataset/sync! (make-dataset [10.5 20.25 30.75] [1 2 3] "released-main")
                   store "main")
    (dataset/sync! (make-dataset [99.0] [9] "released-feature")
                   store "feature")))

(defn- value-at [ds column row]
  (let [idx (:index (dataset/column ds column))]
    (case column
      :price (index/idx-get-double idx row)
      :quantity (index/idx-get-long idx row))))

(defn verify [_]
  (let [store (kstore/connect-store (store-config) {:sync? true})
        main (dataset/load store "main")
        feature (dataset/load store "feature")]
    (assert (= 3 (dataset/row-count main)))
    (assert (== 20.25 (value-at main :price 1)))
    (assert (= "released-main" (dataset/ds-name main)))
    (assert (= 1 (dataset/row-count feature)))
    (assert (== 99.0 (value-at feature :price 0)))
    ;; Reading old bytes is not enough: prove that a dataset restored from the
    ;; release can be changed, published, and opened again by current code.
    (let [working (transient (dataset/fork main))]
      (dataset/set-at! working :price 1 250.5)
      (dataset/sync! (persistent! working) store "main"))
    (let [reopened (dataset/load store "main")
          feature-reopened (dataset/load store "feature")]
      (assert (== 250.5 (value-at reopened :price 1)))
      (assert (== 99.0 (value-at feature-reopened :price 0))))))
