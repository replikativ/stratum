(ns
 stratum-book.api-reference-generated-test
 (:require
  [stratum.api :as st]
  [stratum.dataset :as dataset]
  [stratum.storage :as storage]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def v3_l22 (kind/doc #'st/q))


(def
 v5_l28
 (def
  data
  {:product (into-array String ["A" "B" "A" "B" "A"]),
   :qty (double-array [10 20 30 40 50]),
   :price (double-array [1.0 2.0 1.5 2.5 1.0])}))


(def
 v6_l33
 (st/q
  {:from data,
   :where [[:> :qty 15]],
   :group [:product],
   :agg [[:sum :qty] [:avg :price] [:count]]}))


(deftest
 t7_l38
 (is
  ((fn
    [result]
    (and
     (= 2 (count result))
     (every? (fn* [p1__97069#] (contains? p1__97069# :sum)) result)))
   v6_l33)))


(def
 v9_l45
 (st/q
  "SELECT product, SUM(qty) AS total FROM d GROUP BY product"
  {"d" data}))


(deftest t10_l48 (is ((fn [result] (= 2 (count result))) v9_l45)))


(def v11_l52 (kind/doc #'st/explain))


(def
 v12_l54
 (st/explain {:from data, :where [[:> :qty 15]], :agg [[:sum :qty]]}))


(deftest t13_l58 (is ((fn [plan] (contains? plan :strategy)) v12_l54)))


(def v15_l66 (kind/doc #'st/from-maps))


(def
 v16_l68
 (def
  people
  (st/from-maps
   [{:name "Alice", :age 30}
    {:name "Bob", :age 25}
    {:name "Carol", :age 35}])))


(def
 v17_l73
 (st/q {:from people, :where [[:> :age 28]], :agg [[:count]]}))


(deftest
 t18_l75
 (is ((fn [result] (= 2 (:count (first result)))) v17_l73)))


(def v19_l79 (kind/doc #'st/from-csv))


(def v21_l84 (kind/doc #'st/from-parquet))


(def v23_l93 (kind/doc #'st/encode-column))


(def v24_l95 (st/encode-column (into-array String ["x" "y" "x" "z"])))


(deftest t25_l97 (is ((fn [col] (= :string (:dict-type col))) v24_l95)))


(def v26_l101 (st/encode-column (double-array [1.0 2.0 3.0])))


(deftest t27_l103 (is ((fn [col] (= :float64 (:type col))) v26_l101)))


(def v28_l107 (kind/doc #'st/index-from-seq))


(def v29_l109 (st/index-from-seq :int64 [10 20 30 40 50]))


(deftest t30_l111 (is ((fn [idx] (= 5 (count idx))) v29_l109)))


(def v31_l115 (st/index-from-seq :float64 [1.1 2.2 3.3]))


(deftest t32_l117 (is ((fn [idx] (= 3 (count idx))) v31_l115)))


(def v34_l125 (kind/doc #'st/results->columns))


(def v35_l127 (st/results->columns [{:a 1, :b "x"} {:a 2, :b "y"}]))


(deftest
 t36_l129
 (is
  ((fn [cols] (and (contains? cols :a) (contains? cols :b))) v35_l127)))


(def v37_l134 (kind/doc #'st/tuples->columns))


(def v38_l136 (st/tuples->columns [[1 "Alice"] [2 "Bob"]] [:id :name]))


(deftest
 t39_l138
 (is
  ((fn [cols] (and (contains? cols :id) (contains? cols :name)))
   v38_l136)))


(def v40_l143 (kind/doc #'st/columns->tuples))


(def
 v41_l145
 (st/columns->tuples
  {:id (long-array [1 2]), :name (into-array String ["Alice" "Bob"])}
  [:id :name]))


(deftest
 t42_l149
 (is ((fn [tuples] (= [[1 "Alice"] [2 "Bob"]] tuples)) v41_l145)))


(def v44_l157 (kind/doc #'st/make-dataset))


(def
 v45_l159
 (def
  ds
  (st/make-dataset
   {:x (st/index-from-seq :int64 [1 2 3 4 5]),
    :y (st/index-from-seq :float64 [1.1 2.2 3.3 4.4 5.5])}
   {:name "example"})))


(def v46_l165 (st/name ds))


(deftest t47_l167 (is ((fn [n] (= "example" n)) v46_l165)))


(def v48_l170 (kind/doc #'st/row-count))


(def v49_l172 (st/row-count ds))


(deftest t50_l174 (is ((fn [n] (= 5 n)) v49_l172)))


(def v51_l177 (kind/doc #'st/column-names))


(def v52_l179 (st/column-names ds))


(deftest t53_l181 (is ((fn [names] (= (set names) #{:y :x})) v52_l179)))


(def v54_l185 (kind/doc #'st/schema))


(def v55_l187 (st/schema ds))


(deftest
 t56_l189
 (is ((fn [s] (and (contains? s :x) (contains? s :y))) v55_l187)))


(def v57_l194 (kind/doc #'st/ensure-indexed))


(def
 v58_l196
 (def
  plain
  (st/make-dataset
   {:a (long-array [1 2 3]), :b (double-array [1.0 2.0 3.0])})))


(def v59_l200 (def indexed-ds (st/ensure-indexed plain)))


(def v60_l202 (st/q {:from indexed-ds, :agg [[:sum :b]]}))


(deftest
 t61_l204
 (is ((fn [result] (= 6.0 (:sum (first result)))) v60_l202)))


(def v63_l217 (require '[konserve.store :as kstore]))


(def
 v64_l219
 (def
  store-cfg
  {:backend :file,
   :path "/tmp/stratum-apiref",
   :id #uuid "660e8400-e29b-41d4-a716-446655440001"}))


(def
 v65_l224
 (when
  (kstore/store-exists? store-cfg {:sync? true})
  (kstore/delete-store store-cfg {:sync? true})))


(def v66_l227 (def store (kstore/create-store store-cfg {:sync? true})))


(def v67_l229 (kind/doc #'st/sync!))


(def v68_l231 (def saved (st/sync! ds store "main")))


(def v69_l233 (:id (:commit-info saved)))


(deftest t70_l235 (is ((fn [id] (uuid? id)) v69_l233)))


(def v71_l238 (kind/doc #'st/load))


(def v72_l240 (def loaded (st/load store "main")))


(def v73_l242 (st/row-count loaded))


(deftest t74_l244 (is ((fn [n] (= 5 n)) v73_l242)))


(def v75_l247 (kind/doc #'st/fork))


(def
 v76_l249
 (def
  forked
  (->
   (st/fork ds)
   transient
   (dataset/append! {:x 6, :y 6.6})
   persistent!)))


(def v77_l255 (st/row-count forked))


(deftest t78_l257 (is ((fn [n] (= 6 n)) v77_l255)))


(def v79_l260 (kind/doc #'st/gc!))


(def v80_l262 (st/sync! forked store "experiment"))


(def v81_l264 (dataset/delete-branch! store "experiment"))


(def v82_l266 (st/gc! store))


(deftest
 t83_l268
 (is
  ((fn [gc-result] (contains? gc-result :deleted-pss-nodes)) v82_l266)))


(def v85_l274 (kstore/delete-store store-cfg {:sync? true}))


(def v87_l283 (kind/doc #'st/normalize-pred))


(def v88_l285 (st/normalize-pred [:> :price 10]))


(deftest t89_l287 (is ((fn [pred] (vector? pred)) v88_l285)))


(def v90_l291 (kind/doc #'st/normalize-agg))


(def v91_l293 (st/normalize-agg [:sum :qty]))


(deftest t92_l295 (is ((fn [agg] (= :sum (:op agg))) v91_l293)))


(def v93_l299 (kind/doc #'st/normalize-expr))


(def v94_l301 (st/normalize-expr [:* :price :qty]))


(deftest t95_l303 (is ((fn [expr] (= :mul (:op expr))) v94_l301)))


(def v97_l315 (kind/doc #'st/train-iforest))


(def
 v98_l317
 (def
  normal-data
  {:x
   (double-array
    (repeatedly 500 (fn* [] (+ 50.0 (* 5.0 (- (rand) 0.5)))))),
   :y
   (double-array
    (repeatedly 500 (fn* [] (+ 50.0 (* 5.0 (- (rand) 0.5))))))}))


(def
 v99_l321
 (def
  model
  (st/train-iforest
   {:from normal-data,
    :n-trees 50,
    :sample-size 64,
    :seed 42,
    :contamination 0.05})))


(def v100_l328 (:n-trees model))


(deftest t101_l330 (is ((fn [n] (= 50 n)) v100_l328)))


(def v102_l333 (kind/doc #'st/iforest-score))


(def
 v103_l335
 (def
  test-data
  {:x (double-array [50.0 50.0 999.0]),
   :y (double-array [50.0 50.0 999.0])}))


(def v104_l339 (def scores (st/iforest-score model test-data)))


(def v105_l341 (seq scores))


(deftest t106_l343 (is ((fn [s] (> (nth s 2) (nth s 0))) v105_l341)))


(def v107_l347 (kind/doc #'st/iforest-predict))


(def v108_l349 (seq (st/iforest-predict model test-data)))


(deftest
 t109_l351
 (is ((fn [preds] (= 1 (long (nth preds 2)))) v108_l349)))
