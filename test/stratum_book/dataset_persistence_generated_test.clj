(ns
 stratum-book.dataset-persistence-generated-test
 (:require
  [stratum.dataset :as dataset]
  [stratum.index :as index]
  [stratum.storage :as storage]
  [konserve.memory :refer [new-mem-store]]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def
 v2_l18
 (defn- make-store [] (new-mem-store (atom {}) {:sync? true})))


(def
 v4_l30
 (def
  ds
  (dataset/make-dataset
   {:x (index/index-from-seq :float64 [1.0 2.0 3.0]),
    :y (index/index-from-seq :int64 [10 20 30])}
   {:name "test"})))


(def
 v5_l36
 (let
  [t (transient ds)]
  (dataset/set-at! t :x 0 99.0)
  (let
   [p (persistent! t)]
   {:transient? (dataset/transient? t),
    :persistent? (not (dataset/transient? p)),
    :x0 (index/idx-get-double (:index (dataset/column p :x)) 0)})))


(deftest
 t6_l43
 (is
  ((fn [{:keys [persistent? x0]}] (and persistent? (== 99.0 x0)))
   v5_l36)))


(def
 v8_l49
 (let
  [t (transient ds)]
  (dataset/append! t {:x 4.0, :y 40})
  (let
   [p (persistent! t)]
   {:rows (dataset/row-count p),
    :x3 (index/idx-get-double (:index (dataset/column p :x)) 3),
    :y3 (index/idx-get-long (:index (dataset/column p :y)) 3)})))


(deftest
 t9_l56
 (is
  ((fn [{:keys [rows x3 y3]}] (and (= 4 rows) (== 4.0 x3) (== 40 y3)))
   v8_l49)))


(def
 v11_l65
 (try
  (dataset/set-at! ds :x 0 99.0)
  :no-error
  (catch IllegalStateException _ :threw)))


(deftest t12_l68 (is ((fn [r] (= :threw r)) v11_l65)))


(def
 v13_l71
 (try
  (transient (transient ds))
  :no-error
  (catch IllegalStateException _ :threw)))


(deftest t14_l74 (is ((fn [r] (= :threw r)) v13_l71)))


(def
 v15_l77
 (try
  (persistent! ds)
  :no-error
  (catch IllegalStateException _ :threw)))


(deftest t16_l80 (is ((fn [r] (= :threw r)) v15_l77)))


(def
 v18_l85
 (try
  (let [t (transient ds)] (dataset/append! t {:x 2.0}))
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t19_l90 (is ((fn [r] (= :threw r)) v18_l85)))


(def
 v21_l100
 (let
  [fork (dataset/fork ds) t (transient fork)]
  (dataset/set-at! t :x 0 99.0)
  (let
   [p (persistent! t)]
   {:original (index/idx-get-double (:index (dataset/column ds :x)) 0),
    :fork (index/idx-get-double (:index (dataset/column p :x)) 0)})))


(deftest
 t22_l107
 (is
  ((fn
    [{:keys [original fork]}]
    (and (== 1.0 original) (== 99.0 fork)))
   v21_l100)))


(def
 v24_l113
 (let
  [fa
   (dataset/fork ds)
   fb
   (dataset/fork ds)
   ta
   (transient fa)
   _
   (dataset/set-at! ta :x 0 100.0)
   pa
   (persistent! ta)
   tb
   (transient fb)
   _
   (dataset/set-at! tb :x 0 200.0)
   pb
   (persistent! tb)]
  {:original (index/idx-get-double (:index (dataset/column ds :x)) 0),
   :fork-a (index/idx-get-double (:index (dataset/column pa :x)) 0),
   :fork-b (index/idx-get-double (:index (dataset/column pb :x)) 0)}))


(deftest
 t25_l125
 (is
  ((fn
    [{:keys [original fork-a fork-b]}]
    (and (== 1.0 original) (== 100.0 fork-a) (== 200.0 fork-b)))
   v24_l113)))


(def v27_l136 (def store (make-store)))


(def
 v28_l138
 (def
  saved
  (dataset/sync!
   (dataset/make-dataset
    {:price (index/index-from-seq :float64 [100.0 200.0 300.0]),
     :qty (index/index-from-seq :int64 [10 20 30])}
    {:name "trades", :metadata {:source "test"}})
   store
   "main")))


(def v29_l146 (def loaded (dataset/load store "main")))


(def
 v30_l148
 {:name (dataset/ds-name loaded),
  :metadata (dataset/metadata loaded),
  :rows (dataset/row-count loaded),
  :schema= (= (dataset/schema saved) (dataset/schema loaded)),
  :price0
  (index/idx-get-double (:index (dataset/column loaded :price)) 0),
  :qty2 (index/idx-get-long (:index (dataset/column loaded :qty)) 2)})


(deftest
 t31_l155
 (is
  ((fn
    [{:keys [name metadata rows schema= price0 qty2]}]
    (and
     (= "trades" name)
     (= {:source "test"} metadata)
     (= 3 rows)
     schema=
     (== 100.0 price0)
     (== 30 qty2)))
   v30_l148)))


(def
 v33_l168
 (let
  [store2
   (make-store)
   ds2
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})
   fork
   (dataset/fork ds2)
   t
   (transient fork)]
  (dataset/set-at! t :x 1 99.0)
  (let
   [p
    (persistent! t)
    _
    (dataset/sync! p store2 "main")
    loaded
    (dataset/load store2 "main")]
   (index/idx-get-double (:index (dataset/column loaded :x)) 1))))


(deftest t34_l179 (is ((fn [v] (== 99.0 v)) v33_l168)))


(def
 v36_l186
 (let
  [store3
   (make-store)
   ds-v1
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 [1.0 2.0])})
   ds-v2
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 [10.0 20.0 30.0])})
   s1
   (dataset/sync! ds-v1 store3 "main")
   s2
   (dataset/sync! ds-v2 store3 "main")
   loaded
   (dataset/load store3 "main")]
  {:different-commits?
   (not= (:id (:commit-info s1)) (:id (:commit-info s2))),
   :rows (dataset/row-count loaded),
   :x0 (index/idx-get-double (:index (dataset/column loaded :x)) 0)}))


(deftest
 t37_l196
 (is
  ((fn
    [{:keys [different-commits? rows x0]}]
    (and different-commits? (= 3 rows) (== 10.0 x0)))
   v36_l186)))


(def
 v39_l202
 (let
  [store4
   (make-store)
   ds4
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 [1.0 2.0 3.0])})
   saved
   (dataset/sync! ds4 store4 "main")
   cid
   (:id (:commit-info saved))
   loaded
   (dataset/load store4 cid)]
  (dataset/row-count loaded)))


(deftest t40_l209 (is ((fn [n] (= 3 n)) v39_l202)))


(def
 v42_l216
 (let
  [store5
   (make-store)
   ds5
   (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0])})]
  (dataset/sync! ds5 store5 "main")
  (dataset/sync! ds5 store5 "feature")
  (storage/list-dataset-branches store5)))


(deftest
 t43_l222
 (is ((fn [branches] (= #{"feature" "main"} branches)) v42_l216)))


(def
 v45_l227
 (let
  [store6
   (make-store)
   ds6
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 [1.0 2.0])})]
  (dataset/sync! ds6 store6 "main")
  (dataset/sync! ds6 store6 "feature")
  (dataset/delete-branch! store6 "feature")
  {:branches (storage/list-dataset-branches store6),
   :head-nil? (nil? (storage/load-dataset-head store6 "feature")),
   :main-rows (dataset/row-count (dataset/load store6 "main"))}))


(deftest
 t46_l236
 (is
  ((fn
    [{:keys [branches head-nil? main-rows]}]
    (and (= #{"main"} branches) head-nil? (= 2 main-rows)))
   v45_l227)))


(def
 v48_l250
 (let
  [store7
   (make-store)
   ds-a
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 (range 100))})
   ds-b
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 (range 100 200))})]
  (dataset/sync! ds-a store7 "main")
  (dataset/sync! ds-b store7 "main")
  (let
   [gc (storage/gc! store7)]
   {:deleted-pss (pos? (:deleted-pss-nodes gc)),
    :deleted-idx (pos? (:deleted-index-commits gc)),
    :deleted-ds (pos? (:deleted-dataset-commits gc)),
    :kept (pos? (:kept-pss-nodes gc))})))


(deftest
 t49_l261
 (is
  ((fn
    [{:keys [deleted-pss deleted-idx deleted-ds kept]}]
    (and deleted-pss deleted-idx deleted-ds kept))
   v48_l250)))


(def
 v51_l267
 (let
  [store8
   (make-store)
   ds8
   (dataset/make-dataset
    {:x (index/index-from-seq :float64 (range 100))})]
  (dataset/sync! ds8 store8 "main")
  (dataset/delete-branch! store8 "main")
  (let
   [gc (storage/gc! store8)]
   {:deleted (pos? (:deleted-pss-nodes gc)),
    :kept (= 0 (:kept-pss-nodes gc))})))


(deftest
 t52_l275
 (is ((fn [{:keys [deleted kept]}] (and deleted kept)) v51_l267)))


(def
 v54_l288
 (def base-ds (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})))


(def
 v56_l292
 (let
  [ds2 (assoc base-ds :y (long-array [10 20 30]))]
  {:cols (set (dataset/column-names ds2)),
   :type (:type (dataset/column ds2 :y)),
   :v0 (nth (:data (dataset/column ds2 :y)) 0)}))


(deftest
 t57_l297
 (is
  ((fn
    [{:keys [cols type v0]}]
    (and (= #{:y :x} cols) (= :int64 type) (= 10 v0)))
   v56_l292)))


(def
 v59_l303
 (let
  [y-idx
   (index/index-from-seq :int64 [10 20 30])
   ds2
   (assoc base-ds :y y-idx)]
  {:source (:source (dataset/column ds2 :y)),
   :same? (identical? y-idx (:index (dataset/column ds2 :y)))}))


(deftest
 t60_l308
 (is
  ((fn [{:keys [source same?]}] (and (= :index source) same?))
   v59_l303)))


(def
 v62_l314
 (let
  [ds2 (assoc base-ds :y [10.5 20.5 30.5])]
  (:type (dataset/column ds2 :y))))


(deftest t63_l317 (is ((fn [t] (= :float64 t)) v62_l314)))


(def
 v65_l322
 (try
  (assoc base-ds :y (long-array [10 20]))
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t66_l325 (is ((fn [r] (= :threw r)) v65_l322)))


(def
 v68_l330
 (let
  [ds2 (assoc base-ds :x (long-array [10 20 30]))]
  {:type (:type (dataset/column ds2 :x)),
   :v0 (nth (:data (dataset/column ds2 :x)) 0)}))


(deftest
 t69_l334
 (is
  ((fn [{:keys [type v0]}] (and (= :int64 type) (= 10 v0))) v68_l330)))


(def
 v71_l340
 (let
  [ds2
   (dataset/make-dataset
    {:x (double-array [1.0 2.0]), :y (long-array [10 20])})
   ds3
   (dissoc ds2 :y)]
  {:cols (set (dataset/column-names ds3)),
   :rows (dataset/row-count ds3)}))


(deftest
 t72_l346
 (is
  ((fn [{:keys [cols rows]}] (and (= #{:x} cols) (= 2 rows)))
   v71_l340)))


(def
 v74_l352
 (let [ds2 (dissoc base-ds :nonexistent)] (identical? base-ds ds2)))


(deftest t75_l355 (is (true? v74_l352)))


(def
 v77_l360
 (let
  [ds2 (dataset/rename-column base-ds :x :new-x)]
  {:cols (set (dataset/column-names ds2)),
   :rows (dataset/row-count ds2)}))


(deftest
 t78_l364
 (is
  ((fn [{:keys [cols rows]}] (and (= #{:new-x} cols) (= 3 rows)))
   v77_l360)))


(def
 v80_l370
 {:has-x? (contains? base-ds :x), :has-foo? (contains? base-ds :foo)})


(deftest
 t81_l373
 (is
  ((fn [{:keys [has-x? has-foo?]}] (and has-x? (not has-foo?)))
   v80_l370)))


(def
 v83_l379
 (let
  [ds2
   (dataset/make-dataset
    {:x (double-array [1.0 2.0])}
    {:name "test", :metadata {:k "v"}})]
  {:name (:name ds2),
   :metadata (:metadata ds2),
   :row-count (:row-count ds2),
   :nil-key (:nonexistent ds2)}))


(deftest
 t84_l387
 (is
  ((fn
    [{:keys [name metadata row-count nil-key]}]
    (and
     (= "test" name)
     (= {:k "v"} metadata)
     (= 2 row-count)
     (nil? nil-key)))
   v83_l379)))


(def
 v86_l399
 (let
  [ds2
   (dataset/make-dataset
    {:x (double-array [1.0 2.0 3.0]), :y (long-array [10 20 30])})]
  {:before-x (dataset/has-index? ds2 :x),
   :before-y (dataset/has-index? ds2 :y),
   :after
   (let
    [indexed (dataset/ensure-indexed ds2)]
    {:x (dataset/has-index? indexed :x),
     :y (dataset/has-index? indexed :y),
     :v
     (index/idx-get-double (:index (dataset/column indexed :x)) 2)})}))


(deftest
 t87_l408
 (is
  ((fn
    [{:keys [before-x before-y after]}]
    (and
     (not before-x)
     (not before-y)
     (:x after)
     (:y after)
     (== 3.0 (:v after))))
   v86_l399)))


(def
 v89_l415
 (let
  [x-idx
   (index/index-from-seq :float64 [1.0 2.0 3.0])
   ds2
   (dataset/make-dataset {:x x-idx})
   result
   (dataset/ensure-indexed ds2)]
  (identical? ds2 result)))


(deftest t90_l420 (is (true? v89_l415)))


(def
 v92_l425
 (let
  [ds2
   (dataset/make-dataset
    {:region (into-array String ["US" "EU" "APAC" "US"])})
   indexed
   (dataset/ensure-indexed ds2)
   col
   (dataset/column indexed :region)]
  {:has-index? (dataset/has-index? indexed :region),
   :has-dict? (some? (:dict col)),
   :dict-type (:dict-type col)}))


(deftest
 t93_l432
 (is
  ((fn
    [{:keys [has-index? has-dict? dict-type]}]
    (and has-index? has-dict? (= :string dict-type)))
   v92_l425)))


(def
 v95_l441
 (let
  [store9
   (make-store)
   ds2
   (dataset/make-dataset {:x (double-array [1.0 2.0 3.0])})]
  (try
   (dataset/sync! ds2 store9 "main")
   {:threw false}
   (catch
    clojure.lang.ExceptionInfo
    _
    (let
     [indexed
      (dataset/ensure-indexed ds2)
      saved
      (dataset/sync! indexed store9 "main")
      loaded
      (dataset/load store9 "main")]
     {:threw true,
      :commit? (some? (:commit-info saved)),
      :rows (dataset/row-count loaded)})))))


(deftest
 t96_l455
 (is
  ((fn [{:keys [threw commit? rows]}] (and threw commit? (= 3 rows)))
   v95_l441)))


(def
 v98_l468
 (try
  (dataset/fork (dataset/make-dataset {:x (double-array [1.0])}))
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t99_l472 (is ((fn [r] (= :threw r)) v98_l468)))


(def
 v101_l477
 (try
  (transient (dataset/make-dataset {:x (double-array [1.0])}))
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t102_l481 (is ((fn [r] (= :threw r)) v101_l477)))


(def
 v104_l486
 (let
  [store10
   (make-store)
   ds2
   (dataset/make-dataset {:x (index/index-from-seq :float64 [1.0])})]
  (try
   (dataset/sync! (transient ds2) store10 "main")
   :no-error
   (catch IllegalStateException _ :threw))))


(deftest t105_l491 (is ((fn [r] (= :threw r)) v104_l486)))
