(ns
 stratum-book.tablecloth-interop-generated-test
 (:require
  [stratum.dataset :as dataset]
  [stratum.tablecloth]
  [stratum.index :as idx]
  [stratum.query :as q]
  [tech.v3.dataset :as ds]
  [tech.v3.dataset.protocols :as ds-proto]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def
 v3_l30
 (def
  trades
  (dataset/make-dataset
   {:price (double-array [100.0 200.0 300.0]),
    :qty (long-array [10 20 30])}
   {:name "trades"})))


(def
 v4_l36
 {:dataset? (ds-proto/is-dataset? trades),
  :row-count (ds-proto/row-count trades),
  :column-count (ds-proto/column-count trades),
  :name (dataset/ds-name trades)})


(deftest
 t5_l41
 (is
  ((fn
    [{:keys [dataset? row-count column-count name]}]
    (and
     dataset?
     (= 3 row-count)
     (= 2 column-count)
     (= "trades" name)))
   v4_l36)))


(def
 v7_l50
 (let
  [col (ds/column trades :price)]
  {:count (count col), :v0 (nth col 0), :v2 (nth col 2)}))


(deftest
 t8_l55
 (is
  ((fn
    [{:keys [count v0 v2]}]
    (and (= 3 count) (= 100.0 v0) (= 300.0 v2)))
   v7_l50)))


(def
 v10_l61
 (try
  (ds-proto/column trades :nonexistent)
  :no-error
  (catch RuntimeException _ :threw)))


(deftest t11_l64 (is ((fn [r] (= :threw r)) v10_l61)))


(def
 v13_l69
 (def
  idx-ds
  (dataset/make-dataset
   {:price (idx/index-from-seq :float64 [100.0 200.0 300.0]),
    :qty (idx/index-from-seq :int64 [10 20 30])})))


(def
 v14_l74
 {:row-count (ds/row-count idx-ds),
  :column-count (ds/column-count idx-ds),
  :has-price? (dataset/has-index? idx-ds :price),
  :has-qty? (dataset/has-index? idx-ds :qty)})


(deftest
 t15_l79
 (is
  ((fn
    [{:keys [row-count column-count has-price? has-qty?]}]
    (and (= 3 row-count) (= 2 column-count) has-price? has-qty?))
   v14_l74)))


(def
 v17_l87
 (let
  [rows (ds-proto/rows trades {}) rowvecs (ds-proto/rowvecs trades {})]
  {:row0 (.readObject rows 0), :vec0 (.readObject rowvecs 0)}))


(deftest
 t18_l92
 (is
  ((fn
    [{:keys [row0 vec0]}]
    (and (map? row0) (= 100.0 (:price row0)) (vector? vec0)))
   v17_l87)))


(def v20_l100 (def selected-cols (ds/select-columns trades [:price])))


(def
 v21_l102
 {:type (type selected-cols),
  :col-ct (ds/column-count selected-cols),
  :columns (set (dataset/column-names selected-cols))})


(deftest
 t22_l106
 (is
  ((fn
    [{:keys [type col-ct columns]}]
    (and
     (= stratum.dataset.StratumDataset type)
     (= 1 col-ct)
     (= #{:price} columns)))
   v21_l102)))


(def
 v24_l114
 (def
  selected-rows
  (ds/select-rows
   (dataset/make-dataset
    {:x (double-array [1 2 3 4 5]), :y (long-array [10 20 30 40 50])})
   [0 2 4])))


(def
 v25_l121
 {:type (type selected-rows),
  :row-ct (ds/row-count selected-rows),
  :x-vals (vec (:data (dataset/column selected-rows :x))),
  :y-vals (vec (:data (dataset/column selected-rows :y)))})


(deftest
 t26_l126
 (is
  ((fn
    [{:keys [type row-ct x-vals y-vals]}]
    (and
     (= stratum.dataset.StratumDataset type)
     (= 3 row-ct)
     (= [1.0 3.0 5.0] x-vals)
     (= [10 30 50] y-vals)))
   v25_l121)))


(def
 v28_l139
 (def
  missing-ds
  (dataset/make-dataset
   {:x (double-array [1.0 Double/NaN 3.0]),
    :y (long-array [10 Long/MIN_VALUE 30])})))


(def
 v29_l144
 (let
  [bm (ds-proto/missing missing-ds)]
  {:cardinality (.getCardinality bm),
   :row-1? (.contains bm 1),
   :row-0? (.contains bm 0)}))


(deftest
 t30_l149
 (is
  ((fn
    [{:keys [cardinality row-1? row-0?]}]
    (and (= 1 cardinality) row-1? (not row-0?)))
   v29_l144)))


(def
 v32_l155
 (let
  [col (ds/column missing-ds :x)]
  [(nth col 0) (nth col 1) (nth col 2)]))


(deftest
 t33_l158
 (is
  ((fn [[v0 v1 v2]] (and (= 1.0 v0) (nil? v1) (= 3.0 v2))) v32_l155)))


(def
 v35_l167
 (def
  large-ds
  (dataset/make-dataset
   {:x (idx/index-from-seq :float64 (range 1000000))})))


(def
 v36_l170
 {:rows (ds/row-count large-ds),
  :cols (ds/column-count large-ds),
  :v0 (nth (ds/column large-ds :x) 0),
  :vlast (nth (ds/column large-ds :x) 999999)})


(deftest
 t37_l175
 (is
  ((fn
    [{:keys [rows cols v0 vlast]}]
    (and (= 1000000 rows) (= 1 cols) (= 0.0 v0) (= 999999.0 vlast)))
   v36_l170)))


(def
 v39_l182
 (try
  (dataset/make-dataset {})
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t40_l185 (is ((fn [r] (= :threw r)) v39_l182)))


(def
 v41_l188
 (try
  (dataset/make-dataset
   {:x (double-array [1 2 3]), :y (double-array [1 2])})
  :no-error
  (catch clojure.lang.ExceptionInfo _ :threw)))


(deftest t42_l191 (is ((fn [r] (= :threw r)) v41_l188)))


(def
 v44_l203
 (def
  tds
  (ds/->dataset
   {:x (double-array [1.0 2.0 3.0 4.0 5.0]),
    :y (long-array [10 20 30 40 50])})))


(def v45_l207 (q/q {:from tds, :agg [[:sum :x] [:sum :y]]}))


(deftest
 t46_l209
 (is
  ((fn
    [result]
    (and
     (= 1 (count result))
     (= 15.0 (:sum_x (first result)))
     (= 150.0 (:sum_y (first result)))))
   v45_l207)))


(def
 v48_l217
 (q/q {:from tds, :where [[:> :x 2.0]], :agg [[:sum :y] [:count]]}))


(deftest
 t49_l221
 (is
  ((fn
    [result]
    (and
     (= 3 (:count (first result)))
     (= 120.0 (:sum (first result)))))
   v48_l217)))


(def
 v51_l228
 (def
  cat-tds
  (ds/->dataset
   {:category (into-array String ["A" "B" "A" "B" "A"]),
    :value (double-array [10.0 20.0 15.0 25.0 12.0])})))


(def
 v52_l232
 (let
  [result
   (q/q
    {:from cat-tds, :group [:category], :agg [[:sum :value] [:count]]})
   by-cat
   (into {} (map (fn [r] [(:category r) r])) result)]
  {:a-sum (:sum (get by-cat "A")),
   :a-count (:count (get by-cat "A")),
   :b-sum (:sum (get by-cat "B")),
   :b-count (:count (get by-cat "B"))}))


(deftest
 t53_l239
 (is
  ((fn
    [{:keys [a-sum a-count b-sum b-count]}]
    (and (= 37.0 a-sum) (= 3 a-count) (= 45.0 b-sum) (= 2 b-count)))
   v52_l232)))


(def
 v55_l246
 {:column-names (dataset/column-names tds),
  :row-count (dataset/row-count tds),
  :x-type (dataset/column-type tds :x),
  :y-type (dataset/column-type tds :y)})


(deftest
 t56_l251
 (is
  ((fn
    [{:keys [column-names row-count x-type y-type]}]
    (and
     (= [:x :y] column-names)
     (= 5 row-count)
     (= :float64 x-type)
     (= :int64 y-type)))
   v55_l246)))


(def
 v58_l260
 (let
  [cols (dataset/columns tds)]
  {:x-type (:type (get cols :x)),
   :x-class (class (:data (get cols :x))),
   :y-class (class (:data (get cols :y)))}))


(deftest
 t59_l265
 (is
  ((fn
    [{:keys [x-type x-class y-class]}]
    (and
     (= :float64 x-type)
     (= (Class/forName "[D") x-class)
     (= (Class/forName "[J") y-class)))
   v58_l260)))


(def
 v61_l278
 (def
  idx-trades
  (dataset/make-dataset
   {:price (idx/index-from-seq :float64 [100.0 200.0 300.0 400.0]),
    :qty (idx/index-from-seq :int64 [10 20 30 40])})))


(def
 v62_l283
 (q/q {:from idx-trades, :agg [[:sum :price] [:sum :qty]]}))


(deftest
 t63_l285
 (is
  ((fn
    [result]
    (and
     (= 1000.0 (:sum_price (first result)))
     (= 100.0 (:sum_qty (first result)))))
   v62_l283)))


(def
 v64_l290
 (q/q
  {:from idx-trades,
   :where [[:>= :price 200.0]],
   :agg [[:sum :price] [:count]]}))


(deftest
 t65_l294
 (is
  ((fn
    [result]
    (and
     (= 3 (:count (first result)))
     (= 900.0 (:sum (first result)))))
   v64_l290)))
