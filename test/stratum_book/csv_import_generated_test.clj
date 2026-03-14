(ns
 stratum-book.csv-import-generated-test
 (:require
  [stratum.csv :as csv]
  [stratum.query :as q]
  [stratum.dataset :as dataset]
  [scicloj.kindly.v4.kind :as kind]
  [clojure.test :refer [deftest is]]))


(def
 v3_l24
 (defn
  write-temp-csv
  [content]
  (let
   [f (java.io.File/createTempFile "stratum-book" ".csv")]
   (.deleteOnExit f)
   (spit f content)
   (.getAbsolutePath f))))


(def
 v5_l37
 (def
  basic-ds
  (csv/from-csv
   (write-temp-csv
    "name,age,score\nAlice,30,95.5\nBob,25,87.3\nCarol,35,92.1"))))


(def v6_l41 (dataset/row-count basic-ds))


(deftest t7_l43 (is ((fn [n] (= 3 n)) v6_l41)))


(def v9_l48 (set (dataset/column-names basic-ds)))


(deftest
 t10_l50
 (is ((fn [names] (= #{:age :name :score} names)) v9_l48)))


(def v12_l58 (dataset/column-type basic-ds :age))


(deftest t13_l60 (is ((fn [t] (= :int64 t)) v12_l58)))


(def v14_l63 (dataset/column-type basic-ds :score))


(deftest t15_l65 (is ((fn [t] (= :float64 t)) v14_l63)))


(def v16_l68 (some? (:dict (dataset/column basic-ds :name))))


(deftest t17_l70 (is (true? v16_l68)))


(def
 v19_l80
 (def
  override-ds
  (csv/from-csv
   (write-temp-csv "id,value\n1,100\n2,200\n3,300")
   :types
   {"value" :double})))


(def v20_l85 (dataset/column-type override-ds :value))


(deftest t21_l87 (is ((fn [t] (= :float64 t)) v20_l85)))


(def
 v23_l96
 (def
  tsv-ds
  (csv/from-csv (write-temp-csv "a\tb\n1\t2\n3\t4") :separator \tab)))


(def v24_l101 (count (dataset/column-names tsv-ds)))


(deftest t25_l103 (is ((fn [n] (= 2 n)) v24_l101)))


(def v26_l106 (dataset/column-type tsv-ds :a))


(deftest t27_l108 (is ((fn [t] (= :int64 t)) v26_l106)))


(def
 v29_l117
 (def
  no-header-ds
  (csv/from-csv
   (write-temp-csv "1,hello\n2,world\n3,test")
   :header?
   false)))


(def v30_l122 (set (dataset/column-names no-header-ds)))


(deftest
 t31_l124
 (is
  ((fn [names] (and (contains? names :col0) (contains? names :col1)))
   v30_l122)))


(def
 v33_l135
 (def
  limited-ds
  (csv/from-csv (write-temp-csv "x\n1\n2\n3\n4\n5") :limit 3)))


(def v34_l140 (dataset/row-count limited-ds))


(deftest t35_l142 (is ((fn [n] (= 3 n)) v34_l140)))


(def
 v37_l151
 (def
  products-ds
  (csv/from-csv
   (write-temp-csv
    "product,price,qty\nWidget,10.50,5\nGadget,25.00,3\nWidget,10.50,2"))))


(def
 v38_l155
 (def
  by-product
  (->>
   (q/q {:from products-ds, :group [:product], :agg [[:sum :qty]]})
   (into {} (map (fn [r] [(:product r) (:sum r)]))))))


(def v39_l159 by-product)


(deftest
 t40_l161
 (is
  ((fn [m] (and (= 7.0 (get m "Widget")) (= 3.0 (get m "Gadget"))))
   v39_l159)))


(def
 v42_l175
 (def
  maps-ds
  (csv/from-maps [{:a 1, :b "x"} {:a 2, :b "y"} {:a 3, :b "x"}])))


(def v43_l177 (dataset/column-type maps-ds :a))


(deftest t44_l179 (is ((fn [t] (= :int64 t)) v43_l177)))


(def v46_l184 (some? (:dict (dataset/column maps-ds :b))))


(deftest t47_l186 (is (true? v46_l184)))


(def
 v49_l191
 (dataset/column-type (csv/from-maps [{:v 1.5} {:v 2.5}]) :v))


(deftest t50_l195 (is ((fn [t] (= :float64 t)) v49_l191)))


(def
 v52_l200
 (dataset/row-count (csv/from-maps [{:x 1, :y nil} {:x 2, :y 3}])))


(deftest t53_l203 (is ((fn [n] (= 2 n)) v52_l200)))


(def
 v55_l220
 (def nullds (csv/from-csv (write-temp-csv "a,b,c\n1,3,3\n4,\"\",6"))))
