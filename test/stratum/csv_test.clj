(ns stratum.csv-test
  "Tests for CSV import."
  (:require [clojure.test :refer [deftest testing is]]
            [stratum.csv :as csv]
            [stratum.query :as q]
            [stratum.dataset :as dataset]
            [clojure.java.io :as io]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Helper: write temp CSV files
;; ============================================================================

(defn- write-temp-csv
  "Write a CSV string to a temp file, return the path."
  [content]
  (let [f (java.io.File/createTempFile "stratum-test" ".csv")]
    (.deleteOnExit f)
    (spit f content)
    (.getAbsolutePath f)))

;; ============================================================================
;; from-csv Tests
;; ============================================================================

(deftest from-csv-basic-test
  (testing "Read CSV with header and auto-detect types"
    (let [path (write-temp-csv "name,age,score\nAlice,30,95.5\nBob,25,87.3\nCarol,35,92.1")
          ds (csv/from-csv path)]
      ;; Dataset should have 3 rows and 3 columns
      (is (= 3 (dataset/row-count ds)))
      (is (= 3 (count (dataset/column-names ds))))
      ;; Check columns exist
      (is (contains? (set (dataset/column-names ds)) :name))
      (is (contains? (set (dataset/column-names ds)) :age))
      (is (contains? (set (dataset/column-names ds)) :score))
      ;; age should be long
      (is (= :int64 (dataset/column-type ds :age)))
      ;; score should be double
      (is (= :float64 (dataset/column-type ds :score)))
      ;; name should be string (dict-encoded → :int64 with :dict)
      (is (some? (:dict (dataset/column ds :name)))))))

(deftest from-csv-types-override-test
  (testing "Override type detection"
    (let [path (write-temp-csv "id,value\n1,100\n2,200\n3,300")
          ds (csv/from-csv path :types {"value" :double})]
      (is (= :float64 (dataset/column-type ds :value))))))

(deftest from-csv-separator-test
  (testing "Tab-separated values"
    (let [path (write-temp-csv "a\tb\n1\t2\n3\t4")
          ds (csv/from-csv path :separator \tab)]
      (is (= 2 (count (dataset/column-names ds))))
      (is (= :int64 (dataset/column-type ds :a))))))

(deftest from-csv-no-header-test
  (testing "CSV without header"
    (let [path (write-temp-csv "1,hello\n2,world\n3,test")
          ds (csv/from-csv path :header? false)]
      (is (contains? (set (dataset/column-names ds)) :col0))
      (is (contains? (set (dataset/column-names ds)) :col1)))))

(deftest from-csv-limit-test
  (testing "Limit rows"
    (let [path (write-temp-csv "x\n1\n2\n3\n4\n5")
          ds (csv/from-csv path :limit 3)]
      (is (= 3 (dataset/row-count ds)))
      (is (= 3 (alength ^longs (:data (dataset/column ds :x))))))))

(deftest from-csv-query-integration-test
  (testing "Query over CSV-imported data"
    (let [path (write-temp-csv "product,price,qty\nWidget,10.50,5\nGadget,25.00,3\nWidget,10.50,2")
          ds (csv/from-csv path)
          result (q/q {:from ds
                       :group [:product]
                       :agg [[:sum :qty]]})]
      (is (= 2 (count result)))
      (let [by-prod (into {} (map (fn [r] [(:product r) (:sum r)])) result)]
        (is (= 7.0 (get by-prod "Widget")))
        (is (= 3.0 (get by-prod "Gadget")))))))

;; ============================================================================
;; from-maps Tests
;; ============================================================================

(deftest from-maps-test
  (testing "Basic map conversion"
    (let [ds (csv/from-maps [{:a 1 :b "x"} {:a 2 :b "y"} {:a 3 :b "x"}])]
      (is (= :int64 (dataset/column-type ds :a)))
      (is (some? (:dict (dataset/column ds :b))))))

  (testing "Double values"
    (let [ds (csv/from-maps [{:v 1.5} {:v 2.5}])]
      (is (= :float64 (dataset/column-type ds :v)))))

  (testing "Nil values handled"
    (let [ds (csv/from-maps [{:x 1 :y nil} {:x 2 :y 3}])]
      (is (= 2 (dataset/row-count ds)))
      (is (= 2 (alength ^longs (:data (dataset/column ds :x))))))))

;; ============================================================================
;; PostgreSQL NULL Semantics Tests
;; ============================================================================

(deftest csv-null-semantics-test
  (testing "Unquoted empty → NULL, quoted empty → empty string"
    ;; CSV: a,b,c / 1,,3 / 4,"",6
    ;; Row 1: b is unquoted empty → NULL
    ;; Row 2: b is quoted empty → ""
    (let [path (write-temp-csv "a,b,c\n1,,3\n4,\"\",6")
          ds (csv/from-csv path)]
      (is (= 2 (dataset/row-count ds)))
      ;; a and c should be long (auto-detected from "1","4" and "3","6")
      (is (= :int64 (dataset/column-type ds :a)))
      (is (= :int64 (dataset/column-type ds :c)))
      ;; b is string (dict-encoded → :int64 with :dict)
      (let [b-col (dataset/column ds :b)
            dict ^"[Ljava.lang.String;" (:dict b-col)
            data ^longs (:data b-col)]
        (is (some? dict))
        ;; Row 0 (unquoted empty): Long.MIN_VALUE sentinel (NULL)
        ;; Row 1 (quoted empty): "" in dict
        (let [raw0 (aget data 0)
              v1 (aget dict (int (aget data 1)))]
          (is (= raw0 Long/MIN_VALUE) "Unquoted empty should be Long.MIN_VALUE sentinel")
          (is (= "" v1) "Quoted empty should be empty string")))))

  (testing "Unquoted empty numeric → NULL sentinel"
    (let [path (write-temp-csv "x,y\n1,10.5\n,\n3,30.5")
          ds (csv/from-csv path)]
      (is (= 3 (dataset/row-count ds)))
      ;; Row 1 has unquoted empty for both x and y → NULL sentinels
      (let [x ^longs (:data (dataset/column ds :x))
            y ^doubles (:data (dataset/column ds :y))]
        (is (= 1 (aget x 0)))
        (is (= Long/MIN_VALUE (aget x 1)) "Unquoted empty long → Long/MIN_VALUE")
        (is (= 3 (aget x 2)))
        (is (= 10.5 (aget y 0)))
        (is (Double/isNaN (aget y 1)) "Unquoted empty double → NaN")
        (is (= 30.5 (aget y 2)))))))

(deftest read-csv-direct-test
  (testing "read-csv returns nil for unquoted empty, \"\" for quoted empty"
    (let [rows (vec (csv/read-csv "a,b,c\n1,,3\n4,\"\",6"))]
      (is (= 3 (count rows)))
      ;; Header row — all quoted or non-empty
      (is (= ["a" "b" "c"] (first rows)))
      ;; Row 1: b is unquoted empty → nil
      (is (= ["1" nil "3"] (second rows)))
      ;; Row 2: b is quoted empty → ""
      (is (= ["4" "" "6"] (nth rows 2)))))

  (testing "Trailing comma → trailing nil"
    (let [rows (vec (csv/read-csv "a,b\n1,\n2,3"))]
      (is (= [nil] (subvec (second rows) 1)))
      (is (= "3" (nth (nth rows 2) 1)))))

  (testing "All empty unquoted → all nil"
    (let [rows (vec (csv/read-csv "a,b,c\n,,"))]
      (is (= [nil nil nil] (second rows)))))

  (testing "Quoted field with comma preserved"
    (let [rows (vec (csv/read-csv "a,b\n\"hello,world\",2"))]
      (is (= "hello,world" (first (second rows)))))))
