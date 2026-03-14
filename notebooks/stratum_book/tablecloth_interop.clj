;; # Tablecloth Interop
;;
;; Stratum datasets implement the
;; [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset)
;; protocols, so they work directly with
;; [tablecloth](https://github.com/scicloj/tablecloth) — no conversion
;; step needed. The interop is **bidirectional**: Stratum can also
;; query `tech.ml.dataset` datasets with zero copy.

(ns stratum-book.tablecloth-interop
  (:require
   [stratum.dataset :as dataset]
   [stratum.tablecloth]  ;; load protocol extensions
   [stratum.index :as idx]
   [stratum.query :as q]
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.protocols :as ds-proto]
   [scicloj.kindly.v4.kind :as kind]))

;; ---
;;
;; ## StratumDataset → tech.ml.dataset
;;
;; A `StratumDataset` satisfies `PDataset`, `PMissing`,
;; `PSelectColumns`, and `PSelectRows` — so `ds/*` functions work
;; out of the box.
;;
;; ### Basic protocol compliance

(def trades
  (dataset/make-dataset
   {:price (double-array [100.0 200.0 300.0])
    :qty   (long-array [10 20 30])}
   {:name "trades"}))

{:dataset?     (ds-proto/is-dataset? trades)
 :row-count    (ds-proto/row-count trades)
 :column-count (ds-proto/column-count trades)
 :name         (dataset/ds-name trades)}

(kind/test-last
 [(fn [{:keys [dataset? row-count column-count name]}]
    (and dataset? (= 3 row-count) (= 2 column-count) (= "trades" name)))])

;; ### Column access
;;
;; `ds/column` returns a proper `Column` object supporting `nth`
;; and `count`.

(let [col (ds/column trades :price)]
  {:count (count col)
   :v0    (nth col 0)
   :v2    (nth col 2)})

(kind/test-last
 [(fn [{:keys [count v0 v2]}]
    (and (= 3 count) (= 100.0 v0) (= 300.0 v2)))])

;; Missing column throws:

(try (ds-proto/column trades :nonexistent) :no-error
     (catch RuntimeException _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ### Index-backed datasets

(def idx-ds
  (dataset/make-dataset
   {:price (idx/index-from-seq :float64 [100.0 200.0 300.0])
    :qty   (idx/index-from-seq :int64 [10 20 30])}))

{:row-count    (ds/row-count idx-ds)
 :column-count (ds/column-count idx-ds)
 :has-price?   (dataset/has-index? idx-ds :price)
 :has-qty?     (dataset/has-index? idx-ds :qty)}

(kind/test-last
 [(fn [{:keys [row-count column-count has-price? has-qty?]}]
    (and (= 3 row-count) (= 2 column-count) has-price? has-qty?))])

;; ### Rows and row vectors
;;
;; `rows` returns maps, `rowvecs` returns vectors.

(let [rows    (ds-proto/rows trades {})
      rowvecs (ds-proto/rowvecs trades {})]
  {:row0 (.readObject rows 0)
   :vec0 (.readObject rowvecs 0)})

(kind/test-last
 [(fn [{:keys [row0 vec0]}]
    (and (map? row0)
         (= 100.0 (:price row0))
         (vector? vec0)))])

;; ### Select columns

(def selected-cols (ds/select-columns trades [:price]))

{:type     (type selected-cols)
 :col-ct   (ds/column-count selected-cols)
 :columns  (set (dataset/column-names selected-cols))}

(kind/test-last
 [(fn [{:keys [type col-ct columns]}]
    (and (= stratum.dataset.StratumDataset type)
         (= 1 col-ct)
         (= #{:price} columns)))])

;; ### Select rows

(def selected-rows
  (ds/select-rows
   (dataset/make-dataset
    {:x (double-array [1 2 3 4 5])
     :y (long-array [10 20 30 40 50])})
   [0 2 4]))

{:type     (type selected-rows)
 :row-ct   (ds/row-count selected-rows)
 :x-vals   (vec (:data (dataset/column selected-rows :x)))
 :y-vals   (vec (:data (dataset/column selected-rows :y)))}

(kind/test-last
 [(fn [{:keys [type row-ct x-vals y-vals]}]
    (and (= stratum.dataset.StratumDataset type)
         (= 3 row-ct)
         (= [1.0 3.0 5.0] x-vals)
         (= [10 30 50] y-vals)))])

;; ### Missing values
;;
;; `NaN` (float64) and `Long/MIN_VALUE` (int64) are treated as
;; missing-value sentinels. The `PMissing` protocol reports which
;; rows have missing values.

(def missing-ds
  (dataset/make-dataset
   {:x (double-array [1.0 Double/NaN 3.0])
    :y (long-array [10 Long/MIN_VALUE 30])}))

(let [bm (ds-proto/missing missing-ds)]
  {:cardinality (.getCardinality bm)
   :row-1?     (.contains bm 1)
   :row-0?     (.contains bm 0)})

(kind/test-last
 [(fn [{:keys [cardinality row-1? row-0?]}]
    (and (= 1 cardinality) row-1? (not row-0?)))])

;; Column-level access converts `NaN` to `nil`:

(let [col (ds/column missing-ds :x)]
  [(nth col 0) (nth col 1) (nth col 2)])

(kind/test-last
 [(fn [[v0 v1 v2]]
    (and (= 1.0 v0) (nil? v1) (= 3.0 v2)))])

;; ### Large dataset — no materialization
;;
;; A 1M-row index-backed dataset can be introspected without
;; materializing all values.

(def large-ds
  (dataset/make-dataset {:x (idx/index-from-seq :float64 (range 1000000))}))

{:rows (ds/row-count large-ds)
 :cols (ds/column-count large-ds)
 :v0   (nth (ds/column large-ds :x) 0)
 :vlast (nth (ds/column large-ds :x) 999999)}

(kind/test-last
 [(fn [{:keys [rows cols v0 vlast]}]
    (and (= 1000000 rows) (= 1 cols)
         (= 0.0 v0) (= 999999.0 vlast)))])

;; ### Error handling

(try (dataset/make-dataset {}) :no-error
     (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

(try (dataset/make-dataset {:x (double-array [1 2 3]) :y (double-array [1 2])}) :no-error
     (catch clojure.lang.ExceptionInfo _ :threw))

(kind/test-last
 [(fn [r] (= :threw r))])

;; ---
;;
;; ## tech.ml.dataset → Stratum Query Engine
;;
;; The reverse direction: pass a `tech.ml.Dataset` directly to
;; `q/q`. Stratum reads its columns with zero copy.

;; ### Basic aggregation

(def tds
  (ds/->dataset {:x (double-array [1.0 2.0 3.0 4.0 5.0])
                 :y (long-array [10 20 30 40 50])}))

(q/q {:from tds :agg [[:sum :x] [:sum :y]]})

(kind/test-last
 [(fn [result]
    (and (= 1 (count result))
         (= 15.0 (:sum_x (first result)))
         (= 150.0 (:sum_y (first result)))))])

;; ### Filtered aggregation

(q/q {:from tds
      :where [[:> :x 2.0]]
      :agg [[:sum :y] [:count]]})

(kind/test-last
 [(fn [result]
    (and (= 3 (:count (first result)))
         (= 120.0 (:sum (first result)))))])

;; ### GROUP BY

(def cat-tds
  (ds/->dataset {:category (into-array String ["A" "B" "A" "B" "A"])
                 :value    (double-array [10.0 20.0 15.0 25.0 12.0])}))

(let [result (q/q {:from cat-tds :group [:category] :agg [[:sum :value] [:count]]})
      by-cat (into {} (map (fn [r] [(:category r) r])) result)]
  {:a-sum   (:sum (get by-cat "A"))
   :a-count (:count (get by-cat "A"))
   :b-sum   (:sum (get by-cat "B"))
   :b-count (:count (get by-cat "B"))})

(kind/test-last
 [(fn [{:keys [a-sum a-count b-sum b-count]}]
    (and (= 37.0 a-sum) (= 3 a-count)
         (= 45.0 b-sum) (= 2 b-count)))])

;; ### IDataset protocol on tech.ml.Dataset

{:column-names (dataset/column-names tds)
 :row-count    (dataset/row-count tds)
 :x-type       (dataset/column-type tds :x)
 :y-type       (dataset/column-type tds :y)}

(kind/test-last
 [(fn [{:keys [column-names row-count x-type y-type]}]
    (and (= [:x :y] column-names)
         (= 5 row-count)
         (= :float64 x-type)
         (= :int64 y-type)))])

;; Underlying arrays are preserved (zero copy):

(let [cols (dataset/columns tds)]
  {:x-type  (:type (get cols :x))
   :x-class (class (:data (get cols :x)))
   :y-class (class (:data (get cols :y)))})

(kind/test-last
 [(fn [{:keys [x-type x-class y-class]}]
    (and (= :float64 x-type)
         (= (Class/forName "[D") x-class)
         (= (Class/forName "[J") y-class)))])

;; ---
;;
;; ## Stratum → Query → Stratum
;;
;; StratumDataset works in the query engine with zero-copy index
;; preservation.

(def idx-trades
  (dataset/make-dataset
   {:price (idx/index-from-seq :float64 [100.0 200.0 300.0 400.0])
    :qty   (idx/index-from-seq :int64 [10 20 30 40])}))

(q/q {:from idx-trades :agg [[:sum :price] [:sum :qty]]})

(kind/test-last
 [(fn [result]
    (and (= 1000.0 (:sum_price (first result)))
         (= 100.0 (:sum_qty (first result)))))])

(q/q {:from idx-trades
      :where [[:>= :price 200.0]]
      :agg [[:sum :price] [:count]]})

(kind/test-last
 [(fn [result]
    (and (= 3 (:count (first result)))
         (= 900.0 (:sum (first result)))))])
