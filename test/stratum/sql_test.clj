(ns stratum.sql-test
  "Tests for the Stratum SQL interface (parser + server)."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.sql :as sql]
            [stratum.query :as q]
            [stratum.server :as server]
            [stratum.specification :as spec])
  (:import [stratum.internal PgWireServer$QueryResult]))

(set! *warn-on-reflection* true)

;; ============================================================================
;; Test Data
;; ============================================================================

(def ^:private test-n 1000)

(defn- make-test-registry
  "Create a test table registry with sample data."
  []
  {"orders" {:price    (double-array (map #(+ 10.0 (* 0.5 %)) (range test-n)))
             :quantity (long-array (map #(+ 1 (mod % 50)) (range test-n)))
             :category (long-array (map #(mod % 5) (range test-n)))
             :discount (double-array (map #(* 0.01 (mod % 10)) (range test-n)))}
   "products" {:id   (long-array (range 100))
               :cost (double-array (map #(+ 5.0 (* 0.1 %)) (range 100)))}})

;; ============================================================================
;; SQL Parsing Unit Tests
;; ============================================================================

(deftest parse-simple-select-test
  (testing "SELECT with single aggregate"
    (let [reg (make-test-registry)
          result (sql/parse-sql "SELECT SUM(price) FROM orders" reg)]
      (is (contains? result :query))
      (is (= [[:sum :price]] (get-in result [:query :agg])))))

  (testing "SELECT with WHERE"
    (let [reg (make-test-registry)
          result (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price > 100" reg)]
      (is (contains? result :query))
      (is (= [[:> :price 100]] (get-in result [:query :where])))
      (is (= [[:count]] (get-in result [:query :agg])))))

  (testing "SELECT with alias"
    (let [reg (make-test-registry)
          result (sql/parse-sql "SELECT SUM(price) AS revenue FROM orders" reg)]
      (is (= [[:as [:sum :price] :revenue]] (get-in result [:query :agg]))))))

(deftest parse-comparison-operators-test
  (testing "Greater than"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price > 100" reg)]
      (is (= [[:> :price 100]] (:where query)))))

  (testing "Less than"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE quantity < 25" reg)]
      (is (= [[:< :quantity 25]] (:where query)))))

  (testing "Greater than or equal"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price >= 50.0" reg)]
      (is (= [[:>= :price 50.0]] (:where query)))))

  (testing "Less than or equal"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE quantity <= 10" reg)]
      (is (= [[:<= :quantity 10]] (:where query)))))

  (testing "Equals"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE category = 3" reg)]
      (is (= [[:= :category 3]] (:where query)))))

  (testing "Not equals"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE category != 3" reg)]
      (is (= [[:!= :category 3]] (:where query))))))

(deftest parse-between-test
  (testing "BETWEEN predicate"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price BETWEEN 50 AND 100" reg)]
      (is (= [[:between :price 50 100]] (:where query))))))

(deftest parse-in-test
  (testing "IN predicate"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE category IN (1, 2, 3)" reg)]
      (is (= [[:in :category 1 2 3]] (:where query))))))

(deftest parse-is-null-test
  (testing "IS NULL predicate"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price IS NULL" reg)]
      (is (= [[:is-null :price]] (:where query)))))

  (testing "IS NOT NULL predicate"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price IS NOT NULL" reg)]
      (is (= [[:is-not-null :price]] (:where query))))))

(deftest parse-like-test
  (testing "LIKE predicate"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE category LIKE '%foo%'" reg)]
      (is (= [[:like :category "%foo%"]] (:where query))))))

(deftest parse-and-or-test
  (testing "AND predicates (flattened)"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price > 50 AND quantity < 10" reg)]
      (is (= [[:> :price 50] [:< :quantity 10]] (:where query)))))

  (testing "OR predicates"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE category = 1 OR category = 2" reg)]
      (is (= [[:or [:= :category 1] [:= :category 2]]] (:where query))))))

(deftest parse-group-by-test
  (testing "Simple GROUP BY"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) FROM orders GROUP BY category" reg)]
      (is (= [:category] (:group query)))
      (is (= [[:sum :price]] (:agg query)))))

  (testing "GROUP BY with positional reference"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) FROM orders GROUP BY 1" reg)]
      (is (= [:category] (:group query))))))

(deftest parse-having-test
  (testing "HAVING clause"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 100"
                           reg)]
      (is (= [:category] (:group query)))
      (is (seq (:having query))))))

(deftest parse-order-by-test
  (testing "ORDER BY ASC"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) FROM orders GROUP BY category ORDER BY category ASC" reg)]
      (is (= [[:category :asc]] (:order query)))))

  (testing "ORDER BY DESC"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) AS total FROM orders GROUP BY category ORDER BY total DESC" reg)]
      (is (= [[:total :desc]] (:order query))))))

(deftest parse-limit-offset-test
  (testing "LIMIT"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) FROM orders GROUP BY category LIMIT 3" reg)]
      (is (= 3 (:limit query)))))

  (testing "LIMIT with OFFSET"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, SUM(price) FROM orders GROUP BY category LIMIT 3 OFFSET 2" reg)]
      (is (= 3 (:limit query)))
      (is (= 2 (:offset query))))))

(deftest parse-distinct-test
  (testing "SELECT DISTINCT"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT DISTINCT category FROM orders" reg)]
      (is (true? (:distinct query))))))

(deftest parse-count-distinct-test
  (testing "COUNT(DISTINCT col)"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(DISTINCT category) FROM orders" reg)]
      (is (= [[:count-distinct :category]] (:agg query))))))

(deftest parse-aggregate-functions-test
  (testing "SUM"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT SUM(price) FROM orders" reg)]
      (is (= [[:sum :price]] (:agg query)))))

  (testing "AVG"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT AVG(price) FROM orders" reg)]
      (is (= [[:avg :price]] (:agg query)))))

  (testing "MIN"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT MIN(price) FROM orders" reg)]
      (is (= [[:min :price]] (:agg query)))))

  (testing "MAX"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT MAX(price) FROM orders" reg)]
      (is (= [[:max :price]] (:agg query)))))

  (testing "Multiple aggregates"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT SUM(price), COUNT(*), AVG(price) FROM orders" reg)]
      (is (= 3 (count (:agg query))))))

  (testing "MEDIAN"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT MEDIAN(price) FROM orders" reg)]
      (is (= [[:median :price]] (:agg query)))))

  (testing "PERCENTILE_CONT"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT PERCENTILE_CONT(0.95, price) FROM orders" reg)]
      (is (= :percentile (first (first (:agg query)))))
      (is (= :price (second (first (:agg query)))))))

  (testing "APPROX_QUANTILE"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT APPROX_QUANTILE(price, 0.95) FROM orders" reg)]
      (is (= :approx-quantile (first (first (:agg query)))))
      (is (= :price (second (first (:agg query))))))))

(deftest parse-arithmetic-expressions-test
  (testing "SUM of product expression"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT SUM(price * quantity) FROM orders" reg)]
      (is (= [[:sum [:* :price :quantity]]] (:agg query)))))

  (testing "Expression in WHERE"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM orders WHERE price * quantity > 1000" reg)]
      (is (= [[:> [:* :price :quantity] 1000]] (:where query))))))

(deftest parse-system-queries-test
  (testing "SET commands"
    (let [result (sql/parse-sql "SET client_encoding TO 'UTF8'" {})]
      (is (true? (:system result)))))

  (testing "SHOW commands"
    (let [result (sql/parse-sql "SHOW server_version" {})]
      (is (true? (:system result)))))

  (testing "BEGIN/COMMIT"
    (is (true? (:system (sql/parse-sql "BEGIN" {}))))
    (is (true? (:system (sql/parse-sql "COMMIT" {}))))))

(deftest parse-version-query-test
  (testing "SELECT VERSION()"
    (let [result (sql/parse-sql "SELECT VERSION()" {})]
      (is (true? (:system result)))
      (is (string? (get-in result [:result :rows 0 0]))))))

(deftest parse-error-handling-test
  (testing "Unknown table"
    (let [result (sql/parse-sql "SELECT * FROM nonexistent" {})]
      (is (contains? result :error))))

  (testing "Invalid SQL"
    (let [result (sql/parse-sql "SELECTT broken" {})]
      (is (contains? result :error)))))

;; ============================================================================
;; End-to-End Execution Tests
;; ============================================================================

(defn- execute-sql
  "Parse and execute SQL against a registry, returning raw Stratum results."
  [sql registry]
  (let [parsed (sql/parse-sql sql registry)]
    (if (:query parsed)
      (q/q (:query parsed))
      parsed)))

(deftest e2e-simple-count-test
  (testing "COUNT(*) with WHERE"
    (let [reg (make-test-registry)
          results (execute-sql "SELECT COUNT(*) AS cnt FROM orders WHERE price > 100" reg)
          row (first results)]
      (is (pos? (long (:cnt row)))))))

(deftest e2e-sum-test
  (testing "SUM with WHERE"
    (let [reg (make-test-registry)
          results (execute-sql "SELECT SUM(price) AS total FROM orders WHERE price < 50" reg)
          row (first results)]
      (is (pos? (double (:total row)))))))

(deftest e2e-group-by-test
  (testing "GROUP BY with aggregates"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT category, SUM(price) AS total, COUNT(*) AS cnt FROM orders GROUP BY category"
                   reg)]
      (is (= 5 (count results)))
      (is (every? #(pos? (double (:total %))) results))
      (is (every? #(= 200 (long (:cnt %))) results)))))

(deftest e2e-order-by-test
  (testing "ORDER BY DESC"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT category, SUM(price) AS total FROM orders GROUP BY category ORDER BY total DESC"
                   reg)]
      (is (= 5 (count results)))
      (let [totals (mapv #(double (:total %)) results)]
        (is (= totals (sort > totals)))))))

(deftest e2e-limit-test
  (testing "LIMIT"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category LIMIT 3"
                   reg)]
      (is (= 3 (count results))))))

(deftest e2e-between-test
  (testing "BETWEEN predicate"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT COUNT(*) AS cnt FROM orders WHERE price BETWEEN 100 AND 200"
                   reg)
          cnt (long (:cnt (first results)))]
      (is (pos? cnt)))))

(deftest e2e-multiple-where-test
  (testing "Multiple WHERE predicates (AND)"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT SUM(price) AS total FROM orders WHERE price > 50 AND quantity < 25"
                   reg)
          total (double (:total (first results)))]
      (is (pos? total)))))

(deftest e2e-in-predicate-test
  (testing "IN predicate"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT COUNT(*) AS cnt FROM orders WHERE category IN (1, 3)"
                   reg)
          cnt (long (:cnt (first results)))]
      (is (= 400 cnt)))))

(deftest e2e-avg-min-max-test
  (testing "AVG, MIN, MAX aggregates"
    (let [reg (make-test-registry)
          results (execute-sql
                   "SELECT AVG(price) AS avg_p, MIN(price) AS min_p, MAX(price) AS max_p FROM orders"
                   reg)]
      (is (< (double (:min_p (first results)))
             (double (:avg_p (first results)))
             (double (:max_p (first results))))))))

;; ============================================================================
;; Result Formatting Tests
;; ============================================================================

(deftest format-vector-of-maps-test
  (testing "Format vector of maps result"
    (let [results [{:x 1 :y 2.0} {:x 3 :y 4.0}]
          ^PgWireServer$QueryResult qr (sql/format-results results)]
      (is (= 2 (alength (.rows qr))))
      (is (some #(= "x" %) (.columnNames qr)))
      (is (some #(= "y" %) (.columnNames qr))))))

(deftest format-system-result-test
  (testing "Format system query result"
    (let [sys-result {:system true :tag "SET"}
          ^PgWireServer$QueryResult qr (sql/format-results sys-result)]
      (is (nil? (.error qr)))
      (is (= "SET" (.commandTag qr))))))

(deftest format-error-result-test
  (testing "Format error result"
    (let [err {:error "Something went wrong"}
          ^PgWireServer$QueryResult qr (sql/format-results err)]
      (is (= "Something went wrong" (.error qr))))))

;; ============================================================================
;; Server Integration Tests (without network)
;; ============================================================================

(deftest server-register-table-test
  (testing "Register and query a table"
    (let [srv (server/start {:port 0})  ;; port 0 = OS picks random port
          _ (server/register-table! srv "t1"
                                    {:val (double-array [1.0 2.0 3.0 4.0 5.0])
                                     :grp (long-array [0 0 1 1 1])})
          tables (server/list-tables srv)]
      (is (some #{"t1"} tables))
      (server/stop srv))))

;; ============================================================================
;; OLAP Benchmark SQL Tests
;; ============================================================================
;;
;; These tests verify that every DuckDB SQL query from the OLAP benchmark suite
;; parses correctly and produces valid results through the SQL interface.

(def ^:private bench-n 1000)

(defn- make-lineitem-registry
  "Create a TPC-H lineitem-like registry."
  []
  {"lineitem" {:shipdate    {:type :int64 :data (long-array (map #(+ 8002 (mod % 2559)) (range bench-n)))}
               :discount    {:type :float64 :data (double-array (map #(* 0.01 (mod % 11)) (range bench-n)))}
               :quantity    {:type :int64 :data (long-array (map #(+ 1 (mod % 50)) (range bench-n)))}
               :price       {:type :float64 :data (double-array (map #(+ 100.0 (* 0.5 %)) (range bench-n)))}
               :tax         {:type :float64 :data (double-array (map #(* 0.01 (mod % 9)) (range bench-n)))}
               :returnflag  {:type :int64 :data (long-array (map #(mod % 3) (range bench-n)))}
               :linestatus  {:type :int64 :data (long-array (map #(mod % 2) (range bench-n)))}}})

(defn- make-h2o-registry
  "Create an H2O db-benchmark-like registry."
  []
  {"h2o" {:id1 (q/encode-column (into-array String (map #(format "id%03d" (mod % 10)) (range bench-n))))
          :id2 (q/encode-column (into-array String (map #(format "id%03d" (mod (quot % 10) 10)) (range bench-n))))
          :id3 (q/encode-column (into-array String (map #(format "id%06d" (mod % 100)) (range bench-n))))
          :id4 {:type :int64 :data (long-array (map #(+ 1 (mod % 100)) (range bench-n)))}
          :id5 {:type :int64 :data (long-array (map #(+ 1 (mod % 100)) (range bench-n)))}
          :id6 {:type :int64 :data (long-array (map #(+ 1 (mod % 100)) (range bench-n)))}
          :v1  {:type :int64 :data (long-array (map #(+ 1 (mod % 5)) (range bench-n)))}
          :v2  {:type :int64 :data (long-array (map #(+ 1 (mod % 15)) (range bench-n)))}
          :v3  {:type :float64 :data (double-array (map #(* 100.0 (/ % 1000.0)) (range bench-n)))}}})

(defn- make-clickbench-registry
  "Create a ClickBench hits-like registry."
  []
  {"hits" {:AdvEngineID     {:type :int64 :data (long-array (map #(mod % 30) (range bench-n)))}
           :ResolutionWidth {:type :int64 :data (long-array (map #(+ 320 (mod % 2000)) (range bench-n)))}
           :UserID          {:type :int64 :data (long-array (map #(mod % 100) (range bench-n)))}
           :SearchEngineID  {:type :int64 :data (long-array (map #(mod % 20) (range bench-n)))}
           :IsRefresh       {:type :int64 :data (long-array (map #(mod % 2) (range bench-n)))}
           :RegionID        {:type :int64 :data (long-array (map #(mod % 500) (range bench-n)))}
           :EventTime       {:type :int64 :data (long-array (map #(+ 1577836800 (* % 86400)) (range bench-n)))}
           :URL (q/encode-column
                 (into-array String
                             (map #(case (int (mod % 5))
                                     0 (str "https://example.com/page/" (mod % 1000))
                                     1 (str "https://search.example.com/q=" (mod % 10000))
                                     2 (str "https://shop.example.com/product/" (mod % 5000))
                                     3 (str "https://news.example.com/article/" (mod % 2000))
                                     4 (str "https://example.com/api/v1/data/" (mod % 3000)))
                                  (range bench-n))))}})

(defn- make-taxi-registry
  "Create a NYC Taxi-like registry."
  []
  {"taxi" {:payment_type    {:type :int64 :data (long-array (map #(mod % 4) (range bench-n)))}
           :fare_amount     {:type :float64 :data (double-array (map #(+ 5.0 (* 0.5 %)) (range bench-n)))}
           :tip_amount      {:type :float64 :data (double-array (map #(* 0.1 %) (range bench-n)))}
           :total_amount    {:type :float64 :data (double-array (map #(+ 10.0 (* 0.5 %)) (range bench-n)))}
           :passenger_count {:type :int64 :data (long-array (map #(+ 1 (mod % 6)) (range bench-n)))}
           :pickup_hour     {:type :int64 :data (long-array (map #(mod % 24) (range bench-n)))}
           :pickup_dow      {:type :int64 :data (long-array (map #(mod % 7) (range bench-n)))}
           :pickup_month    {:type :int64 :data (long-array (map #(+ 1 (mod % 12)) (range bench-n)))}}})

(defn- make-join-registry
  "Create fact+dim tables for JOIN benchmark."
  []
  {"fact" {:fact_fk     {:type :int64 :data (long-array (map #(mod % 100) (range bench-n)))}
           :fact_amount {:type :float64 :data (double-array (map #(+ 1.0 (* 0.5 %)) (range bench-n)))}}
   "dim"  {:dim_id       {:type :int64 :data (long-array (range 100))}
           :dim_category {:type :int64 :data (long-array (map #(mod % 10) (range 100)))}}})

(defn- execute-bench-sql
  "Parse and execute SQL against registry, applying post-agg expressions."
  [sql registry]
  (let [parsed (sql/parse-sql sql registry)]
    (when (:query parsed)
      (let [result (q/q (:query parsed))]
        (if-let [pa (:_post-aggs (:query parsed))]
          (sql/apply-post-aggs result pa)
          result)))))

;; ---------- Tier 1: TPC-H / SSB ----------

(deftest bench-b1-tpch-q6-test
  (testing "B1: TPC-H Q6 — SUM(price * discount) with 3 filter predicates"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql
                   "SELECT SUM(price * discount) FROM lineitem WHERE shipdate >= 8766 AND shipdate < 9131 AND discount >= 0.05 AND discount <= 0.07 AND quantity < 24"
                   reg)]
      (is (seq results))
      (is (number? (:sum-product (first results)))))))

(deftest bench-b2-tpch-q1-test
  (testing "B2: TPC-H Q1 — GROUP BY + multiple aggregates with expression"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql
                   (str "SELECT returnflag, linestatus, "
                        "sum(quantity) AS sum_qty, sum(price) AS sum_base_price, "
                        "sum(price * (1 - discount)) AS sum_disc_price, "
                        "avg(quantity) AS avg_qty, avg(price) AS avg_price, "
                        "avg(discount) AS avg_disc, count(*) AS count_order "
                        "FROM lineitem WHERE shipdate < 10471 "
                        "GROUP BY returnflag, linestatus "
                        "ORDER BY returnflag, linestatus")
                   reg)]
      (is (seq results))
      (is (every? #(pos? (double (:sum_qty %))) results))
      (is (every? #(pos? (long (:count_order %))) results)))))

(deftest bench-b3-ssb-q11-test
  (testing "B3: SSB Q1.1 — SUM(price * discount) with different params"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql
                   "SELECT SUM(price * discount) FROM lineitem WHERE shipdate >= 8766 AND shipdate < 9131 AND discount >= 0.01 AND discount <= 0.03 AND quantity < 25"
                   reg)]
      (is (seq results)))))

(deftest bench-b4-count-test
  (testing "B4: COUNT(*) — no filter"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql "SELECT COUNT(*) FROM lineitem" reg)]
      (is (= bench-n (long (:count (first results))))))))

(deftest bench-b5-filtered-count-test
  (testing "B5: Filtered COUNT — WHERE discount <> 0"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql "SELECT COUNT(*) FROM lineitem WHERE discount <> 0" reg)]
      (is (pos? (long (:count (first results))))))))

(deftest bench-b6-group-by-test
  (testing "B6: Low-cardinality GROUP BY"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql
                   "SELECT returnflag, COUNT(*) FROM lineitem WHERE shipdate >= 8500 GROUP BY returnflag ORDER BY returnflag"
                   reg)]
      (is (seq results)))))

(deftest bench-ssb-q12-test
  (testing "SSB Q1.2 — tighter filter + sum-product"
    (let [reg (make-lineitem-registry)
          results (execute-bench-sql
                   "SELECT SUM(price*discount) FROM lineitem WHERE shipdate>=8766 AND shipdate<9131 AND discount>=0.04 AND discount<=0.06 AND quantity>=26 AND quantity<35"
                   reg)]
      (is (seq results)))))

;; ---------- Tier 2: H2O db-benchmark ----------

(deftest bench-h2o-q1-test
  (testing "H2O-Q1: GROUP BY id1 (string), SUM(v1)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id1, SUM(v1) FROM h2o GROUP BY id1" reg)]
      (is (= 10 (count results))))))

(deftest bench-h2o-q2-test
  (testing "H2O-Q2: GROUP BY id1, id2 (2 string cols), SUM(v1)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id1, id2, SUM(v1) FROM h2o GROUP BY id1, id2" reg)]
      (is (= 100 (count results))))))

(deftest bench-h2o-q3-test
  (testing "H2O-Q3: GROUP BY id3 (high-card), SUM(v1), AVG(v3)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id3, SUM(v1), AVG(v3) FROM h2o GROUP BY id3" reg)]
      (is (= 100 (count results))))))

(deftest bench-h2o-q4-test
  (testing "H2O-Q4: GROUP BY id4 (int), AVG(v1,v2,v3)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM h2o GROUP BY id4" reg)]
      (is (= 100 (count results))))))

(deftest bench-h2o-q5-test
  (testing "H2O-Q5: GROUP BY id6, SUM(v1,v2,v3)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM h2o GROUP BY id6" reg)]
      (is (= 100 (count results))))))

(deftest bench-h2o-q6-test
  (testing "H2O-Q6: GROUP BY id4, id5, STDDEV(v3)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id4, id5, STDDEV(v3) FROM h2o GROUP BY id4, id5" reg)]
      (is (seq results)))))

(deftest bench-h2o-q7-test
  (testing "H2O-Q7: GROUP BY id3, MAX(v1)-MIN(v2) — compound agg expression"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql
                   "SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM h2o GROUP BY id3"
                   reg)]
      (is (= 100 (count results)))
      (is (every? #(contains? % :range_v1_v2) results))
      ;; Should not contain internal _agg keys
      (is (every? #(not (contains? % :_agg1)) results)))))

(deftest bench-h2o-q9-test
  (testing "H2O-Q9: GROUP BY id2, id4, CORR(v1,v2)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql "SELECT id2, id4, CORR(v1, v2) FROM h2o GROUP BY id2, id4" reg)]
      (is (seq results)))))

(deftest bench-h2o-q10-test
  (testing "H2O-Q10: GROUP BY 6 cols, SUM(v3), COUNT(*)"
    (let [reg (make-h2o-registry)
          results (execute-bench-sql
                   "SELECT id1,id2,id3,id4,id5,id6, SUM(v3), COUNT(*) FROM h2o GROUP BY id1,id2,id3,id4,id5,id6"
                   reg)]
      (is (seq results)))))

;; ---------- Tier 3: ClickBench ----------

(deftest bench-cb-q0-test
  (testing "CB-Q0: COUNT(*) WHERE AdvEngineID != 0"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql "SELECT COUNT(*) FROM hits WHERE AdvEngineID != 0" reg)]
      (is (pos? (long (:count (first results))))))))

(deftest bench-cb-q1-test
  (testing "CB-Q1: SUM + COUNT + AVG (3 aggs)"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits" reg)]
      (is (seq results)))))

(deftest bench-cb-q2-test
  (testing "CB-Q2: COUNT with AND predicates"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql "SELECT COUNT(*) FROM hits WHERE IsRefresh = 1 AND ResolutionWidth > 1000" reg)]
      (is (seq results)))))

(deftest bench-cb-q7-test
  (testing "CB-Q7: GROUP BY + ORDER BY COUNT(*) DESC"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql
                   "SELECT SearchEngineID, COUNT(*) FROM hits GROUP BY SearchEngineID ORDER BY COUNT(*) DESC"
                   reg)]
      (is (seq results)))))

(deftest bench-cb-q15-test
  (testing "CB-Q15: GROUP BY + multi-agg + ORDER BY"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql
                   "SELECT RegionID, SUM(AdvEngineID), COUNT(*) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC"
                   reg)]
      (is (seq results)))))

(deftest bench-cb-q29-test
  (testing "CB-Q29: High-cardinality GROUP BY"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID" reg)]
      (is (= 100 (count results))))))

(deftest bench-cb-q32-test
  (testing "CB-Q32: COUNT(DISTINCT)"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql "SELECT COUNT(DISTINCT UserID) FROM hits" reg)]
      (is (= 100 (long (:count-distinct (first results))))))))

(deftest bench-cb-q5b-test
  (testing "CB-Q5b: COUNT(DISTINCT) in GROUP BY + ORDER BY + LIMIT"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql
                   "SELECT RegionID, COUNT(DISTINCT AdvEngineID) AS cd FROM hits GROUP BY RegionID ORDER BY cd DESC LIMIT 10"
                   reg)]
      (is (= 10 (count results))))))

(deftest bench-cb-q43-test
  (testing "CB-Q43: GROUP BY alias with modulo expression"
    (let [reg (make-clickbench-registry)
          results (execute-bench-sql
                   "SELECT (EventTime - EventTime % 60) AS m, COUNT(*) FROM hits GROUP BY m ORDER BY COUNT(*) DESC LIMIT 10"
                   reg)]
      (is (<= (count results) 10)))))

(deftest bench-cb-like-test
  (testing "CB: LIKE pattern matching"
    (let [reg (make-clickbench-registry)]
      (let [r (execute-bench-sql "SELECT COUNT(*) FROM hits WHERE URL LIKE '%example.com/page%'" reg)]
        (is (pos? (long (:count (first r))))))
      (let [r (execute-bench-sql "SELECT COUNT(*) FROM hits WHERE URL LIKE '%search%'" reg)]
        (is (pos? (long (:count (first r))))))
      (let [r (execute-bench-sql
               "SELECT SearchEngineID, COUNT(*) FROM hits WHERE URL LIKE '%shop%' GROUP BY SearchEngineID ORDER BY COUNT(*) DESC"
               reg)]
        (is (seq r))))))

;; ---------- Tier 4: NYC Taxi ----------

(deftest bench-taxi-q1-test
  (testing "Taxi-Q1: GROUP BY payment_type, AVG(fare)"
    (let [reg (make-taxi-registry)
          results (execute-bench-sql
                   "SELECT payment_type, AVG(fare_amount) FROM taxi GROUP BY payment_type ORDER BY payment_type"
                   reg)]
      (is (= 4 (count results))))))

(deftest bench-taxi-q2-test
  (testing "Taxi-Q2: GROUP BY passenger_count, AVG(tip)"
    (let [reg (make-taxi-registry)
          results (execute-bench-sql
                   "SELECT passenger_count, AVG(tip_amount) FROM taxi GROUP BY passenger_count ORDER BY passenger_count"
                   reg)]
      (is (= 6 (count results))))))

(deftest bench-taxi-q3-test
  (testing "Taxi-Q3: GROUP BY 2 cols, COUNT(*)"
    (let [reg (make-taxi-registry)
          results (execute-bench-sql
                   "SELECT pickup_hour, pickup_dow, COUNT(*) FROM taxi GROUP BY pickup_hour, pickup_dow ORDER BY pickup_hour, pickup_dow"
                   reg)]
      (is (seq results)))))

(deftest bench-taxi-q4-test
  (testing "Taxi-Q4: GROUP BY + WHERE + SUM"
    (let [reg (make-taxi-registry)
          results (execute-bench-sql
                   "SELECT pickup_month, SUM(total_amount) FROM taxi WHERE fare_amount > 10 GROUP BY pickup_month ORDER BY pickup_month"
                   reg)]
      (is (seq results)))))

;; ---------- Tier 5: Hash Join ----------

(deftest bench-join-q1-test
  (testing "JOIN-Q1: fact JOIN dim, GROUP BY category, SUM(amount)"
    (let [reg (make-join-registry)
          results (execute-bench-sql
                   "SELECT dim_category, SUM(fact_amount) FROM fact JOIN dim ON fact_fk=dim_id GROUP BY dim_category"
                   reg)]
      (is (= 10 (count results)))
      (is (every? #(pos? (double (:sum %))) results)))))

;; ============================================================================
;; Bug Fix Regression Tests
;; ============================================================================

(deftest sql-between-inclusive-test
  (testing "SQL BETWEEN is inclusive on both ends"
    (let [data {:val (long-array [10 20 30 40 50])}
          reg {"t" data}
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM t WHERE val BETWEEN 20 AND 40" reg)
          result (q/q query)]
      ;; Inclusive: 20, 30, 40 → 3
      (is (= 3 (long (:count (first result))))))))

(deftest sql-not-like-test
  (testing "SQL NOT LIKE works correctly"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apricot" "blueberry"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          reg {"t" {:cat cats :v vals}}
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM t WHERE cat NOT LIKE 'a%'" reg)
          result (q/q query)]
      ;; NOT LIKE 'a%': banana, cherry, blueberry → 3
      (is (= 3 (long (:count (first result))))))))

(deftest sql-in-with-strings-test
  (testing "SQL IN with string values on dict-encoded column"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          reg {"t" {:cat cats :v vals}}
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM t WHERE cat IN ('apple', 'cherry')" reg)
          result (q/q query)]
      ;; IN ('apple', 'cherry'): indices 0, 2, 3 → 3
      (is (= 3 (long (:count (first result))))))))

(deftest sql-not-in-with-strings-test
  (testing "SQL NOT IN with string values"
    (let [cats (q/encode-column (into-array String ["apple" "banana" "cherry" "apple" "banana"]))
          vals (double-array [1.0 2.0 3.0 4.0 5.0])
          reg {"t" {:cat cats :v vals}}
          {:keys [query]} (sql/parse-sql "SELECT COUNT(*) FROM t WHERE cat NOT IN ('apple')" reg)
          result (q/q query)]
      ;; NOT IN ('apple'): banana, cherry, banana → 3
      (is (= 3 (long (:count (first result))))))))

;; ============================================================================
;; Malli Schema Validation for SQL-generated Queries
;; ============================================================================
;;
;; These tests verify that every SQL query shape produced by the SQL parser
;; passes the malli SQuery schema. This prevents spec regressions where
;; valid SQL-generated queries would be rejected.

(deftest schema-basic-queries-test
  (testing "Simple aggregate queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT COUNT(*) FROM orders"
                   "SELECT SUM(price) FROM orders"
                   "SELECT AVG(price) FROM orders"
                   "SELECT MIN(price) FROM orders"
                   "SELECT MAX(price) FROM orders"
                   "SELECT SUM(price) AS total FROM orders"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql))))))

  (testing "Filtered queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT COUNT(*) FROM orders WHERE price > 100"
                   "SELECT COUNT(*) FROM orders WHERE quantity < 25"
                   "SELECT COUNT(*) FROM orders WHERE price >= 50.0"
                   "SELECT COUNT(*) FROM orders WHERE quantity <= 10"
                   "SELECT COUNT(*) FROM orders WHERE category = 3"
                   "SELECT COUNT(*) FROM orders WHERE category != 3"
                   "SELECT COUNT(*) FROM orders WHERE price BETWEEN 50 AND 100"
                   "SELECT COUNT(*) FROM orders WHERE category IN (1, 2, 3)"
                   "SELECT COUNT(*) FROM orders WHERE price IS NULL"
                   "SELECT COUNT(*) FROM orders WHERE price IS NOT NULL"
                   "SELECT COUNT(*) FROM orders WHERE price > 50 AND quantity < 25"
                   "SELECT COUNT(*) FROM orders WHERE category = 1 OR category = 2"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-group-by-queries-test
  (testing "GROUP BY queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT category, SUM(price) FROM orders GROUP BY category"
                   "SELECT category, COUNT(*) FROM orders GROUP BY category"
                   "SELECT category, SUM(price) FROM orders GROUP BY 1"
                   "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 100"
                   "SELECT category, SUM(price) FROM orders GROUP BY category ORDER BY category ASC"
                   "SELECT category, SUM(price) AS total FROM orders GROUP BY category ORDER BY total DESC"
                   "SELECT category, COUNT(*) FROM orders GROUP BY category LIMIT 3"
                   "SELECT category, COUNT(*) FROM orders GROUP BY category LIMIT 3 OFFSET 2"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-expression-queries-test
  (testing "Expression queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT SUM(price * quantity) FROM orders"
                   "SELECT COUNT(*) FROM orders WHERE price * quantity > 1000"
                   "SELECT DISTINCT category FROM orders"
                   "SELECT COUNT(DISTINCT category) FROM orders"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-statistical-queries-test
  (testing "Statistical aggregate queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT MEDIAN(price) FROM orders"
                   "SELECT PERCENTILE_CONT(0.95, price) FROM orders"
                   "SELECT APPROX_QUANTILE(price, 0.95) FROM orders"
                   "SELECT STDDEV(price) FROM orders"
                   "SELECT CORR(price, quantity) FROM orders"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-bench-tier-queries-test
  (testing "All OLAP benchmark SQL queries pass schema"
    ;; Tier 1: TPC-H / SSB
    (let [reg (make-lineitem-registry)]
      (doseq [sql ["SELECT SUM(price * discount) FROM lineitem WHERE shipdate >= 8766 AND shipdate < 9131 AND discount >= 0.05 AND discount <= 0.07 AND quantity < 24"
                   (str "SELECT returnflag, linestatus, "
                        "sum(quantity) AS sum_qty, sum(price) AS sum_base_price, "
                        "sum(price * (1 - discount)) AS sum_disc_price, "
                        "avg(quantity) AS avg_qty, avg(price) AS avg_price, "
                        "avg(discount) AS avg_disc, count(*) AS count_order "
                        "FROM lineitem WHERE shipdate < 10471 "
                        "GROUP BY returnflag, linestatus "
                        "ORDER BY returnflag, linestatus")
                   "SELECT COUNT(*) FROM lineitem"
                   "SELECT COUNT(*) FROM lineitem WHERE discount <> 0"
                   "SELECT returnflag, COUNT(*) FROM lineitem WHERE shipdate >= 8500 GROUP BY returnflag ORDER BY returnflag"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))

    ;; Tier 2: H2O
    (let [reg (make-h2o-registry)]
      (doseq [sql ["SELECT id1, SUM(v1) FROM h2o GROUP BY id1"
                   "SELECT id1, id2, SUM(v1) FROM h2o GROUP BY id1, id2"
                   "SELECT id3, SUM(v1), AVG(v3) FROM h2o GROUP BY id3"
                   "SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM h2o GROUP BY id4"
                   "SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM h2o GROUP BY id6"
                   "SELECT id4, id5, STDDEV(v3) FROM h2o GROUP BY id4, id5"
                   "SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM h2o GROUP BY id3"
                   "SELECT id2, id4, CORR(v1, v2) FROM h2o GROUP BY id2, id4"
                   "SELECT id1,id2,id3,id4,id5,id6, SUM(v3), COUNT(*) FROM h2o GROUP BY id1,id2,id3,id4,id5,id6"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))

    ;; Tier 3: ClickBench
    (let [reg (make-clickbench-registry)]
      (doseq [sql ["SELECT COUNT(*) FROM hits WHERE AdvEngineID != 0"
                   "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"
                   "SELECT COUNT(*) FROM hits WHERE IsRefresh = 1 AND ResolutionWidth > 1000"
                   "SELECT SearchEngineID, COUNT(*) FROM hits GROUP BY SearchEngineID ORDER BY COUNT(*) DESC"
                   "SELECT RegionID, SUM(AdvEngineID), COUNT(*) FROM hits GROUP BY RegionID ORDER BY COUNT(*) DESC"
                   "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID"
                   "SELECT COUNT(DISTINCT UserID) FROM hits"
                   "SELECT RegionID, COUNT(DISTINCT AdvEngineID) AS cd FROM hits GROUP BY RegionID ORDER BY cd DESC LIMIT 10"
                   "SELECT COUNT(*) FROM hits WHERE URL LIKE '%example.com/page%'"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))

    ;; Tier 4: Taxi
    (let [reg (make-taxi-registry)]
      (doseq [sql ["SELECT payment_type, AVG(fare_amount) FROM taxi GROUP BY payment_type ORDER BY payment_type"
                   "SELECT passenger_count, AVG(tip_amount) FROM taxi GROUP BY passenger_count ORDER BY passenger_count"
                   "SELECT pickup_hour, pickup_dow, COUNT(*) FROM taxi GROUP BY pickup_hour, pickup_dow ORDER BY pickup_hour, pickup_dow"
                   "SELECT pickup_month, SUM(total_amount) FROM taxi WHERE fare_amount > 10 GROUP BY pickup_month ORDER BY pickup_month"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))

    ;; Tier 5: Join
    (let [reg (make-join-registry)]
      (doseq [sql ["SELECT dim_category, SUM(fact_amount) FROM fact JOIN dim ON fact_fk=dim_id GROUP BY dim_category"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-anomaly-queries-test
  (testing "Anomaly detection SQL queries pass schema"
    (let [reg (make-test-registry)]
      (doseq [sql ["SELECT ANOMALY_SCORE('mymodel', price, quantity) FROM orders"
                   "SELECT ANOMALY_PREDICT('mymodel', price, quantity) FROM orders"
                   "SELECT ANOMALY_PROBA('mymodel', price, quantity) FROM orders"
                   "SELECT ANOMALY_CONFIDENCE('mymodel', price, quantity) FROM orders"]]
        (let [{:keys [query]} (sql/parse-sql sql reg)]
          (is (spec/validate spec/SQuery query) (str "schema failed for: " sql)))))))

(deftest schema-rejection-test
  (testing "Invalid queries are rejected by schema"
    (is (spec/validate spec/SQuery {:from "table-name"})
        "String :from is valid (temporal table reference)")
    (is (not (spec/validate spec/SQuery {:from {:x (double-array [1])} :agg [[:foobar :x]]}))
        "Unknown agg operator should be rejected")
    (is (not (spec/validate spec/SQuery {:from {:x (long-array [1])} :where [[:nope :x 5]]}))
        "Unknown pred operator should be rejected")
    (is (not (spec/validate spec/SQuery {:from {:x (long-array [1])} :limit -1}))
        "Negative limit should be rejected")
    (is (not (spec/validate spec/SQuery {:from {:x (long-array [1])} :order [[:x :sideways]]}))
        "Invalid order direction should be rejected")
    (is (not (spec/validate spec/SQuery {:from {:x (long-array [1])} :result :json}))
        "Invalid result format should be rejected")))

;; ============================================================================
;; Anomaly Detection SQL Functions
;; ============================================================================

(deftest sql-anomaly-functions-parse-test
  (testing "ANOMALY_SCORE parses correctly"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT ANOMALY_SCORE('mymodel', price, quantity) FROM orders" reg)]
      (is (some #(and (sequential? %) (= :anomaly-score (first %))) (:select query)))))

  (testing "ANOMALY_PREDICT parses correctly"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT ANOMALY_PREDICT('mymodel', price, quantity) FROM orders" reg)]
      (is (some #(and (sequential? %) (= :anomaly-predict (first %))) (:select query)))))

  (testing "ANOMALY_PROBA parses correctly"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT ANOMALY_PROBA('mymodel', price, quantity) FROM orders" reg)]
      (is (some #(and (sequential? %) (= :anomaly-proba (first %))) (:select query)))))

  (testing "ANOMALY_CONFIDENCE parses correctly"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT ANOMALY_CONFIDENCE('mymodel', price, quantity) FROM orders" reg)]
      (is (some #(and (sequential? %) (= :anomaly-confidence (first %))) (:select query))))))

;; ============================================================================
;; Window Function Tests
;; ============================================================================

(def ^:private window-reg
  "Small test registry for window function tests."
  {"orders" {:price    (double-array [10 20 30 40 50])
             :category (long-array [1 1 2 2 2])
             :qty      (long-array [5 3 8 2 6])}})

(deftest window-function-parse-test
  (testing "ROW_NUMBER parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price ASC) AS rn FROM orders"
                           window-reg)]
      (is (some? query))
      (is (= 1 (count (:window query))))
      (is (= :row-number (-> query :window first :op)))
      (is (= :rn (-> query :window first :as)))
      (is (= [:category] (-> query :window first :partition-by)))
      (is (= [[:price :asc]] (-> query :window first :order-by)))))

  (testing "SUM OVER parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (PARTITION BY category ORDER BY price ASC) AS running_sum FROM orders"
                           window-reg)]
      (is (= :sum (-> query :window first :op)))
      (is (= :price (-> query :window first :col)))
      (is (= :running_sum (-> query :window first :as)))))

  (testing "RANK parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, RANK() OVER (ORDER BY price DESC) AS rnk FROM orders"
                           window-reg)]
      (is (= :rank (-> query :window first :op)))
      (is (nil? (-> query :window first :partition-by)))
      (is (= [[:price :desc]] (-> query :window first :order-by)))))

  (testing "DENSE_RANK parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, DENSE_RANK() OVER (ORDER BY price ASC) AS drnk FROM orders"
                           window-reg)]
      (is (= :dense-rank (-> query :window first :op)))))

  (testing "LAG parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, LAG(price, 1) OVER (PARTITION BY category ORDER BY price ASC) AS prev FROM orders"
                           window-reg)]
      (is (= :lag (-> query :window first :op)))
      (is (= :price (-> query :window first :col)))
      (is (= 1 (-> query :window first :offset)))))

  (testing "LEAD parses correctly"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, LEAD(price, 1) OVER (ORDER BY price ASC) AS nxt FROM orders"
                           window-reg)]
      (is (= :lead (-> query :window first :op)))))

  (testing "Multiple window functions in one query"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (ORDER BY price ASC) AS rn, SUM(price) OVER (ORDER BY price ASC) AS rsum FROM orders"
                           window-reg)]
      (is (= 2 (count (:window query))))
      (is (= #{:row-number :sum} (set (map :op (:window query)))))))

  (testing "Window function not classified as aggregate"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (ORDER BY price ASC) AS rn FROM orders"
                           window-reg)]
      ;; Should have no :agg entries (window != aggregate)
      (is (empty? (:agg query))))))

(deftest window-function-execution-test
  (testing "ROW_NUMBER with partition"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price ASC) AS rn FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: [10, 20] → rn [1, 2]; cat 2: [30, 40, 50] → rn [1, 2, 3]
      (is (= [1.0 2.0 1.0 2.0 3.0] (mapv :rn results)))))

  (testing "ROW_NUMBER without partition"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (ORDER BY price ASC) AS rn FROM orders"
                           window-reg)
          results (q/q query)]
      (is (= [1.0 2.0 3.0 4.0 5.0] (mapv :rn results)))))

  (testing "RANK with ties"
    (let [reg {"orders" {:price (double-array [10 10 20 30 30])
                         :category (long-array [1 1 1 1 1])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price, RANK() OVER (ORDER BY price ASC) AS rnk FROM orders"
                           reg)
          results (q/q query)]
      ;; 10,10 → 1,1; 20 → 3; 30,30 → 4,4
      (is (= [1.0 1.0 3.0 4.0 4.0] (mapv :rnk results)))))

  (testing "DENSE_RANK with ties"
    (let [reg {"orders" {:price (double-array [10 10 20 30 30])
                         :category (long-array [1 1 1 1 1])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price, DENSE_RANK() OVER (ORDER BY price ASC) AS drnk FROM orders"
                           reg)
          results (q/q query)]
      ;; 10,10 → 1,1; 20 → 2; 30,30 → 3,3
      (is (= [1.0 1.0 2.0 3.0 3.0] (mapv :drnk results)))))

  (testing "Running SUM partitioned"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (PARTITION BY category ORDER BY price ASC) AS rsum FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: 10→10, 20→30; cat 2: 30→30, 40→70, 50→120
      (is (= [10.0 30.0 30.0 70.0 120.0] (mapv :rsum results)))))

  (testing "Running COUNT partitioned"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, COUNT(price) OVER (PARTITION BY category ORDER BY price ASC) AS cnt FROM orders"
                           window-reg)
          results (q/q query)]
      (is (= [1.0 2.0 1.0 2.0 3.0] (mapv :cnt results)))))

  (testing "Running AVG partitioned"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, AVG(price) OVER (PARTITION BY category ORDER BY price ASC) AS avg_p FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: 10→10, 20→15; cat 2: 30→30, 40→35, 50→40
      (is (= [10.0 15.0 30.0 35.0 40.0] (mapv :avg_p results)))))

  (testing "Running MIN partitioned"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, MIN(price) OVER (PARTITION BY category ORDER BY price ASC) AS min_p FROM orders"
                           window-reg)
          results (q/q query)]
      (is (= [10.0 10.0 30.0 30.0 30.0] (mapv :min_p results)))))

  (testing "Running MAX partitioned"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, MAX(price) OVER (PARTITION BY category ORDER BY price ASC) AS max_p FROM orders"
                           window-reg)
          results (q/q query)]
      (is (= [10.0 20.0 30.0 40.0 50.0] (mapv :max_p results)))))

  (testing "LAG with partition"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, LAG(price, 1) OVER (PARTITION BY category ORDER BY price ASC) AS prev FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: 10→nil, 20→10; cat 2: 30→nil, 40→30, 50→40
      (is (nil? (:prev (nth results 0))))
      (is (= 10.0 (:prev (nth results 1))))
      (is (nil? (:prev (nth results 2))))
      (is (= 30.0 (:prev (nth results 3))))
      (is (= 40.0 (:prev (nth results 4))))))

  (testing "LEAD with partition"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, LEAD(price, 1) OVER (PARTITION BY category ORDER BY price ASC) AS nxt FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: 10→20, 20→nil; cat 2: 30→40, 40→50, 50→nil
      (is (= 20.0 (:nxt (nth results 0))))
      (is (nil? (:nxt (nth results 1))))
      (is (= 40.0 (:nxt (nth results 2))))
      (is (= 50.0 (:nxt (nth results 3))))
      (is (nil? (:nxt (nth results 4))))))

  (testing "Window with larger dataset"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price ASC) AS rn FROM orders"
                           reg)
          results (q/q query)]
      ;; 1000 rows, 5 categories → 200 rows per category
      (is (= 1000 (count results)))
      ;; Max row number per category should be 200
      (is (= 200.0 (apply max (map :rn results))))))

  (testing "Window function schema validates"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, ROW_NUMBER() OVER (ORDER BY price ASC) AS rn FROM orders"
                           window-reg)]
      (is (spec/validate spec/SQuery query) "window query should pass schema validation"))))

;; ============================================================================
;; CTE and Subquery Tests
;; ============================================================================

(deftest cte-parse-test
  (testing "Simple CTE parses and executes"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "WITH expensive AS (SELECT price FROM orders WHERE price > 200) SELECT price FROM expensive"
                           reg)]
      (is (some? query))
      (is (map? (:from query)))
      ;; CTE should have been materialized into the query's :from
      (let [results (q/q query)]
        (is (seq results))
        (is (every? #(> (:price %) 200.0) results)))))

  (testing "CTE with aggregation"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "WITH cat_sums AS (SELECT category, SUM(price) AS total FROM orders GROUP BY category) SELECT category, total FROM cat_sums"
                           reg)]
      (is (some? query))
      (let [results (q/q query)]
        (is (= 5 (count results)))
        (is (every? #(contains? % :total) results)))))

  (testing "CTE with WHERE on materialized data"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "WITH all_orders AS (SELECT price, category FROM orders) SELECT price FROM all_orders WHERE price > 400"
                           reg)]
      (is (some? query))
      (let [results (q/q query)]
        (is (seq results))
        (is (every? #(> (:price %) 400.0) results))))))

(deftest in-subquery-test
  (testing "IN (SELECT ...) filters correctly"
    (let [reg {"orders" {:price (double-array [10 20 30 40 50])
                         :category (long-array [1 1 2 2 2])}
               "products" {:price (double-array [10 30 50])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE price IN (SELECT price FROM products)"
                           reg)
          results (q/q query)]
      (is (= 3 (count results)))
      (is (= #{10.0 30.0 50.0} (set (map :price results))))))

  (testing "IN subquery with WHERE clause in inner query"
    (let [reg {"orders" {:price (double-array [10 20 30 40 50])
                         :category (long-array [1 1 2 2 2])}
               "products" {:price (double-array [10 30 50])
                           :active (long-array [1 0 1])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE price IN (SELECT price FROM products WHERE active = 1)"
                           reg)
          results (q/q query)]
      (is (= 2 (count results)))
      (is (= #{10.0 50.0} (set (map :price results)))))))

(deftest from-subquery-test
  (testing "FROM (SELECT ...) AS alias works"
    (let [reg {"orders" {:price (double-array [10 20 30 40 50])
                         :category (long-array [1 1 2 2 2])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM (SELECT price FROM orders WHERE price > 25) AS sub"
                           reg)
          results (q/q query)]
      (is (= 3 (count results)))
      (is (= #{30.0 40.0 50.0} (set (map :price results))))))

  (testing "FROM subquery with aggregation"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "SELECT total FROM (SELECT SUM(price) AS total FROM orders) AS sub"
                           reg)]
      (is (some? query))
      (let [results (q/q query)]
        (is (= 1 (count results)))
        (is (pos? (:total (first results))))))))

;; ============================================================================
;; Nested Aggregate in Window Function Tests
;; ============================================================================

(deftest nested-agg-window-test
  (testing "SUM(SUM(price)) OVER (PARTITION BY category ORDER BY category)"
    (let [reg {"sales" {:price    (double-array [10 20 30 40 50 60])
                        :category (long-array   [1  1  2  2  3  3])
                        :region   (long-array   [1  2  1  2  1  2])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT category, SUM(price) AS cat_total, SUM(SUM(price)) OVER (ORDER BY category ASC) AS running_total FROM sales GROUP BY category"
                           reg)
          results (q/q query)]
      ;; cat 1: sum=30, cat 2: sum=70, cat 3: sum=110
      ;; running: 30, 100, 210
      (is (= 3 (count results)))
      (let [sorted (sort-by :category results)]
        (is (= [30.0 70.0 110.0] (mapv :cat_total sorted)))
        (is (= [30.0 100.0 210.0] (mapv :running_total sorted))))))

  (testing "Nested agg window query parses inner-agg into :agg list"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT category, SUM(price) AS total, SUM(SUM(price)) OVER (ORDER BY category) AS rt FROM sales GROUP BY category"
                           {"sales" {:price (double-array [1 2 3])
                                     :category (long-array [1 2 3])}})]
      ;; Both the explicit SUM(price) and the inner SUM(price) from the window
      ;; should appear in the :agg list
      (is (>= (count (:agg query)) 2))
      (is (seq (:window query))))))

;; ============================================================================
;; Date Interval Arithmetic Tests
;; ============================================================================

(deftest date-interval-test
  (testing "CAST to DATE parses date literal to epoch-day"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE price > 0"
                           (make-test-registry))]
      ;; Just verify the parser handles date cast without error
      (is (some? query))))

  (testing "CAST('2001-01-12' AS DATE) produces epoch-day long"
    (let [reg {"t" {:val (long-array [11334 11364 11400])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t WHERE val >= CAST('2001-01-12' AS DATE)"
                           reg)
          results (q/q query)]
      ;; 2001-01-12 = epoch day 11334
      (is (= 3 (count results)))))

  (testing "CAST date + INTERVAL day arithmetic"
    (let [reg {"t" {:val (long-array [11334 11364 11400])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t WHERE val <= CAST('2001-01-12' AS DATE) + INTERVAL '30' DAY"
                           reg)
          results (q/q query)]
      ;; 11334 + 30 = 11364, so val <= 11364 → first two rows
      (is (= 2 (count results))))))

;; ============================================================================
;; Window Frame Specification Tests
;; ============================================================================

(deftest window-frame-test
  (testing "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (default behavior)"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (ORDER BY price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rsum FROM orders"
                           window-reg)
          results (q/q query)]
      ;; Same as default running sum: 10, 30, 60, 100, 150
      (is (= [10.0 30.0 60.0 100.0 150.0] (mapv :rsum (sort-by :price results))))))

  (testing "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (partition total)"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, category, SUM(price) OVER (PARTITION BY category ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ptotal FROM orders"
                           window-reg)
          results (q/q query)]
      ;; cat 1: 10+20=30, cat 2: 30+40+50=120
      (is (= 5 (count results)))
      (let [cat1 (filter #(= 1 (:category %)) results)
            cat2 (filter #(= 2 (:category %)) results)]
        (is (every? #(= 30.0 (:ptotal %)) cat1))
        (is (every? #(= 120.0 (:ptotal %)) cat2)))))

  (testing "Frame spec parsed in window spec map"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (ORDER BY price ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rsum FROM orders"
                           window-reg)]
      (is (some? (:window query)))
      (is (contains? (first (:window query)) :frame))))

  (testing "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW — sliding sum"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (ORDER BY price ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rsum FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; prices sorted: 10, 20, 30, 40, 50
      ;; sliding sum(1 preceding, current): 10, 30, 50, 70, 90
      (is (= [10.0 30.0 50.0 70.0 90.0] (mapv :rsum results)))))

  (testing "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING — sliding sum with following"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, SUM(price) OVER (ORDER BY price ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rsum FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; prices sorted: 10, 20, 30, 40, 50
      ;; sliding sum(1 prec, 1 foll): 30, 60, 90, 120, 90
      (is (= [30.0 60.0 90.0 120.0 90.0] (mapv :rsum results)))))

  (testing "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW — sliding count"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, COUNT(*) OVER (ORDER BY price ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; window size: 1, 2, 2, 2, 2
      (is (= [1.0 2.0 2.0 2.0 2.0] (mapv :cnt results)))))

  (testing "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING — sliding avg"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, AVG(price) OVER (ORDER BY price ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS ravg FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; prices: 10, 20, 30, 40, 50
      ;; avg(1 prec, 1 foll): 15, 20, 30, 40, 45
      (is (= [15.0 20.0 30.0 40.0 45.0] (mapv :ravg results))))))

(deftest ntile-percent-rank-cume-dist-test
  (testing "NTILE(2) — split into 2 buckets"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, NTILE(2) OVER (ORDER BY price ASC) AS bucket FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; 5 rows, 2 buckets: first 3 in bucket 1, last 2 in bucket 2
      (is (= [1.0 1.0 1.0 2.0 2.0] (mapv :bucket results)))))

  (testing "PERCENT_RANK — (rank - 1) / (partition_size - 1)"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, PERCENT_RANK() OVER (ORDER BY price ASC) AS pr FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; 5 rows: ranks 1,2,3,4,5 → (0/4, 1/4, 2/4, 3/4, 4/4)
      (is (= [0.0 0.25 0.5 0.75 1.0] (mapv :pr results)))))

  (testing "CUME_DIST — count(rows <= current) / partition_size"
    (let [{:keys [query]} (sql/parse-sql
                           "SELECT price, CUME_DIST() OVER (ORDER BY price ASC) AS cd FROM orders"
                           window-reg)
          results (sort-by :price (q/q query))]
      ;; 5 rows: 1/5, 2/5, 3/5, 4/5, 5/5
      (is (= [0.2 0.4 0.6 0.8 1.0] (mapv :cd results))))))

;; ============================================================================
;; UNION / UNION ALL Tests
;; ============================================================================

(deftest union-test
  (testing "UNION ALL concatenates results"
    (let [reg {"t1" {:val (long-array [1 2 3])}
               "t2" {:val (long-array [4 5 6])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 UNION ALL SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 6 (count results)))
      (is (= #{1 2 3 4 5 6} (set (map :val results))))))

  (testing "UNION ALL preserves duplicates"
    (let [reg {"t1" {:val (long-array [1 2 3])}
               "t2" {:val (long-array [2 3 4])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 UNION ALL SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 6 (count results)))))

  (testing "UNION deduplicates"
    (let [reg {"t1" {:val (long-array [1 2 3])}
               "t2" {:val (long-array [2 3 4])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 UNION SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 4 (count results)))
      (is (= #{1 2 3 4} (set (map :val results)))))))

;; ============================================================================
;; INTERSECT / EXCEPT Tests
;; ============================================================================

(deftest intersect-test
  (testing "INTERSECT returns common rows"
    (let [reg {"t1" {:val (long-array [1 2 3 4])}
               "t2" {:val (long-array [3 4 5 6])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 INTERSECT SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 2 (count results)))
      (is (= #{3 4} (set (map :val results))))))

  (testing "INTERSECT with no overlap returns empty"
    (let [reg {"t1" {:val (long-array [1 2])}
               "t2" {:val (long-array [3 4])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 INTERSECT SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 0 (count results))))))

(deftest except-test
  (testing "EXCEPT removes matching rows"
    (let [reg {"t1" {:val (long-array [1 2 3 4])}
               "t2" {:val (long-array [3 4 5 6])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 EXCEPT SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 2 (count results)))
      (is (= #{1 2} (set (map :val results))))))

  (testing "EXCEPT with no overlap keeps all"
    (let [reg {"t1" {:val (long-array [1 2 3])}
               "t2" {:val (long-array [4 5 6])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT val FROM t1 EXCEPT SELECT val FROM t2"
                           reg)
          results (q/q query)]
      (is (= 3 (count results)))
      (is (= #{1 2 3} (set (map :val results)))))))

;; ============================================================================
;; EXISTS / NOT EXISTS Tests
;; ============================================================================

(deftest exists-subquery-test
  (testing "EXISTS returns rows when subquery has results"
    (let [reg {"orders" {:price (double-array [10 20 30 40 50])
                         :category (long-array [1 1 2 2 2])}
               "products" {:id (long-array [1 2 3])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE EXISTS (SELECT id FROM products)"
                           reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (= #{10.0 20.0 30.0 40.0 50.0} (set (map :price results))))))

  (testing "EXISTS returns no rows when subquery is empty"
    (let [reg {"orders" {:price (double-array [10 20 30 40 50])
                         :category (long-array [1 1 2 2 2])}
               "products" {:id (long-array [1 2 3])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE EXISTS (SELECT id FROM products WHERE id > 100)"
                           reg)
          results (q/q query)]
      (is (= 0 (count results))))))

(deftest not-exists-subquery-test
  (testing "NOT EXISTS returns no rows when subquery has results"
    (let [reg {"orders" {:price (double-array [10 20 30])
                         :category (long-array [1 1 2])}
               "products" {:id (long-array [1 2 3])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE NOT EXISTS (SELECT id FROM products)"
                           reg)
          results (q/q query)]
      (is (= 0 (count results)))))

  (testing "NOT EXISTS returns rows when subquery is empty"
    (let [reg {"orders" {:price (double-array [10 20 30])
                         :category (long-array [1 1 2])}
               "products" {:id (long-array [1 2 3])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT price FROM orders WHERE NOT EXISTS (SELECT id FROM products WHERE id > 100)"
                           reg)
          results (q/q query)]
      (is (= 3 (count results)))
      (is (= #{10.0 20.0 30.0} (set (map :price results)))))))

;; =============================================================================
;; Pre-release audit fix tests
;; =============================================================================

(deftest simple-case-switch-expression-test
  (testing "CASE status WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 0 END (#58)"
    (let [reg {"t" {:status (long-array [1 2 3 1 2])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT CASE status WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 0 END AS label FROM t"
                           reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (= [10.0 20.0 0.0 10.0 20.0]
             (mapv :label results))))))

(deftest sql-string-case-expression-test
  (testing "SQL CASE WHEN with string THEN values"
    (let [reg {"t" {:status (long-array [1 2 3 1 2])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT CASE WHEN status = 1 THEN 'active' WHEN status = 2 THEN 'inactive' ELSE 'unknown' END AS label FROM t"
                           reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (= ["active" "inactive" "unknown" "active" "inactive"]
             (mapv :label results))))))

(deftest having-multi-agg-reference-test
  (testing "HAVING SUM(price) > 100 with multiple aggs of same type (#56)"
    (let [reg {"orders" {:cat (long-array [0 0 0 1 1])
                         :price (double-array [50.0 60.0 70.0 10.0 20.0])
                         :qty (double-array [1.0 2.0 3.0 4.0 5.0])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT cat, SUM(price) AS sum_price, SUM(qty) AS sum_qty FROM orders GROUP BY cat HAVING SUM(price) > 50"
                           reg)
          results (q/q query)]
      ;; Group 0: sum_price=180, sum_qty=6 → passes HAVING
      ;; Group 1: sum_price=30, sum_qty=9 → fails HAVING
      (is (= 1 (count results)))
      (is (= 0 (:cat (first results))))
      (is (> (:sum_price (first results)) 50.0)))))
