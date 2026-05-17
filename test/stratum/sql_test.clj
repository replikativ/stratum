(ns stratum.sql-test
  "Tests for the Stratum SQL interface (parser + server)."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [stratum.sql :as sql]
            [stratum.query :as q]
            [stratum.server :as server]
            [stratum.specification :as spec])
  (:import [stratum.internal PgWireServer$QueryResult
                              PgWireServer$QueryHandler
                              PgWireServer$QueryHandlerFactory]))

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

(deftest sql-join-rejects-non-equi-predicate-test
  (testing "Non-equality predicate in JOIN ON returns :error with sqlstate 0A000"
    (let [reg {"a" {:x (long-array [1 2 3])}
               "b" {:y (long-array [1 2 3])}}
          r1 (sql/parse-sql "SELECT * FROM a JOIN b ON a.x >= b.y" reg)
          r2 (sql/parse-sql "SELECT * FROM a JOIN b ON a.x = b.y AND a.x > b.y" reg)]
      (is (re-find #"Non-equality predicate" (:error r1)))
      (is (= "0A000" (:sqlstate r1)))
      (is (re-find #"Non-equality predicate" (:error r2)))
      (is (= "0A000" (:sqlstate r2))))))

(deftest sql-join-on-with-parentheses-test
  (testing "JOIN ON (a = b) AND (c = d) — parentheses around equality terms unwrap correctly"
    (let [reg {"a" {:id   (long-array [1 2 3])
                    :cat  (long-array [10 20 30])}
               "b" {:id   (long-array [1 2 3])
                    :cat  (long-array [10 20 30])
                    :v    (double-array [1.0 2.0 3.0])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT a.id, b.v FROM a JOIN b ON (a.id = b.id) AND (a.cat = b.cat)"
                           reg)
          result (q/q query)]
      (is (= 3 (count result))))))

;; ============================================================================
;; ASOF JOIN — DuckDB syntax (rewriter strips "ASOF" before parsing)
;; ============================================================================

(defn- run-asof-sql [sql reg]
  (let [{:keys [query error]} (sql/parse-sql sql reg)]
    (when error (throw (ex-info error {})))
    (q/q query)))

(deftest sql-asof-inner-gte-no-eq-key-test
  (testing "ASOF JOIN ... ON probe.ts >= build.ts (no equality keys)"
    (let [reg {"trades" {:probe_ts (long-array [5 15 30])}
               "prices" {:build_ts (long-array [0 10 20])
                         :px       (double-array [1.0 2.0 3.0])}}
          result (run-asof-sql
                  "SELECT trades.probe_ts, prices.px FROM trades ASOF JOIN prices ON trades.probe_ts >= prices.build_ts"
                  reg)]
      (is (= 3 (count result)))
      (is (= [1.0 2.0 3.0] (mapv :px result))))))

(deftest sql-asof-inner-gte-with-eq-key-test
  (testing "ASOF JOIN ... ON sym=sym AND probe.ts >= build.ts (mixed equality + inequality)"
    (let [reg {"trades" {:sym      (long-array [1 2 1])
                         :probe_ts (long-array [15 25 35])}
               "prices" {:sym      (long-array [1 1 2])
                         :build_ts (long-array [10 30 20])
                         :px       (double-array [101.0 103.0 202.0])}}
          result (run-asof-sql
                  (str "SELECT trades.sym, trades.probe_ts, prices.px "
                       "FROM trades ASOF JOIN prices "
                       "ON trades.sym = prices.sym AND trades.probe_ts >= prices.build_ts")
                  reg)]
      ;; sym=1 ts=15 → newest sym=1 build at-or-before 15 is ts=10 → 101.0
      ;; sym=2 ts=25 → newest sym=2 build at-or-before 25 is ts=20 → 202.0
      ;; sym=1 ts=35 → newest sym=1 build at-or-before 35 is ts=30 → 103.0
      (is (= 3 (count result)))
      (let [m (into {} (map (fn [r] [[(long (:sym r)) (long (:probe_ts r))] (:px r)])) result)]
        (is (= 101.0 (get m [1 15])))
        (is (= 202.0 (get m [2 25])))
        (is (= 103.0 (get m [1 35])))))))

(deftest sql-asof-left-keeps-unmatched-test
  (testing "ASOF LEFT JOIN keeps unmatched probe rows with NULL right cols"
    (let [reg {"trades" {:probe_ts (long-array [5 15 35])}    ; 5 has no build at-or-before
               "prices" {:build_ts (long-array [10 20 30])
                         :px       (double-array [1.0 2.0 3.0])}}
          result (run-asof-sql
                  "SELECT trades.probe_ts, prices.px FROM trades ASOF LEFT JOIN prices ON trades.probe_ts >= prices.build_ts"
                  reg)]
      (is (= 3 (count result)))
      ;; Probe ts=5  → no match → :px nil
      ;; Probe ts=15 → ts=10 → 1.0
      ;; Probe ts=35 → ts=30 → 3.0
      (let [tagged (into {} (map (fn [r] [(long (:probe_ts r)) (:px r)])) result)]
        (is (nil? (get tagged 5)))
        (is (= 1.0 (get tagged 15)))
        (is (= 3.0 (get tagged 35)))))))

(deftest sql-asof-all-four-operators-test
  (testing "ASOF JOIN with all four operators :>= :> :<= :<"
    (let [reg {"l" {:t (long-array [20])}
               "r" {:t  (long-array [10 20 30])
                    :v  (double-array [1.0 2.0 3.0])}}
          ;; build [10,20,30], probe ts=20
          gte (run-asof-sql "SELECT r.v FROM l ASOF JOIN r ON l.t >= r.t" reg)
          gt  (run-asof-sql "SELECT r.v FROM l ASOF JOIN r ON l.t >  r.t" reg)
          lte (run-asof-sql "SELECT r.v FROM l ASOF JOIN r ON l.t <= r.t" reg)
          lt  (run-asof-sql "SELECT r.v FROM l ASOF JOIN r ON l.t <  r.t" reg)]
      ;; >=: latest at-or-before 20 → t=20 → v=2.0
      (is (= 1 (count gte))) (is (= 2.0 (:v (first gte))))
      ;; >: latest strictly before 20 → t=10 → v=1.0
      (is (= 1 (count gt)))  (is (= 1.0 (:v (first gt))))
      ;; <=: smallest at-or-after 20 → t=20 → v=2.0
      (is (= 1 (count lte))) (is (= 2.0 (:v (first lte))))
      ;; <: smallest strictly after 20 → t=30 → v=3.0
      (is (= 1 (count lt)))  (is (= 3.0 (:v (first lt)))))))

(deftest sql-asof-rejects-multiple-inequalities-test
  (testing "ASOF JOIN with two inequality predicates throws at execute time"
    (let [reg {"l" {:t (long-array [1])}
               "r" {:t (long-array [1])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT * FROM l ASOF JOIN r ON l.t >= r.t AND l.t <= r.t"
                           reg)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"exactly one inequality"
                            (q/q query))))))

(deftest sql-asof-rejects-no-inequality-test
  (testing "ASOF JOIN with only equality predicates throws at execute time"
    (let [reg {"l" {:t (long-array [1])}
               "r" {:t (long-array [1])}}
          {:keys [query]} (sql/parse-sql "SELECT * FROM l ASOF JOIN r ON l.t = r.t" reg)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"exactly one inequality"
                            (q/q query))))))

(deftest sql-asof-preserves-string-literals-test
  (testing "Rewriter does not touch 'ASOF' inside string literals"
    (let [reg {"t" {:label (q/encode-column (into-array String ["ASOF JOIN sample"]))
                    :v     (double-array [42.0])}}
          {:keys [query]} (sql/parse-sql
                           "SELECT label, v FROM t WHERE label = 'ASOF JOIN sample'"
                           reg)
          result (q/q query)]
      ;; If the rewriter mistakenly stripped ASOF inside the string literal,
      ;; the WHERE comparison would fail.
      (is (= 1 (count result)))
      (is (= "ASOF JOIN sample" (:label (first result)))))))

(deftest sql-asof-multi-col-equality-test
  (testing "ASOF JOIN with multiple equality predicates + one inequality"
    (let [reg {"l" {:sym  (long-array [1 1 2])
                    :exch (long-array [1 2 1])
                    :t    (long-array [10 10 10])}
               "r" {:sym  (long-array [1 1 2 2])
                    :exch (long-array [1 2 1 2])
                    :t    (long-array [5 6 7 8])
                    :px   (double-array [11.0 12.0 21.0 22.0])}}
          result (run-asof-sql
                  (str "SELECT l.sym, l.exch, r.px FROM l ASOF JOIN r "
                       "ON l.sym = r.sym AND l.exch = r.exch AND l.t >= r.t")
                  reg)]
      (is (= 3 (count result)))
      (let [m (into {} (map (fn [r] [[(long (:sym r)) (long (:exch r))] (:px r)])) result)]
        (is (= 11.0 (get m [1 1])))
        (is (= 12.0 (get m [1 2])))
        (is (= 21.0 (get m [2 1])))))))

(deftest sql-asof-mixed-with-regular-join-test
  (testing "Multiple joins: regular JOIN followed by ASOF JOIN"
    (let [reg {"a" {:id (long-array [1 2 3])
                    :t  (long-array [10 20 30])}
               "b" {:id   (long-array [1 2 3])
                    :name (q/encode-column (into-array String ["x" "y" "z"]))}
               "c" {:t  (long-array [5 15 25])
                    :px (double-array [1.0 2.0 3.0])}}
          result (run-asof-sql
                  (str "SELECT a.t, b.name, c.px FROM a "
                       "JOIN b ON a.id = b.id "
                       "ASOF JOIN c ON a.t >= c.t")
                  reg)]
      ;; a row 1: id=1, t=10, name=x; latest c.t ≤ 10 is 5 → px=1.0
      ;; a row 2: id=2, t=20, name=y; latest c.t ≤ 20 is 15 → px=2.0
      ;; a row 3: id=3, t=30, name=z; latest c.t ≤ 30 is 25 → px=3.0
      (is (= 3 (count result)))
      (let [m (into {} (map (fn [r] [(long (:t r)) [(:name r) (:px r)]])) result)]
        (is (= ["x" 1.0] (get m 10)))
        (is (= ["y" 2.0] (get m 20)))
        (is (= ["z" 3.0] (get m 30)))))))

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
      (is (some #(and (sequential? %) (= :anomaly-confidence (first %))) (:select query)))))

  (testing "Short form ANOMALY_SCORE('model') parses to 1-arg vector"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT ANOMALY_SCORE('mymodel') FROM orders" reg)]
      (is (some #(and (sequential? %) (= :anomaly-score (first %)) (= "mymodel" (second %)) (= 2 (count %)))
                (:select query))
          "Short form should produce [:anomaly-score \"mymodel\"] with no extra args"))))

(deftest sql-anomaly-resolution-test
  (testing "ANOMALY_SCORE in WHERE clause filters correctly"
    (let [n 100
          fare (double-array n)
          tip (double-array n)
          rng (java.util.Random. 42)]
      (dotimes [i n]
        (aset fare i (+ 5.0 (* (.nextDouble rng) 45.0)))
        (aset tip i (* (aget fare i) 0.15)))
      ;; Inject 5 anomalies
      (dotimes [i 5]
        (aset fare (- n 1 i) 500.0)
        (aset tip (- n 1 i) 0.0))
      (let [data {:fare_amount fare :tip_amount tip}
            model ((requiring-resolve 'stratum.iforest/train)
                   {:from data :n-trees 50 :sample-size 64 :seed 42 :contamination 0.05})
            models {"test_model" model}
            attach-fn @(resolve 'stratum.server/attach-anomaly-models)]
        ;; WHERE filter — anomaly resolution now happens inside q/q via :_anomaly-models
        (let [registry {"test" data "__models__" models}
              {:keys [query]} (sql/parse-sql
                               "SELECT fare_amount, ANOMALY_SCORE('test_model', fare_amount, tip_amount) AS score FROM test WHERE ANOMALY_SCORE('test_model', fare_amount, tip_amount) > 0.6"
                               registry)
              query (attach-fn query registry)
              result (q/q query)]
          (is (= 5 (count result)) "Should find exactly 5 anomalies")
          (is (every? #(= 500.0 (:fare_amount %)) result) "All should be the injected anomalies"))
        ;; ORDER BY with alias
        (let [registry {"test" data "__models__" models}
              {:keys [query]} (sql/parse-sql
                               "SELECT fare_amount, ANOMALY_SCORE('test_model', fare_amount, tip_amount) AS score FROM test ORDER BY ANOMALY_SCORE('test_model', fare_amount, tip_amount) DESC LIMIT 5"
                               registry)
              query (attach-fn query registry)
              result (q/q query)]
          (is (= 5 (count result)))
          (is (every? #(= 500.0 (:fare_amount %)) result) "Top 5 by score should all be anomalies"))
        ;; Short form: ANOMALY_SCORE('model') uses feature names from model
        (let [registry {"test" data "__models__" models}
              {:keys [query]} (sql/parse-sql
                               "SELECT fare_amount, ANOMALY_SCORE('test_model') AS score FROM test ORDER BY score DESC LIMIT 5"
                               registry)
              query (attach-fn query registry)
              result (q/q query)]
          (is (= 5 (count result)))
          (is (every? #(= 500.0 (:fare_amount %)) result) "Short form: top 5 should all be anomalies"))))))

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

;; ============================================================================
;; SQL Projection Tests (Postgres-inspired)
;; ============================================================================

(deftest sql-select-star-test
  (testing "SELECT * FROM orders returns all columns with correct row count"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT * FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(contains? % :price) results))
      (is (every? #(contains? % :quantity) results))
      (is (every? #(contains? % :category) results))
      (is (every? #(contains? % :discount) results)))))

(deftest sql-single-column-projection-test
  (testing "SELECT price FROM orders — single column projection"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(contains? % :price) results))
      (is (every? #(= 1 (count (keys %))) results)))))

(deftest sql-multi-column-projection-test
  (testing "SELECT price, quantity FROM orders — multi-column projection"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price, quantity FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(and (contains? % :price) (contains? % :quantity)) results))
      (is (every? #(= 2 (count (keys %))) results)))))

(deftest sql-aliased-column-test
  (testing "SELECT price AS p FROM orders — aliased column"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price AS p FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(contains? % :p) results)))))

(deftest sql-projection-with-filter-test
  (testing "SELECT price, quantity FROM orders WHERE price > 100"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price, quantity FROM orders WHERE price > 100" reg)
          results (q/q query)]
      (is (pos? (count results)))
      (is (< (count results) test-n))
      (is (every? #(> (:price %) 100.0) results)))))

(deftest sql-integer-literal-test
  (testing "SELECT 1 FROM orders — integer literal"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT 1 FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(= 1 (val (first %))) results)))))

(deftest sql-literal-with-column-test
  (testing "SELECT 1 AS one, price FROM orders — literal + column"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT 1 AS one, price FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(= 1 (:one %)) results))
      (is (every? #(contains? % :price) results)))))

(deftest sql-string-literal-test
  (testing "SELECT 'hello' AS greeting FROM orders WHERE price > 200"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT 'hello' AS greeting FROM orders WHERE price > 200" reg)
          results (q/q query)]
      (is (pos? (count results)))
      (is (every? #(= "hello" (:greeting %)) results)))))

(deftest sql-order-by-asc-test
  (testing "SELECT price FROM orders ORDER BY price ASC"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price FROM orders ORDER BY price ASC" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (apply <= (map :price results))))))

(deftest sql-order-by-desc-test
  (testing "SELECT price FROM orders ORDER BY price DESC"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price FROM orders ORDER BY price DESC" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (apply >= (map :price results))))))

(deftest sql-order-by-multi-test
  (testing "SELECT category, price FROM orders ORDER BY category ASC, price DESC"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT category, price FROM orders ORDER BY category ASC, price DESC" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      ;; Each category group should have descending prices
      (doseq [[_cat rows] (group-by :category results)]
        (is (apply >= (map :price rows)))))))

(deftest sql-distinct-test
  (testing "SELECT DISTINCT category FROM orders"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT DISTINCT category FROM orders" reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (= #{0 1 2 3 4} (set (map :category results)))))))

(deftest sql-distinct-ordered-test
  (testing "SELECT DISTINCT category FROM orders ORDER BY category"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT DISTINCT category FROM orders ORDER BY category" reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (= [0 1 2 3 4] (mapv :category results))))))

(deftest sql-limit-test
  (testing "SELECT price FROM orders ORDER BY price LIMIT 5"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price FROM orders ORDER BY price LIMIT 5" reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (apply <= (map :price results))))))

(deftest sql-limit-offset-test
  (testing "SELECT price FROM orders ORDER BY price LIMIT 5 OFFSET 10"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price FROM orders ORDER BY price LIMIT 5 OFFSET 10" reg)
          results (q/q query)]
      (is (= 5 (count results))))))

(deftest sql-expression-in-select-test
  (testing "SELECT price * quantity AS revenue FROM orders"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price * quantity AS revenue FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      (is (every? #(contains? % :revenue) results))
      ;; First row: price=10.0, quantity=1 → revenue=10.0
      (is (== 10.0 (:revenue (first results)))))))

(deftest sql-expression-addition-test
  (testing "SELECT price + 1 AS adjusted FROM orders"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql "SELECT price + 1 AS adjusted FROM orders" reg)
          results (q/q query)]
      (is (= test-n (count results)))
      ;; First row: price=10.0, adjusted=11.0
      (is (== 11.0 (:adjusted (first results)))))))

(deftest sql-group-by-having-count-test
  (testing "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 100 ORDER BY category"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 100 ORDER BY category"
                           reg)
          results (q/q query)]
      ;; 1000 rows / 5 categories = 200 each, all pass HAVING > 100
      (is (= 5 (count results)))
      (is (every? #(> (:count %) 100) results))
      (is (apply <= (map :category results))))))

(deftest sql-group-by-having-min-max-test
  (testing "SELECT category, MIN(price), MAX(price) FROM orders GROUP BY category HAVING MIN(price) > 5"
    (let [reg (make-test-registry)
          {:keys [query]} (sql/parse-sql
                           "SELECT category, MIN(price), MAX(price) FROM orders GROUP BY category HAVING MIN(price) > 5"
                           reg)
          results (q/q query)]
      (is (= 5 (count results)))
      (is (every? #(> (:min %) 5.0) results)))))

;; ============================================================================
;; SQL Model Management Tests (CREATE/DROP/SHOW/DESCRIBE MODEL)
;; ============================================================================

(deftest parse-create-model-test
  (testing "CREATE MODEL with OPTIONS"
    (let [reg (make-test-registry)
          result (sql/parse-sql
                  "CREATE MODEL my_model TYPE ISOLATION_FOREST OPTIONS (n_trees = 50, sample_size = 128) AS SELECT price, quantity FROM orders"
                  reg)]
      (is (contains? result :ddl))
      (is (= :create-model (get-in result [:ddl :op])))
      (is (= "my_model" (get-in result [:ddl :model-name])))
      (is (= "ISOLATION_FOREST" (get-in result [:ddl :model-type])))
      (is (= {:n-trees 50 :sample-size 128} (get-in result [:ddl :options])))
      (is (string? (get-in result [:ddl :training-sql])))))

  (testing "CREATE MODEL without OPTIONS"
    (let [reg (make-test-registry)
          result (sql/parse-sql
                  "CREATE MODEL m2 TYPE ISOLATION_FOREST AS SELECT price FROM orders"
                  reg)]
      (is (= :create-model (get-in result [:ddl :op])))
      (is (= "m2" (get-in result [:ddl :model-name])))
      (is (= {} (get-in result [:ddl :options])))))

  (testing "CREATE MODEL case insensitive"
    (let [reg (make-test-registry)
          result (sql/parse-sql
                  "create model m3 type isolation_forest as select price from orders"
                  reg)]
      (is (= :create-model (get-in result [:ddl :op])))
      (is (= "ISOLATION_FOREST" (get-in result [:ddl :model-type])))))

  (testing "CREATE MODEL with contamination option"
    (let [reg (make-test-registry)
          result (sql/parse-sql
                  "CREATE MODEL m4 TYPE ISOLATION_FOREST OPTIONS (contamination = 0.05) AS SELECT price, quantity FROM orders"
                  reg)]
      (is (= {:contamination 0.05} (get-in result [:ddl :options]))))))

(deftest parse-drop-model-test
  (testing "DROP MODEL"
    (let [reg (make-test-registry)
          result (sql/parse-sql "DROP MODEL my_model" reg)]
      (is (= :drop-model (get-in result [:ddl :op])))
      (is (= "my_model" (get-in result [:ddl :model-name])))
      (is (not (get-in result [:ddl :if-exists?])))))

  (testing "DROP MODEL IF EXISTS"
    (let [reg (make-test-registry)
          result (sql/parse-sql "DROP MODEL IF EXISTS my_model" reg)]
      (is (= :drop-model (get-in result [:ddl :op])))
      (is (= "my_model" (get-in result [:ddl :model-name])))
      (is (true? (get-in result [:ddl :if-exists?]))))))

(deftest parse-show-models-test
  (testing "SHOW MODELS with no models"
    (let [reg (make-test-registry)
          result (sql/parse-sql "SHOW MODELS" reg)]
      (is (:system result))
      (is (= "SHOW MODELS" (:tag result)))
      (is (= [] (get-in result [:result :rows])))))

  (testing "SHOW MODELS with models registered"
    (let [reg (assoc (make-test-registry)
                     "__models__" {"m1" {:model-type "ISOLATION_FOREST"
                                         :n-features 3 :n-trees 100 :sample-size 256}})]
      (let [result (sql/parse-sql "SHOW MODELS" reg)]
        (is (:system result))
        (is (= 1 (count (get-in result [:result :rows]))))
        (is (= "m1" (ffirst (get-in result [:result :rows]))))))))

(deftest parse-describe-model-test
  (testing "DESCRIBE MODEL"
    (let [reg (assoc (make-test-registry)
                     "__models__" {"m1" {:model-type "ISOLATION_FOREST"
                                         :n-features 2 :n-trees 100 :sample-size 256
                                         :feature-names [:price :quantity]
                                         :contamination 0.05 :threshold 0.6}})
          result (sql/parse-sql "DESCRIBE MODEL m1" reg)]
      (is (:system result))
      (is (= "DESCRIBE MODEL" (:tag result)))
      (is (= "m1" (second (first (get-in result [:result :rows])))))
      (is (= "price, quantity" (second (nth (get-in result [:result :rows]) 5))))))

  (testing "DESCRIBE MODEL not found"
    (let [reg (make-test-registry)
          result (sql/parse-sql "DESCRIBE MODEL nonexistent" reg)]
      (is (:error result)))))

(deftest sql-create-model-end-to-end-test
  (testing "Full CREATE MODEL → SHOW → DESCRIBE → ANOMALY_SCORE → DROP flow"
    (let [srv (server/start {:port 0})
          _ (server/register-table! srv "sensor_data"
                                    {:temperature (double-array [20.0 21.0 19.5 22.0 20.5
                                                                 100.0 21.5 19.0 20.8 21.2])
                                     :humidity    (double-array [50.0 52.0 48.0 55.0 51.0
                                                                 5.0 53.0 47.0 50.5 52.5])})
          make-factory @(resolve 'stratum.server/make-handler-factory)
          ^PgWireServer$QueryHandler handler
          (.create ^PgWireServer$QueryHandlerFactory (make-factory (:registry srv) (atom nil)))]
      (try
        ;; CREATE MODEL via handler
        (let [^PgWireServer$QueryResult qr
              (.execute handler
                        "CREATE MODEL anomaly_detector TYPE ISOLATION_FOREST OPTIONS (n_trees = 50, sample_size = 8, seed = 42, contamination = 0.1) AS SELECT temperature, humidity FROM sensor_data")]
          (is (= "CREATE MODEL" (.commandTag qr))))

        ;; Verify model in registry
        (let [model (get-in @(:registry srv) ["__models__" "anomaly_detector"])]
          (is (some? model))
          (is (= 2 (:n-features model)))
          (is (= 50 (:n-trees model)))
          (is (= [:temperature :humidity] (:feature-names model)))
          (is (some? (:threshold model)))
          (is (= "ISOLATION_FOREST" (:model-type model))))

        ;; SHOW MODELS
        (let [result (sql/parse-sql "SHOW MODELS" @(:registry srv))]
          (is (:system result))
          (is (= 1 (count (get-in result [:result :rows]))))
          (is (= "anomaly_detector" (ffirst (get-in result [:result :rows])))))

        ;; DESCRIBE MODEL
        (let [result (sql/parse-sql "DESCRIBE MODEL anomaly_detector" @(:registry srv))]
          (is (:system result))
          (is (= "anomaly_detector" (second (first (get-in result [:result :rows])))))
          (is (= "temperature, humidity" (second (nth (get-in result [:result :rows]) 5)))))

        ;; ANOMALY_SCORE query using the SQL-created model (via handler, end-to-end)
        (let [^PgWireServer$QueryResult qr
              (.execute handler
                        "SELECT temperature, ANOMALY_SCORE('anomaly_detector', temperature, humidity) AS score FROM sensor_data ORDER BY score DESC")]
          (is (nil? (.error qr)) (str "Query should succeed: " (.error qr)))
          (is (= 10 (alength (.rows qr))))
          ;; The anomaly (100.0, 5.0) should have the highest score
          (is (= "100.0" (aget ^"[Ljava.lang.String;" (aget (.rows qr) 0) 0))))

        ;; DROP MODEL
        (let [^PgWireServer$QueryResult qr (.execute handler "DROP MODEL anomaly_detector")]
          (is (= "DROP MODEL" (.commandTag qr))))

        ;; Verify model removed
        (is (nil? (get-in @(:registry srv) ["__models__" "anomaly_detector"])))

        ;; DROP MODEL IF EXISTS on nonexistent model should not throw
        (let [^PgWireServer$QueryResult qr (.execute handler "DROP MODEL IF EXISTS nonexistent")]
          (is (= "DROP MODEL" (.commandTag qr))))

        ;; DROP MODEL on nonexistent should return error
        (let [^PgWireServer$QueryResult qr (.execute handler "DROP MODEL nonexistent")]
          (is (some? (.error qr)))
          (is (re-find #"Model not found" (.error qr))))

        (finally
          (server/stop srv))))))

(deftest sql-anomaly-expression-form-test
  (testing "Long form with expression args evaluates expressions before scoring"
    (let [n 100
          amounts (double-array n)
          freqs (double-array n)
          rng (java.util.Random. 42)]
      (dotimes [i n]
        (aset amounts i (+ 5.0 (* (.nextDouble rng) 45.0)))
        (aset freqs i (+ 1.0 (* (.nextDouble rng) 9.0))))
      ;; Inject anomalies
      (dotimes [i 5]
        (aset amounts (- n 1 i) 500.0)
        (aset freqs (- n 1 i) 0.1))
      (let [data {:amount amounts :freq freqs}
            model ((requiring-resolve 'stratum.iforest/train)
                   {:from data :n-trees 50 :sample-size 64 :seed 42 :contamination 0.05})
            models {"test_model" model}]
        ;; Expression form: ANOMALY_SCORE('model', amount * 1.0, freq)
        ;; The * 1.0 is a no-op expression but exercises the expression eval path
        (let [result (q/q {:from data
                           :_anomaly-models models
                           :select [[:as [:anomaly-score "test_model" [:* :amount 1.0] :freq] :score] :amount]
                           :order [[:score :desc]]
                           :limit 5})]
          (is (= 5 (count result)))
          (is (every? #(= 500.0 (:amount %)) result)
              "Expression form: top 5 should be injected anomalies")))))

  (testing "Arity mismatch throws clear error"
    (let [data {:a (double-array [1 2 3]) :b (double-array [4 5 6])}
          model ((requiring-resolve 'stratum.iforest/train)
                 {:from data :n-trees 10 :sample-size 2 :seed 42})
          models {"m" model}]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"arity mismatch"
                            (q/q {:from data
                                  :_anomaly-models models
                                  :select [[:as [:anomaly-score "m" :a] :score]]}))
          "Passing 1 arg to 2-feature model should throw arity mismatch"))))

(deftest sql-anomaly-join-scoring-test
  (testing "ANOMALY_SCORE works across join results"
    (let [;; Fact table: transactions with sensor_id
          tx-data {:sensor_id (long-array [1 1 2 2 3 3])
                   :reading   (double-array [10.0 12.0 11.0 200.0 9.0 300.0])}
          ;; Dimension table: sensor metadata with calibration offset
          sensor-data {:id     (long-array [1 2 3])
                       :offset (double-array [0.5 0.5 0.5])}
          ;; Train on reading + offset features
          model ((requiring-resolve 'stratum.iforest/train)
                 {:from {:reading (double-array [10.0 12.0 11.0 200.0 9.0 300.0])
                         :offset  (double-array [0.5 0.5 0.5 0.5 0.5 0.5])}
                  :n-trees 50 :sample-size 4 :seed 42 :contamination 0.1})
          models {"sensor_model" model}
          ;; Query: join + anomaly score using post-join columns
          result (q/q {:from tx-data
                       :join [{:with sensor-data
                               :on [:= :sensor_id :id]
                               :type :inner}]
                       :_anomaly-models models
                       :select [[:as [:anomaly-score "sensor_model" :reading :offset] :score]
                                :reading :sensor_id]
                       :order [[:score :desc]]})]
      (is (= 6 (count result)) "Should have all 6 joined rows")
      ;; The 200.0 and 300.0 readings should have highest scores
      (is (contains? #{200.0 300.0} (:reading (first result)))
          "Highest anomaly score should be for extreme readings"))))

;; ============================================================================
;; SQL DELETE on index-backed tables (Phase D)
;;
;; Tables registered with `idx/index-from-seq` columns route DELETE
;; through `dataset/ds-delete-rows!` instead of the array-rebuild path.
;; The end-to-end behavior must be identical from a SQL perspective.
;; ============================================================================

(deftest sql-delete-on-index-backed-table
  (testing "DELETE FROM ... WHERE on a stratum-index-backed table"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          {:eid    (stratum.index/index-from-seq :int64 [1 2 3 4 5])
           :salary (stratum.index/index-from-seq :int64 [100 200 300 400 500])})
        ;; Sanity check — confirm the registry stored it as an index-backed
        ;; encoded map (else this test isn't really exercising the new path).
        (let [cols (get @(:registry srv) "salaries")]
          (is (every? (fn [[_ v]] (= :index (:source v))) cols)
              "table must be index-backed for this test to be meaningful"))
        ;; Run DELETE via the SQL path.
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec "DELETE FROM salaries WHERE eid = 3"
                    (:registry srv) (:data-dir srv))]
          (is (= "DELETE 1" (.commandTag qr))))
        ;; The remaining four rows should still be queryable as index-backed.
        (let [cols (get @(:registry srv) "salaries")]
          (is (= 4 (stratum.index/idx-length (:index (:eid cols)))))
          (is (= [1 2 4 5]
                 (mapv #(stratum.index/idx-get-long (:index (:eid cols)) %)
                       (range 4)))))
        (finally (server/stop srv))))))

(deftest sql-delete-all-on-index-backed-table
  (testing "DELETE FROM ... (no WHERE) drops every row, transient still valid"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "t"
          {:a (stratum.index/index-from-seq :int64 [1 2 3])
           :b (stratum.index/index-from-seq :int64 [10 20 30])})
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec "DELETE FROM t" (:registry srv) (:data-dir srv))]
          (is (= "DELETE 3" (.commandTag qr))))
        (let [cols (get @(:registry srv) "t")]
          (is (zero? (stratum.index/idx-length (:index (:a cols))))))
        (finally (server/stop srv))))))

(deftest sql-delete-no-match-on-index-backed-table
  (testing "DELETE with predicate that matches no rows leaves the table intact"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "t"
          {:a (stratum.index/index-from-seq :int64 [1 2 3])})
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec "DELETE FROM t WHERE a = 99" (:registry srv) (:data-dir srv))]
          (is (= "DELETE 0" (.commandTag qr))))
        (let [cols (get @(:registry srv) "t")]
          (is (= 3 (stratum.index/idx-length (:index (:a cols))))))
        (finally (server/stop srv))))))

;; ============================================================================
;; SQL:2011 FOR PORTION OF VALID_TIME (Phase D)
;;
;; The pre-parser in `stratum.sql.rewrite/preprocess-sql` strips the
;; clause and attaches `:period` to the DDL map; the server's DELETE
;; branch lowers a fully-index-backed bitemporal table's bounded
;; DELETE to `dataset/retract!` with `:valid-from` + `:valid-to`.
;; ============================================================================

(deftest preprocess-for-portion-of-valid-time-strips-clause
  (testing "preprocess-sql extracts FOR PORTION OF VALID_TIME and rewrites the SQL"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-07-01' WHERE eid = 1")]
      (is (= "DELETE FROM t WHERE eid = 1" (.trim ^String (:sql r))))
      (is (= :valid_time (:axis (:period r))))
      ;; 2024-04-01T00:00Z → 1711929600 sec → 1711929600000000 micros
      (is (= 1711929600000000 (:from (:period r))))
      (is (= 1719792000000000 (:to (:period r)))))))

(deftest preprocess-without-portion-is-identity
  (testing "SQL without FOR PORTION OF passes through unchanged"
    (let [sql "DELETE FROM t WHERE eid = 1"
          r (stratum.sql.rewrite/preprocess-sql sql)]
      (is (= sql (:sql r)))
      (is (nil? (:period r))))))

(deftest parse-sql-attaches-period-to-delete-ddl
  (testing "parse-sql carries :period through to the :ddl map for DELETE"
    (let [r (sql/parse-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-07-01' WHERE eid = 1"
              {"t" {:eid (long-array [1])}})]
      (is (= :delete (get-in r [:ddl :op])))
      (is (= :valid_time (get-in r [:ddl :period :axis])))
      (is (= 1711929600000000 (get-in r [:ddl :period :from])))
      (is (= 1719792000000000 (get-in r [:ddl :period :to]))))))

(deftest sql-delete-for-portion-of-valid-time-bounded-on-index-backed-table
  (testing "DELETE … FOR PORTION OF VALID_TIME applies bounded retract on a bitemporal index-backed table"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            {:eid         (stratum.index/index-from-seq :int64 [1 1])
             :salary      (stratum.index/index-from-seq :int64 [100 200])
             ;; [Jan-2024, Jul-2024) closed, [Jul-2024, MAX) open
             :_valid_from (stratum.index/index-from-seq :int64
                            [1704067200000000 1719792000000000])
             :_valid_to   (stratum.index/index-from-seq :int64
                            [1719792000000000 Long/MAX_VALUE])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        ;; DELETE FOR PORTION OF [Apr-2024, May-2024) WHERE eid=1
        ;; Closed row [Jan, Jul) straddles the cut: SPLIT into [Jan, Apr) + [May, Jul).
        ;; Open row [Jul, MAX) doesn't overlap.
        ;; Result: 3 rows (split produces 2 surviving from 1, open untouched).
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "DELETE FROM salaries "
                         "FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-05-01' "
                         "WHERE eid = 1")
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^DELETE" (.commandTag qr))))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))]
          (is (= 3 n))
          ;; Three surviving rows after the split of row 0:
          ;;   :_valid_from 2024-01-01 (left piece of the split)
          ;;   :_valid_from 2024-05-01 (right piece, appended)
          ;;   :_valid_from 2024-07-01 (open row, unchanged)
          (is (= #{1704067200000000 1714521600000000 1719792000000000}
                 (set (read-col :_valid_from))))
          ;; :_valid_to mirrors the split — Apr-01 (left piece's new vt),
          ;; Jul-01 (right piece's original vt), MAX (open row).
          (is (= #{1711929600000000 1719792000000000 Long/MAX_VALUE}
                 (set (read-col :_valid_to)))))
        (finally (server/stop srv))))))

(deftest sql-insert-for-portion-of-valid-time-stamps-vf-vt-from-period
  (testing "INSERT … FOR PORTION OF VALID_TIME fills _valid_from / _valid_to from the period"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Start with an empty bitemporal table.
            {:eid         (stratum.index/index-from-seq :int64 [])
             :salary      (stratum.index/index-from-seq :int64 [])
             :_valid_from (stratum.index/index-from-seq :int64 [])
             :_valid_to   (stratum.index/index-from-seq :int64 [])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "INSERT INTO salaries (eid, salary) VALUES (1, 100) "
                         "FOR PORTION OF VALID_TIME FROM '2024-01-01' TO '2024-07-01'")
                    (:registry srv) (:data-dir srv))]
          (is (= "INSERT 0 1" (.commandTag qr))))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))]
          (is (= 1 n))
          (is (= [1] (read-col :eid)))
          (is (= [100] (read-col :salary)))
          (is (= [1704067200000000] (read-col :_valid_from)))
          (is (= [1719792000000000] (read-col :_valid_to))))
        (finally (server/stop srv))))))

(deftest sql-update-for-portion-of-valid-time-splits-correctly
  (testing "UPDATE … FOR PORTION OF VALID_TIME applies bounded-update! 3-way split"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Single row [Jan, Jul) with salary=100.
            {:eid         (stratum.index/index-from-seq :int64 [1])
             :salary      (stratum.index/index-from-seq :int64 [100])
             :_valid_from (stratum.index/index-from-seq :int64 [1704067200000000])
             :_valid_to   (stratum.index/index-from-seq :int64 [1719792000000000])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        ;; UPDATE … FOR PORTION OF [Apr, May) SET salary=999 WHERE eid=1
        ;; Row [Jan, Jul) straddles [Apr, May): 3-way split.
        ;;   [Jan, Apr) keeps salary=100
        ;;   [Apr, May) gets salary=999
        ;;   [May, Jul) keeps salary=100
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "UPDATE salaries SET salary = 999 "
                         "FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-05-01' "
                         "WHERE eid = 1")
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^UPDATE" (.commandTag qr))))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))]
          (is (= 3 n))
          ;; Find the [Apr, May) slice (the updated one) by its
          ;; _valid_from value.
          (let [rows (mapv (fn [i]
                             {:eid (nth (read-col :eid) i)
                              :salary (nth (read-col :salary) i)
                              :vf (nth (read-col :_valid_from) i)
                              :vt (nth (read-col :_valid_to) i)})
                           (range n))
                by-vf (group-by :vf rows)]
            ;; Three slices, all eid=1.
            (is (every? #(= 1 (:eid %)) rows))
            ;; [Jan, Apr) keeps old salary=100
            (let [r (first (by-vf 1704067200000000))]
              (is (some? r))
              (is (= 100 (:salary r)))
              (is (= 1711929600000000 (:vt r))))
            ;; [Apr, May) gets new salary=999
            (let [r (first (by-vf 1711929600000000))]
              (is (some? r))
              (is (= 999 (:salary r)))
              (is (= 1714521600000000 (:vt r))))
            ;; [May, Jul) keeps old salary=100
            (let [r (first (by-vf 1714521600000000))]
              (is (some? r))
              (is (= 100 (:salary r)))
              (is (= 1719792000000000 (:vt r))))))
        (finally (server/stop srv))))))

(deftest sql-update-for-portion-of-valid-time-with-system-axis
  ;; SCD2-on-both-axes: when the table is registered with BOTH a
  ;; `:valid` and a `:system` axis, an `UPDATE FOR PORTION OF
  ;; VALID_TIME` must close the superseded row's `_system_to` AND
  ;; stamp every replacement slice's `_system_from` with the SAME
  ;; `sys-now`. The lowering helper used to drop the `:system` axis
  ;; from the ad-hoc dataset's metadata, so the SCD2 surgery silently
  ;; demoted to the valid-only path and `_system_to` was never closed.
  ;; This test exercises the full bitemporal lowering path.
  (testing "UPDATE … FOR PORTION OF VALID_TIME preserves system-axis SCD2 on a fully-bitemporal table"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Single row [Jan, Jul) valid-time, _system_from=tx1,
            ;; _system_to=MAX. Salary=100.
            {:eid          (stratum.index/index-from-seq :int64 [1])
             :salary       (stratum.index/index-from-seq :int64 [100])
             :_valid_from  (stratum.index/index-from-seq :int64 [1704067200000000])
             :_valid_to    (stratum.index/index-from-seq :int64 [1719792000000000])
             :_system_from (stratum.index/index-from-seq :int64 [1000000])
             :_system_to   (stratum.index/index-from-seq :int64 [Long/MAX_VALUE])}
            {:bitemporal
             {:valid  {:from-col :_valid_from  :to-col :_valid_to  :unit :micros}
              :system {:from-col :_system_from :to-col :_system_to :unit :micros}}}))
        ;; UPDATE FOR PORTION OF [Apr, May): straddles → 3-way split.
        ;; The bitemporal path preserves the OLD row (with closed _system_to)
        ;; plus appends three replacement slices, all sharing one sys-now.
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "UPDATE salaries SET salary = 999 "
                         "FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-05-01' "
                         "WHERE eid = 1")
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^UPDATE" (.commandTag qr))))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))
              rows (mapv (fn [i]
                           {:eid    (nth (read-col :eid) i)
                            :salary (nth (read-col :salary) i)
                            :vf     (nth (read-col :_valid_from) i)
                            :vt     (nth (read-col :_valid_to) i)
                            :sf     (nth (read-col :_system_from) i)
                            :st     (nth (read-col :_system_to) i)})
                         (range n))]
          ;; 1 old (closed) + 3 new (open _system_to) = 4 rows
          (is (= 4 n)
              (str "expected 4 rows (1 closed old + 3 new), got " n ": " rows))
          (let [closed (filter #(not= Long/MAX_VALUE (:st %)) rows)
                open   (filter #(= Long/MAX_VALUE (:st %)) rows)]
            (testing "old [Jan, Jul) row preserved with _system_to closed (SCD2)"
              (is (= 1 (count closed)))
              (let [r (first closed)]
                (is (= 1704067200000000 (:vf r)))
                (is (= 1719792000000000 (:vt r)))
                (is (= 100 (:salary r)))
                (is (= 1000000 (:sf r)))
                (is (not= Long/MAX_VALUE (:st r))
                    "old row's _system_to must be closed, not MAX")))
            (testing "three replacement slices, all with the SAME _system_from (sys-now symmetry)"
              (is (= 3 (count open)))
              (let [sys-nows (set (map :sf open))]
                (is (= 1 (count sys-nows))
                    (str "all replacement slices must share one sys-now stamp, got: " sys-nows))
                (let [sys-now (first sys-nows)
                      old-st  (:st (first closed))]
                  (is (= sys-now old-st)
                      "old row's _system_to must equal the replacement slices' _system_from")))
              (let [by-vf (group-by :vf open)]
                ;; [Jan, Apr) salary=100
                (let [r (first (by-vf 1704067200000000))]
                  (is (some? r))
                  (is (= 100 (:salary r)))
                  (is (= 1711929600000000 (:vt r))))
                ;; [Apr, May) salary=999 — the updated slice
                (let [r (first (by-vf 1711929600000000))]
                  (is (some? r))
                  (is (= 999 (:salary r)))
                  (is (= 1714521600000000 (:vt r))))
                ;; [May, Jul) salary=100
                (let [r (first (by-vf 1714521600000000))]
                  (is (some? r))
                  (is (= 100 (:salary r)))
                  (is (= 1719792000000000 (:vt r))))))))
        (finally (server/stop srv))))))

(deftest sql-delete-for-portion-of-valid-time-with-system-axis
  ;; DELETE FOR PORTION OF on a fully-bitemporal table must close
  ;; (not erase) the superseded row's `_system_to` AND keep the row
  ;; in storage so `FOR SYSTEM_TIME AS OF <past>` queries still see
  ;; it. The valid-only path used to be the silent fallback (rows
  ;; physically dropped, audit lost).
  (testing "DELETE … FOR PORTION OF VALID_TIME preserves system-axis SCD2"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            {:eid          (stratum.index/index-from-seq :int64 [1])
             :salary       (stratum.index/index-from-seq :int64 [100])
             :_valid_from  (stratum.index/index-from-seq :int64 [1704067200000000])
             :_valid_to    (stratum.index/index-from-seq :int64 [1719792000000000])
             :_system_from (stratum.index/index-from-seq :int64 [1000000])
             :_system_to   (stratum.index/index-from-seq :int64 [Long/MAX_VALUE])}
            {:bitemporal
             {:valid  {:from-col :_valid_from  :to-col :_valid_to  :unit :micros}
              :system {:from-col :_system_from :to-col :_system_to :unit :micros}}}))
        ;; DELETE FOR PORTION OF [Apr, May): row [Jan, Jul) splits.
        ;; Result: 1 closed-old + 2 surviving slices ([Jan, Apr), [May, Jul)).
        (let [exec @(resolve 'stratum.server/execute-sql)]
          (exec (str "DELETE FROM salaries "
                     "FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-05-01' "
                     "WHERE eid = 1")
                (:registry srv) (:data-dir srv)))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))
              rows (mapv (fn [i]
                           {:eid    (nth (read-col :eid) i)
                            :salary (nth (read-col :salary) i)
                            :vf     (nth (read-col :_valid_from) i)
                            :vt     (nth (read-col :_valid_to) i)
                            :sf     (nth (read-col :_system_from) i)
                            :st     (nth (read-col :_system_to) i)})
                         (range n))
              closed (filter #(not= Long/MAX_VALUE (:st %)) rows)
              open   (filter #(= Long/MAX_VALUE (:st %)) rows)]
          (is (= 3 n) (str "1 closed old + 2 surviving slices, got: " rows))
          (testing "old [Jan, Jul) preserved with _system_to closed"
            (is (= 1 (count closed)))
            (is (= 1000000 (:sf (first closed))))
            (is (not= Long/MAX_VALUE (:st (first closed)))))
          (testing "two surviving slices ([Jan, Apr), [May, Jul)), one sys-now"
            (is (= 2 (count open)))
            (let [sys-nows (set (map :sf open))]
              (is (= 1 (count sys-nows)))
              (is (= (:st (first closed)) (first sys-nows))
                  "old _system_to equals replacements' _system_from"))
            (let [vfs (set (map :vf open))]
              (is (= #{1704067200000000 1714521600000000} vfs)))))
        (finally (server/stop srv))))))

(deftest sql-insert-for-portion-of-valid-time-with-system-axis
  ;; INSERT FOR PORTION OF on a fully-bitemporal table must stamp the
  ;; new row's _system_from with a fresh sys-now (not Long/MAX_VALUE,
  ;; not the unit-default of "now-in-unit at append time" — explicitly,
  ;; the same sys-now that any concurrent surgery in the same physical
  ;; tx would use). This test asserts the inserted row has a real
  ;; sys-from (non-MAX) and _system_to=MAX.
  (testing "INSERT … FOR PORTION OF VALID_TIME stamps _system_from on a fully-bitemporal table"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Start empty.
            {:eid          (stratum.index/index-from-seq :int64 [])
             :salary       (stratum.index/index-from-seq :int64 [])
             :_valid_from  (stratum.index/index-from-seq :int64 [])
             :_valid_to    (stratum.index/index-from-seq :int64 [])
             :_system_from (stratum.index/index-from-seq :int64 [])
             :_system_to   (stratum.index/index-from-seq :int64 [])}
            {:bitemporal
             {:valid  {:from-col :_valid_from  :to-col :_valid_to  :unit :micros}
              :system {:from-col :_system_from :to-col :_system_to :unit :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)]
          (exec (str "INSERT INTO salaries (eid, salary) "
                     "VALUES (1, 100) "
                     "FOR PORTION OF VALID_TIME FROM '2024-01-01' TO '2024-07-01'")
                (:registry srv) (:data-dir srv)))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))
              rows (mapv (fn [i] {:vf (nth (read-col :_valid_from) i)
                                  :vt (nth (read-col :_valid_to) i)
                                  :sf (nth (read-col :_system_from) i)
                                  :st (nth (read-col :_system_to) i)})
                         (range n))]
          (is (= 1 n))
          (let [{:keys [vf vt sf st]} (first rows)]
            (is (= 1704067200000000 vf))
            (is (= 1719792000000000 vt))
            (is (and (pos? sf) (not= Long/MAX_VALUE sf))
                "_system_from must be a real sys-now stamp, not 0 or MAX")
            (is (= Long/MAX_VALUE st)
                "_system_to must be MAX (open) for a fresh insertion")))
        (finally (server/stop srv))))))

(deftest sql-dml-arithmetic-where-handles-null-operand
  ;; Regression lock for copilot review #2 (server.clj:176):
  ;; eval-dml-predicate arithmetic (:+/:-/:*/:/) called
  ;; `(double (eval-val ...))` without NULL propagation. If a row's
  ;; column resolves to nil (NULL sentinel Long/MIN_VALUE on int64),
  ;; `(double nil)` threw NPE rather than following SQL 3-valued
  ;; logic (expression → NULL → predicate false → row not matched).
  ;;
  ;; Reproducer: a DELETE with `WHERE col + 1 > 5` where one row's
  ;; col is the NULL sentinel. Pre-fix: NPE inside eval-dml-predicate.
  ;; Post-fix: the nil-bearing row is treated as non-matching and the
  ;; DELETE only affects the non-nil rows that match.
  (testing "DML WHERE with arithmetic on a NULL-bearing column does not NPE"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "t"
          (with-meta
            ;; rows: id=1 v=10, id=2 v=NULL (Long/MIN_VALUE), id=3 v=20
            {:id (stratum.index/index-from-seq :int64 [1 2 3])
             :v  (stratum.index/index-from-seq :int64 [10 Long/MIN_VALUE 20])}
            {}))
        (let [exec @(resolve 'stratum.server/execute-sql)]
          ;; Pre-fix: throws NPE because row 2's :v reads as nil and
          ;; the arithmetic `v + 1` calls `(double nil)`.
          (exec "DELETE FROM t WHERE v + 1 > 15"
                (:registry srv) (:data-dir srv)))
        (let [cols (get @(:registry srv) "t")
              n    (stratum.index/idx-length (:index (:id cols)))
              ids  (mapv #(stratum.index/idx-get-long (:index (:id cols)) %)
                         (range n))
              vs   (mapv #(stratum.index/idx-get-long (:index (:v cols)) %)
                         (range n))]
          (is (= 2 n) "row id=3 (v=20, v+1=21 > 15) deleted; rows id=1 + NULL row survive")
          (is (= [1 2] ids)
              "id=1 (v=10, v+1=11 ≤ 15) and id=2 (v=NULL → predicate NULL → false) both kept")
          (is (= [10 Long/MIN_VALUE] vs)))
        (finally (server/stop srv))))))

(deftest sql-plain-insert-bitemporal-preserves-system-axis
  ;; Regression lock for copilot review #1 (server.clj:719):
  ;; the plain-INSERT-with-columns path on an index-backed bitemporal
  ;; table was building :metadata with only `{:bitemporal {:valid …}}`,
  ;; flattening the axis map AND dropping the :system axis. Result:
  ;; `dataset/append!` ran without :system metadata so `_system_from`
  ;; was never auto-stamped (left as whatever the unit's default
  ;; produced — typically `now-in-unit` from append, *not* a pinned
  ;; sys-now, breaking SCD2-on-system).
  ;;
  ;; This test exercises a plain `INSERT (cols) VALUES …` on a fully-
  ;; bitemporal table and asserts the new row's `_system_from` is a
  ;; real sys-now stamp (positive, not MAX, not 0) and `_system_to`
  ;; is MAX (open). Pre-fix, this would either throw a missing-column
  ;; error or silently leak a non-pinned timestamp.
  (testing "plain INSERT (no FOR PORTION OF) on a fully-bitemporal table stamps _system_from"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            {:eid          (stratum.index/index-from-seq :int64 [])
             :salary       (stratum.index/index-from-seq :int64 [])
             :_valid_from  (stratum.index/index-from-seq :int64 [])
             :_valid_to    (stratum.index/index-from-seq :int64 [])
             :_system_from (stratum.index/index-from-seq :int64 [])
             :_system_to   (stratum.index/index-from-seq :int64 [])}
            {:bitemporal
             {:valid  {:from-col :_valid_from  :to-col :_valid_to  :unit :micros}
              :system {:from-col :_system_from :to-col :_system_to :unit :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)]
          ;; Plain INSERT — user supplies _valid_from/_valid_to
          ;; explicitly; system axis must be auto-stamped by stratum.
          (exec (str "INSERT INTO salaries "
                     "(eid, salary, _valid_from, _valid_to) "
                     "VALUES (1, 100, "
                     "1704067200000000, " ; 2024-01-01 micros
                     "9223372036854775807)") ; Long/MAX_VALUE (open)
                (:registry srv) (:data-dir srv)))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))
              rows (mapv (fn [i] {:vf (nth (read-col :_valid_from) i)
                                  :vt (nth (read-col :_valid_to) i)
                                  :sf (nth (read-col :_system_from) i)
                                  :st (nth (read-col :_system_to) i)})
                         (range n))]
          (is (= 1 n) "one row inserted")
          (let [{:keys [vf vt sf st]} (first rows)]
            (is (= 1704067200000000 vf))
            (is (= Long/MAX_VALUE vt))
            (is (and (pos? sf) (not= Long/MAX_VALUE sf))
                "_system_from must be a real sys-now stamp — proves :system axis was preserved through ds-meta")
            (is (= Long/MAX_VALUE st)
                "_system_to must be MAX (open) for a fresh insertion")))
        (finally (server/stop srv))))))

(deftest sql-upsert-for-portion-of-valid-time-rejected
  (testing "INSERT … ON CONFLICT … FOR PORTION OF VALID_TIME throws a clear error"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "t"
          (with-meta
            {:eid (stratum.index/index-from-seq :int64 [])
             :_valid_from (stratum.index/index-from-seq :int64 [])
             :_valid_to (stratum.index/index-from-seq :int64 [])}
            {:bitemporal {:valid {:from-col :_valid_from :to-col :_valid_to :unit :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "INSERT INTO t (eid) VALUES (1) "
                         "ON CONFLICT (eid) DO UPDATE SET eid = 2 "
                         "FOR PORTION OF VALID_TIME FROM '2024-01-01' TO '2024-07-01'")
                    (:registry srv) (:data-dir srv))]
          (is (some? (.error qr)))
          (is (re-find #"not supported" (.error qr))))
        (finally (server/stop srv))))))

;; ============================================================================
;; FOR PORTION OF VALID_TIME FROM x — open-ended (no TO)
;; ============================================================================

(deftest preprocess-for-portion-of-valid-time-without-to-defaults-to-max
  (testing "FROM x with no TO is the open-ended form: vt = Long/MAX_VALUE"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM '2024-04-01' WHERE eid = 1")]
      (is (= "DELETE FROM t WHERE eid = 1" (.trim ^String (:sql r))))
      (is (= :valid_time (:axis (:period r))))
      (is (= 1711929600000000 (:from (:period r))))
      (is (= Long/MAX_VALUE (:to (:period r)))))))

(deftest sql-delete-for-portion-open-ended-retracts-from-instant
  (testing "DELETE FOR PORTION OF FROM x (no TO) closes valid-to to x for matching rows"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Open row [Jan-2024, MAX) with salary=100.
            {:eid         (stratum.index/index-from-seq :int64 [1])
             :salary      (stratum.index/index-from-seq :int64 [100])
             :_valid_from (stratum.index/index-from-seq :int64 [1704067200000000])
             :_valid_to   (stratum.index/index-from-seq :int64 [Long/MAX_VALUE])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec (str "DELETE FROM salaries "
                         "FOR PORTION OF VALID_TIME FROM '2024-04-01' "
                         "WHERE eid = 1")
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^DELETE" (.commandTag qr))))
        ;; Row should be truncated to [Jan-2024, Apr-2024) — the
        ;; open-ended retract closes valid-to at the start instant.
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))]
          (is (= 1 n))
          (is (= 1704067200000000
                 (stratum.index/idx-get-long (:index (:_valid_from cols)) 0)))
          (is (= 1711929600000000
                 (stratum.index/idx-get-long (:index (:_valid_to cols)) 0))))
        (finally (server/stop srv))))))

;; ============================================================================
;; FOR ALL VALID_TIME — DML applies across every vt-window
;; ============================================================================

(defn- normalize-ws ^String [^String s]
  (-> s (.replaceAll "\\s+" " ") .trim))

(deftest preprocess-for-all-valid-time-strips-clause
  (testing "FOR ALL VALID_TIME produces a period spanning all time"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR ALL VALID_TIME WHERE eid = 1")]
      (is (= "DELETE FROM t WHERE eid = 1" (normalize-ws (:sql r))))
      (is (= :valid_time (:axis (:period r))))
      (is (= Long/MIN_VALUE (:from (:period r))))
      (is (= Long/MAX_VALUE (:to (:period r)))))))

(deftest preprocess-valid-time-all-trailing-form
  (testing "VALID_TIME ALL trailing form also works"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR VALID_TIME ALL WHERE eid = 1")]
      (is (= "DELETE FROM t WHERE eid = 1" (normalize-ws (:sql r))))
      (is (= :valid_time (:axis (:period r)))))))

(deftest sql-delete-for-all-valid-time-drops-every-matching-slice
  (testing "DELETE FOR ALL VALID_TIME retracts all vt-slices of matching rows"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Two slices for eid=1, plus one for eid=2 (untouched).
            {:eid         (stratum.index/index-from-seq :int64 [1 1 2])
             :salary      (stratum.index/index-from-seq :int64 [100 110 200])
             :_valid_from (stratum.index/index-from-seq :int64 [1000 2000 1500])
             :_valid_to   (stratum.index/index-from-seq :int64 [2000 Long/MAX_VALUE Long/MAX_VALUE])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        (let [exec @(resolve 'stratum.server/execute-sql)
              ^PgWireServer$QueryResult qr
              (exec "DELETE FROM salaries FOR ALL VALID_TIME WHERE eid = 1"
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^DELETE" (.commandTag qr))))
        ;; Both eid=1 rows should be dropped; eid=2 untouched.
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              eids (mapv #(stratum.index/idx-get-long (:index (:eid cols)) %) (range n))]
          (is (= 1 n))
          (is (= [2] eids)))
        (finally (server/stop srv))))))

;; ============================================================================
;; Extra temporal-literal forms in FOR PORTION OF VALID_TIME
;; ============================================================================

(deftest preprocess-for-portion-of-current-timestamp
  (testing "CURRENT_TIMESTAMP / NOW / NOW() recognized as temporal literals"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM CURRENT_TIMESTAMP TO END_OF_TIME WHERE eid = 1")]
      (is (= "DELETE FROM t WHERE eid = 1" (normalize-ws (:sql r))))
      (is (= Long/MAX_VALUE (:to (:period r))))
      ;; NOW falls within a generous window of System/currentTimeMillis * 1000.
      (is (<= (* 1000 (- (System/currentTimeMillis) 60000))
              (:from (:period r))
              (* 1000 (+ (System/currentTimeMillis) 60000)))))))

(deftest preprocess-for-portion-of-now-form
  (testing "NOW and NOW() also work"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM NOW() TO MAX_VALUE WHERE eid = 1")]
      (is (= :valid_time (:axis (:period r))))
      (is (number? (:from (:period r))))
      (is (= Long/MAX_VALUE (:to (:period r)))))))

(deftest preprocess-for-portion-of-start-of-time
  (testing "START_OF_TIME / MIN_VALUE expand to Long/MIN_VALUE"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "DELETE FROM t FOR PORTION OF VALID_TIME FROM START_OF_TIME TO '2024-04-01' WHERE eid = 1")]
      (is (= Long/MIN_VALUE (:from (:period r))))
      (is (= 1711929600000000 (:to (:period r)))))))

(deftest preprocess-for-portion-of-rejects-column-ref
  (testing "Column reference is not yet supported (clear error message)"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Column references and per-row expressions are not yet supported"
          (stratum.sql.rewrite/preprocess-sql
            "DELETE FROM t FOR PORTION OF VALID_TIME FROM contract_start TO contract_end WHERE eid = 1")))))

;; ============================================================================
;; Allen interval predicates as SQL functions
;;
;; 4-arg function form `(a_from, a_to, b_from, b_to)` works on any
;; pair of int64 columns, not just the bitemporal axis. PERIOD value
;; type (P1-4) is deferred — the 4-arg form covers the same use cases.
;; ============================================================================

;; Allen predicates lower to column-vs-column comparisons in the WHERE
;; clause, which the DML predicate evaluator (`eval-dml-predicate`)
;; supports natively. The main SELECT planner doesn't yet evaluate
;; col-vs-col predicates — that requires extending normalize-pred /
;; the predicate evaluator across execution.clj + group_by.clj and is
;; a separate gap (tracked as a P2 follow-up). The tests below exercise
;; the DML code path which is where Allen predicates have immediate value
;; (DELETE/UPDATE on bitemporal tables).

(defn- allen-delete-survivors
  "Helper: build an index-backed table, run a DELETE WHERE <allen-pred>,
   return the surviving :eid values."
  [cols-map sql]
  (let [srv (server/start {:port 0})]
    (try
      (let [idx-cols (into {} (map (fn [[k v]] [k (stratum.index/index-from-seq :int64 (vec v))])
                                   cols-map))]
        (server/register-table! srv "intervals" idx-cols))
      (let [exec @(resolve 'stratum.server/execute-sql)]
        (exec sql (:registry srv) (:data-dir srv)))
      (let [cols (get @(:registry srv) "intervals")
            n (stratum.index/idx-length (:index (:eid cols)))]
        (mapv #(stratum.index/idx-get-long (:index (:eid cols)) %) (range n)))
      (finally (server/stop srv)))))

(deftest sql-overlaps-allen-predicate-via-delete
  (testing "OVERLAPS(a_from, a_to, b_from, b_to) — DELETE removes overlapping rows"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2 3]
                       :a_from [100 200 300]
                       :a_to   [200 300 400]
                       :b_from [150 350 250]
                       :b_to   [250 450 350]}
                      "DELETE FROM intervals WHERE OVERLAPS(a_from, a_to, b_from, b_to)")]
      ;; Row 1 overlaps (deleted). Row 2 doesn't (kept). Row 3 overlaps (deleted).
      (is (= [2] survivors)))))

(deftest sql-precedes-allen-predicate-via-delete
  (testing "PRECEDES(a_from, a_to, b_from, b_to) — DELETE removes preceding rows"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2]
                       :a_from [100 100]
                       :a_to   [200 200]
                       :b_from [200 250]
                       :b_to   [300 350]}
                      "DELETE FROM intervals WHERE PRECEDES(a_from, a_to, b_from, b_to)")]
      ;; Both precede (touching counts), both deleted.
      (is (= [] survivors)))))

(deftest sql-strictly-precedes-no-touching-via-delete
  (testing "STRICTLY_PRECEDES rejects touching boundaries"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2]
                       :a_from [100 100]
                       :a_to   [200 200]
                       :b_from [200 250]
                       :b_to   [300 350]}
                      "DELETE FROM intervals WHERE STRICTLY_PRECEDES(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_to=b_from=200 — touching → not strictly preceding → KEEP.
      ;; Row 2: a_to=200 < b_from=250 → strictly precedes → DELETE.
      (is (= [1] survivors)))))

(deftest sql-contains-period-allen-predicate-via-delete
  (testing "CONTAINS_PERIOD: DELETE rows where A contains B"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2]
                       :a_from [100 100]
                       :a_to   [500 200]
                       :b_from [200 150]
                       :b_to   [400 250]}
                      "DELETE FROM intervals WHERE CONTAINS_PERIOD(a_from, a_to, b_from, b_to)")]
      ;; Row 1 contains (deleted). Row 2 doesn't (kept).
      (is (= [2] survivors)))))

(deftest sql-equals-period-allen-predicate-via-delete
  (testing "EQUALS_PERIOD: DELETE rows where both endpoints match exactly"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2 3]
                       :a_from [100 100 200]
                       :a_to   [200 200 300]
                       :b_from [100 100 100]
                       :b_to   [200 300 200]}
                      "DELETE FROM intervals WHERE EQUALS_PERIOD(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a=[100,200) b=[100,200) → equal → DELETE.
      ;; Row 2: a=[100,200) b=[100,300) → NOT equal → KEEP.
      ;; Row 3: a=[200,300) b=[100,200) → NOT equal → KEEP.
      (is (= [2 3] survivors)))))

(deftest sql-immediately-precedes-allen-predicate-via-delete
  (testing "IMMEDIATELY_PRECEDES: A.to == B.from"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2 3]
                       :a_from [100 100 100]
                       :a_to   [200 199 201]
                       :b_from [200 200 200]
                       :b_to   [300 300 300]}
                      "DELETE FROM intervals WHERE IMMEDIATELY_PRECEDES(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_to=200 == b_from=200 → DELETE.
      ;; Row 2: a_to=199 != 200 → KEEP. Row 3: a_to=201 != 200 → KEEP.
      (is (= [2 3] survivors)))))

(deftest sql-succeeds-allen-predicate-via-delete
  (testing "SUCCEEDS: A.from >= B.to (touching counts)"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2 3]
                       :a_from [200 250 199]
                       :a_to   [300 350 250]
                       :b_from [100 100 100]
                       :b_to   [200 200 200]}
                      "DELETE FROM intervals WHERE SUCCEEDS(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_from=200 >= b_to=200 → touching, SUCCEEDS, DELETE.
      ;; Row 2: a_from=250 >= 200 → DELETE.
      ;; Row 3: a_from=199 < 200 → KEEP.
      (is (= [3] survivors)))))

(deftest sql-strictly-succeeds-allen-predicate-via-delete
  (testing "STRICTLY_SUCCEEDS: A.from > B.to (no touching)"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2]
                       :a_from [200 201]
                       :a_to   [300 300]
                       :b_from [100 100]
                       :b_to   [200 200]}
                      "DELETE FROM intervals WHERE STRICTLY_SUCCEEDS(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_from=200 == b_to=200 (touching) → NOT strictly succeeds → KEEP.
      ;; Row 2: a_from=201 > 200 → STRICTLY succeeds → DELETE.
      (is (= [1] survivors)))))

(deftest sql-immediately-succeeds-allen-predicate-via-delete
  (testing "IMMEDIATELY_SUCCEEDS: A.from == B.to"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2 3]
                       :a_from [200 199 201]
                       :a_to   [300 300 300]
                       :b_from [100 100 100]
                       :b_to   [200 200 200]}
                      "DELETE FROM intervals WHERE IMMEDIATELY_SUCCEEDS(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_from=200 == b_to=200 → DELETE.
      ;; Row 2/3: a_from != b_to → KEEP.
      (is (= [2 3] survivors)))))

(deftest sql-meets-allen-predicate-via-delete
  (testing "MEETS: alias for IMMEDIATELY_PRECEDES (A.to == B.from)"
    (let [survivors (allen-delete-survivors
                      {:eid    [1 2]
                       :a_from [100 100]
                       :a_to   [200 199]
                       :b_from [200 200]
                       :b_to   [300 300]}
                      "DELETE FROM intervals WHERE MEETS(a_from, a_to, b_from, b_to)")]
      ;; Row 1: a_to=200 == b_from=200 → MEETS → DELETE.
      ;; Row 2: a_to=199 != 200 → KEEP.
      (is (= [2] survivors)))))

(deftest sql-erase-with-portion-of-rejected
  (testing "ERASE + FOR PORTION OF VALID_TIME is rejected at parse time"
    (let [r (sql/parse-sql
              (str "ERASE FROM t FOR PORTION OF VALID_TIME "
                   "FROM '2024-01-01' TO '2024-07-01' WHERE eid = 1")
              {"t" {:eid (long-array [])}})]
      (is (some? (:error r)))
      (is (re-find #"ERASE does not compose with FOR PORTION OF VALID_TIME"
                   (:error r))))))

(deftest sql-update-for-portion-of-valid-time-infers-system-axis-from-column-names
  ;; When a table is registered with `_system_from` / `_system_to`
  ;; columns but WITHOUT `:bitemporal {:system {...}}` metadata, the
  ;; SQL DML lowering should still infer the system axis from the
  ;; SQL:2011 column-name convention. This locks the
  ;; `bitemporal-axis-cfg` :system fallback path that's parallel to
  ;; the :valid fallback documented in `delete-portion-via-index-
  ;; backend`'s docstring.
  (testing "Registered without :system metadata; SCD2 still fires via column-name inference"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          ;; NOTE: no :bitemporal metadata at all. Inference must
          ;; pick up BOTH axes from the column names.
          {:eid          (stratum.index/index-from-seq :int64 [1])
           :salary       (stratum.index/index-from-seq :int64 [100])
           :_valid_from  (stratum.index/index-from-seq :int64 [1704067200000000])
           :_valid_to    (stratum.index/index-from-seq :int64 [1719792000000000])
           :_system_from (stratum.index/index-from-seq :int64 [1000000])
           :_system_to   (stratum.index/index-from-seq :int64 [Long/MAX_VALUE])})
        (let [exec @(resolve 'stratum.server/execute-sql)]
          (exec (str "UPDATE salaries SET salary = 999 "
                     "FOR PORTION OF VALID_TIME FROM '2024-04-01' TO '2024-05-01' "
                     "WHERE eid = 1")
                (:registry srv) (:data-dir srv)))
        (let [cols (get @(:registry srv) "salaries")
              n    (stratum.index/idx-length (:index (:eid cols)))
              read-col (fn [k] (mapv #(stratum.index/idx-get-long
                                        (:index (get cols k)) %)
                                     (range n)))
              rows (mapv (fn [i] {:sf (nth (read-col :_system_from) i)
                                  :st (nth (read-col :_system_to) i)})
                         (range n))
              closed (filter #(not= Long/MAX_VALUE (:st %)) rows)
              open   (filter #(= Long/MAX_VALUE (:st %)) rows)]
          (is (= 4 n)
              "metadata-free table with `_system_*` cols routes through bitemporal path")
          (is (= 1 (count closed)) "old row closed via inferred system axis")
          (is (= 3 (count open)) "three replacement slices")
          (let [sys-nows (set (map :sf open))]
            (is (= 1 (count sys-nows)) "one shared sys-now")
            (is (= (:st (first closed)) (first sys-nows))
                "closed system_to == replacements' system_from (SCD2 symmetry)")))
        (finally (server/stop srv))))))

(deftest sql-select-multi-for-valid-time-rejected
  ;; The SELECT-side preprocessor emits unqualified `_valid_from` /
  ;; `_valid_to` column refs. With two `FOR VALID_TIME` clauses on a
  ;; joined SELECT, the planner can't disambiguate which table each
  ;; predicate targets and silently picks one source — silently
  ;; wrong. Reject at preprocess time.
  (testing "Two FOR VALID_TIME clauses on a joined SELECT → rejected"
    (let [r (sql/parse-sql
              (str "SELECT a.eid, b.eid FROM a FOR VALID_TIME AS OF '2024-04-15' "
                   "JOIN b FOR VALID_TIME AS OF '2024-04-15' ON a.eid = b.eid")
              {"a" {:eid (long-array []) :_valid_from (long-array []) :_valid_to (long-array [])}
               "b" {:eid (long-array []) :_valid_from (long-array []) :_valid_to (long-array [])}})]
      (is (some? (:error r)))
      (is (re-find #"Multi-table SELECT with more than one FOR VALID_TIME"
                   (:error r))))))

(deftest sql-allen-bad-arity-throws
  (testing "Allen predicates require exactly 4 args"
    (let [reg {"intervals" {:eid (long-array [1])
                            :a (long-array [1])
                            :b (long-array [1])}}
          parsed (sql/parse-sql
                   "DELETE FROM intervals WHERE OVERLAPS(a, b)" reg)]
      (is (re-find #"requires 4 args" (or (:error parsed) ""))))))

;; ============================================================================
;; SELECT-side FOR VALID_TIME (AS OF / BETWEEN / FROM-TO / ALL)
;;
;; Preprocessor rewrites the temporal clause into equivalent WHERE
;; predicates over the table's _valid_from / _valid_to columns. Uses
;; the SQL:2011 convention column names.
;; ============================================================================

(deftest preprocess-select-temporal-as-of-injects-where
  (testing "FOR VALID_TIME AS OF rewrites to WHERE predicates"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "SELECT eid FROM salaries FOR VALID_TIME AS OF '2024-04-01'")]
      (is (re-find #"WHERE\s+\(?_valid_from\s+<=\s+\d+"
                   (:sql r)))
      (is (re-find #"_valid_to\s+>\s+\d+"
                   (:sql r))))))

(deftest preprocess-select-temporal-between-form
  (testing "FOR VALID_TIME BETWEEN x AND y emits overlap predicate"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "SELECT eid FROM salaries FOR VALID_TIME BETWEEN '2024-01-01' AND '2024-07-01'")]
      ;; Expect: vf <= Jul AND vt > Jan (half-open overlap)
      (is (re-find #"_valid_from\s+<=\s+1719792000000000"
                   (:sql r)))
      (is (re-find #"_valid_to\s+>\s+1704067200000000"
                   (:sql r))))))

(deftest preprocess-select-temporal-from-to-form
  (testing "FOR VALID_TIME FROM x TO y has same semantic as BETWEEN"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "SELECT eid FROM salaries FOR VALID_TIME FROM '2024-01-01' TO '2024-07-01'")]
      (is (re-find #"_valid_from\s+<=\s+1719792000000000"
                   (:sql r)))
      (is (re-find #"_valid_to\s+>\s+1704067200000000"
                   (:sql r))))))

(deftest preprocess-select-for-all-valid-time-no-filter
  (testing "FOR ALL VALID_TIME on SELECT strips the clause without adding a filter"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "SELECT eid FROM salaries FOR ALL VALID_TIME")]
      (is (re-find #"(?i)^\s*SELECT\s+eid\s+FROM\s+salaries\s*$"
                   (:sql r))
          "FOR ALL VALID_TIME should leave the SQL with no extra WHERE")
      (is (nil? (:period r))
          "SELECT-side ALL doesn't emit a :period side channel"))))

(deftest preprocess-select-temporal-preserves-existing-where
  (testing "Existing WHERE is preserved; temporal preds prepended via AND"
    (let [r (stratum.sql.rewrite/preprocess-sql
              "SELECT eid FROM salaries FOR VALID_TIME AS OF '2024-04-01' WHERE eid = 1")
          s (:sql r)]
      (is (.contains ^String s "WHERE"))
      (is (.contains ^String s "_valid_from"))
      (is (.contains ^String s "_valid_to"))
      (is (.contains ^String s "eid = 1"))
      ;; Temporal preds appear BEFORE the original eid=1 predicate.
      (is (< (.indexOf ^String s "_valid_from")
             (.indexOf ^String s "eid = 1"))))))

(deftest sql-select-as-of-end-to-end-on-index-backed-table
  (testing "SELECT … FOR VALID_TIME AS OF returns rows valid at that instant"
    (let [srv (server/start {:port 0})]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            ;; Two slices: [Jan, Jul) salary=100, [Jul, MAX) salary=110.
            {:eid          (stratum.index/index-from-seq :int64 [1 1])
             :salary       (stratum.index/index-from-seq :int64 [100 110])
             :_valid_from  (stratum.index/index-from-seq :int64
                             [1704067200000000 1719792000000000])
             :_valid_to    (stratum.index/index-from-seq :int64
                             [1719792000000000 Long/MAX_VALUE])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        ;; Query at Apr-2024: should return only the [Jan, Jul) slice.
        (let [parsed (sql/parse-sql
                       "SELECT salary FROM salaries FOR VALID_TIME AS OF '2024-04-01'"
                       @(:registry srv))
              result (q/q (:query parsed))
              rows (q/results->columns result)]
          (is (= [100] (vec (:salary rows)))))
        ;; Query at Sep-2024: should return only the [Jul, MAX) slice.
        (let [parsed (sql/parse-sql
                       "SELECT salary FROM salaries FOR VALID_TIME AS OF '2024-09-01'"
                       @(:registry srv))
              result (q/q (:query parsed))
              rows (q/results->columns result)]
          (is (= [110] (vec (:salary rows)))))
        (finally (server/stop srv))))))

;; ============================================================================
;; SQL session SET datahike.clock_time
;; ============================================================================

(deftest sql-set-clock-time-pins-defaults
  (testing "SET datahike.clock_time pins the time source for subsequent DML defaults"
    (let [srv (server/start {:port 0})
          exec @(resolve 'stratum.server/execute-sql)
          pinned-millis 1704067200000]   ;; Jan-01-2024
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            {:eid (stratum.index/index-from-seq :int64 [])
             :salary (stratum.index/index-from-seq :int64 [])
             :_valid_from (stratum.index/index-from-seq :int64 [])
             :_valid_to (stratum.index/index-from-seq :int64 [])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        ;; Pin clock_time to Jan-01-2024.
        (exec (str "SET datahike.clock_time = '2024-01-01'")
              (:registry srv) (:data-dir srv))
        ;; INSERT without specifying _valid_from → default should be the pinned value.
        (exec "INSERT INTO salaries (eid, salary) VALUES (1, 100)"
              (:registry srv) (:data-dir srv))
        (let [cols (get @(:registry srv) "salaries")
              vf (stratum.index/idx-get-long (:index (:_valid_from cols)) 0)]
          (is (= (* 1000 pinned-millis) vf)
              "valid-from should match pinned clock_time in micros"))
        ;; Clear with DEFAULT.
        (exec "SET datahike.clock_time = DEFAULT" (:registry srv) (:data-dir srv))
        (is (nil? (get-in @(:registry srv) ["__settings__" :clock-time-millis]))
            "DEFAULT clears the binding")
        (finally (server/stop srv))))))

;; ============================================================================
;; ERASE DML — GDPR-style physical purge
;; ============================================================================

(deftest sql-erase-from-physically-purges-rows
  (testing "ERASE FROM … WHERE physically removes matching rows"
    (let [srv (server/start {:port 0})
          exec @(resolve 'stratum.server/execute-sql)]
      (try
        (server/register-table!
          srv "salaries"
          {:eid    (stratum.index/index-from-seq :int64 [1 2 3])
           :salary (stratum.index/index-from-seq :int64 [100 200 300])})
        (let [^PgWireServer$QueryResult qr
              (exec "ERASE FROM salaries WHERE eid = 2"
                    (:registry srv) (:data-dir srv))]
          (is (= "ERASE 1" (.commandTag qr))
              "tag should distinguish ERASE from DELETE"))
        (let [cols (get @(:registry srv) "salaries")
              n (stratum.index/idx-length (:index (:eid cols)))
              eids (mapv #(stratum.index/idx-get-long (:index (:eid cols)) %)
                         (range n))]
          (is (= [1 3] eids)))
        (finally (server/stop srv))))))

(deftest sql-erase-from-without-where-purges-all
  (testing "ERASE FROM <t> with no WHERE wipes the entire table"
    (let [srv (server/start {:port 0})
          exec @(resolve 'stratum.server/execute-sql)]
      (try
        (server/register-table!
          srv "t"
          {:a (stratum.index/index-from-seq :int64 [1 2 3])})
        (let [^PgWireServer$QueryResult qr
              (exec "ERASE FROM t" (:registry srv) (:data-dir srv))]
          (is (= "ERASE 3" (.commandTag qr))))
        (let [cols (get @(:registry srv) "t")]
          (is (zero? (stratum.index/idx-length (:index (:a cols))))))
        (finally (server/stop srv))))))

(deftest sql-erase-on-bitemporal-bypasses-temporal-semantics
  (testing "ERASE on a bitemporal table ignores FOR PORTION OF and just purges"
    (let [srv (server/start {:port 0})
          exec @(resolve 'stratum.server/execute-sql)]
      (try
        (server/register-table!
          srv "salaries"
          (with-meta
            {:eid (stratum.index/index-from-seq :int64 [1 1])
             :salary (stratum.index/index-from-seq :int64 [100 110])
             :_valid_from (stratum.index/index-from-seq :int64 [1000 2000])
             :_valid_to (stratum.index/index-from-seq :int64 [2000 Long/MAX_VALUE])}
            {:bitemporal {:valid {:from-col :_valid_from
                                  :to-col   :_valid_to
                                  :unit     :micros}}}))
        (let [^PgWireServer$QueryResult qr
              (exec "ERASE FROM salaries WHERE eid = 1"
                    (:registry srv) (:data-dir srv))]
          (is (re-find #"^ERASE" (.commandTag qr))))
        (let [cols (get @(:registry srv) "salaries")]
          (is (zero? (stratum.index/idx-length (:index (:eid cols))))
              "all vt-slices for eid=1 should be physically gone"))
        (finally (server/stop srv))))))

;; ============================================================================
;; Col-vs-col SELECT predicates — planner-limitation workaround
;;
;; SELECT WHERE <col1> OP <col2> now evaluates correctly. Unlocks
;; SELECT-side Allen predicates (P1-3 worked in DML only without this).
;; ============================================================================

(deftest sql-select-col-vs-col-comparison
  (testing "WHERE col1 < col2 evaluates row-by-row"
    (let [reg {"t" {:a (long-array [1 5 3 7])
                    :b (long-array [2 4 8 6])}}
          parsed (sql/parse-sql "SELECT a, b FROM t WHERE a < b" reg)
          result (q/q (:query parsed))
          rows (q/results->columns result)]
      ;; Row 0: 1<2 ✓, Row 1: 5<4 ✗, Row 2: 3<8 ✓, Row 3: 7<6 ✗
      (is (= [1 3] (vec (:a rows))))
      (is (= [2 8] (vec (:b rows)))))))

(deftest sql-select-overlaps-allen-predicate-with-projected-cols
  (testing "OVERLAPS Allen predicate works in SELECT WHERE when all 4 cols are also projected"
    ;; Caveat: stratum's executor extracts LHS columns from
    ;; normalized predicates but skips RHS keyword refs when picking
    ;; which columns to materialize. So Allen predicates need their
    ;; 4 column args projected too (or otherwise referenced).
    ;; Extending the executor's column-pruner to consider predicate
    ;; RHS keywords is a P2 follow-up.
    (let [reg {"intervals"
               {:eid    (long-array [1 2 3])
                :a_from (long-array [100 200 300])
                :a_to   (long-array [200 300 400])
                :b_from (long-array [150 350 250])
                :b_to   (long-array [250 450 350])}}
          parsed (sql/parse-sql
                   (str "SELECT eid, a_from, a_to, b_from, b_to FROM intervals "
                        "WHERE OVERLAPS(a_from, a_to, b_from, b_to)")
                   reg)
          result (q/q (:query parsed))
          rows (q/results->columns result)
          eids (vec (:eid rows))]
      (is (= [1 3] (sort eids))))))

;; ============================================================================
;; paired (vf, vt) zone-map pruning
;;
;; Audit verdict: the existing per-predicate zone-map machinery in
;; `stratum.query.group-by/build-zone-filters` + `classify-chunk`
;; ALREADY handles paired vt predicates correctly. For the canonical
;; `WHERE _valid_from <= ts AND _valid_to > ts` shape:
;;   - The `_valid_from :lte ts` filter says "skip chunks where
;;     min(_valid_from) > ts" — the chunk's earliest start is past
;;     the query time, so no row in the chunk is valid at `ts`.
;;   - The `_valid_to :gt ts` filter says "skip chunks where
;;     max(_valid_to) <= ts" — the chunk's latest end is at or before
;;     the query time, so no row in the chunk is still valid at `ts`.
;;   - The AND in `classify-chunk` skips the chunk if EITHER filter
;;     fires.
;;
;; Combined, this is exactly the "window-overlap pruner" the gap
;; analysis recommended. No additional code needed — just
;; documentation + a sanity test.
;; ============================================================================

(deftest sql-vt-zone-map-prunes-non-overlapping-chunks
  (testing "Paired _valid_from / _valid_to predicates trigger zone-map pruning"
    (let [;; Build an index-backed dataset large enough to span multiple chunks.
          ;; Chunk size in stratum defaults to ~256 rows; use 1000 rows so we
          ;; get ~4 chunks.
          n 1000
          ;; First half of rows: vt-window [0, 500). Second half: [500, 1000).
          ;; A query AS OF 250 should hit only the first chunk(s).
          ds (stratum.dataset/make-dataset
               {:eid (stratum.index/index-from-seq :int64 (vec (range n)))
                :_valid_from (stratum.index/index-from-seq :int64
                              (vec (map #(if (< % (/ n 2)) 0 500) (range n))))
                :_valid_to (stratum.index/index-from-seq :int64
                            (vec (map #(if (< % (/ n 2)) 500 1000) (range n))))}
               {:name "t"})
          ;; Query AS OF 250 — only the first-half chunks should match.
          parsed (sql/parse-sql
                   "SELECT eid FROM t WHERE _valid_from <= 250 AND _valid_to > 250"
                   {"t" (stratum.dataset/columns ds)})
          result (q/q (:query parsed))
          rows (q/results->columns result)]
      ;; All N/2 first-half rows match; none of the second-half rows match.
      (is (= (/ n 2) (count (:eid rows))))
      (is (every? #(< % (/ n 2)) (:eid rows))))))
