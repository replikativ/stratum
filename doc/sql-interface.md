# SQL Interface

Stratum includes a PostgreSQL wire protocol (pgwire) server that accepts standard SQL queries from any PostgreSQL-compatible client.

## Overview

```
psql / DBeaver / JDBC / psycopg2
         │
         │  PostgreSQL wire protocol v3
         ▼
    server.clj (pgwire)
         │
         │  SQL string
         ▼
      sql.clj (JSqlParser)
         │
         │  Stratum query map
         ▼
     query.clj (execution)
```

The server speaks the PostgreSQL wire protocol (version 3), handling authentication (trust mode), query parsing, parameter binding, and result serialization. Any client that can connect to PostgreSQL can connect to Stratum.

## Starting the Server

### Standalone JAR

Download the release JAR and run with Java 21+:

```bash
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar [options]
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port N` | 5432 | TCP port to listen on |
| `--data-dir DIR` | `~/.local/share/stratum` | Directory for per-file index stores |
| `--index NAME:FILE` | — | Pre-index FILE and register as table NAME |
| `--demo` | — | Load synthetic demo tables (lineitem, taxi) |

`NAME` is optional — omitting it derives the table name from the filename stem:

```bash
# Start server, auto-index two files
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar \
  --index orders:/data/orders.csv \
  --index events:/data/events.parquet \
  --port 5432

# Short form: table name derived from filename
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar \
  --index /data/orders.csv        # registers as "orders"

# Demo mode
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar --demo
```

### From Clojure CLI

```bash
# Demo mode
clj -M:server --demo

# Pre-index files
clj -M:server --index orders:/data/orders.csv --index /data/events.parquet

# Custom data directory and port
clj -M:server --data-dir /mnt/analytics --port 5433
```

### From Clojure API

```clojure
(require '[stratum.api :as st])

;; Start on default port
(def srv (st/start-server {:port 5432}))

;; Register tables
(st/register-table! srv "lineitem" lineitem-data)
(st/register-table! srv "taxi" (st/from-csv "data/taxi.csv"))

;; Stop
(st/stop-server srv)
```

## File Indexing

Stratum builds persistent column indices from CSV and Parquet files and caches them on disk. On subsequent loads only the lazy PSS tree is restored — zero file re-reading.

### Ad-hoc file queries (auto-indexed on first access)

Any SQL query can reference a file directly. The file is indexed on first access and cached at `<data-dir>/<filename>.stratum/`:

```sql
-- DuckDB-style table function
SELECT region, SUM(amount) FROM read_csv('/data/sales.csv') GROUP BY region;

-- Quoted path in FROM clause
SELECT product, COUNT(*) FROM '/data/orders.parquet' GROUP BY product;
```

String GROUP BY columns are automatically dictionary-encoded and decode back to strings in results.

### Pre-indexing at startup

Pre-index files at server startup via `--index` (CLI) or `st/index-file!` (Clojure):

```bash
# CLI: index at startup, register as "orders"
clj -M:server --index orders:/data/orders.csv
```

```clojure
;; Clojure API
(st/index-file! srv "orders" "/data/orders.csv" "/tmp/stratum-data")
```

The indexed store lives at `<data-dir>/<filename>.stratum/`. On server restart with the same `--data-dir`, the index loads instantly (mtime check — re-indexed only if the source file changed).

### Bring-your-own store (Clojure)

For custom storage backends (S3, Redis, etc.), pass an already-open konserve store:

```clojure
(require '[stratum.files :as files]
         '[konserve.s3 :as s3])  ;; example backend

(def store (s3/connect-store {:bucket "my-bucket"} {:sync? true}))

;; Load/index into your own store — any konserve backend works
(def ds (files/load-or-index-file! "/data/orders.csv" {:store store}))

;; Query
(st/q {:from ds :group [:region] :agg [[:sum :amount]]})
```

## Table Registration

Tables are registered as column maps. String columns are automatically dictionary-encoded for optimal SIMD group-by performance:

```clojure
;; From raw arrays
(st/register-table! srv "orders"
  {:price  (double-array [10.0 20.0 30.0])
   :qty    (long-array [1 2 3])
   :region (into-array String ["US" "EU" "US"])})

;; From CSV (arrays — re-read on each server restart)
(st/register-table! srv "taxi" (st/from-csv "data/taxi.csv"))

;; From CSV with persistent index (survives restarts, zone-map pruning)
(st/index-file! srv "taxi" "data/taxi.csv" "/tmp/stratum-data")

;; Live table: always resolves current branch HEAD from storage
(st/register-live-table! srv "orders" store "main")
```

## SQL Translation

The `sql.clj` module uses JSqlParser to parse SQL into an AST, then translates it to a Stratum query map:

```sql
SELECT region, SUM(price * qty) AS revenue, COUNT(*)
FROM orders
WHERE qty > 5 AND price BETWEEN 10 AND 100
GROUP BY region
HAVING COUNT(*) > 2
ORDER BY revenue DESC
LIMIT 10
```

Translates to:

```clojure
{:from   <registered table "orders">
 :where  [[:> :qty 5] [:between :price 10 100]]
 :agg    [[:sum [:* :price :qty]] [:count]]
 :group  [:region]
 :having [[:> :count 2]]
 :order  [[:revenue :desc]]
 :limit  10
 :select [:region [:as [:sum [:* :price :qty]] :revenue] [:as [:count] :count]]}
```

## Supported SQL Subset

### Supported

| Feature | Examples |
|---------|----------|
| SELECT with expressions | `SELECT a + b, a * 2` |
| Column aliases | `SELECT SUM(x) AS total` |
| WHERE with AND/OR/NOT | `WHERE a > 5 AND (b < 10 OR c = 1)` |
| Comparison operators | `=, <>, <, >, <=, >=, BETWEEN, IN, NOT IN` |
| LIKE / ILIKE patterns | `WHERE name LIKE '%foo%'`, `WHERE name ILIKE '%foo%'` |
| IS NULL / IS NOT NULL | `WHERE col IS NOT NULL` |
| Aggregate functions | `SUM, COUNT, AVG, MIN, MAX, STDDEV, VARIANCE, CORR` |
| FILTER clause | `SUM(x) FILTER (WHERE status = 1)` |
| Statistical aggregates | `MEDIAN, PERCENTILE_CONT, APPROX_QUANTILE` |
| COUNT(DISTINCT col) | `SELECT COUNT(DISTINCT region)` |
| Anomaly detection | `ANOMALY_SCORE, ANOMALY_PREDICT, ANOMALY_PROBA, ANOMALY_CONFIDENCE` |
| GROUP BY | `GROUP BY col1, col2` |
| HAVING | `HAVING COUNT(*) > 10` |
| ORDER BY | `ORDER BY col ASC, col2 DESC` |
| LIMIT / OFFSET | `LIMIT 100 OFFSET 50` |
| DISTINCT | `SELECT DISTINCT region` |
| Math functions | `ABS, SQRT, LOG, EXP, ROUND, FLOOR, CEIL, SIGN, MOD, POWER` |
| GREATEST / LEAST | `GREATEST(a, b, c)`, `LEAST(a, b)` |
| String functions | `UPPER, LOWER, LENGTH, SUBSTR, REPLACE, TRIM, CONCAT` |
| Date functions | `DATE_TRUNC, EXTRACT, DATE_ADD, DATE_DIFF` |
| Type casts | `CAST(col AS DOUBLE)` |
| COALESCE / NULLIF | `COALESCE(col, 0)` |
| EXPLAIN | `EXPLAIN SELECT ...` |
| Table functions | `SELECT * FROM read_csv('file.csv')` |
| JOINs | `SELECT ... FROM a JOIN b ON a.id = b.id` |
| CREATE TABLE / DROP TABLE | `CREATE TABLE t (col TYPE, ...)` |
| INSERT INTO | `INSERT INTO t VALUES (1, 'a'), (2, 'b')` |
| UPDATE SET | `UPDATE t SET col = val WHERE pred` |
| UPDATE FROM | `UPDATE t SET col = expr FROM t2 WHERE t.id = t2.id` |
| DELETE FROM | `DELETE FROM t WHERE pred` |
| UPSERT | `INSERT ... ON CONFLICT (col) DO UPDATE SET ...` |

### Statistical Aggregates

```sql
-- Exact median (QuickSelect, O(N) average)
SELECT MEDIAN(price) FROM orders;

-- Exact percentile (interpolated)
SELECT PERCENTILE_CONT(0.95, price) FROM orders;

-- Approximate quantile (t-digest, O(N) with bounded memory)
SELECT APPROX_QUANTILE(price, 0.95) FROM orders;

-- All work with GROUP BY
SELECT region, MEDIAN(price), APPROX_QUANTILE(price, 0.99)
FROM orders GROUP BY region;
```

### Anomaly Detection

Isolation forest scoring is available as SQL functions. Models must be registered via the Clojure API:

```clojure
;; Register model with server
(def model (st/train-iforest {:from data :contamination 0.05}))
(st/register-model! srv "fraud_model" model)
```

Then query via SQL:

```sql
-- Raw anomaly score [0, 1]
SELECT *, ANOMALY_SCORE('fraud_model', amount, freq) AS score
FROM transactions WHERE ANOMALY_SCORE('fraud_model', amount, freq) > 0.7;

-- Binary prediction (1 = anomaly, 0 = normal)
SELECT *, ANOMALY_PREDICT('fraud_model', amount, freq) AS is_anomaly
FROM transactions;

-- Calibrated probability [0, 1]
SELECT *, ANOMALY_PROBA('fraud_model', amount, freq) AS prob
FROM transactions;

-- Prediction confidence (tree agreement) [0, 1]
SELECT *, ANOMALY_CONFIDENCE('fraud_model', amount, freq) AS conf
FROM transactions;
```

### Window Functions

Window functions are fully supported with `OVER (PARTITION BY ... ORDER BY ...)`:

```sql
-- Row numbering
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees;

-- Lag/Lead
SELECT *, LAG(price, 1) OVER (ORDER BY date) AS prev_price
FROM stocks;

-- Running aggregates
SELECT *, SUM(amount) OVER (PARTITION BY account ORDER BY date) AS running_total
FROM transactions;

-- RANK
SELECT *, RANK() OVER (ORDER BY score DESC) AS rank
FROM scores;
```

Supported window functions: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `SUM`, `COUNT`, `AVG` (with optional frame clauses).

### DML Statements

Stratum supports full DML for mutable table management via the PostgreSQL wire protocol:

```sql
-- INSERT
INSERT INTO orders (id, qty, price) VALUES (1, 5, 9.99), (2, 3, 24.50);

-- UPDATE with WHERE
UPDATE orders SET price = price * 1.1 WHERE region = 'US';

-- UPDATE FROM (joined update)
UPDATE orders SET total = qty * unit_price
  FROM prices WHERE orders.product_id = prices.price_id;

-- DELETE with WHERE
DELETE FROM orders WHERE status = 'cancelled';

-- UPSERT (INSERT ... ON CONFLICT)
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;

INSERT INTO users (id, name) VALUES (1, 'Alice')
  ON CONFLICT (id) DO NOTHING;
```

Table-qualified column references are supported for disambiguation in UPDATE FROM and JOINs (e.g., `employees.dept_code = departments.dept_code`).

### Not Supported

| Feature | Notes |
|---------|-------|
| Subqueries | `WHERE x IN (SELECT ...)` |
| CTEs (WITH) | `WITH t AS (SELECT ...)` |
| Transactions | Single-query execution model |

## Table Functions

SQL table functions provide direct file access. Files are auto-indexed on first access and cached in `<data-dir>/<filename>.stratum/`:

```sql
-- CSV: table function syntax (DuckDB-compatible)
SELECT payment_type, AVG(tip_amount)
FROM read_csv('/data/taxi.csv')
GROUP BY payment_type;

-- Parquet: table function syntax
SELECT product, SUM(revenue)
FROM read_parquet('/data/orders.parquet')
GROUP BY product;

-- Quoted path in FROM clause
SELECT region, SUM(amount) FROM '/data/sales.csv' GROUP BY region;
```

Cached indices use zone map pruning and SIMD-accelerated execution — subsequent queries on the same file are much faster than re-reading from disk.

## Client Examples

### psql

```bash
psql -h localhost -p 5432 -U stratum
\d              -- list tables
\d tablename    -- describe table
SELECT ...
```

### Python (psycopg2)

```python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="stratum")
cur = conn.cursor()
cur.execute("SELECT region, SUM(price) FROM orders GROUP BY region")
rows = cur.fetchall()
```

### JDBC

```java
Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stratum");
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders WHERE price > 100");
```

## Related Documentation

- [Architecture](architecture.md) — System overview
- [Query Engine](query-engine.md) — Query map format and execution
