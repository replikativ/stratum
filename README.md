# Stratum

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/stratum.svg)](https://clojars.org/org.replikativ/stratum)
[![CircleCI](https://circleci.com/gh/replikativ/stratum.svg?style=shield)](https://circleci.com/gh/replikativ/stratum)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)

*Stratum is in beta. Please provide feedback and report any issues you run into.*

SIMD-accelerated SQL engine for the JVM. Every table is a branchable, copy-on-write value.

Stratum is a columnar analytics engine that combines the performance of fused SIMD execution with the semantics of immutable data. Tables are persistent values — fork one in O(1), modify it independently, persist snapshots to named branches, and time-travel to any previous commit. It's the same model as Clojure's persistent collections and git's object store, applied to analytical data.

## 30-Second Demo

Start a PostgreSQL-compatible server and query CSV/Parquet files directly:

```bash
# Standalone JAR — no Clojure needed, just Java 21+
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar --demo

# Or with your own data
java --add-modules jdk.incubator.vector -jar stratum-standalone.jar \
  --index /data/orders.csv --index /data/events.parquet

# Or with Clojure CLI
clj -M:server --index /data/orders.csv --index /data/events.parquet

# Open to network (default: localhost only)
clj -M:server --host 0.0.0.0 --port 5432 --index /data/orders.csv
```

Then connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432 -U stratum

-- Query pre-indexed tables
SELECT region, SUM(amount), COUNT(*) FROM orders GROUP BY region;

-- Or query files directly (auto-indexed on first access, cached on disk)
SELECT region, AVG(price) FROM read_csv('/data/sales.csv') GROUP BY region;

-- Demo mode: built-in sample data
```

```bash
clj -M:server --demo
```

## Performance

Stratum's architecture — fused SIMD execution over copy-on-write columnar data — delivers strong analytical performance.

Single-threaded comparison vs DuckDB v1.4.4 (JDBC in-process) on 10M rows, 8-core Intel Lunar Lake. Full results in [doc/benchmarks.md](doc/benchmarks.md).

```
Stratum vs DuckDB (10M rows, single-threaded, best of array/index mode)
══════════════════════════════════════════════════════════════════════════
                                    Stratum    DuckDB    Ratio
──────────────────────────────────────────────────────────────────────
TPC-H Q6 (filter+sum-product)        13ms       28ms    2.2x faster
SSB Q1.1 (filter+sum-product)        13ms       28ms    2.2x faster
Filtered COUNT (NEQ pred)             3ms       12ms    4.0x faster
Group-by COUNT (3 groups)            17ms       24ms    1.4x faster
TPC-H Q1 (7 aggs, 4 groups)          75ms       93ms    1.2x faster
H2O Q3 (100K string groups)          71ms      362ms    5.1x faster
H2O Q2 (10K string groups)           33ms      123ms    3.7x faster
H2O Q6 (STDDEV, 10K groups)          30ms       81ms    2.7x faster
H2O Q9 (CORR, 10K groups)            61ms      134ms    2.2x faster
H2O Q10 (10M groups, 6 cols)        832ms     7056ms    8.5x faster
LIKE '%search%' (string scan)        47ms      240ms    5.1x faster
AVG(LENGTH(URL)) (string fn)         38ms      170ms    4.5x faster
──────────────────────────────────────────────────────────────────────
COUNT(DISTINCT) 1.1M                109ms       61ms    DuckDB 1.8x
COUNT WHERE sparse pred              22ms        5ms    DuckDB 4.9x
──────────────────────────────────────────────────────────────────────
```

**Summary (10M rows, single-threaded, best of array/index mode vs DuckDB 1T):**

| Category | Queries | Stratum faster | DuckDB faster |
|----------|---------|---------------|--------------|
| TPC-H / SSB | 7 | 7 (1.2–4.0x) | 0 |
| H2O Group-By | 10 | 9 (1.0–8.5x) | 1 |
| H2O Join | 3 | 2 (1.1x) | 1 |
| ClickBench | 22 | 15 (1.0–105x) | 7 |
| NYC Taxi | 4 | 2 (1.0–1.4x) | 2 |
| Statistical | 5 | 5 (1.1–2.4x) | 0 |
| Window | 3 | 3 (1.3–2.1x) | 0 |
| Hash Join + TPC-DS | 3 | 2 (1.3–2.1x) | 1 |

DuckDB wins on sparse-selectivity filters, global COUNT(DISTINCT), and window-based top-N. Stratum wins on arithmetic-heavy aggregates, string operations, group-by, LIKE, statistical aggregates, and window functions.

Run benchmarks yourself:

```bash
clj -M:olap              # All tiers, 10M rows (default)
clj -M:olap 1000000      # Custom scale
clj -M:olap h2o           # H2O tier only
clj -M:olap cb            # ClickBench tier only
```

## Clojure Quick Start

```clojure
(require '[stratum.api :as st])

;; Query with Stratum DSL
(st/q {:from {:price (double-array [10 20 30 40 50])
              :qty   (long-array [1 2 3 4 5])}
      :where [[:< :qty 4]]
      :agg [[:sum [:* :price :qty]]]})
;; => [{:sum 140.0, :_count 3}]

;; Query with SQL
(st/q "SELECT SUM(price * qty) FROM orders WHERE qty < 4"
     {"orders" {:price (double-array [10 20 30 40 50])
                :qty   (long-array [1 2 3 4 5])}})
;; => [{:sum 140.0, :_count 3}]

;; Import CSV (arrays, re-read each time)
(def taxi (st/from-csv "data/taxi.csv"))
(st/q {:from taxi :group [:payment_type] :agg [[:avg :tip_amount] [:count]]})

;; Import Parquet
(def orders (st/from-parquet "data/orders.parquet"))
(st/q "SELECT product, SUM(revenue) FROM t GROUP BY product" {"t" orders})

;; From Clojure maps
(def ds (st/from-maps [{:name "Alice" :age 30} {:name "Bob" :age 25}]))
(st/q {:from ds :agg [[:avg :age]]})
```

## Snapshots and Branching

Every Stratum dataset is a copy-on-write value. Fork one in O(1) to create an isolated branch — modifications only touch the changed chunks, everything else is structurally shared. Persist snapshots to named branches, load them back, or time-travel to any previous commit.

```clojure
(require '[stratum.api :as st])

;; Create a dataset
(def ds (st/make-dataset {:price (double-array [10 20 30])
                          :qty   (long-array [1 2 3])}
                         {:name "orders"}))

;; O(1) fork — structural sharing, independent mutations
(def experiment (st/fork ds))

;; Persist to storage
(st/sync! ds store)

;; Load from storage
(def restored (st/load store "orders"))

;; Time-travel to a specific commit
(def snapshot (st/resolve store "orders" {:as-of commit-uuid}))
```

## SQL Capabilities

**DML**: SELECT, INSERT, UPDATE, DELETE, UPSERT (INSERT ON CONFLICT), UPDATE FROM (joined updates), CREATE TABLE, DROP TABLE

**Joins**: INNER, LEFT, RIGHT, FULL — single and multi-column keys

**Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST, LAG, LEAD, SUM/AVG/COUNT/MIN/MAX OVER — with PARTITION BY, ORDER BY, and frame clauses

**Subqueries and composition**: CTEs (WITH), correlated and uncorrelated subqueries, IN/NOT IN/EXISTS, set operations (UNION, INTERSECT, EXCEPT)

**Expressions**: CASE WHEN, COALESCE, NULLIF, GREATEST, LEAST, CAST, arithmetic (+, -, \*, /, %)

**Date/time**: DATE_TRUNC, DATE_ADD, DATE_DIFF, EXTRACT, EPOCH_DAYS, EPOCH_SECONDS

**String**: LIKE, ILIKE, LENGTH, UPPER, LOWER, SUBSTR

**Aggregates**: SUM, COUNT, AVG, MIN, MAX, STDDEV, VARIANCE, CORR, MEDIAN, PERCENTILE_CONT, APPROX_QUANTILE, COUNT(DISTINCT), FILTER clause

**Ad-hoc file queries**: `read_csv()`, `read_parquet()`

**Analytics**: ANOMALY_SCORE, ANOMALY_PREDICT (isolation forest via SQL)

**Other**: EXPLAIN, SELECT DISTINCT, LIMIT/OFFSET, IS NULL/IS NOT NULL

## Clojure Data Science Integration

Stratum datasets work directly with [tablecloth](https://github.com/scicloj/tablecloth) and [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset):

```clojure
(require '[stratum.tablecloth])  ;; Load protocol extensions
(require '[tablecloth.api :as tc])

(def ds (st/make-dataset {:x (double-array [1 2 3])
                          :y (double-array [4 5 6])}))

(tc/info ds)                    ;; Works directly
(tc/select-columns ds [:x])     ;; Returns StratumDataset
(tc/row-count ds)               ;; No conversion needed
```

Bidirectional support: query `tech.ml.dataset` datasets directly with the Stratum engine (zero-copy when array-backed). Datasets implement `IEditableCollection`, `ITransientCollection`, `IPersistentCollection`, and `ILookup`.

## Query DSL Reference

> **Note:** The DSL is still a work in progress. SQL strings are the more complete interface — use the DSL when you want to compose queries programmatically or pass in Clojure data directly without a SQL layer.

The DSL is intentionally flat. Every clause resolves column names by keyword lookup against a single merged map: `:from` establishes the base columns, `:join` merges in the dimension table's columns, and all subsequent clauses (`:where`, `:agg`, `:group`, `:select`, `:having`, `:order`) reference any column by its keyword. This makes it straightforward to build queries from Clojure data — no quoting, no SQL string interpolation, just maps and vectors. Composition (the DSL equivalent of SQL CTEs/subqueries) is done with Clojure `let`/`def` — see [Column Scoping and Composition](doc/query-engine.md#column-scoping-and-composition) for details.

```clojure
;; Full query map
{:from    {:col1 data1 :col2 data2}     ;; Column data (arrays, indices, or encoded)
 :join    [{:with {:k data}             ;; Dimension table columns
            :on   [:= :col1 :k]        ;; :col1 from :from, :k from :with — both visible after join
            :type :inner}]
 :where   [[:< :col1 100] [:like :name "%foo%"]]             ;; Predicates
 :select  [:col1 [:as [:* :col2 100] :pct]]                  ;; Projection
 :agg     [[:sum :col1] [:avg :col2] [:count] [:stddev :col3]]  ;; Aggregations
 :group   [:category]                                          ;; Group by
 :having  [[:> :count 10]]                                     ;; Post-agg filter
 :order   [[:sum :desc]]                                       ;; Sort
 :limit   100                                                  ;; Limit
 :offset  50                                                   ;; Offset
 :result  :columns}                                            ;; Columnar output

;; Supported aggregations: :sum :count :avg :min :max :stddev :variance :corr
;;                         :count-distinct :sum-product
;;                         :median :percentile :approx-quantile
;; Supported predicates:   :< :<= :> :>= := :!= :between :in :not-in
;;                         :like :ilike :is-null :is-not-null :or :not
;; Expressions:            [:+ :a :b] [:- :a 1] [:* :price :qty] [:/ :a :b]
;;                         [:date-trunc :day :ts] [:extract :hour :ts]
;;                         [:coalesce :a 0] [:nullif :a 0]
;;                         [:greatest :a :b] [:least :a :b]
```

## Ecosystem

Stratum is part of the [Replikativ](https://github.com/replikativ) ecosystem — a set of composable, immutable data systems:

- **[Datahike](https://github.com/replikativ/datahike)** — immutable graph database with Datalog queries
- **[Yggdrasil](https://github.com/replikativ/yggdrasil)** — branching protocol for multi-system snapshots
- **[Scriptum](https://github.com/replikativ/scriptum)** — full-text search
- **[Proximum](https://github.com/replikativ/proximum)** — vector search

All share copy-on-write semantics and can be branched together via Yggdrasil.

## Features

- **SQL**: PostgreSQL wire protocol (v3), full DML, CTEs, window functions, joins, subqueries, set operations
- **Performance**: SIMD filter/aggregate/group-by via Java Vector API, fused single-pass execution, zone map pruning, parallel execution
- **Persistence**: O(1) CoW snapshots, branching, time-travel, lazy loading from storage
- **Data**: CSV/Parquet import, dictionary-encoded strings, PostgreSQL NULL semantics, ad-hoc file queries
- **Integration**: tablecloth/tech.ml.dataset interop, Datahike, Yggdrasil
- **Analytics**: Isolation forest anomaly detection (training, scoring, online rotation)

## Architecture

```
User → stratum.api/q
         ├── SQL string → query map
         └── query map → execution strategy
                           ├── Fused SIMD (filter+agg, single pass)
                           ├── Dense/hash group-by
                           ├── Chunked streaming (index inputs)
                           └── Scalar fallback
                                  ↓
                           Copy-on-write columnar storage
                           (branch, snapshot, lazy load)
```

**Data representations:**
- `long[]` / `double[]` — heap arrays for raw columnar data
- `PersistentColumnIndex` — chunked B-tree with per-chunk statistics and zone maps
- `String[]` → dictionary-encoded `long[]` for group-by and LIKE

## Installation

### Clojure CLI (deps.edn)

```clojure
{:deps {org.replikativ/stratum {:mvn/version "0.1.0"}}}
```

JVM flags required:

```clojure
:jvm-opts ["--add-modules=jdk.incubator.vector"
           "--enable-native-access=ALL-UNNAMED"]
```

### Requirements

- **JDK 21+** (for Vector API incubator module)
- Clojure 1.12+

## Development

```bash
# Start REPL
clj -M:repl

# Run tests
clj -M:test

# Build JAR
clj -T:build jar

# Build standalone uberjar
clj -T:build uber

# Install locally
clj -T:build install
```

After modifying Java files in `src-java/`:

```bash
clj -T:build compile-java

# or in Java
javac --add-modules jdk.incubator.vector -d target/classes \
  src-java/stratum/internal/ColumnOps.java \
  src-java/stratum/internal/ColumnOpsExt.java \
  src-java/stratum/internal/ColumnOpsChunked.java \
  src-java/stratum/internal/ColumnOpsChunkedSimd.java \
  src-java/stratum/internal/ColumnOpsAnalytics.java
# Restart REPL (JVM can't reload classes)
```

## Commercial Support

Need SIMD-accelerated analytics in your JVM stack? We offer integration support, custom development, and commercial licensing. Contact [contact@datahike.io](mailto:contact@datahike.io) or visit [datahike.io](https://datahike.io/about).

## License

Copyright (c) 2026 Christian Weilbach and contributors.

Apache 2.0. See [LICENSE](LICENSE).
