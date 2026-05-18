# Stratum

[![Clojars Project](https://img.shields.io/clojars/v/org.replikativ/stratum.svg)](https://clojars.org/org.replikativ/stratum)
[![CircleCI](https://circleci.com/gh/replikativ/stratum.svg?style=shield)](https://circleci.com/gh/replikativ/stratum)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)
[![Docs](https://img.shields.io/badge/docs-clay_notebooks-blue.svg)](https://replikativ.github.io/stratum/)

*Stratum is in beta. Please provide feedback and report any issues you run into.*

SIMD-accelerated SQL engine for the JVM. Every table is a branchable, copy-on-write value.

Stratum is a columnar analytics engine that combines the performance of fused SIMD execution with the semantics of immutable data. Tables are persistent values - fork one in O(1), modify it independently, persist snapshots to named branches, and time-travel to any previous commit. It's the same model as Clojure's persistent collections and git's object store, applied to analytical data.

## 30-Second Demo

Start a PostgreSQL-compatible server and query CSV/Parquet files directly:

```bash
# Standalone JAR - no Clojure needed, just Java 22+
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

Stratum's architecture - fused SIMD execution over copy-on-write columnar data - delivers strong analytical performance.

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
clj -M:olap h2o          # H2O tier only
clj -M:olap cb           # ClickBench tier only
clj -M:olap asof         # ASOF JOIN tier only (3 canonical shapes)
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

;; Open a Parquet file (lazy decode, zone-map pruning)
(def orders (st/parquet-dataset "data/orders.parquet"))
(st/q "SELECT product, SUM(revenue) FROM t GROUP BY product" {"t" orders})

;; From Clojure maps
(def ds (st/from-maps [{:name "Alice" :age 30} {:name "Bob" :age 25}]))
(st/q {:from ds :agg [[:avg :age]]})
```

## Server-side durability

By default, a Stratum PgWire server starts with an empty in-memory registry — convenient for ad-hoc demos and read-only queries against pre-indexed files (`--index`), but SQL `CREATE TABLE`, `INSERT`/`UPDATE`/`DELETE`, `CREATE MODEL`, and `register-live-table!` bindings live only in heap and evaporate on restart.

Pass a Konserve store via `:store` (or the CLI `--data-dir`) to make those durable:

```clojure
(require '[stratum.server :as srv]
         '[konserve.filestore :as kfile])

(def store
  (kfile/connect-fs-store "/var/lib/stratum/server"
                          {:sync? true}))

(def server (srv/start {:port 5432 :store store}))

;; CREATE TABLE foo (id INT, price DOUBLE);
;; INSERT INTO foo VALUES (1, 9.5);
;; — stop the server, restart, foo still has its rows
```

What's persisted:

- **SQL CREATE TABLE** — every column type (int, float, temporal, TEXT/VARCHAR, ENUM) lands as a real index-backed dataset
- **CREATE TYPE … AS ENUM** — declarations + OIDs survive restart; INSERT validation enforces the declared label set against the live table
- **INSERT / UPDATE / DELETE / UPSERT** — each statement re-syncs to a per-table branch; string columns encode through the column's dict, extending it amortised-O(1) when new labels appear
- **CREATE MODEL** (isolation forest, plain-data serialization)
- **register-live-table!** bindings whose Konserve store is the same as the server's `:store` (foreign-store live-tables stay session-scoped and emit a warning)

What stays transient (matches PostgreSQL semantics): per-connection transaction state, prepared statements, cursors, session settings (`SET …`).

Without `:store` the server prints a startup warning naming the surprise — programmatic users who manage their own datasets with `dataset/sync!` are unaffected and run as before.

## Snapshots and Branching

Every Stratum dataset is a copy-on-write value. Fork one in O(1) to create an isolated branch - modifications only touch the changed chunks, everything else is structurally shared. Persist snapshots to named branches, load them back, or time-travel to any previous commit.

```clojure
(require '[stratum.api :as st])

;; Create a dataset
(def ds (st/make-dataset {:price (double-array [10 20 30])
                          :qty   (long-array [1 2 3])}
                         {:name "orders"}))

;; O(1) fork - structural sharing, independent mutations
(def experiment (st/fork ds))

;; Persist to storage
(st/sync! ds store)

;; Load from storage
(def restored (st/load store "orders"))

;; Time-travel to a specific commit
(def snapshot (st/resolve store "orders" {:as-of commit-uuid}))
```

## Audit and Integrity

Stores configured with `:crypto-hash? true` produce content-addressed commits whose UUIDs are deterministic hashes of the payload. Any bytes-level tampering on the underlying konserve blobs surfaces as a recomputed UUID that no longer matches the address it was stored under. `stratum.audit` exposes the verification surface that turns this into actionable reports.

```clojure
(require '[stratum.audit :as audit])

;; Write with crypto-hashed commit IDs
(st/sync! ds store "main" {:crypto-hash? true})

;; Walk the commit DAG and verify every commit's recomputed cid
(audit/verify-chain store {:branch "main"})
;; => {:status :ok :commits [{:cid ... :recomputed ... :status :ok} ...]}

;; Deep walk: also verify every column's PSS-tree node hashes
(audit/verify-chain store {:branch "main" :deep? true})
```

Adapters that embed stratum inside a larger storage substrate (e.g. as a Datalog secondary index) can implement the `IAuditable` protocol on their wrapper types to compose stratum verification with other engines' verification in one pass. The result-map shape (`:ok`, `:mismatch`, `:unsupported`, `:advisory`, `:incomplete`) is stable so multi-engine drivers don't need translation layers. See [doc/audit.md](doc/audit.md) for the full surface.

## Bitemporal (SQL:2011 valid- and system-time)

Datasets opt into bitemporal semantics via `:metadata`. Axis columns (`_valid_from`/`_valid_to`, `_system_from`/`_system_to`) are tracked separately, write primitives enforce non-overlap and append system-time history, and queries can pin both axes independently.

```clojure
(def t (st/make-dataset
        {:_valid_from   (long-array [...])  ;; epoch-micros
         :_valid_to     (long-array [...])
         :_system_from  (long-array [...])
         :_system_to    (long-array [...])
         :eid           (long-array [...])
         :price         (double-array [...])}
        {:metadata
         {:bitemporal
          {:valid  {:from-col :_valid_from :to-col :_valid_to :unit :micros}
           :system {:from-col :_system_from :to-col :_system_to :unit :micros}}}}))

;; SQL:2011 DML — surgical replace of a sub-period:
(st/q "DELETE FROM t FOR PORTION OF VALID_TIME
       FROM '2024-04-01' TO '2024-07-01'
       WHERE eid = 1" {"t" t})

;; Time-travel reads on either axis:
(st/q "SELECT * FROM t FOR SYSTEM_TIME AS OF '2024-06-01'
                       FOR VALID_TIME AS OF '2024-03-15'" {"t" t})
```

Allen interval predicates (`OVERLAPS`, `CONTAINS`, `PRECEDES`, `MEETS`, …) and `FOR PORTION OF VALID_TIME` DML are supported; `FOR SYSTEM_TIME` DML is intentionally not exposed (audit-integrity by construction). See [doc/temporal-design.md](doc/temporal-design.md) for the full design, including the rationale for the system-axis read-only stance and how stratum compares to XTDB / SQL:2011 RDBMSes / cloud-warehouse Time Travel.

## SQL Capabilities

**DML**: SELECT, INSERT, UPDATE, DELETE, UPSERT (INSERT ON CONFLICT), UPDATE FROM (joined updates), CREATE TABLE, DROP TABLE

**Bitemporal (SQL:2011)**: `FOR PORTION OF VALID_TIME` DML on bitemporal tables, `FOR VALID_TIME AS OF` / `FOR SYSTEM_TIME AS OF` time-travel reads, Allen interval predicates (OVERLAPS / CONTAINS / PRECEDES / MEETS / …). See [doc/temporal-design.md](doc/temporal-design.md).

**Joins**: INNER, LEFT, RIGHT, FULL, ASOF (with optional LEFT outer) - single and multi-column keys

**ASOF JOIN**: `ASOF [LEFT] JOIN dim ON l.key = r.key AND l.ts >= r.ts` - DuckDB-style syntax. Each probe row matches the closest preceding (or following) build row per partition. Radix-partitioned, parallel, two-pointer merge.

**Time-series helpers** (Clojure API): `stratum.api/window-join` (q-style `wj`: aggregate all right rows in `[t+lo, t+hi]` per left row, prefix-sum-accelerated for SUM/AVG/COUNT), `stratum.api/latest-on` (most-recent row per partition, equivalent to `DISTINCT ON`), `stratum.api/generate-series` (dense numeric or temporal spine for gap-filling joins).

**Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, SUM/AVG/COUNT/MIN/MAX OVER - with PARTITION BY, ORDER BY, and frame clauses (both `ROWS` and `RANGE BETWEEN INTERVAL ...` for value-distance sliding windows on irregular time series)

**Subqueries and composition**: CTEs (WITH), uncorrelated subqueries (IN/NOT IN), derived tables in FROM

**Expressions**: CASE WHEN, COALESCE, NULLIF, GREATEST, LEAST, CAST, arithmetic (+, -, \*, /, %)

**Date/time**: DATE_TRUNC, DATE_ADD, DATE_DIFF, EXTRACT (year/month/day/hour/minute/second/millisecond/microsecond/day-of-week/week-of-year), TIME_BUCKET, EPOCH_DAYS, EPOCH_SECONDS. TIMESTAMP columns track precision via `:temporal-unit` metadata (`:days` / `:seconds` / `:millis` / `:micros`); the kernels dispatch on the unit, with microseconds the DuckDB-compatible default.

**String**: LIKE, ILIKE, LENGTH, UPPER, LOWER, SUBSTR (usable in SELECT, WHERE, GROUP BY, ORDER BY)

**Aggregates**: SUM, COUNT, AVG, MIN, MAX, STDDEV, VARIANCE, CORR, MEDIAN, PERCENTILE_CONT, APPROX_QUANTILE, COUNT(DISTINCT), FILTER clause

**Ad-hoc file queries**: `read_csv()`, `read_parquet()`

**Analytics**: CREATE MODEL, ANOMALY_SCORE('model') / ANOMALY_SCORE('model', expr, ...), ANOMALY_PREDICT, ANOMALY_PROBA, ANOMALY_CONFIDENCE (isolation forest via SQL)

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

> **Note:** The DSL is still a work in progress. SQL strings are the more complete interface - use the DSL when you want to compose queries programmatically or pass in Clojure data directly without a SQL layer.

The DSL is intentionally flat. Every clause resolves column names by keyword lookup against a single merged map: `:from` establishes the base columns, `:join` merges in the dimension table's columns, and all subsequent clauses (`:where`, `:agg`, `:group`, `:select`, `:having`, `:order`) reference any column by its keyword. This makes it straightforward to build queries from Clojure data - no quoting, no SQL string interpolation, just maps and vectors. Composition (the DSL equivalent of SQL CTEs/subqueries) is done with Clojure `let`/`def` - see [Column Scoping and Composition](doc/query-engine.md#column-scoping-and-composition) for details.

```clojure
;; Full query map
{:from    {:col1 data1 :col2 data2}     ;; Column data (arrays, indices, or encoded)
 :join    [{:with {:k data}             ;; Dimension table columns
            :on   [:= :col1 :k]        ;; :col1 from :from, :k from :with - both visible after join
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
;;                         [:date-trunc :day :ts] [:hour :ts]  ;; (or :year :month :millisecond :microsecond, etc.)
;;                         [:time-bucket 5 :minutes :ts]       ;; arbitrary-width bucketing
;;                         [:coalesce :a 0] [:nullif :a 0]
;;                         [:greatest :a :b] [:least :a :b]
;; Window ops:             :row-number :rank :dense-rank :ntile :percent-rank :cume-dist
;;                         :sum :count :avg :min :max :lag :lead
;;                         :first-value :last-value :nth-value
;;                         :fills (LOCF) :ema :rleid
;;                         :mavg :msum :mmin :mmax :mcount :mdev  ;; q-style sliding aggregates
;; Window frames:          {:type :rows  :start [N :preceding]    :end :current-row}
;;                         {:type :range :start [interval :preceding] :end :current-row}
```

## Ecosystem

Stratum is part of the [Replikativ](https://github.com/replikativ) ecosystem - a set of composable, immutable data systems:

- **[Datahike](https://github.com/replikativ/datahike)** - immutable graph database with Datalog queries
- **[Yggdrasil](https://github.com/replikativ/yggdrasil)** - branching protocol for multi-system snapshots
- **[Scriptum](https://github.com/replikativ/scriptum)** - full-text search
- **[Proximum](https://github.com/replikativ/proximum)** - vector search

All share copy-on-write semantics and can be branched together via Yggdrasil.

## Features

- **SQL**: PostgreSQL wire protocol (v3), full DML, CTEs, window functions, joins, subqueries
- **Query planner**: cost-based IR planner with predicate pushdown, top-N rewrite, window-having pushdown, NDV-based join cardinality, operator fusion, column pruning
- **Performance**: SIMD filter/aggregate/group-by via Java Vector API, fused single-pass execution, zone map pruning, parallel execution
- **Persistence**: O(1) CoW snapshots, branching, time-travel, lazy loading from storage; SQL `CREATE TABLE`/`INSERT`/`CREATE MODEL` + live-table bindings survive restart when the server is started with `:store`
- **Audit**: content-addressed `:crypto-hash?` commits, `stratum.audit/verify-chain` for tamper detection (shallow + deep PSS-tree walks), `IAuditable` protocol for embedding into larger storage substrates ([doc/audit.md](doc/audit.md))
- **Bitemporal**: SQL:2011 valid- and system-time axes, `FOR PORTION OF VALID_TIME` DML, `FOR (VALID|SYSTEM)_TIME AS OF` time-travel reads, Allen interval predicates ([doc/temporal-design.md](doc/temporal-design.md))
- **Data**: CSV/Parquet import, dictionary-encoded strings, PostgreSQL NULL semantics, ad-hoc file queries, user-defined `ENUM` types (`CREATE TYPE … AS ENUM (…)` with INSERT validation + `pg_type`/`pg_enum` introspection)
- **Integration**: tablecloth/tech.ml.dataset interop, Datahike, Yggdrasil
- **Analytics**: Isolation forest anomaly detection (SQL model management, scoring, online rotation)
- **Time-series**: microsecond-precision TIMESTAMP, RANGE-BETWEEN-INTERVAL frames, TIME_BUCKET, FIRST/LAST/NTH_VALUE, FILLS/LOCF, EMA, RLEID, q-style moving aggregates (MAVG/MSUM/MMIN/MMAX/MDEV), window-join (`wj`) and LATEST ON (DISTINCT ON) helpers

## Architecture

```
User → stratum.api/q
         ├── SQL string → query map
         └── query map
              ↓
         IR Planner (build-logical-plan → optimize → execute-physical)
              ├── Predicate pushdown, top-N rewrite, window-having pushdown
              ├── Strategy selection (SIMD vs chunked vs hash, dense vs hash group-by)
              ├── Operator fusion (join+group, join+global-agg, bitmap semi-join)
              └── Cardinality-aware join reordering (NDV-based)
              ↓
         Physical execution
              ├── Fused SIMD (filter+agg, single pass)
              ├── Dense/hash group-by (radix-partitioned)
              ├── Chunked streaming (index inputs, zone-map pruning)
              ├── ASOF / hash join (parallel, fused with adjacent ops)
              └── Scalar fallback
              ↓
         Copy-on-write columnar storage
         (branch, snapshot, lazy load)
```

**Data representations:**
- `long[]` / `double[]` - heap arrays for raw columnar data
- `PersistentColumnIndex` - chunked B-tree with per-chunk statistics and zone maps
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

- **JDK 22+** (for Vector API incubator module + foreign-memory API)
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

# or directly with javac (`clj -T:build compile-java` is preferred — it
# auto-discovers every .java under src-java/ and uses the deps.edn classpath)
javac --add-modules jdk.incubator.vector -d target/classes \
  src-java/stratum/internal/*.java
# Restart REPL (JVM can't reload classes)
```

## Work with us

If you need help getting Stratum into production, we can help with integration, custom development, and support contracts. Contact [contact@datahike.io](mailto:contact@datahike.io) or visit [datahike.io](https://datahike.io/about).

## License

Copyright (c) 2026 Christian Weilbach and contributors.

Apache 2.0. See [LICENSE](LICENSE).
