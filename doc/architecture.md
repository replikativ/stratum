# Stratum Architecture

Stratum is a SIMD-accelerated columnar analytics engine for the JVM, written in Clojure with performance-critical paths in Java. It uses the Java Vector API (JDK 21+ incubator) for SIMD operations and runs entirely on heap memory managed by the JVM garbage collector.

## System Overview

```
                       ┌──────────────┐
                       │  User Input  │
                       └──────┬───────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
          SQL string              Query map (EDN)
                 │                         │
        ┌────────▼────────┐                │
        │    sql.clj      │                │
        │  (JSqlParser)   │                │
        └────────┬────────┘                │
                 │ Stratum query map       │
                 └────────────┬────────────┘
                              │
                    ┌─────────▼──────────┐
                    │     query.clj      │
                    │  Dispatch + Compile │
                    └─────────┬──────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
  ┌───────▼───────┐  ┌───────▼───────┐  ┌───────▼───────┐
  │ Fused SIMD    │  │ Dense Group   │  │ Hash Group    │
  │ filter+agg    │  │ (array-idx)   │  │ (radix-part)  │
  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              │
                    ┌──────────────▼──────────────┐
                    │       Java SIMD Layer       │
                    │  ColumnOps.java             │
                    │  ColumnOpsExt.java          │
                    │  ColumnOpsChunked.java      │
                    │  ColumnOpsChunkedSimd.java  │
                    │  ColumnOpsAnalytics.java    │
                    └─────────────────────────────┘
```

## Data Representations

Stratum operates on three data representations:

### Raw Heap Arrays

The simplest input format. Columns are `long[]` or `double[]` arrays on the JVM heap. Dictionary-encoded string columns are represented as a `long[]` of codes plus a `String[]` dictionary.

```clojure
{:price  (double-array [10.0 20.0 30.0])
 :qty    (long-array [1 2 3])
 :region (q/encode-column (into-array String ["US" "EU" "US"]))}
```

### PersistentColumnIndex (Chunked B-Tree)

A persistent sorted set (PSS) tree of `ChunkEntry` records, each containing:
- **chunk-id**: position range `[start, end]`
- **PersistentColChunk**: CoW wrapper around a `long[]` or `double[]` (8192 elements default)
- **ChunkStats**: per-chunk count, sum, sum-of-squares, min, max

Indices support O(1) fork via structural sharing and copy-on-write on mutation. The query engine can stream over chunks without materializing the full array (64KB per chunk fits L2 cache). When persisted, the PSS tree is stored in konserve and lazy-loaded on demand — opening a billion-row index costs nothing until chunks are actually accessed.

### Dictionary-Encoded Strings

`encode-column` maps `String[]` to sequential `long[]` codes plus a reverse `String[]` dictionary. This enables numeric SIMD operations on string group-by keys, and fast LIKE pattern matching via dictionary pre-filtering.

## Module Map

| File | Responsibility | Size |
|------|---------------|------|
| `src/stratum/api.clj` | Public API (q, explain, from-csv, from-parquet, server, iforest) | ~235 LOC |
| `src/stratum/query.clj` | Query compilation, dispatch, execution | ~6600 LOC |
| `src/stratum/sql.clj` | JSqlParser AST → query map / DDL translation (SELECT, INSERT, UPDATE, DELETE, UPSERT) | ~1570 LOC |
| `src/stratum/server.clj` | PostgreSQL wire protocol (pgwire) server with DML execution | ~720 LOC |
| `src/stratum/csv.clj` | CSV import with auto type detection | ~160 LOC |
| `src/stratum/parquet.clj` | Parquet import via parquet-java | ~190 LOC |
| `src/stratum/index.clj` | PersistentColumnIndex (PSS tree) | ~1340 LOC |
| `src/stratum/chunk.clj` | PersistentColChunk (CoW arrays) | ~390 LOC |
| `src/stratum/stats.clj` | ChunkStats, zone map predicates | ~400 LOC |
| `src/stratum/storage.clj` | Konserve storage backend, GC, commit/branch management | ~250 LOC |
| `src/stratum/cached_storage.clj` | PSS IStorage impl: LRU cache, Fressian handlers, lazy loading | ~310 LOC |
| `src/stratum/dataset.clj` | StratumDataset (deftype, persistence) | ~640 LOC |
| `src/stratum/iforest.clj` | Isolation forest anomaly detection (train, score, predict, rotate) | ~290 LOC |
| `src/stratum/specification.cljc` | Malli schemas for API validation (query, iforest, SQL types) | ~550 LOC |
| `src-java/.../ColumnOps.java` | Core SIMD: filter, aggregate, group-by, join, date/string ops | ~76KB bytecode |
| `src-java/.../ColumnOpsExt.java` | JIT-isolated: VARIANCE/CORR, LIKE, extract+count, LongVector, COUNT DISTINCT | ~26KB bytecode |
| `src-java/.../ColumnOpsChunked.java` | Chunked dense group-by for index streaming | ~12KB bytecode |
| `src-java/.../ColumnOpsChunkedSimd.java` | Chunked fused filter+aggregate SIMD, chunked COUNT | ~15KB bytecode |
| `src-java/.../ColumnOpsAnalytics.java` | T-digest, isolation forest, window functions, top-N | ~24KB bytecode |

## Walkthrough: TPC-H Q6

Query: _Sum revenue where shipdate in 1994, discount between 0.05-0.07, quantity < 24._

```clojure
(q/q {:from {:shipdate shipdate-arr :discount discount-arr
                  :quantity quantity-arr :price price-arr}
          :where [[:between :shipdate 8766 9131]     ;; 1994 epoch-days
                  [:between :discount 0.05 0.07]
                  [:< :quantity 24]]
          :agg [[:sum [:* :price :discount]]]})
```

**Step-by-step execution:**

1. **prepare-columns**: Resolve column references to typed arrays. Detect 2 long predicates + 1 double predicate, 1 SUM_PRODUCT aggregation.

2. **Dispatch**: Single aggregation with ≤4L+4D predicates on ≥1000 rows → fused SIMD path.

3. **query-compiler**: Build parallel arrays for Java: `longPredTypes=[PRED_RANGE, PRED_LT]`, `longCols=[shipdate, quantity]`, bounds arrays, `aggType=AGG_SUM_PRODUCT`, `aggCol1=price`, `aggCol2=discount`.

4. **fusedSimdParallel** (Java): Morsel-driven parallel execution:
   - Split 6M rows into 64K morsels across N threads
   - Each morsel: SIMD predicate evaluation (DoubleVector/LongVector broadcasts + compare → VectorMask AND-chain) → masked accumulation (`sum += price[i] * discount[i]` where all predicates pass)
   - Thread-local results merged via double[2] addition (sum + count)

5. **Result**: `[{:sum 1234567.89 :_count 114160}]`

Total time: ~4ms single-threaded, ~1ms multi-threaded (6M rows).

## Related Documentation

- [SIMD Internals](simd-internals.md) — Java Vector API patterns, fused filter+aggregate, morsel-driven parallelism
- [Query Engine](query-engine.md) — Dispatch logic, expression evaluation, optimization
- [Storage and Indices](storage-and-indices.md) — Chunks, CoW semantics, zone maps, Konserve
- [Benchmarks](benchmarks.md) — Methodology, results, reproducing
- [SQL Interface](sql-interface.md) — PgWire server, SQL translation, supported subset
- [Anomaly Detection](anomaly-detection.md) — Isolation forest training, scoring, online rotation
