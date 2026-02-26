# AGENTS.md

This file provides guidance working with code in this repository.

## Project Overview

**Stratum** is a persistent columnar analytics engine for the JVM, written in Clojure with Java Vector API SIMD for performance-critical paths. It provides fused filter+aggregate execution, dense and hash group-by, zone map pruning, and a PostgreSQL wire protocol server.

## Common Commands

### REPL Development

Start an nREPL server with all development dependencies:
```bash
clj -M:repl
```

Discover running nREPL ports:
```bash
clj-nrepl-eval --discover-ports
```

Evaluate Clojure code via nREPL (always use `:reload` when requiring namespaces):
```bash
clj-nrepl-eval -p <port> "(require '[stratum.index :as index] :reload)"
```

### Testing

Run all tests:
```bash
clj -M:test
```

### Benchmarks

Run OLAP benchmark suite (TPC-H, SSB, H2O, ClickBench, NYC Taxi, Hash Join):
```bash
clj -M:olap              # All tiers, 6M rows
clj -M:olap 1000000      # Custom scale
clj -M:olap h2o           # H2O tier only
```

### Building

```bash
clj -T:build jar          # Library JAR
clj -T:build uber         # Standalone uberjar
clj -T:build install      # Install locally
```

### Java Recompilation

After modifying Java files in `src-java/`:
```bash
javac --add-modules jdk.incubator.vector -d target/classes \
  src-java/stratum/internal/ColumnOps.java \
  src-java/stratum/internal/ColumnOpsExt.java \
  src-java/stratum/internal/ColumnOpsVar.java \
  src-java/stratum/internal/ColumnOpsChunked.java \
  src-java/stratum/internal/ColumnOpsChunkedSimd.java \
  src-java/stratum/internal/ColumnOpsAnalytics.java
```
**REPL must restart** after Java recompilation (JVM can't reload classes).

### JVM Options

The project requires specific JVM flags for SIMD:
- `--add-modules=jdk.incubator.vector` - Enable Vector API for SIMD
- `--enable-native-access=ALL-UNNAMED` - Enable foreign memory API

These are already configured in `deps.edn` aliases.

## Architecture

### Core Abstractions

**Query Engine** (`stratum.query`):
- Declarative query DSL (maps) and SQL string input
- Compiles to fused SIMD execution strategies
- Routes to optimal path based on query shape and data size
- Expression evaluation via Java array operations

**Java SIMD Layer** (`src-java/stratum/internal/`):
- `ColumnOps.java` (~76KB bytecode): Core filter, aggregate, group-by, join, date/string ops
- `ColumnOpsExt.java` (~24KB bytecode): JIT-isolated VARIANCE/CORR, LIKE fast-path, LongVector multi-sum, COUNT DISTINCT
- `ColumnOpsChunked.java` (~15KB bytecode): Chunked dense group-by for index streaming
- `ColumnOpsChunkedSimd.java` (~14KB bytecode): Chunked fused filter+aggregate SIMD, chunked COUNT
- `ColumnOpsAnalytics.java` (~24KB bytecode): T-digest, isolation forest, window functions, top-N
- Split into five classes to prevent JIT degradation from bytecode bloat

**PersistentColChunk** (`stratum.chunk`):
- Copy-on-write wrapper around heap arrays (`long[]` / `double[]`)
- Structural sharing until first mutation (O(chunk) CoW cost)
- Provides transient/persistent modes like Clojure collections

**PersistentColumnIndex** (`stratum.index`):
- Tree of ColChunks using persistent-sorted-set (PSS)
- O(log N) point lookup, efficient range scans
- Default chunk size: 8192 elements (L2 cache optimal)
- Per-chunk statistics (ChunkStats) for zone map pruning

**SQL Interface** (`stratum.sql`, `stratum.server`):
- PostgreSQL wire protocol v3 server (psql, DBeaver, JDBC, psycopg2)
- JSqlParser-based SQL to query map translation (SELECT, INSERT, UPDATE, DELETE, UPSERT)
- Full DML: UPDATE FROM (joined updates), INSERT ON CONFLICT, FILTER clause, GREATEST/LEAST
- Table registration with automatic dictionary encoding

**Data Import** (`stratum.csv`, `stratum.parquet`):
- CSV import with auto type detection (long/double/string)
- Parquet import via parquet-java (no Hadoop runtime)

**Storage Layer** (`stratum.storage`, `stratum.cached_storage`):
- PSS-backed lazy loading via IStorage protocol with LRU-cached konserve backend
- Lazy restore: zero startup cost, chunks loaded on demand via Fressian deserialization
- Incremental sync: only dirty PSS nodes written (leaf-to-root path)
- Atomic commit/branch tracking for snapshot isolation
- Mark-and-sweep GC walks PSS trees to find live nodes

### Key Design Patterns

1. **Fused Execution**: Predicate evaluation + aggregation in a single pass (no intermediate arrays)
2. **SIMD Vectorization**: Java Vector API (DoubleVector/LongVector) for all hot paths
3. **Dense Group-By**: Direct array indexing for low-cardinality groups (≤200K), no hash function
4. **Radix-Partitioned Hash**: 256 L2-sized hash tables for high-cardinality groups
5. **Morsel-Driven Parallelism**: 64KB work units keep data L2-cache-resident
6. **Zone Map Pruning**: Skip entire chunks based on min/max statistics
7. **Structural Sharing**: Fork is O(1), modifications use CoW on affected chunks only

### Data Flow

```
User → stratum.api/q
         ├── SQL string → sql.clj (JSqlParser) → query map
         └── query map → query.clj → execution strategy
                           ├── Fused SIMD (filter+agg, single pass)
                           ├── Multi-sum SIMD (up to 4 aggs, single pass)
                           ├── Dense group-by (array-indexed, ≤200K groups)
                           ├── Hash group-by (radix-partitioned, >200K groups)
                           ├── Chunked streaming (index → chunk-at-a-time)
                           └── Scalar fallback (<1000 rows)
                                  ↓
                           Java SIMD hot paths
                           ├── ColumnOps.java (~76KB bytecode)
                           ├── ColumnOpsExt.java (~24KB bytecode)
                           ├── ColumnOpsChunked.java (~15KB bytecode)
                           ├── ColumnOpsChunkedSimd.java (~14KB bytecode)
                           └── ColumnOpsAnalytics.java (~24KB bytecode)
```

**Data representations:**
- `long[]` / `double[]` — heap arrays (JVM GC managed)
- `PersistentColumnIndex` — chunked B-tree with per-chunk statistics and zone maps
- `String[]` → dictionary-encoded `long[]` for group-by and LIKE

## Important Constraints

### Transient/Persistent Discipline

Like Clojure collections, mutations require transient mode:
```clojure
;; CORRECT
(-> idx
    index/idx-transient
    (index/idx-set 0 999.0)
    index/idx-persistent!)

;; WRONG - will throw IllegalStateException
(index/idx-set idx 0 999.0)
```

### Memory Considerations

- All data lives on the JVM heap as `long[]` / `double[]` arrays
- Forking is cheap (structural sharing), but each modified chunk is copied
- For batch updates, use transient mode to amortize CoW overhead
- Dictionary encoding reduces memory for repeated string values
- Restored indices are lazy-loaded: only accessed PSS nodes are loaded from storage
- LRU cache (default 1000 nodes) manages working set for storage-backed indices

## Development Dependencies

The project uses local checkouts for development (see `deps.edn`):
- `persistent-sorted-set` - Tree structure for indices
- `konserve` - Durable storage backend

For CI/release, use the `:release` alias to switch to published mvn/git deps:
```bash
clj -M:release:test
```

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| idx-fork | O(1) | Structural sharing |
| idx-get | O(log chunks) + O(1) | ~100-200ns typical |
| idx-set (transient) | O(1) amortized | CoW on first write per chunk |
| idx-sum (streaming) | O(N) | SIMD-accelerated |
| idx-filter | O(N) | Returns SelectionVector, no copy |
| idx-group-by | O(N + G log G) | G = # of groups |

## Technical Documentation

See `doc/` for in-depth documentation:
- [Architecture](doc/architecture.md) — System overview, module map, walkthrough
- [SIMD Internals](doc/simd-internals.md) — Java Vector API patterns, JIT lessons
- [Query Engine](doc/query-engine.md) — Dispatch logic, expressions, optimization
- [Storage and Indices](doc/storage-and-indices.md) — Chunks, CoW, zone maps
- [Benchmarks](doc/benchmarks.md) — Methodology, results, reproducing
- [SQL Interface](doc/sql-interface.md) — PgWire server, SQL translation
- [Anomaly Detection](doc/anomaly-detection.md) — Isolation forest training, scoring, online rotation
