# Benchmarks

Performance comparison of Stratum vs DuckDB across standard analytical benchmarks. Primary results on an 8-core Intel Lunar Lake laptop (10M rows), with partial 100M-row results from a 20-core AMD EPYC server.

## Methodology

- **Warmup**: 5 iterations (10M) / 2 iterations (100M) before measurement
- **Measurement**: 10 iterations (10M) / 5 iterations (100M), median reported
- **GC**: `System.gc()` + 200ms sleep between benchmarks to prevent GC pause inflation
- **Threading**: 1T = single-threaded, NT = all cores
- **DuckDB**: v1.4.4, JDBC in-process, `SET threads TO 1` for single-threaded comparisons
- **Data**: Real CSV inputs (TPC-H lineitem, ClickBench hits, NYC taxi) and synthetic data (H2O db-benchmark, TPC-DS via DuckDB)
- **Input modes**: Arrays (raw `long[]`/`double[]`) and Indices (`PersistentColumnIndex`). Tables show best of both modes.
- **Validation**: All queries cross-checked against DuckDB results
- **JIT warmup**: All Java hot paths pre-exercised with tiny data (1000 rows) before benchmarks to ensure stable JIT compilations

Ratio > 1.0x = Stratum faster, < 1.0x = DuckDB faster.

## Results (10M rows)

Hardware: Intel Core Ultra 7 258V (8 cores, Lunar Lake), Linux, JVM 25.0.1, DuckDB v1.4.4.

### Tier 1: TPC-H / SSB

Standard decision-support queries on TPC-H lineitem data (6M rows from CSV).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| B1 | TPC-H Q6: filter + SUM(price*discount) | **16.9ms** | 10.5ms | 28.3ms | 5.6ms | **1.7x** |
| B2 | TPC-H Q1: GROUP BY + 7 aggregates | 108.7ms | 93.0ms | **93.6ms** | 17.3ms | DuckDB 1.2x |
| B3 | SSB Q1.1: filter + SUM(price*discount) | **16.5ms** | 7.2ms | 28.4ms | 5.8ms | **1.7x** |
| B4 | COUNT(*) no filter | **0.2ms** | - | 0.4ms | 0.3ms | **2.2x** |
| B5 | Filtered COUNT (NEQ predicate) | **3.3ms** | 1.6ms | 11.5ms | 2.6ms | **3.4x** |
| B6 | Low-cardinality GROUP BY + COUNT | **16.9ms** | 7.5ms | 24.4ms | 4.7ms | **1.4x** |
| SSB-Q1.2 | Tighter filter + SUM(price*discount) | **15.9ms** | 7.0ms | 23.4ms | 4.5ms | **1.5x** |

Stratum's fused filter+aggregate execution evaluates predicates and accumulates results in a single SIMD pass, avoiding intermediate arrays.

### Tier 2: H2O.ai db-benchmark

Group-by queries from the [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/), testing various group cardinalities and aggregation types.

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | GROUP BY id1 (string, 100 groups), SUM | **22.7ms** | 9.3ms | 45.4ms | 13.4ms | **2.0x** |
| Q2 | GROUP BY id1,id2 (string, 10K groups), SUM | **30.5ms** | 14.4ms | 124.7ms | 47.9ms | **4.1x** |
| Q3 | GROUP BY id3 (string, 100K groups), SUM+AVG | **67.2ms** | 68.6ms | 364.4ms | 143.2ms | **5.4x** |
| Q4 | GROUP BY id4 (int, 100 groups), 3xAVG | 44.3ms | 9.4ms | **46.5ms** | 8.5ms | **1.1x** |
| Q5 | GROUP BY id6 (int, 100K groups), 3xSUM | **80.3ms** | 82.1ms | 142.7ms | 92.8ms | **1.8x** |
| Q6 | GROUP BY id4,id5 (10K groups), STDDEV | **29.7ms** | 13.6ms | 81.1ms | 36.2ms | **2.7x** |
| Q7 | GROUP BY id3 (string, 100K groups), MAX-MIN | **83.6ms** | 84.6ms | 360.7ms | 143.3ms | **4.3x** |
| Q8 | Top-2 per group (ROW_NUMBER window) | 1406.9ms | 821.2ms | **1230.5ms** | 312.8ms | DuckDB 1.1x |
| Q9 | GROUP BY id2,id4 (10K groups), CORR | **61.0ms** | 24.5ms | 135.8ms | 50.5ms | **2.2x** |
| Q10 | GROUP BY 6 columns (10M unique groups) | **754.4ms** | 588.5ms | 7110.1ms | 6020.3ms | **9.4x** |

**Integer Pipeline Queries** (native long[] accumulation, no longToDouble conversion):

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| INT-Q1 | GROUP BY id4 (100 groups), SUM(v1)+SUM(v2)+COUNT | 41.9ms | 7.6ms | **30.8ms** | 5.7ms | DuckDB 1.4x |
| INT-Q2 | GROUP BY id4 (100 groups), SUM+MIN+MAX+COUNT | 63.1ms | 13.9ms | **35.3ms** | 7.7ms | DuckDB 1.8x |
| INT-Q3 | SUM(v1)+SUM(v2)+COUNT (global multi-sum) | **0.5ms** | 0.5ms | 13.2ms | 2.4ms | **25.4x** |

INT-Q3 demonstrates the all-long SIMD multi-sum path with LongVector accumulators: 34x faster than DuckDB by avoiding all type conversion overhead. INT-Q1/Q2 show the dense group-by long path; the Clojure orchestration overhead is visible at 100 groups where Java compute is only ~20ms.

**H2O Join Queries:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| J1 | INNER JOIN small (100 rows), 2xSUM | 44.8ms | 13.4ms | **27.0ms** | 4.8ms | DuckDB 1.7x |
| J2 | INNER JOIN medium (10K), 2-column key, SUM | **67.1ms** | 31.0ms | 71.2ms | 47.7ms | **1.1x** |
| J3 | LEFT JOIN medium (10K), 2-column key, SUM | **78.1ms** | 34.4ms | 84.2ms | 47.6ms | **1.1x** |

**Bitmap Semi-Join Queries** (existence-only joins via BitSet probe):

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| SEMI-Q1 | SUM(v1) WHERE EXISTS customer(nation=5) | **44.4ms** | 48.4ms | 243.4ms | 84.8ms | **5.5x** |
| SEMI-Q2 | SUM(v1) GROUP BY id4 WHERE EXISTS | **62.9ms** | 61.6ms | 278.3ms | 93.6ms | **4.4x** |
| SEMI-Q3 | COUNT(*) WHERE EXISTS customer(nation=5) | **37.6ms** | 83.0ms | 215.1ms | 80.8ms | **5.7x** |

Bitmap semi-join fires automatically when a join only tests existence (no dimension columns in output). Replaces hash join with BitSet probe -- no hash table build, no row materialization.

### Tier 3: ClickBench

Queries from the [ClickBench](https://benchmark.clickhouse.com/) web analytics benchmark (6M rows from CSV). Organized by query type.

**Metadata / Stats-only queries** (answered from pre-computed per-chunk statistics without scanning data; filter-aware stats-only promotes when zone maps fully classify all chunks):

| Query | Description | Stratum 1T | DuckDB 1T | 1T Ratio |
|-------|-------------|-----------|----------|---------|
| Q2 | SUM + COUNT + AVG (3 aggregates) | **0.3ms** | 9.0ms | **~35x** |
| Q3 | AVG(UserID) | **0.2ms** | 7.0ms | **~43x** |
| Q6 | MIN + MAX(EventTime) | **0.2ms** | 6.6ms | **~33x** |

**Filter + aggregate:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | COUNT WHERE AdvEngineID != 0 | 3.2ms | 7.6ms | **5.9ms** | 1.2ms | 1.9x |
| Q7 | COUNT WHERE 2 predicates | 36.8ms | 15.5ms | **8.7ms** | 1.9ms | DuckDB 4.2x |

**Group-by:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| GRP-SE | GROUP BY SearchEngineID (65 groups) | **12.3ms** | 6.7ms | 14.2ms | 2.7ms | **1.2x** |
| GRP-REG | GROUP BY RegionID (3K groups), SUM+COUNT | 31.9ms | 31.3ms | **30.6ms** | 20.0ms | 1.0x |
| Q15 | GROUP BY UserID (1.1M groups), COUNT | 125.0ms | 57.4ms | **75.2ms** | 33.4ms | DuckDB 1.7x |
| Q19+ | GROUP BY EXTRACT(minute), COUNT | **10.2ms** | 2.8ms | 1070.6ms | 189.5ms | **105x** |
| Q43 | GROUP BY DATE_TRUNC(minute), COUNT | 71.2ms | 44.5ms | **48.3ms** | 23.4ms | DuckDB 1.5x |
| Q12 | GROUP BY SearchPhrase (string), COUNT | 293.5ms | 263.3ms | **107.9ms** | 25.1ms | DuckDB 2.7x |
| Q33 | GROUP BY URL (1.9M string groups), COUNT | 965.2ms | 1017.8ms | **373.8ms** | 103.7ms | DuckDB 2.6x |

**COUNT DISTINCT:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q5 | COUNT(DISTINCT UserID) -- 1.1M distinct | 112.1ms | 113.1ms | **61.1ms** | 25.4ms | DuckDB 1.8x |
| CD-GRP | COUNT(DISTINCT AdvEngineID) GROUP BY RegionID | 27.9ms | 14.8ms | **26.4ms** | 20.3ms | 1.0x |
| Q8 | GROUP BY RegionID, COUNT(DISTINCT UserID), TOP 10 | **69.4ms** | 77.3ms | 76.4ms | 31.9ms | **1.1x** |
| Q9 | GROUP BY RegionID, SUM+COUNT+AVG+COUNT(DISTINCT) | **111.9ms** | 119.6ms | 118.0ms | 53.0ms | **1.1x** |

**LIKE pattern matching:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| LIKE1 | COUNT WHERE URL LIKE '%example.com/page%' | **19.2ms** | 30.0ms | 272.5ms | 46.7ms | **14.2x** |
| LIKE2 | COUNT WHERE URL LIKE '%search%' | **47.2ms** | 27.8ms | 246.4ms | 41.7ms | **5.2x** |
| LIKE3 | GROUP BY SearchEngineID WHERE URL LIKE '%shop%' | **25.1ms** | 26.0ms | 254.0ms | 44.5ms | **10.1x** |
| Q20 | COUNT WHERE URL LIKE '%google%' | **24.9ms** | 38.1ms | 184.1ms | 32.1ms | **7.4x** |

**String functions:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q27 | GROUP BY CounterID, AVG(LENGTH(URL)) HAVING COUNT > 100K | **45.0ms** | 28.3ms | 197.5ms | 34.1ms | **4.4x** |
| Q28 | AVG(LENGTH(URL)) | **41.3ms** | 25.5ms | 173.4ms | 29.9ms | **4.2x** |

+CB-Q19: DuckDB v1.4.4 regression -- EXTRACT uses full scan instead of direct aggregation. Fixed in DuckDB v1.5.0.

### Tier 4: NYC Taxi

Real-world trip data (~5.8M rows from CSV).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | AVG(fare) GROUP BY payment_type | **15.4ms** | 2.8ms | 21.7ms | 4.1ms | **1.4x** |
| Q2 | AVG(tip) GROUP BY passenger_count | 46.7ms | 31.4ms | **23.2ms** | 4.4ms | DuckDB 2.0x |
| Q3 | COUNT GROUP BY hour, day-of-week | **16.8ms** | 10.7ms | 17.4ms | 3.3ms | **1.0x** |
| Q4 | SUM(total) WHERE fare > 10 GROUP BY month | 44.1ms | 9.4ms | **31.6ms** | 6.2ms | DuckDB 1.4x |

### Tier 5: Hash Join

Fact table (10M rows) joined to dimension table (1K rows), followed by GROUP BY + SUM.

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| JOIN-Q1 | Fact JOIN Dim, GROUP BY category, SUM | **21.9ms** | 6.4ms | 36.3ms | 6.8ms | **1.7x** |

### Tier 6: Statistical Aggregates

Exact median/percentile (QuickSelect) and approximate quantiles (t-digest) on TPC-H price column (6M rows).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| STAT-Q1 | MEDIAN(price) | **66.6ms** | 65.6ms | 150.2ms | 133.0ms | **2.3x** |
| STAT-Q2 | GROUP BY returnflag, MEDIAN(price) | **96.4ms** | 96.6ms | 157.1ms | 135.7ms | **1.6x** |
| STAT-Q3 | PERCENTILE_CONT(0.95, price) | **51.7ms** | 36.8ms | 128.9ms | 114.6ms | **2.5x** |
| STAT-Q4 | APPROX_QUANTILE(price, 0.95) | **241.0ms** | 42.8ms | 276.7ms | 45.7ms | **1.1x** |
| STAT-Q5 | P25, P50, P75 of price | **188.1ms** | 187.6ms | 443.8ms | 413.5ms | **2.4x** |

### Tier 7: Window Functions

Window operations on TPC-H lineitem (6M rows).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| WIN-Q1 | ROW_NUMBER() OVER (PARTITION BY orderkey) | **327.3ms** | 197.3ms | 442.2ms | 133.2ms | **1.4x** |
| WIN-Q2 | LAG(price, 1) OVER (PARTITION BY orderkey) | **361.8ms** | 222.3ms | 505.8ms | 139.3ms | **1.4x** |
| WIN-Q3 | Running SUM(price) OVER (PARTITION BY orderkey) | **408.5ms** | 271.0ms | 855.1ms | 276.1ms | **2.1x** |

Window functions benefit from multi-threading when partition sizes exceed 8 rows. Single-threaded performance is 1.3-2.1x faster than DuckDB. DuckDB's parallel window implementation achieves stronger NT scaling (3-4x vs Stratum's 1.5-1.7x).

### Tier 8: TPC-DS (sf=1, ~2.9M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| DS-Q1 | GROUP BY store, SUM + COUNT | 24.0ms | 12.4ms | **12.2ms** | 2.9ms | DuckDB 2.0x |
| DS-Q98 | ROW_NUMBER OVER (PARTITION BY store) | **307.9ms** | 148.1ms | 388.1ms | 94.4ms | **1.3x** |

### 10M Summary

**Stratum wins 41 of 55 queries, DuckDB wins 14** (single-threaded comparison, queries > 0.1ms). Last verified 2026-05-08 with query planner enabled (filters-on-scan + selectivity-gated dynamic filter pushdown, linear-agg rewrite, filter-aware stats-only).

### Isolation Forest

| Operation | 100K rows | 1M rows |
|-----------|----------|---------|
| Train (100 trees x 256) | 27ms | 6ms |
| Score (parallel) | 155ms | 419ms |
| Score (1-thread) | 152ms | 1555ms |

---

## Results (100M rows)

Hardware: AMD EPYC 7313 16-Core Processor (20 cores), Linux, JVM 25.0.1 (OpenJDK), DuckDB v1.4.4. Index mode only.

### Tier 1: TPC-H / SSB (100M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| B1 | TPC-H Q6: filter + SUM(price*discount) | **362.3ms** | 65.5ms | 681.8ms | 57.6ms | **1.9x** |
| B2 | TPC-H Q1: GROUP BY + 7 aggregates | **2525.7ms** | 218.7ms | 2547.0ms | 197.9ms | **1.0x** |
| B3 | SSB Q1.1: filter + SUM(price*discount) | **349.7ms** | 57.1ms | 771.4ms | 64.6ms | **2.2x** |
| B4 | COUNT(*) no filter | **0.3ms** | -- | 8.5ms | 1.4ms | **27x** |
| B5 | Filtered COUNT (NEQ predicate) | **49.5ms** | 15.6ms | 423.9ms | 37.3ms | **8.6x** |
| B6 | Low-cardinality GROUP BY + COUNT | 957.6ms | 92.4ms | **781.0ms** | 59.2ms | DuckDB 1.2x |
| SSB-Q1.2 | Tighter filter + SUM(price*discount) | **366.1ms** | 50.6ms | 501.2ms | 43.7ms | **1.4x** |

### Tier 2: H2O.ai Group-By (100M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | GROUP BY id1 (string, 100 groups) | **398.8ms** | 44.4ms | 1636.1ms | 146.1ms | **4.1x** |
| Q2 | GROUP BY id1,id2 (string, 10K groups) | **482.3ms** | 64.3ms | 3286.9ms | 296.7ms | **6.8x** |
| Q3 | GROUP BY id3 (string, 1M groups) | **7356.0ms** | 1997.7ms | 7821.1ms | 1300.1ms | **1.1x** |
| Q4 | GROUP BY id4 (int, 100 groups), 3xAVG | 1092.0ms | 79.4ms | **705.8ms** | 62.1ms | DuckDB 1.5x |
| Q5 | GROUP BY id6 (int, 1M groups), 3xSUM | 9102.3ms | 2324.4ms | **3623.5ms** | 758.1ms | DuckDB 2.5x |
| Q7 | GROUP BY id3 (string, 1M groups), MAX-MIN | 9194.8ms | 2278.0ms | **7991.2ms** | 1218.4ms | DuckDB 1.2x |
| Q10 | GROUP BY 6 columns (100M unique groups) | **137.9s** | 135.3s | 175.4s | 144.1s | **1.3x** |

**H2O Join Queries (100M rows):**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| J1 | INNER JOIN small (100 rows), 2xSUM | 527.6ms | 52.7ms | **414.0ms** | 34.2ms | DuckDB 1.3x |
| J2 | INNER JOIN medium (10K), 2-column key, SUM | **5979.7ms** | 668.3ms | 6645.9ms | 2243.4ms | **1.1x** |
| J3 | LEFT JOIN medium (10K), 2-column key, SUM | **6361.8ms** | 664.1ms | 6893.5ms | 2253.5ms | **1.1x** |

At 100M rows, multi-column joins (J2, J3) show Stratum's NT advantage growing to 3.4x over DuckDB NT.

### Tier 3: ClickBench (100M rows)

~100M rows from the full ClickBench hits dataset.

**Metadata / Stats-only queries** (answered from pre-computed per-chunk statistics without scanning data):

| Query | Description | Stratum 1T | DuckDB 1T | 1T Ratio |
|-------|-------------|-----------|----------|---------|
| Q2 | SUM + COUNT + AVG (3 aggregates) | **1.9ms** | 193.6ms | **102x** |
| Q3 | AVG(UserID) | **0.4ms** | 159.8ms | **400x** |
| Q6 | MIN + MAX(EventTime) | **0.7ms** | 200.5ms | **286x** |

**Filter + aggregate:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | COUNT WHERE AdvEngineID != 0 | 142.5ms | 59.8ms | **66.8ms** | 8.1ms | DuckDB 2.1x |
| Q7 | COUNT WHERE 2 predicates | 879.8ms | 341.8ms | **160.6ms** | 22.9ms | DuckDB 5.5x |

**Group-by:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| GRP-SE | GROUP BY SearchEngineID (96 groups) | **382.6ms** | 33.9ms | 417.9ms | 28.8ms | **1.1x** |
| GRP-REG | GROUP BY RegionID (9K groups), SUM+COUNT | **669.8ms** | 191.2ms | 748.9ms | 99.0ms | **1.1x** |
| Q15 | GROUP BY UserID (17.6M groups), COUNT | 4191.6ms | 1163.8ms | **3275.6ms** | 327.4ms | DuckDB 1.3x |
| Q19+ | GROUP BY EXTRACT(minute), COUNT | **301.1ms** | 138.0ms | 25144.8ms | 1804.6ms | regression+ |
| Q43 | GROUP BY DATE_TRUNC(minute), COUNT | 3511.0ms | 944.9ms | **1112.1ms** | 153.2ms | DuckDB 3.2x |
| Q12 | GROUP BY SearchPhrase (string), COUNT | 7762.1ms | 7497.4ms | **3947.2ms** | 241.1ms | DuckDB 2.0x |
| Q33 | GROUP BY URL (18.3M string groups), COUNT | **6551.8ms** | 2460.0ms | 11608.9ms | 907.5ms | **1.8x** |

**COUNT DISTINCT:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q5 | COUNT(DISTINCT UserID) -- 17.6M distinct | 3663.9ms | 4059.9ms | **2961.3ms** | 198.5ms | DuckDB 1.2x |
| CD-GRP | COUNT(DISTINCT AdvEngineID) GROUP BY RegionID | 1204.8ms | 382.4ms | **766.3ms** | 105.5ms | DuckDB 1.6x |
| Q8 | GROUP BY RegionID, COUNT(DISTINCT UserID), TOP 10 | 6963.6ms | 1974.7ms | **5969.9ms** | 314.4ms | DuckDB 1.2x |
| Q9 | GROUP BY RegionID, SUM+COUNT+AVG+COUNT(DISTINCT) | 9114.1ms | 3811.1ms | **6962.1ms** | 435.1ms | DuckDB 1.3x |

**String functions:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q28 | AVG(LENGTH(URL)) | **1151.2ms** | 580.0ms | 4435.5ms | 325.3ms | **3.9x** |

+CB-Q19: DuckDB v1.4.4 regression -- EXTRACT uses full scan instead of direct aggregation. Fixed in DuckDB v1.5.0.

### Tier 4: NYC Taxi (100M run, ~39.7M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | AVG(fare) GROUP BY payment_type | **149.7ms** | 17.5ms | 248.2ms | 17.5ms | **1.7x** |
| Q2 | AVG(tip) GROUP BY passenger_count | 784.1ms | 548.1ms | **257.9ms** | 19.7ms | DuckDB 3.0x |
| Q3 | COUNT GROUP BY hour, day-of-week | 183.9ms | 22.7ms | **183.8ms** | 15.3ms | 1.0x |
| Q4 | SUM(total) WHERE fare > 10 GROUP BY month | 537.9ms | 49.2ms | **400.1ms** | 32.1ms | DuckDB 1.3x |

### Tier 5: Hash Join (100M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| JOIN-Q1 | Fact JOIN Dim, GROUP BY category, SUM | 833.6ms | 54.8ms | **576.0ms** | 48.5ms | DuckDB 1.4x |

### Tier 6: Statistical Aggregates (100M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| STAT-Q1 | MEDIAN(price) | **1486.0ms** | 1650.4ms | 4315.5ms | 4198.5ms | **2.9x** |
| STAT-Q2 | GROUP BY returnflag, MEDIAN(price) | **1844.1ms** | 1830.5ms | 5091.3ms | 3617.1ms | **2.8x** |
| STAT-Q3 | PERCENTILE_CONT(0.95, price) | **1300.6ms** | 1474.6ms | 3874.9ms | 2684.0ms | **3.0x** |
| STAT-Q4 | APPROX_QUANTILE(price, 0.95) | **6139.9ms** | 657.9ms | 6182.4ms | 378.9ms | **1.0x** |
| STAT-Q5 | P25, P50, P75 of price | **4189.6ms** | 4149.4ms | 12291.1ms | 11955.1ms | **2.9x** |

Exact median and percentile operations scale well -- Stratum maintains a ~3x advantage at 100M rows using O(N) QuickSelect.

### Tier 7: TPC-DS (sf=1, ~2.9M rows, 100M run)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| DS-Q1 | GROUP BY store, SUM + COUNT | 36.5ms | 14.2ms | **19.5ms** | 4.0ms | DuckDB 1.9x |

### 100M Summary

**Stratum wins 15 of 34 queries, DuckDB wins 19** (single-threaded comparison, queries > 0.1ms).

Window functions, LIKE pattern matching, VARIANCE/CORR group-by, and string function group-by benchmarks are omitted from the 100M results -- these code paths were optimized after this hardware was last available and will be added when re-run.

DuckDB's advantage at 100M is concentrated in high-cardinality hash group-by (1M-100M unique groups) where hash tables become DRAM-bound. Stratum wins on filter+aggregate, statistical aggregates, and moderate-cardinality group-by.

---

## Key Performance Characteristics

- **Query planner**: Multi-pass optimizer builds physical plan trees with predicate pushdown, operator fusion, cost-based strategy selection (zone-map + sample selectivity estimation), and bitmap semi-join rewriting. Planning overhead is ~0.1% of query time (~5μs).
- **Fused execution**: Predicate evaluation and aggregation run in a single SIMD pass, avoiding intermediate array allocation
- **SIMD vectorization**: Java Vector API (DoubleVector/LongVector) processes 4 elements per cycle for all filter, aggregate, and group-by operations
- **Native long[] pipeline**: Integer columns stay as long[] through expressions, aggregation, and results -- no longToDouble conversion overhead (INT-Q3: 39x vs DuckDB)
- **Dense group-by**: Direct array-indexed accumulation for low/moderate-cardinality groups -- no hash function overhead
- **Bitmap semi-join**: Existence-only joins use BitSet probe instead of hash tables (3-4x vs DuckDB on filtered joins)
- **Zone map pruning**: Per-chunk min/max statistics allow skipping entire chunks that cannot match predicates
- **Stats-only aggregation**: SUM/COUNT/MIN/MAX/AVG answered directly from chunk statistics without touching data (CB-Q2/Q6: 0.1-0.3ms at 10M). **Filter-aware**: when zone maps prove every chunk is fully-classified (`:stats-only` or `:skip`), even predicated multi-aggs short-circuit the SIMD path
- **Linear-aggregate rewrite**: `SUM/AVG/MIN/MAX(s·col + o)` → base agg + decode-time recipe, eliminating temp-column materialisation. Composes with stats-only above so e.g. `SUM(price·1.05)` on indexed data runs in O(chunks)
- **Dynamic filter pushdown**: hash-join build side derives `[min,max]` of the join key and pushes a range predicate onto the probe-side scan before probing — DuckDB's `DynamicTableFilterSet` pattern. Selectivity-gated via the planner's three-tier estimator (zone-map / 128-sample / heuristic), so the push only fires when ≥20% of probe rows would be eliminated — the FK-join case where build covers the full probe range is correctly skipped
- **LIKE fast-path**: Dictionary pre-filtering with bitmask pruning + contains/startsWith/endsWith (5-11x vs DuckDB on string pattern matching)
- **QuickSelect for median/percentile**: O(N) average-case exact quantile computation (2-3x vs DuckDB)
- **Hash join with fused aggregation**: Single-pass probe + accumulate eliminates intermediate materialization
- **JIT warmup on startup**: Pre-exercises all Java code paths to produce stable polymorphic JIT compilations before real queries

## Where DuckDB Wins

- **High-cardinality group-by at scale**: At 100M rows with 1M+ unique groups, DuckDB's parallel hash aggregate scales better
- **COUNT(DISTINCT) on high cardinality**: DuckDB's HyperLogLog is faster for 1M+ distinct values
- **Sparse-selectivity filters (CB-Q1, Q7)**: When few rows match, DuckDB's column compression and zone maps can skip more data
- **Multi-threaded scaling**: DuckDB's NT advantage is generally larger -- better thread utilization at high parallelism, particularly for window functions and high-cardinality group-by
- **Dense group-by Clojure overhead**: For small group counts (100 groups), Clojure orchestration overhead (~20ms) is visible when Java compute is fast (INT-Q1/Q2)

## Known Multi-Threading Limitations

In some scenarios, Stratum's multi-threaded execution does not improve over single-threaded:

- **High-cardinality group-by** (H2O-Q3/Q5/Q7, 60K-100K groups): NT ~ 1T -- no speedup. Accumulator memory exceeds L2 cache per thread, causing DRAM bandwidth saturation. L3-adaptive thread capping mitigates the worst cases but doesn't eliminate the fundamental issue.
- **Mixed aggregation with COUNT DISTINCT** (CB-Q8/Q9): NT is slightly slower than 1T due to per-group hash table overhead scaling with thread count.
- **Statistical aggregates** (STAT-Q1/Q2/Q5): MEDIAN and multi-percentile use sequential QuickSelect, preventing parallelism.

**Root causes and planned improvements:**
1. High-cardinality group-by needs pre-aggregation or partition-aware scheduling to keep working sets L2-resident
2. COUNT DISTINCT could benefit from radix-partitioned per-group hash tables to reduce sequential merge cost
3. String-heavy paths need better parallelization of dictionary encoding and hash table probing

## Reproducing

```bash
# All tiers, 10M rows (default)
clj -M:olap

# Custom scale
clj -M:olap 1000000
clj -M:olap 100000000

# Specific tiers
clj -M:olap t1          # TPC-H/SSB only
clj -M:olap h2o         # H2O.ai only
clj -M:olap cb          # ClickBench only
clj -M:olap taxi        # NYC Taxi only
clj -M:olap join        # Hash Join only
clj -M:olap stat        # Statistical only

# Isolation forest
clj -M:iforest

# With indices
clj -M:olap idx
```

## Insert Benchmark

Throughput for appending to and maintaining a sorted `PersistentColumnIndex`. Run with `clj -M:insert-bench`.

### Append-only (1M elements)

Append to the end of the index in batches. This is the common case for time-series data.

| Batch size | Time | Throughput |
|-----------|------|-----------|
| 1 | 8308ms | 120K ops/sec |
| 10 | 1622ms | 617K ops/sec |
| 100 | 967ms | 1.03M ops/sec |
| 1000 | 900ms | 1.11M ops/sec |

### Sorted insert (100K elements)

Insert into a sorted position. Binary search via `idx-get-long` to find the position, then copy-on-write of the modified chunk.

| Batch size | Time | Throughput |
|-----------|------|-----------|
| 1 | 1066ms | 94K ops/sec |
| 10 | 996ms | 100K ops/sec |
| 100 | 1065ms | 94K ops/sec |
| 1000 | 1060ms | 94K ops/sec |

### Mixed 80/20 (100K elements)

80% append, 20% sorted insert -- similar to a write-ahead index with mostly new entries and occasional backdated facts.

| Batch size | Time | Throughput |
|-----------|------|-----------|
| 1 | 1151ms | 87K ops/sec |
| 10 | 353ms | 283K ops/sec |
| 100 | 312ms | 321K ops/sec |
| 1000 | 318ms | 315K ops/sec |

## Related Documentation

- [Architecture](architecture.md) -- System overview
- [SIMD Internals](simd-internals.md) -- How the optimizations work
- [Anomaly Detection](anomaly-detection.md) -- Isolation forest benchmarks
