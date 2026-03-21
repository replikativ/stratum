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
| B1 | TPC-H Q6: filter + SUM(price*discount) | **13.8ms** | 7.8ms | 27.7ms | 5.9ms | **2.0x** |
| B2 | TPC-H Q1: GROUP BY + 7 aggregates | **74.4ms** | 23.3ms | 93.4ms | 18.4ms | **1.3x** |
| B3 | SSB Q1.1: filter + SUM(price*discount) | **13.6ms** | 5.2ms | 28.3ms | 6.0ms | **2.1x** |
| B4 | COUNT(*) no filter | **0.1ms** | - | 0.4ms | 0.3ms | **4.0x** |
| B5 | Filtered COUNT (NEQ predicate) | **2.8ms** | 2.6ms | 11.3ms | 2.5ms | **4.0x** |
| B6 | Low-cardinality GROUP BY + COUNT | **16.6ms** | 9.8ms | 24.7ms | 5.1ms | **1.5x** |
| SSB-Q1.2 | Tighter filter + SUM(price*discount) | **12.8ms** | 4.8ms | 23.7ms | 4.9ms | **1.8x** |

Stratum's fused filter+aggregate execution evaluates predicates and accumulates results in a single SIMD pass, avoiding intermediate arrays.

### Tier 2: H2O.ai db-benchmark

Group-by queries from the [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/), testing various group cardinalities and aggregation types.

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | GROUP BY id1 (string, 100 groups), SUM | **27.2ms** | 9.8ms | 44.7ms | 13.8ms | **1.6x** |
| Q2 | GROUP BY id1,id2 (string, 10K groups), SUM | **42.2ms** | 23.6ms | 122.8ms | 49.8ms | **2.9x** |
| Q3 | GROUP BY id3 (string, 100K groups), SUM+AVG | **74.2ms** | 79.1ms | 366.3ms | 155.5ms | **4.9x** |
| Q4 | GROUP BY id4 (int, 100 groups), 3xAVG | **58.7ms** | 9.6ms | 53.2ms | 9.2ms | 0.9x |
| Q5 | GROUP BY id6 (int, 100K groups), 3xSUM | **85.8ms** | 89.9ms | 149.6ms | 109.8ms | **1.7x** |
| Q6 | GROUP BY id4,id5 (10K groups), STDDEV | **34.2ms** | 14.8ms | 91.4ms | 39.7ms | **2.7x** |
| Q7 | GROUP BY id3 (string, 100K groups), MAX-MIN | **101.7ms** | 115.0ms | 395.4ms | 202.7ms | **3.9x** |
| Q8 | Top-2 per group (ROW_NUMBER window) | 1709.6ms | 1166.6ms | **1237.6ms** | 330.7ms | DuckDB 1.4x |
| Q9 | GROUP BY id2,id4 (10K groups), CORR | **68.1ms** | 24.0ms | 143.4ms | 54.7ms | **2.1x** |
| Q10 | GROUP BY 6 columns (10M unique groups) | **885.9ms** | 722.1ms | 7226.7ms | 6180.3ms | **8.2x** |

**Integer Pipeline Queries** (native long[] accumulation, no longToDouble conversion):

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| INT-Q1 | GROUP BY id4 (100 groups), SUM(v1)+SUM(v2)+COUNT | 46.6ms | 11.0ms | **36.2ms** | 6.2ms | DuckDB 1.3x |
| INT-Q2 | GROUP BY id4 (100 groups), SUM+MIN+MAX+COUNT | 63.2ms | 17.9ms | **35.5ms** | 7.2ms | DuckDB 1.8x |
| INT-Q3 | SUM(v1)+SUM(v2)+COUNT (global multi-sum) | **0.4ms** | 0.4ms | 14.4ms | 2.7ms | **34x** |

INT-Q3 demonstrates the all-long SIMD multi-sum path with LongVector accumulators: 34x faster than DuckDB by avoiding all type conversion overhead. INT-Q1/Q2 show the dense group-by long path; the Clojure orchestration overhead is visible at 100 groups where Java compute is only ~20ms.

**H2O Join Queries:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| J1 | INNER JOIN small (100 rows), 2xSUM | 37.9ms | 5.5ms | **35.5ms** | 5.1ms | DuckDB 1.1x |
| J2 | INNER JOIN medium (10K), 2-column key, SUM | **75.0ms** | 31.6ms | 77.8ms | 23.7ms | **1.0x** |
| J3 | LEFT JOIN medium (10K), 2-column key, SUM | **76.5ms** | 32.0ms | 83.6ms | 47.6ms | **1.1x** |

**Bitmap Semi-Join Queries** (existence-only joins via BitSet probe):

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| SEMI-Q1 | SUM(v1) WHERE EXISTS customer(nation=5) | **88.1ms** | 29.4ms | 264.2ms | 114.9ms | **3.0x** |
| SEMI-Q2 | SUM(v1) GROUP BY id4 WHERE EXISTS | **72.0ms** | 60.3ms | 280.9ms | 104.0ms | **3.9x** |
| SEMI-Q3 | COUNT(*) WHERE EXISTS customer(nation=5) | **77.9ms** | 36.4ms | 236.9ms | 91.7ms | **3.0x** |

Bitmap semi-join fires automatically when a join only tests existence (no dimension columns in output). Replaces hash join with BitSet probe -- no hash table build, no row materialization.

### Tier 3: ClickBench

Queries from the [ClickBench](https://benchmark.clickhouse.com/) web analytics benchmark (6M rows from CSV). Organized by query type.

**Metadata / Stats-only queries** (answered from pre-computed per-chunk statistics without scanning data):

| Query | Description | Stratum 1T | DuckDB 1T | 1T Ratio |
|-------|-------------|-----------|----------|---------|
| Q2 | SUM + COUNT + AVG (3 aggregates) | **0.2ms** | 9.9ms | **~48x** |
| Q3 | AVG(UserID) | **0.1ms** | 8.7ms | **~76x** |
| Q6 | MIN + MAX(EventTime) | **0.1ms** | 7.7ms | **~76x** |

**Filter + aggregate:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | COUNT WHERE AdvEngineID != 0 | 24.4ms | 13.1ms | **5.3ms** | 1.1ms | DuckDB 4.6x |
| Q7 | COUNT WHERE 2 predicates | 51.3ms | 14.2ms | **11.4ms** | 2.2ms | DuckDB 4.5x |

**Group-by:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| GRP-SE | GROUP BY SearchEngineID (65 groups) | **15.6ms** | 7.9ms | 20.1ms | 3.8ms | **1.3x** |
| GRP-REG | GROUP BY RegionID (3K groups), SUM+COUNT | **34.6ms** | 24.9ms | 40.3ms | 23.9ms | **1.2x** |
| Q15 | GROUP BY UserID (1.1M groups), COUNT | 127.3ms | 60.9ms | **88.4ms** | 33.4ms | DuckDB 1.4x |
| Q19+ | GROUP BY EXTRACT(minute), COUNT | **12.0ms** | 1.9ms | 1043.7ms | 190.6ms | **87x** |
| Q43 | GROUP BY DATE_TRUNC(minute), COUNT | 73.3ms | 38.0ms | **59.2ms** | 25.0ms | DuckDB 1.2x |
| Q12 | GROUP BY SearchPhrase (string), COUNT | 314.8ms | 281.1ms | **114.6ms** | 26.8ms | DuckDB 2.7x |
| Q33 | GROUP BY URL (1.9M string groups), COUNT | **134.1ms** | 78.3ms | 389.0ms | 114.5ms | **2.9x** |

**COUNT DISTINCT:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q5 | COUNT(DISTINCT UserID) -- 1.1M distinct | 123.2ms | 142.6ms | **67.4ms** | 27.0ms | DuckDB 1.8x |
| CD-GRP | COUNT(DISTINCT AdvEngineID) GROUP BY RegionID | **33.3ms** | 16.9ms | 37.3ms | 23.4ms | **1.1x** |
| Q8 | GROUP BY RegionID, COUNT(DISTINCT UserID), TOP 10 | **81.8ms** | 93.7ms | 84.6ms | 33.3ms | **1.0x** |
| Q9 | GROUP BY RegionID, SUM+COUNT+AVG+COUNT(DISTINCT) | **121.1ms** | 126.5ms | 125.8ms | 58.0ms | **1.0x** |

**LIKE pattern matching:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| LIKE1 | COUNT WHERE URL LIKE '%example.com/page%' | **26.4ms** | 23.9ms | 280.6ms | 49.0ms | **10.6x** |
| LIKE2 | COUNT WHERE URL LIKE '%search%' | **55.7ms** | 27.0ms | 268.7ms | 45.2ms | **4.8x** |
| LIKE3 | GROUP BY SearchEngineID WHERE URL LIKE '%shop%' | **31.5ms** | 25.2ms | 271.6ms | 47.7ms | **8.6x** |
| Q20 | COUNT WHERE URL LIKE '%google%' | **27.7ms** | 34.7ms | 205.9ms | 33.8ms | **7.4x** |

**String functions:**

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q27 | GROUP BY CounterID, AVG(LENGTH(URL)) HAVING COUNT > 100K | **45.8ms** | 26.3ms | 200.9ms | 36.2ms | **4.4x** |
| Q28 | AVG(LENGTH(URL)) | **42.0ms** | 24.6ms | 197.2ms | 32.0ms | **4.7x** |

+CB-Q19: DuckDB v1.4.4 regression -- EXTRACT uses full scan instead of direct aggregation. Fixed in DuckDB v1.5.0.

### Tier 4: NYC Taxi

Real-world trip data (~5.8M rows from CSV).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| Q1 | AVG(fare) GROUP BY payment_type | **15.5ms** | 4.4ms | 21.9ms | 5.4ms | **1.4x** |
| Q2 | AVG(tip) GROUP BY passenger_count | 45.3ms | 31.9ms | **24.1ms** | 4.4ms | DuckDB 1.9x |
| Q3 | COUNT GROUP BY hour, day-of-week | **16.7ms** | 10.9ms | 17.6ms | 3.3ms | **1.1x** |
| Q4 | SUM(total) WHERE fare > 10 GROUP BY month | 41.2ms | 8.5ms | **30.9ms** | 6.5ms | DuckDB 1.3x |

### Tier 5: Hash Join

Fact table (10M rows) joined to dimension table (1K rows), followed by GROUP BY + SUM.

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| JOIN-Q1 | Fact JOIN Dim, GROUP BY category, SUM | **19.8ms** | 3.8ms | 37.6ms | 7.1ms | **1.9x** |

### Tier 6: Statistical Aggregates

Exact median/percentile (QuickSelect) and approximate quantiles (t-digest) on TPC-H price column (6M rows).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| STAT-Q1 | MEDIAN(price) | **67.8ms** | 67.8ms | 157.6ms | 134.0ms | **2.3x** |
| STAT-Q2 | GROUP BY returnflag, MEDIAN(price) | **96.1ms** | 96.8ms | 157.6ms | 131.8ms | **1.6x** |
| STAT-Q3 | PERCENTILE_CONT(0.95, price) | **52.7ms** | 37.4ms | 128.2ms | 114.2ms | **2.4x** |
| STAT-Q4 | APPROX_QUANTILE(price, 0.95) | **243.3ms** | 41.4ms | 278.1ms | 43.9ms | **1.1x** |
| STAT-Q5 | P25, P50, P75 of price | **190.9ms** | 191.1ms | 428.5ms | 408.8ms | **2.2x** |

### Tier 7: Window Functions

Window operations on TPC-H lineitem (6M rows).

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| WIN-Q1 | ROW_NUMBER() OVER (PARTITION BY orderkey) | **316.3ms** | 191.1ms | 425.6ms | 118.4ms | **1.3x** |
| WIN-Q2 | LAG(price, 1) OVER (PARTITION BY orderkey) | **352.2ms** | 215.1ms | 501.7ms | 137.0ms | **1.4x** |
| WIN-Q3 | Running SUM(price) OVER (PARTITION BY orderkey) | **387.2ms** | 263.8ms | 823.4ms | 252.5ms | **2.1x** |

Window functions benefit from multi-threading when partition sizes exceed 8 rows. Single-threaded performance is 1.3-2.1x faster than DuckDB. DuckDB's parallel window implementation achieves stronger NT scaling (3-4x vs Stratum's 1.5-1.7x).

### Tier 8: TPC-DS (sf=1, ~2.9M rows)

| Query | Description | Stratum 1T | Stratum NT | DuckDB 1T | DuckDB NT | 1T Ratio |
|-------|-------------|-----------|-----------|----------|----------|---------|
| DS-Q1 | GROUP BY store, SUM + COUNT | 22.7ms | 11.1ms | **12.6ms** | 2.9ms | DuckDB 1.8x |
| DS-Q98 | ROW_NUMBER OVER (PARTITION BY store) | **299.2ms** | 98.8ms | 376.1ms | 89.2ms | **1.3x** |

### 10M Summary

**Stratum wins 39 of 52 queries, DuckDB wins 13** (single-threaded comparison, queries > 0.1ms).

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

- **Fused execution**: Predicate evaluation and aggregation run in a single SIMD pass, avoiding intermediate array allocation
- **SIMD vectorization**: Java Vector API (DoubleVector/LongVector) processes 4 elements per cycle for all filter, aggregate, and group-by operations
- **Native long[] pipeline**: Integer columns stay as long[] through expressions, aggregation, and results -- no longToDouble conversion overhead (INT-Q3: 34x vs DuckDB)
- **Dense group-by**: Direct array-indexed accumulation for low/moderate-cardinality groups -- no hash function overhead
- **Bitmap semi-join**: Existence-only joins use BitSet probe instead of hash tables (3-4x vs DuckDB on filtered joins)
- **Zone map pruning**: Per-chunk min/max statistics allow skipping entire chunks that cannot match predicates
- **Stats-only aggregation**: SUM/COUNT/MIN/MAX/AVG answered directly from chunk statistics without touching data (CB-Q2/Q6: 0.1-0.2ms at 10M)
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
