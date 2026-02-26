# SIMD Internals

Stratum's performance-critical paths are implemented in Java using the JDK Vector API (incubator module since JDK 16, available in JDK 21+). Five class files contain all SIMD and analytics code:

- **ColumnOps.java** (~76KB bytecode): Core operations — filter, aggregate, group-by, join, date/string array ops
- **ColumnOpsExt.java** (~26KB bytecode): JIT-isolated extensions — VARIANCE/CORR, LIKE fast-path, LongVector multi-sum, fused extract+count, COUNT DISTINCT
- **ColumnOpsChunked.java** (~12KB bytecode): Chunked dense group-by for index streaming (no SIMD filter+agg)
- **ColumnOpsChunkedSimd.java** (~15KB bytecode): Chunked fused filter+aggregate SIMD, chunked COUNT for index streaming
- **ColumnOpsAnalytics.java** (~24KB bytecode): T-digest, isolation forest, window functions, top-N

The split into five classes is deliberate: JIT compilation quality degrades as class bytecode size grows (see [JIT Lessons](#jit-optimization-lessons)). Methods serving different execution paths (array vs chunked vs analytics) are isolated to prevent cross-section JIT interference.

## Java Vector API Primer

The Vector API provides fixed-width SIMD registers as Java objects:

```java
// Species defines vector width (machine-dependent)
static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;
static final VectorSpecies<Long>   LONG_SPECIES   = LongVector.SPECIES_PREFERRED;
// On AVX-512: 8 doubles/8 longs per register. On AVX2: 4/4.

// Load from array, compute, store
DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, prices, i);
DoubleVector d = DoubleVector.fromArray(DOUBLE_SPECIES, discounts, i);
DoubleVector product = v.mul(d);

// Predicate → mask → masked accumulation
VectorMask<Double> mask = v.compare(VectorOperators.LT, threshold);
sum = sum.add(product, mask);  // only accumulate where mask is true
```

Key concepts:
- **VectorMask**: Boolean mask from comparison, supports AND/OR
- **Masked operations**: `v.add(other, mask)` — only lanes where mask is true
- **Reduce**: `sum.reduceLanes(VectorOperators.ADD)` — horizontal sum to scalar
- **Tail handling**: Remaining elements (length % lanes) processed in scalar loop

## Fused Filter+Aggregate

The key optimization is **fusing** predicate evaluation and aggregation into a single pass. Traditional approaches materialize intermediate arrays (selection vectors), causing extra memory traffic. Stratum evaluates predicates and accumulates results in the same loop iteration. Fused filter+aggregate is an established technique in native SIMD engines (Velox, Intel OAP, many vectorized database prototypes); Stratum's contribution is achieving it via the Java Vector API without native compilation or JNI.

### fusedSimdUnrolledRange

The workhorse method for single-aggregation queries. Accepts up to 4 long predicates and 4 double predicates with one aggregation:

```java
// Simplified structure (actual code handles all pred/agg type combinations)
for (int i = 0; i < length; i += DOUBLE_LANES) {
    // 1. Evaluate long predicates → combined mask
    LongVector lv0 = LongVector.fromArray(LONG_SPECIES, longCol0, i);
    VectorMask<Long> lm = lv0.compare(GTE, longLo0).and(lv0.compare(LT, longHi0));
    // ... AND additional long predicates

    // 2. Convert long mask to double mask
    VectorMask<Double> mask = VectorMask.fromLong(DOUBLE_SPECIES, lm.toLong());

    // 3. Evaluate double predicates
    DoubleVector dv0 = DoubleVector.fromArray(DOUBLE_SPECIES, dblCol0, i);
    mask = mask.and(dv0.compare(GTE, dblLo0)).and(dv0.compare(LT, dblHi0));
    // ... AND additional double predicates

    // 4. Masked aggregation
    DoubleVector agg = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol, i);
    sum = sum.add(agg, mask);
    count += Long.bitCount(mask.toLong());
}
```

Predicate types are encoded as integer constants: `PRED_RANGE=0`, `PRED_LT=1`, `PRED_GT=2`, `PRED_EQ=3`, `PRED_LTE=4`, `PRED_GTE=5`, `PRED_NEQ=6`, `PRED_NOT_RANGE=7`. The method contains unrolled code for each predicate count (0-4 long, 0-4 double) to avoid switches in the hot loop.

Aggregation types: `AGG_SUM_PRODUCT=0`, `AGG_SUM=1`, `AGG_COUNT=2`, `AGG_MIN=3`, `AGG_MAX=4`.

### Shared JIT Compilation Unit

A critical pattern: `fusedSimdUnrolled(...)` delegates to `fusedSimdUnrolledRange(0, length)`. This ensures the JIT compiles a single method body that serves both the full-array and range-based entry points. Without this, JIT compiles them independently — warming one doesn't benefit the other (measured: 21ms → 3.2ms).

## Morsel-Driven Parallelism

Stratum uses morsel-driven execution for parallel operations:

```
ForkJoinPool (Runtime.availableProcessors() threads)
    │
    ├── Thread 0: morsels [0..64K), [N*64K..(N+1)*64K), ...
    ├── Thread 1: morsels [64K..128K), ...
    ├── Thread 2: ...
    └── Thread N-1: ...
```

Each morsel is 64K elements (~512KB for doubles), fitting comfortably in L2 cache (~1-2MB). The alternative — one large range per thread — causes cache thrashing when 8 threads each process 750K elements (24MB scattered across DRAM).

Implementation: N futures submitted to ForkJoinPool, each with an internal morsel loop. This avoids FJP overhead from submitting one future per morsel (which would be 96 futures for 6M rows).

Thread-local results are merged after all threads complete: `double[2]` addition for scalar aggregations, `double[maxKey * accSize]` element-wise merge for group-by.

## Dense Group-By

For low-cardinality groups (≤200K distinct keys), Stratum uses direct array indexing instead of hash tables:

```java
// Flat accumulator array: maxKey * accSize (e.g., 2 for SUM: [sum, count])
double[] accs = new double[maxKey * accSize];

for (int i = start; i < end; i++) {
    if (evaluatePredicates(i, ...)) {
        int key = (int) groupCol[i];
        int base = key * accSize;
        // Branchless accumulation
        accs[base]     += aggCol[i];    // sum
        accs[base + 1] += 1.0;         // count
    }
}
```

Key optimizations:
- **Flat contiguous arrays**: `double[maxKey * accSize]` instead of `double[maxKey][]` — eliminates pointer chasing, better cache utilization
- **Branchless accumulation**: `accs[base] += val * matchBit` eliminates branch misprediction at ~50% selectivity
- **Conditional morsel split**: If `maxKey * accSize * 8 < 64KB`, use morsels (data locality); otherwise single call per thread (avoids massive per-morsel allocation)
- **L3-adaptive thread capping**: Read L3 cache size from sysfs, cap threads so total accumulator memory fits L3
- **Dense key normalization**: When keys have large absolute values but small range (e.g., epoch-second date-trunc), subtract min to create a compact key space

### COUNT-Only Group-By

Separated into `fusedFilterGroupCountDenseRange` for JIT isolation. Uses inline SIMD predicate evaluation and bit-manipulation mask extraction (`toLong()` + `numberOfTrailingZeros()` + `bits &= bits-1`) instead of boolean array intermediaries. Thread-local `long[]` counts merged at end.

## Hash Group-By

For high-cardinality groups (>200K), Stratum uses an open-addressed hash table with Fibonacci hashing:

```java
// Hash table: flat long[] keys + double[] accumulators
long[] htKeys = new long[capacity];  // EMPTY_KEY sentinel
double[] htAccs = new double[capacity * accSize];

long hash = key * 0x9E3779B97F4A7C15L;
int slot = (int)(hash >>> shift);
// Linear probing with wrap-around
```

### Radix Partitioning

For parallel execution, hash group-by uses radix partitioning (256 partitions):

1. **Phase 1** (parallel): Evaluate predicates, extract group keys
2. **Phase 2** (parallel): Scatter rows into 256 partition buffers based on top 8 bits of hash
3. **Phase 3** (parallel): Per-partition aggregation using `groupAggregateHashRange` (already JIT-compiled from other paths)
4. **Phase 4**: Concatenate partition results

Each partition's hash table is ~768KB (fits L2 cache), versus a single 344MB table. A different hash constant is used for partitioning (`0x517CC1B727220A95L`) versus the per-partition hash table to avoid catastrophic collision chains.

## Multi-Aggregation

`fusedSimdMultiSumParallel` processes up to 4 SUM/SUM_PRODUCT/COUNT/AVG aggregations in a single pass. The same predicate mask is applied to all accumulators.

### LongVector Accumulators

When all aggregation columns are `long[]`, the `fusedSimdMultiSumAllLongParallel` path (in ColumnOpsExt) accumulates using `LongVector.add()` instead of converting to double first. This avoids two full-array `longToDouble` conversion passes. Conversion to double happens only at `reduceLanes()`.

## Native VARIANCE/CORR

`fusedFilterGroupAggregateDenseVarParallel` (ColumnOpsExt) uses variable-width accumulators:
- **VARIANCE**: 3 slots per group `[sum, sum_sq, count]`
- **CORR**: 6 slots per group `[sum_x, sum_y, sum_xy, sum_xx, sum_yy, count]`

All slots are additive, enabling simple parallel merge. Final computation (Welford formula, Pearson coefficient) happens in Clojure decode.

### Chunked Streaming

For PersistentColumnIndex inputs, `fusedGroupAggregateDenseChunkedParallel` streams over chunks without materializing the full array. Each 8192-element chunk is copied from native memory into a temporary 64KB array (L2-resident), scattered into accumulators, then discarded. This is 2.5x faster than the array path for compound aggregations.

## LIKE Fast-Path

`arrayStringLikeFast` (ColumnOpsExt) recognizes three common LIKE patterns and avoids full regex:
- `%literal%` → `String.contains()`
- `prefix%` → `String.startsWith()`
- `%suffix` → `String.endsWith()`

For dictionary-encoded columns with >100K entries, the dictionary is filtered in parallel, then a bitset of matching codes is checked per row.

## JIT Optimization Lessons

Hard-won rules from extensive benchmarking:

1. **Class size matters**: Adding bytecode to a class file degrades JIT quality of all methods in that class, even unrelated ones. Keep classes under ~80KB bytecode. ColumnOpsExt exists for this reason.

2. **Avoid switches in hot loops**: Inner loops with `switch(aggType)` prevent SIMD vectorization (106ms vs 16ms). Use separate methods for each aggregation type.

3. **Object[][] kills specialization**: JIT can't type-specialize through `Object[][]` — every `(long[]) arr[p][c]` cast prevents bounds-check hoisting. Use typed arrays (`long[][][]`, `double[][][]`).

4. **LongVector.convertShape is not intrinsified**: `convertShape(L2D, ...)` caused 8x regression. Convert at reduceLanes only.

5. **Shared compilation units**: If `foo()` and `fooRange(start, end)` exist, JIT compiles them independently. Make `foo()` delegate to `fooRange(0, length)`.

6. **Separate JIT profiles**: Different aggregation types (COUNT vs SUM) need separate methods. Mixing them causes `UnreachedCode` speculation failures and deoptimization cascades.

7. **Method size limits**: Keep hot methods compact. Code bloat from unrolling (e.g., 8 predicates) triggers deoptimization.

## Related Documentation

- [Architecture](architecture.md) — System overview and module map
- [Query Engine](query-engine.md) — How queries compile to these SIMD paths
- [Benchmarks](benchmarks.md) — Performance measurements
