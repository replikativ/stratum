package stratum.internal;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Extended columnar operations — separated from ColumnOps to avoid JIT interference.
 *
 * Contains:
 * 1. Parallel COUNT DISTINCT (BitSet + hash table paths)
 * 2. Multi-sum fused SIMD (single-pass N aggregates)
 * 3. Fused extract+count group-by
 * 4. Join global aggregate (hash + perfect)
 * 5. LIKE pattern matching
 *
 * Note: VARIANCE/CORR dense group-by moved to ColumnOpsVar.java for
 * further JIT isolation (was causing 2-6x regressions from class-level
 * JIT budget exhaustion when sharing this 64KB compilation unit).
 */
public final class ColumnOpsExt {

    // Re-use ColumnOps pool and thresholds
    private static ForkJoinPool POOL = ColumnOps.POOL;

    // Agg type constants (same values as ColumnOps)
    public static final int AGG_SUM = ColumnOps.AGG_SUM;
    public static final int AGG_COUNT = ColumnOps.AGG_COUNT;
    public static final int AGG_SUM_PRODUCT = ColumnOps.AGG_SUM_PRODUCT;
    public static final int AGG_MIN = ColumnOps.AGG_MIN;
    public static final int AGG_MAX = ColumnOps.AGG_MAX;
    private static int MORSEL_SIZE = ColumnOps.MORSEL_SIZE;

    /** Maximum range for BitSet-based COUNT DISTINCT (64MB bits = 8MB memory). */
    private static final long BITSET_RANGE_THRESHOLD = 67108864L;

    // SIMD species (same as ColumnOps)
    private static final VectorSpecies<Long> LONG_SPECIES = ColumnOps.LONG_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = ColumnOps.DOUBLE_SPECIES;
    private static final int LONG_LANES = ColumnOps.LONG_LANES;

    // =========================================================================
    // Parallel COUNT DISTINCT
    // =========================================================================

    /**
     * Parallel count distinct using per-thread hash tables + merge.
     * Falls back to single-threaded for small inputs.
     */
    public static long countDistinctLongParallel(long[] data, int length) {
        if (length < ColumnOps.PARALLEL_THRESHOLD)
            return ColumnOps.countDistinctLong(data, length);

        // Quick scan for min/max to decide strategy (skip NULL sentinel)
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            long v = data[i];
            if (v == Long.MIN_VALUE) continue; // skip NULL
            if (v < min) min = v;
            if (v > max) max = v;
        }
        if (min > max) return 0; // all NULL
        long range = max - min + 1;

        // BitSet parallel path: each thread fills a local BitSet, then OR them
        if (range > 0 && range <= BITSET_RANGE_THRESHOLD) {
            return countDistinctBitSetParallel(data, length, min, (int) range);
        }
        // Hash table parallel path
        return countDistinctHashParallel(data, length);
    }

    @SuppressWarnings("unchecked")
    private static long countDistinctBitSetParallel(long[] data, int length, long min, int range) {
        int nThreads = POOL.getParallelism();
        int threadRange = (length + nThreads - 1) / nThreads;

        Future<java.util.BitSet>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int tS = t * threadRange, tE = Math.min(tS + threadRange, length);
            if (tS >= length) { futures[t] = null; continue; }
            final long fMin = min;
            futures[t] = POOL.submit(() -> {
                java.util.BitSet bs = new java.util.BitSet(range);
                for (int i = tS; i < tE; i++) {
                    if (data[i] == Long.MIN_VALUE) continue; // skip NULL
                    bs.set((int) (data[i] - fMin));
                }
                return bs;
            });
        }

        try {
            java.util.BitSet merged = null;
            for (Future<java.util.BitSet> f : futures) {
                if (f == null) continue;
                java.util.BitSet bs = f.get();
                if (merged == null) merged = bs;
                else merged.or(bs);
            }
            return merged == null ? 0 : merged.cardinality();
        } catch (Exception e) {
            throw new RuntimeException("Parallel count distinct failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static long countDistinctHashParallel(long[] data, int length) {
        int nThreads = POOL.getParallelism();
        int threadRange = (length + nThreads - 1) / nThreads;

        Future<Object[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int tS = t * threadRange, tE = Math.min(tS + threadRange, length);
            if (tS >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                int len = tE - tS;
                int cap = Integer.highestOneBit((int) Math.min(Integer.MAX_VALUE / 2, Math.max(16L, (long) len * 2))) << 1;
                long[] table = new long[cap];
                byte[] occupied = new byte[cap];
                int distinct = 0;
                long MULT = 0x9E3779B97F4A7C15L;
                for (int i = tS; i < tE; i++) {
                    long v = data[i];
                    if (v == Long.MIN_VALUE) continue; // skip NULL
                    int slot = (int) ((v * MULT) >>> (64 - Integer.numberOfTrailingZeros(cap))) & (cap - 1);
                    while (true) {
                        if (occupied[slot] == 0) {
                            table[slot] = v;
                            occupied[slot] = 1;
                            distinct++;
                            break;
                        }
                        if (table[slot] == v) break;
                        slot = (slot + 1) & (cap - 1);
                    }
                }
                return new Object[] { table, occupied, cap, distinct };
            });
        }

        try {
            long[] mergedTable = null;
            byte[] mergedOccupied = null;
            int mergedCap = 0;
            int mergedDistinct = 0;
            long MULT = 0x9E3779B97F4A7C15L;

            for (Future<Object[]> f : futures) {
                if (f == null) continue;
                Object[] res = f.get();
                long[] tbl = (long[]) res[0];
                byte[] occ = (byte[]) res[1];
                int cap = (int) res[2];

                if (mergedTable == null) {
                    mergedTable = tbl;
                    mergedOccupied = occ;
                    mergedCap = cap;
                    mergedDistinct = (int) res[3];
                } else {
                    for (int s = 0; s < cap; s++) {
                        if (occ[s] == 0) continue;
                        long v = tbl[s];
                        if (mergedDistinct * 10 > mergedCap * 7) {
                            int newCap = mergedCap << 1;
                            int newMask = newCap - 1;
                            long[] newTable = new long[newCap];
                            byte[] newOcc = new byte[newCap];
                            for (int j = 0; j < mergedCap; j++) {
                                if (mergedOccupied[j] != 0) {
                                    long mv = mergedTable[j];
                                    int ns = (int) ((mv * MULT) >>> (64 - Integer.numberOfTrailingZeros(newCap))) & newMask;
                                    while (newOcc[ns] != 0) ns = (ns + 1) & newMask;
                                    newTable[ns] = mv;
                                    newOcc[ns] = 1;
                                }
                            }
                            mergedTable = newTable;
                            mergedOccupied = newOcc;
                            mergedCap = newCap;
                        }
                        int mask = mergedCap - 1;
                        int slot = (int) ((v * MULT) >>> (64 - Integer.numberOfTrailingZeros(mergedCap))) & mask;
                        while (true) {
                            if (mergedOccupied[slot] == 0) {
                                mergedTable[slot] = v;
                                mergedOccupied[slot] = 1;
                                mergedDistinct++;
                                break;
                            }
                            if (mergedTable[slot] == v) break;
                            slot = (slot + 1) & mask;
                        }
                    }
                }
            }
            return mergedDistinct;
        } catch (Exception e) {
            throw new RuntimeException("Parallel count distinct failed", e);
        }
    }

    /**
     * Parallel COUNT DISTINCT with mask: per-thread hash tables + merge.
     */
    public static long countDistinctLongMaskedParallel(long[] data, long[] mask, int length) {
        if (length < ColumnOps.PARALLEL_THRESHOLD)
            return ColumnOps.countDistinctLongMasked(data, mask, length);

        int included = 0;
        for (int i = 0; i < length; i++) if (mask[i] == 1L) included++;
        if (included == 0) return 0;

        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1L) {
                long v = data[i];
                if (v == Long.MIN_VALUE) continue; // skip NULL
                if (v < min) min = v;
                if (v > max) max = v;
            }
        }
        if (min > max) return 0; // all NULL
        long range = max - min + 1;

        if (range > 0 && range <= BITSET_RANGE_THRESHOLD) {
            int nThreads = POOL.getParallelism();
            int threadRange = (length + nThreads - 1) / nThreads;
            @SuppressWarnings("unchecked")
            Future<java.util.BitSet>[] futures = new Future[nThreads];
            final long fMin = min;
            final int fRange = (int) range;
            for (int t = 0; t < nThreads; t++) {
                final int tS = t * threadRange, tE = Math.min(tS + threadRange, length);
                if (tS >= length) { futures[t] = null; continue; }
                futures[t] = POOL.submit(() -> {
                    java.util.BitSet bs = new java.util.BitSet(fRange);
                    for (int i = tS; i < tE; i++) {
                        if (mask[i] == 1L && data[i] != Long.MIN_VALUE) bs.set((int) (data[i] - fMin));
                    }
                    return bs;
                });
            }
            try {
                java.util.BitSet merged = null;
                for (Future<java.util.BitSet> f : futures) {
                    if (f == null) continue;
                    java.util.BitSet bs = f.get();
                    if (merged == null) merged = bs;
                    else merged.or(bs);
                }
                return merged == null ? 0 : merged.cardinality();
            } catch (Exception e) {
                throw new RuntimeException("Parallel masked count distinct failed", e);
            }
        }

        // Hash path — fall back to single-threaded masked
        return ColumnOps.countDistinctLongMasked(data, mask, length);
    }

    // =========================================================================
    // Hash Group-By COUNT DISTINCT
    // =========================================================================

    // Use same sentinel as ColumnOps to avoid collision with legitimate composite keys.
    private static final long EMPTY_KEY = ColumnOps.EMPTY_KEY;

    private static int nextPow2(int n) {
        int v = Math.max(16, n);
        return Integer.highestOneBit(v - 1) << 1;
    }

    private static int hashSlot(long key, int capacity) {
        return (int) ((key * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(capacity)));
    }

    /**
     * Hash group-by with COUNT DISTINCT for non-dense key spaces.
     * Uses open-addressed hash tables for groups and per-group distinct sets.
     * Returns Object[] { long[] groupKeys, int[] distinctCounts }.
     */
    public static Object[] fusedFilterGroupCountDistinctHash(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            long[] distinctCol, int length) {

        // Group hash table
        int capacity = nextPow2(length / 4);
        int mask = capacity - 1;
        long[] htKeys = new long[capacity];
        int[] htGroupIdx = new int[capacity];
        java.util.Arrays.fill(htKeys, EMPTY_KEY);
        int numGroups = 0;

        // Per-group distinct sets (indexed by group ordinal)
        int maxGroups = Math.min(1024, length / 4);
        long[][] groupTables = new long[maxGroups][];
        byte[][] groupOcc = new byte[maxGroups][];
        int[] groupCounts = new int[maxGroups];
        int[] groupCaps = new int[maxGroups];

        final long[] gc0 = numGroupCols > 0 ? groupCols[0] : null;
        final long[] gc1 = numGroupCols > 1 ? groupCols[1] : null;
        final long[] gc2 = numGroupCols > 2 ? groupCols[2] : null;
        final long[] gc3 = numGroupCols > 3 ? groupCols[3] : null;
        final long[] gc4 = numGroupCols > 4 ? groupCols[4] : null;
        final long[] gc5 = numGroupCols > 5 ? groupCols[5] : null;
        final long gm0 = numGroupCols > 0 ? groupMuls[0] : 0;
        final long gm1 = numGroupCols > 1 ? groupMuls[1] : 0;
        final long gm2 = numGroupCols > 2 ? groupMuls[2] : 0;
        final long gm3 = numGroupCols > 3 ? groupMuls[3] : 0;
        final long gm4 = numGroupCols > 4 ? groupMuls[4] : 0;
        final long gm5 = numGroupCols > 5 ? groupMuls[5] : 0;

        for (int i = 0; i < length; i++) {
            if (!ColumnOps.evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                              numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i))
                continue;

            long key = gc0[i] * gm0;
            if (numGroupCols > 1) key += gc1[i] * gm1;
            if (numGroupCols > 2) key += gc2[i] * gm2;
            if (numGroupCols > 3) key += gc3[i] * gm3;
            if (numGroupCols > 4) key += gc4[i] * gm4;
            if (numGroupCols > 5) key += gc5[i] * gm5;
            for (int g = 6; g < numGroupCols; g++) key += groupCols[g][i] * groupMuls[g];
            key = ColumnOps.safeKey(key);

            long val = distinctCol[i];
            if (val == Long.MIN_VALUE) continue; // skip NULL distinct values

            // Find or insert group
            int slot = hashSlot(key, capacity) & mask;
            while (htKeys[slot] != EMPTY_KEY && htKeys[slot] != key) {
                slot = (slot + 1) & mask;
            }

            int gIdx;
            if (htKeys[slot] == EMPTY_KEY) {
                htKeys[slot] = key;
                gIdx = numGroups++;

                // Grow per-group arrays if needed
                if (gIdx >= maxGroups) {
                    int newMax = maxGroups * 2;
                    groupTables = java.util.Arrays.copyOf(groupTables, newMax);
                    groupOcc = java.util.Arrays.copyOf(groupOcc, newMax);
                    groupCounts = java.util.Arrays.copyOf(groupCounts, newMax);
                    groupCaps = java.util.Arrays.copyOf(groupCaps, newMax);
                    maxGroups = newMax;
                }
                // Init distinct set for this group
                int dCap = 16;
                groupTables[gIdx] = new long[dCap];
                groupOcc[gIdx] = new byte[dCap];
                groupCaps[gIdx] = dCap;
                htGroupIdx[slot] = gIdx;

                // Resize group hash table if needed (70% load factor)
                if (numGroups * 10 > capacity * 7) {
                    int newCap = capacity << 1;
                    int newMask = newCap - 1;
                    long[] newKeys = new long[newCap];
                    int[] newIdx = new int[newCap];
                    java.util.Arrays.fill(newKeys, EMPTY_KEY);
                    for (int s = 0; s < capacity; s++) {
                        if (htKeys[s] != EMPTY_KEY) {
                            int ns = hashSlot(htKeys[s], newCap) & newMask;
                            while (newKeys[ns] != EMPTY_KEY) ns = (ns + 1) & newMask;
                            newKeys[ns] = htKeys[s];
                            newIdx[ns] = htGroupIdx[s];
                        }
                    }
                    htKeys = newKeys;
                    htGroupIdx = newIdx;
                    capacity = newCap;
                    mask = newMask;
                }
            } else {
                gIdx = htGroupIdx[slot];
            }

            // Insert val into group's distinct set
            long[] dTable = groupTables[gIdx];
            byte[] dOcc = groupOcc[gIdx];
            int dCap = groupCaps[gIdx];
            int dMask = dCap - 1;
            int dSlot = hashSlot(val, dCap) & dMask;
            while (dOcc[dSlot] != 0 && dTable[dSlot] != val) {
                dSlot = (dSlot + 1) & dMask;
            }
            if (dOcc[dSlot] == 0) {
                dTable[dSlot] = val;
                dOcc[dSlot] = 1;
                groupCounts[gIdx]++;
                // Resize distinct set if needed
                if (groupCounts[gIdx] * 10 > dCap * 7) {
                    int newDCap = dCap << 1;
                    int newDMask = newDCap - 1;
                    long[] newDTable = new long[newDCap];
                    byte[] newDOcc = new byte[newDCap];
                    for (int s = 0; s < dCap; s++) {
                        if (dOcc[s] != 0) {
                            int ns = hashSlot(dTable[s], newDCap) & newDMask;
                            while (newDOcc[ns] != 0) ns = (ns + 1) & newDMask;
                            newDTable[ns] = dTable[s];
                            newDOcc[ns] = 1;
                        }
                    }
                    groupTables[gIdx] = newDTable;
                    groupOcc[gIdx] = newDOcc;
                    groupCaps[gIdx] = newDCap;
                }
            }
        }

        // Extract results
        long[] outKeys = new long[numGroups];
        int[] outCounts = new int[numGroups];
        for (int s = 0; s < capacity; s++) {
            if (htKeys[s] != EMPTY_KEY) {
                int gIdx = htGroupIdx[s];
                outKeys[gIdx] = htKeys[s];
                outCounts[gIdx] = groupCounts[gIdx];
            }
        }
        return new Object[] { outKeys, outCounts };
    }

    // =========================================================================
    // Native VARIANCE/CORR — moved to ColumnOpsVar.java for JIT isolation
    // =========================================================================

    // =========================================================================
    // Fused Max/Min Scan
    // =========================================================================

    /** Returns [max, min, hasNull] in a single pass. Skips Long.MIN_VALUE (NULL sentinel).
     *  hasNull is 1 if any value equals Long.MIN_VALUE, 0 otherwise. */
    public static long[] arrayMaxMinLong(long[] data, int length) {
        long mx = Long.MIN_VALUE, mn = Long.MAX_VALUE;
        long hasNull = 0;
        for (int i = 0; i < length; i++) {
            long v = data[i];
            if (v == Long.MIN_VALUE) { hasNull = 1; continue; }
            if (v > mx) mx = v;
            if (v < mn) mn = v;
        }
        return new long[]{mx, mn, hasNull};
    }

    /** Compute GCD of all non-zero values in array. Returns 1 if no common factor. */
    public static long arrayGcdLong(long[] data, int length) {
        long g = 0;
        for (int i = 0; i < length; i++) {
            long v = data[i];
            if (v != 0) {
                if (v < 0) v = -v;
                g = gcd(g, v);
                if (g == 1) return 1;
            }
        }
        return g == 0 ? 1 : g;
    }

    private static long gcd(long a, long b) {
        while (b != 0) { long t = b; b = a % b; a = t; }
        return a;
    }

    /** Divide all elements by scalar, returning new array. */
    public static long[] arrayDivLongScalar(long[] data, long divisor, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = data[i] / divisor;
        return r;
    }

    // =========================================================================
    // Fused Extract + COUNT Group-By
    // =========================================================================

    // Extract type constants
    public static final int EXTRACT_MINUTE = 0;
    public static final int EXTRACT_HOUR = 1;
    public static final int EXTRACT_SECOND = 2;
    public static final int EXTRACT_DAY_OF_WEEK = 3;

    /** Return the maximum key (exclusive) for a given extract type. */
    public static int extractMaxKey(int exprType) {
        switch (exprType) {
            case EXTRACT_MINUTE:     return 60;
            case EXTRACT_HOUR:       return 24;
            case EXTRACT_SECOND:     return 60;
            case EXTRACT_DAY_OF_WEEK: return 7;
            default: throw new IllegalArgumentException("Unknown extract type: " + exprType);
        }
    }

    /**
     * Core tight loop: extract time field from raw epoch column and accumulate counts.
     * Switch is OUTSIDE the loop for JIT — each case compiles to a tight scalar loop.
     */
    private static void accumulateExtractCountRange(
            long[] rawCol, int exprType, int start, int end, long[] counts) {
        switch (exprType) {
            case EXTRACT_MINUTE:
                for (int i = start; i < end; i++)
                    counts[(int)(Math.floorMod(rawCol[i], 3600L) / 60)]++;
                break;
            case EXTRACT_HOUR:
                for (int i = start; i < end; i++)
                    counts[(int)(Math.floorMod(rawCol[i], 86400L) / 3600)]++;
                break;
            case EXTRACT_SECOND:
                for (int i = start; i < end; i++)
                    counts[(int)(Math.floorMod(rawCol[i], 60L))]++;
                break;
            case EXTRACT_DAY_OF_WEEK:
                for (int i = start; i < end; i++)
                    counts[(int)(Math.floorMod(rawCol[i] + 3, 7L))]++;
                break;
        }
    }

    /**
     * Fused extract + COUNT group-by: morsel-driven parallel.
     * Same pattern as fusedFilterGroupCountDenseParallel but fuses extraction
     * into the scatter loop, eliminating the 48MB intermediate long[] array.
     *
     * Returns double[numAggs][maxKey] — same format as COUNT dense path expects.
     * All aggs are COUNT so each slot is just the count value.
     */
    @SuppressWarnings("unchecked")
    public static double[][] fusedExtractCountDenseParallel(
            long[] rawCol, int exprType, int numAggs, int length) {

        int maxKey = extractMaxKey(exprType);

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            // Single-threaded path
            long[] counts = new long[maxKey];
            accumulateExtractCountRange(rawCol, exprType, 0, length, counts);
            // Convert to double[][] format: [maxKey][numAggs*2] where slot 0=value, slot 1=count
            double[][] result = new double[maxKey][];
            int accSize = numAggs * 2;
            for (int k = 0; k < maxKey; k++) {
                if (counts[k] > 0) {
                    result[k] = new double[accSize];
                    // For COUNT aggs, slot 0 is unused, slot 1 is count
                    // Fill all agg count slots with the same count
                    for (int a = 0; a < numAggs; a++) {
                        result[k][a * 2 + 1] = (double) counts[k];
                    }
                }
            }
            return result;
        }

        // Parallel path: thread-local long[] counts (tiny: 480 bytes for minute)
        int nThreads = POOL.getParallelism();
        int threadRange = (length + nThreads - 1) / nThreads;

        Future<long[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int tStart = t * threadRange;
            final int tEnd = Math.min(tStart + threadRange, length);
            if (tStart >= length) { futures[t] = null; continue; }
            final int fMaxKey = maxKey;
            futures[t] = POOL.submit(() -> {
                long[] localCounts = new long[fMaxKey];
                // Morsel loop for cache friendliness
                int ms = tStart;
                while (ms < tEnd) {
                    int me = Math.min(ms + MORSEL_SIZE, tEnd);
                    accumulateExtractCountRange(rawCol, exprType, ms, me, localCounts);
                    ms = me;
                }
                return localCounts;
            });
        }

        // Merge thread-local counts
        long[] merged = new long[maxKey];
        try {
            for (Future<long[]> f : futures) {
                if (f == null) continue;
                long[] partial = f.get();
                for (int k = 0; k < maxKey; k++) {
                    merged[k] += partial[k];
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel fused extract count failed", e);
        }

        // Convert to double[][] format
        double[][] result = new double[maxKey][];
        int accSize = numAggs * 2;
        for (int k = 0; k < maxKey; k++) {
            if (merged[k] > 0) {
                result[k] = new double[accSize];
                for (int a = 0; a < numAggs; a++) {
                    result[k][a * 2 + 1] = (double) merged[k];
                }
            }
        }
        return result;
    }

    // =========================================================================
    // Extract operations returning long[] directly
    // =========================================================================

    public static long[] arrayExtractYearLong(long[] epochDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long y = yoe + era * 400;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            long m = mp + (mp < 10 ? 3 : -9);
            r[i] = y + (m <= 2 ? 1 : 0);
        }
        return r;
    }

    public static long[] arrayExtractMonthLong(long[] epochDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            r[i] = mp + (mp < 10 ? 3 : -9);
        }
        return r;
    }

    public static long[] arrayExtractDayLong(long[] epochDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            r[i] = doy - (153*mp + 2)/5 + 1;
        }
        return r;
    }

    public static long[] arrayExtractHourLong(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorMod(epochSeconds[i], 86400L) / 3600;
        }
        return r;
    }

    public static long[] arrayExtractMinuteLong(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorMod(epochSeconds[i], 3600L) / 60;
        }
        return r;
    }

    public static long[] arrayExtractSecondLong(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorMod(epochSeconds[i], 60L);
        }
        return r;
    }

    public static long[] arrayExtractDayOfWeekLong(long[] epochDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorMod(epochDays[i] + 3, 7L);
        }
        return r;
    }

    public static long[] arrayExtractWeekOfYearLong(long[] epochDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long ed = epochDays[i];
            long dow = ((ed % 7) + 10) % 7;
            long thu = ed + (3 - dow);
            long z = thu + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long y = yoe + era * 400;
            long doy2 = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy2 + 2) / 153;
            long m = mp + (mp < 10 ? 3 : -9);
            long thuYear = y + (m <= 2 ? 1 : 0);
            long yy = thuYear - (m <= 2 ? 1 : 0);
            long eraY = (yy >= 0 ? yy : yy - 399) / 400;
            long yoeY = yy - eraY * 400;
            long doyY = (365*yoeY + yoeY/4 - yoeY/100);
            long jan1 = eraY * 146097 + doyY - 719468;
            long jan1dow = ((jan1 % 7) + 10) % 7;
            long week1Mon = jan1 + ((jan1dow <= 3) ? -jan1dow : 7 - jan1dow);
            r[i] = (ed - week1Mon) / 7 + 1;
        }
        return r;
    }

    // =========================================================================
    // Fast-path LIKE matching (moved from ColumnOps to reduce bytecode bloat)
    // =========================================================================

    /** LIKE predicate on dict-encoded column with fast-path detection + parallel matching. */
    public static long[] arrayStringLikeFast(long[] codes, String[] dict, String pattern, int length) {
        boolean[] dictMatch = new boolean[dict.length];
        if (dict.length > 100000) {
            matchDictLikeParallel(dict, pattern, dictMatch);
        } else {
            matchDictLike(dict, pattern, dictMatch);
        }
        long[] r = new long[length];
        // Sequential broadcast — trivial per-element work (single array lookup)
        // does not benefit from parallelism; FJP overhead dominates at all scales.
        for (int i = 0; i < length; i++) r[i] = dictMatch[(int) codes[i]] ? 1L : 0L;
        return r;
    }

    /** LIKE predicate on raw String[] column with fast-path detection + parallel scan. */
    public static long[] arrayRawStringLikeFast(String[] strings, String pattern, int length) {
        long[] r = new long[length];
        if (length > ColumnOps.PARALLEL_THRESHOLD) {
            int nThreads = Math.min(POOL.getParallelism(), length / MORSEL_SIZE);
            nThreads = Math.max(nThreads, 2);
            int range = (length + nThreads - 1) / nThreads;
            @SuppressWarnings("unchecked")
            java.util.concurrent.Future<?>[] futures = new java.util.concurrent.Future[nThreads];
            for (int t = 0; t < nThreads; t++) {
                final int start = t * range;
                final int end = Math.min(start + range, length);
                if (start >= length) break;
                futures[t] = POOL.submit(() -> {
                    rawStringLikeRange(strings, pattern, r, start, end);
                });
            }
            for (int t = 0; t < nThreads; t++) {
                if (futures[t] == null) break;
                try { futures[t].get(); } catch (Exception e) { throw new RuntimeException(e); }
            }
        } else {
            rawStringLikeRange(strings, pattern, r, 0, length);
        }
        return r;
    }

    /** Apply LIKE pattern to a range of raw strings with fast-path detection. */
    private static void rawStringLikeRange(String[] strings, String pattern, long[] r, int start, int end) {
        int pLen = pattern.length();
        boolean startsPercent = pLen > 0 && pattern.charAt(0) == '%';
        boolean endsPercent = pLen > 0 && pattern.charAt(pLen - 1) == '%';
        String inner = pattern.substring(startsPercent ? 1 : 0, endsPercent ? pLen - 1 : pLen);
        boolean innerHasWild = inner.indexOf('%') >= 0 || inner.indexOf('_') >= 0;
        if (!innerHasWild && startsPercent && endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].contains(inner) ? 1L : 0L;
        } else if (!innerHasWild && !startsPercent && endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].startsWith(inner) ? 1L : 0L;
        } else if (!innerHasWild && startsPercent && !endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].endsWith(inner) ? 1L : 0L;
        } else {
            String regex = ColumnOps.likeToRegex(pattern);
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (int i = start; i < end; i++) r[i] = p.matcher(strings[i]).matches() ? 1L : 0L;
        }
    }

    /** Parallel dict matching for large dictionaries (>100K entries). */
    private static void matchDictLikeParallel(String[] dict, String pattern, boolean[] dictMatch) {
        int nThreads = POOL.getParallelism();
        int chunkSize = (dict.length + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        java.util.concurrent.Future<?>[] futures = new java.util.concurrent.Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, dict.length);
            if (start >= dict.length) break;
            futures[t] = POOL.submit(() -> {
                matchDictLikeRange(dict, pattern, dictMatch, start, end);
            });
        }
        for (int t = 0; t < nThreads; t++) {
            if (futures[t] == null) break;
            try { futures[t].get(); } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    /** Match a range of dictionary entries with fast-path detection. */
    private static void matchDictLikeRange(String[] dict, String pattern, boolean[] dictMatch, int start, int end) {
        int pLen = pattern.length();
        boolean startsPercent = pLen > 0 && pattern.charAt(0) == '%';
        boolean endsPercent = pLen > 0 && pattern.charAt(pLen - 1) == '%';
        String inner = pattern.substring(startsPercent ? 1 : 0, endsPercent ? pLen - 1 : pLen);
        boolean innerHasWild = inner.indexOf('%') >= 0 || inner.indexOf('_') >= 0;

        if (!innerHasWild && startsPercent && endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) dictMatch[d] = dict[d].contains(inner);
        } else if (!innerHasWild && !startsPercent && endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) dictMatch[d] = dict[d].startsWith(inner);
        } else if (!innerHasWild && startsPercent && !endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) dictMatch[d] = dict[d].endsWith(inner);
        } else {
            String regex = ColumnOps.likeToRegex(pattern);
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (int d = start; d < end; d++) dictMatch[d] = p.matcher(dict[d]).matches();
        }
    }

    /** Match dictionary entries against LIKE pattern with fast-path for common patterns. */
    private static void matchDictLike(String[] dict, String pattern, boolean[] dictMatch) {
        int pLen = pattern.length();
        boolean startsPercent = pLen > 0 && pattern.charAt(0) == '%';
        boolean endsPercent = pLen > 0 && pattern.charAt(pLen - 1) == '%';
        String inner = pattern.substring(startsPercent ? 1 : 0, endsPercent ? pLen - 1 : pLen);
        boolean innerHasWild = inner.indexOf('%') >= 0 || inner.indexOf('_') >= 0;

        if (!innerHasWild && startsPercent && endsPercent && inner.length() > 0) {
            for (int d = 0; d < dict.length; d++) dictMatch[d] = dict[d].contains(inner);
        } else if (!innerHasWild && !startsPercent && endsPercent && inner.length() > 0) {
            for (int d = 0; d < dict.length; d++) dictMatch[d] = dict[d].startsWith(inner);
        } else if (!innerHasWild && startsPercent && !endsPercent && inner.length() > 0) {
            for (int d = 0; d < dict.length; d++) dictMatch[d] = dict[d].endsWith(inner);
        } else {
            String regex = ColumnOps.likeToRegex(pattern);
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (int d = 0; d < dict.length; d++) dictMatch[d] = p.matcher(dict[d]).matches();
        }
    }

    // =========================================================================
    // SIMD predicate helpers (duplicated from ColumnOps for JIT isolation)
    // =========================================================================

    private static final int PRED_RANGE = ColumnOps.PRED_RANGE;
    private static final int PRED_LT = ColumnOps.PRED_LT;
    private static final int PRED_GT = ColumnOps.PRED_GT;
    private static final int PRED_EQ = ColumnOps.PRED_EQ;
    private static final int PRED_LTE = ColumnOps.PRED_LTE;
    private static final int PRED_GTE = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    private static VectorMask<Long> applyLongPred(
            VectorMask<Long> mask, LongVector v,
            int type, LongVector loVec, LongVector hiVec) {
        switch (type) {
            case PRED_RANGE: return mask.and(v.compare(VectorOperators.GE, loVec)
                                              .and(v.compare(VectorOperators.LE, hiVec)));
            case PRED_LT:    return mask.and(v.compare(VectorOperators.LT, hiVec));
            case PRED_GT:    return mask.and(v.compare(VectorOperators.GT, loVec));
            case PRED_EQ:    return mask.and(v.compare(VectorOperators.EQ, loVec));
            case PRED_LTE:   return mask.and(v.compare(VectorOperators.LT, hiVec)
                                              .or(v.compare(VectorOperators.EQ, hiVec)));
            case PRED_GTE:   return mask.and(v.compare(VectorOperators.GE, loVec));
            case PRED_NEQ:   return mask.and(v.compare(VectorOperators.NE, loVec));
            case PRED_NOT_RANGE: return mask.and(v.compare(VectorOperators.LT, loVec)
                                                  .or(v.compare(VectorOperators.GT, hiVec)));
            default:         return mask;
        }
    }

    // Deliberate duplicate of applyDoublePred for JIT isolation
    private static VectorMask<Double> applyDoublePred(
            VectorMask<Double> mask, DoubleVector v,
            int type, DoubleVector loVec, DoubleVector hiVec) {
        switch (type) {
            case PRED_RANGE: return mask.and(v.compare(VectorOperators.GE, loVec)
                                              .and(v.compare(VectorOperators.LE, hiVec)));
            case PRED_LT:    return mask.and(v.compare(VectorOperators.LT, hiVec));
            case PRED_GT:    return mask.and(v.compare(VectorOperators.GT, loVec));
            case PRED_EQ:    return mask.and(v.compare(VectorOperators.EQ, loVec));
            case PRED_LTE:   return mask.and(v.compare(VectorOperators.LT, hiVec)
                                              .or(v.compare(VectorOperators.EQ, hiVec)));
            case PRED_GTE:   return mask.and(v.compare(VectorOperators.GE, loVec));
            case PRED_NEQ:   return mask.and(v.compare(VectorOperators.NE, loVec));
            case PRED_NOT_RANGE: return mask.and(v.compare(VectorOperators.LT, loVec)
                                                  .or(v.compare(VectorOperators.GT, hiVec)));
            default:         return mask;
        }
    }

    // Deliberate duplicate of evaluatePredicates for JIT isolation
    private static boolean evaluatePredicates(
            int numLongPreds, int[] longPredTypes, long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes, double[][] dblCols, double[] dblLo, double[] dblHi,
            int i) {
        for (int p = 0; p < numLongPreds; p++) {
            long v = longCols[p][i];
            switch (longPredTypes[p]) {
                case PRED_RANGE: if (v < longLo[p] || v > longHi[p]) return false; break;
                case PRED_LT:    if (v >= longHi[p]) return false; break;
                case PRED_GT:    if (v <= longLo[p]) return false; break;
                case PRED_EQ:    if (v != longLo[p]) return false; break;
                case PRED_LTE:   if (v > longHi[p]) return false; break;
                case PRED_GTE:   if (v < longLo[p]) return false; break;
                case PRED_NEQ:   if (v == longLo[p]) return false; break;
                case PRED_NOT_RANGE: if (v >= longLo[p] && v <= longHi[p]) return false; break;
            }
        }
        for (int p = 0; p < numDblPreds; p++) {
            double v = dblCols[p][i];
            switch (dblPredTypes[p]) {
                case PRED_RANGE: if (v < dblLo[p] || v > dblHi[p]) return false; break;
                case PRED_LT:    if (v >= dblHi[p]) return false; break;
                case PRED_GT:    if (v <= dblLo[p]) return false; break;
                case PRED_EQ:    if (v != dblLo[p]) return false; break;
                case PRED_LTE:   if (v > dblHi[p]) return false; break;
                case PRED_GTE:   if (v < dblLo[p]) return false; break;
                case PRED_NEQ:   if (v == dblLo[p]) return false; break;
                case PRED_NOT_RANGE: if (v >= dblLo[p] && v <= dblHi[p]) return false; break;
            }
        }
        return true;
    }

    private static final int DOUBLE_LANES = (int) DOUBLE_SPECIES.length();

    // =========================================================================
    // Multi-Sum Fused SIMD (single-pass N aggregates) — moved from ColumnOps for JIT isolation
    // =========================================================================

    /**
     * Single-pass multi-SUM: evaluate predicates once, accumulate up to 4 SUM/SUM_PRODUCT aggs.
     * Returns double[numSumAggs + 1]: [sum0, sum1, ..., count].
     * sumCols2[i] is null for SUM, non-null for SUM_PRODUCT.
     */
    public static double[] fusedSimdMultiSum(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, double[][] sumCols1, double[][] sumCols2,
            int length, boolean nanSafe) {
        return fusedSimdMultiSumRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numSumAggs, sumCols1, sumCols2, 0, length, nanSafe);
    }

    /**
     * Parallel multi-SUM with morsel-driven execution.
     * All SUM accumulators merge via addition.
     */
    public static double[] fusedSimdMultiSumParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, double[][] sumCols1, double[][] sumCols2,
            int length, boolean nanSafe) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedSimdMultiSum(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numSumAggs, sumCols1, sumCols2, length, nanSafe);
        }

        int nThreads = ColumnOps.effectiveScanThreads();
        int threadRange = (length + nThreads - 1) / nThreads;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                double[] accums = new double[numSumAggs + 1];
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[] partial = fusedSimdMultiSumRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numSumAggs, sumCols1, sumCols2, ms, me, nanSafe);
                    for (int a = 0; a <= numSumAggs; a++) accums[a] += partial[a];
                }
                return accums;
            });
        }

        double[] result = new double[numSumAggs + 1];
        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                for (int a = 0; a <= numSumAggs; a++) result[a] += partial[a];
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
        return result;
    }

    /**
     * Multi-SUM range worker. Evaluates predicates once per SIMD vector,
     * accumulates up to 4 SUM/SUM_PRODUCT values per matching row.
     */
    private static double[] fusedSimdMultiSumRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, double[][] sumCols1, double[][] sumCols2,
            int start, int end, boolean nanSafe) {

        int rangeLen = end - start;
        int matchCount = 0;
        int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

        // Extract pred columns as final locals for JIT
        final long[] lc0 = numLongPreds > 0 ? longCols[0] : null;
        final long[] lc1 = numLongPreds > 1 ? longCols[1] : null;
        final long[] lc2 = numLongPreds > 2 ? longCols[2] : null;
        final long[] lc3 = numLongPreds > 3 ? longCols[3] : null;
        final double[] dc0 = numDblPreds > 0 ? dblCols[0] : null;
        final double[] dc1 = numDblPreds > 1 ? dblCols[1] : null;
        final double[] dc2 = numDblPreds > 2 ? dblCols[2] : null;
        final double[] dc3 = numDblPreds > 3 ? dblCols[3] : null;

        final int lt0 = numLongPreds > 0 ? longPredTypes[0] : -1;
        final int lt1 = numLongPreds > 1 ? longPredTypes[1] : -1;
        final int lt2 = numLongPreds > 2 ? longPredTypes[2] : -1;
        final int lt3 = numLongPreds > 3 ? longPredTypes[3] : -1;
        final int dt0 = numDblPreds > 0 ? dblPredTypes[0] : -1;
        final int dt1 = numDblPreds > 1 ? dblPredTypes[1] : -1;
        final int dt2 = numDblPreds > 2 ? dblPredTypes[2] : -1;
        final int dt3 = numDblPreds > 3 ? dblPredTypes[3] : -1;

        // SIMD pred broadcasts
        final LongVector llo0 = numLongPreds > 0 ? LongVector.broadcast(LONG_SPECIES, longLo[0]) : null;
        final LongVector lhi0 = numLongPreds > 0 ? LongVector.broadcast(LONG_SPECIES, longHi[0]) : null;
        final LongVector llo1 = numLongPreds > 1 ? LongVector.broadcast(LONG_SPECIES, longLo[1]) : null;
        final LongVector lhi1 = numLongPreds > 1 ? LongVector.broadcast(LONG_SPECIES, longHi[1]) : null;
        final LongVector llo2 = numLongPreds > 2 ? LongVector.broadcast(LONG_SPECIES, longLo[2]) : null;
        final LongVector lhi2 = numLongPreds > 2 ? LongVector.broadcast(LONG_SPECIES, longHi[2]) : null;
        final LongVector llo3 = numLongPreds > 3 ? LongVector.broadcast(LONG_SPECIES, longLo[3]) : null;
        final LongVector lhi3 = numLongPreds > 3 ? LongVector.broadcast(LONG_SPECIES, longHi[3]) : null;

        final DoubleVector dlo0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[0]) : null;
        final DoubleVector dhi0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[0]) : null;
        final DoubleVector dlo1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[1]) : null;
        final DoubleVector dhi1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[1]) : null;
        final DoubleVector dlo2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[2]) : null;
        final DoubleVector dhi2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[2]) : null;
        final DoubleVector dlo3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[3]) : null;
        final DoubleVector dhi3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[3]) : null;

        // Extract agg columns as final locals
        final double[] sc0 = numSumAggs > 0 ? sumCols1[0] : null;
        final double[] sc0b = (numSumAggs > 0 && sumCols2[0] != null) ? sumCols2[0] : null;
        final double[] sc1 = numSumAggs > 1 ? sumCols1[1] : null;
        final double[] sc1b = (numSumAggs > 1 && sumCols2[1] != null) ? sumCols2[1] : null;
        final double[] sc2 = numSumAggs > 2 ? sumCols1[2] : null;
        final double[] sc2b = (numSumAggs > 2 && sumCols2[2] != null) ? sumCols2[2] : null;
        final double[] sc3 = numSumAggs > 3 ? sumCols1[3] : null;
        final double[] sc3b = (numSumAggs > 3 && sumCols2[3] != null) ? sumCols2[3] : null;

        // SUM accumulator vectors
        DoubleVector sv0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv2 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv3 = DoubleVector.zero(DOUBLE_SPECIES);
        boolean[] maskBits = new boolean[DOUBLE_LANES];

        for (int i = start; i < upperBound; i += LONG_LANES) {
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            VectorMask<Double> dm;
            if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
            else { for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0); }

            if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }

            matchCount += dm.trueCount();
            if (numSumAggs > 0) { DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, sc0, i); VectorMask<Double> vm; if (nanSafe) { vm = dm.andNot(v.test(VectorOperators.IS_NAN)); if (sc0b != null) { DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, sc0b, i); vm = vm.andNot(b.test(VectorOperators.IS_NAN)); v = v.mul(b); } } else { vm = dm; if (sc0b != null) { v = v.mul(DoubleVector.fromArray(DOUBLE_SPECIES, sc0b, i)); } } sv0 = sv0.add(v, vm); }
            if (numSumAggs > 1) { DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, sc1, i); VectorMask<Double> vm; if (nanSafe) { vm = dm.andNot(v.test(VectorOperators.IS_NAN)); if (sc1b != null) { DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, sc1b, i); vm = vm.andNot(b.test(VectorOperators.IS_NAN)); v = v.mul(b); } } else { vm = dm; if (sc1b != null) { v = v.mul(DoubleVector.fromArray(DOUBLE_SPECIES, sc1b, i)); } } sv1 = sv1.add(v, vm); }
            if (numSumAggs > 2) { DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, sc2, i); VectorMask<Double> vm; if (nanSafe) { vm = dm.andNot(v.test(VectorOperators.IS_NAN)); if (sc2b != null) { DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, sc2b, i); vm = vm.andNot(b.test(VectorOperators.IS_NAN)); v = v.mul(b); } } else { vm = dm; if (sc2b != null) { v = v.mul(DoubleVector.fromArray(DOUBLE_SPECIES, sc2b, i)); } } sv2 = sv2.add(v, vm); }
            if (numSumAggs > 3) { DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, sc3, i); VectorMask<Double> vm; if (nanSafe) { vm = dm.andNot(v.test(VectorOperators.IS_NAN)); if (sc3b != null) { DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, sc3b, i); vm = vm.andNot(b.test(VectorOperators.IS_NAN)); v = v.mul(b); } } else { vm = dm; if (sc3b != null) { v = v.mul(DoubleVector.fromArray(DOUBLE_SPECIES, sc3b, i)); } } sv3 = sv3.add(v, vm); }
        }

        double[] sums = new double[numSumAggs + 1];
        if (numSumAggs > 0) sums[0] = sv0.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 1) sums[1] = sv1.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 2) sums[2] = sv2.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 3) sums[3] = sv3.reduceLanes(VectorOperators.ADD);

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            if (evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                   numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                if (numSumAggs > 0) { double v0 = sc0[i]; if (!nanSafe || v0 == v0) { if (sc0b != null) { double b0 = sc0b[i]; if (!nanSafe || b0 == b0) sums[0] += v0 * b0; } else sums[0] += v0; } }
                if (numSumAggs > 1) { double v1 = sc1[i]; if (!nanSafe || v1 == v1) { if (sc1b != null) { double b1 = sc1b[i]; if (!nanSafe || b1 == b1) sums[1] += v1 * b1; } else sums[1] += v1; } }
                if (numSumAggs > 2) { double v2 = sc2[i]; if (!nanSafe || v2 == v2) { if (sc2b != null) { double b2 = sc2b[i]; if (!nanSafe || b2 == b2) sums[2] += v2 * b2; } else sums[2] += v2; } }
                if (numSumAggs > 3) { double v3 = sc3[i]; if (!nanSafe || v3 == v3) { if (sc3b != null) { double b3 = sc3b[i]; if (!nanSafe || b3 == b3) sums[3] += v3 * b3; } else sums[3] += v3; } }
                matchCount++;
            }
        }
        sums[numSumAggs] = (double) matchCount;
        return sums;
    }

    // =========================================================================
    // Native Long Multi-Sum SIMD — avoids longToDouble allocation
    // =========================================================================

    /**
     * All-long multi-sum: all agg columns are long[], predicates are long-only.
     * Uses LongVector accumulators directly (no long→double conversion in hot loop).
     * Converts to double only at the end via reduceLanes.
     */
    private static double[] fusedSimdMultiSumAllLongRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numSumAggs, long[][] sumLongCols,
            int start, int end) {

        int rangeLen = end - start;
        int matchCount = 0;
        int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

        // Extract pred columns as final locals for JIT
        final long[] lc0 = numLongPreds > 0 ? longCols[0] : null;
        final long[] lc1 = numLongPreds > 1 ? longCols[1] : null;
        final long[] lc2 = numLongPreds > 2 ? longCols[2] : null;
        final long[] lc3 = numLongPreds > 3 ? longCols[3] : null;

        final int lt0 = numLongPreds > 0 ? longPredTypes[0] : -1;
        final int lt1 = numLongPreds > 1 ? longPredTypes[1] : -1;
        final int lt2 = numLongPreds > 2 ? longPredTypes[2] : -1;
        final int lt3 = numLongPreds > 3 ? longPredTypes[3] : -1;

        final LongVector llo0 = numLongPreds > 0 ? LongVector.broadcast(LONG_SPECIES, longLo[0]) : null;
        final LongVector lhi0 = numLongPreds > 0 ? LongVector.broadcast(LONG_SPECIES, longHi[0]) : null;
        final LongVector llo1 = numLongPreds > 1 ? LongVector.broadcast(LONG_SPECIES, longLo[1]) : null;
        final LongVector lhi1 = numLongPreds > 1 ? LongVector.broadcast(LONG_SPECIES, longHi[1]) : null;
        final LongVector llo2 = numLongPreds > 2 ? LongVector.broadcast(LONG_SPECIES, longLo[2]) : null;
        final LongVector lhi2 = numLongPreds > 2 ? LongVector.broadcast(LONG_SPECIES, longHi[2]) : null;
        final LongVector llo3 = numLongPreds > 3 ? LongVector.broadcast(LONG_SPECIES, longLo[3]) : null;
        final LongVector lhi3 = numLongPreds > 3 ? LongVector.broadcast(LONG_SPECIES, longHi[3]) : null;

        // Extract agg columns as final locals
        final long[] sl0 = numSumAggs > 0 ? sumLongCols[0] : null;
        final long[] sl1 = numSumAggs > 1 ? sumLongCols[1] : null;
        final long[] sl2 = numSumAggs > 2 ? sumLongCols[2] : null;
        final long[] sl3 = numSumAggs > 3 ? sumLongCols[3] : null;

        // LongVector accumulators — exact integer arithmetic, no conversion
        LongVector sv0 = LongVector.zero(LONG_SPECIES);
        LongVector sv1 = LongVector.zero(LONG_SPECIES);
        LongVector sv2 = LongVector.zero(LONG_SPECIES);
        LongVector sv3 = LongVector.zero(LONG_SPECIES);

        for (int i = start; i < upperBound; i += LONG_LANES) {
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            matchCount += lm.trueCount();
            LongVector nullSentinel = LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE);
            if (numSumAggs > 0) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl0, i); sv0 = sv0.add(v, lm.and(v.compare(VectorOperators.NE, nullSentinel))); }
            if (numSumAggs > 1) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl1, i); sv1 = sv1.add(v, lm.and(v.compare(VectorOperators.NE, nullSentinel))); }
            if (numSumAggs > 2) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl2, i); sv2 = sv2.add(v, lm.and(v.compare(VectorOperators.NE, nullSentinel))); }
            if (numSumAggs > 3) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl3, i); sv3 = sv3.add(v, lm.and(v.compare(VectorOperators.NE, nullSentinel))); }
        }

        double[] sums = new double[numSumAggs + 1];
        if (numSumAggs > 0) sums[0] = (double) sv0.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 1) sums[1] = (double) sv1.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 2) sums[2] = (double) sv2.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 3) sums[3] = (double) sv3.reduceLanes(VectorOperators.ADD);

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            if (ColumnOps.evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                             0, null, null, null, null, i)) {
                if (numSumAggs > 0 && sl0[i] != Long.MIN_VALUE) sums[0] += sl0[i];
                if (numSumAggs > 1 && sl1[i] != Long.MIN_VALUE) sums[1] += sl1[i];
                if (numSumAggs > 2 && sl2[i] != Long.MIN_VALUE) sums[2] += sl2[i];
                if (numSumAggs > 3 && sl3[i] != Long.MIN_VALUE) sums[3] += sl3[i];
                matchCount++;
            }
        }
        sums[numSumAggs] = (double) matchCount;
        return sums;
    }

    /** Delegates to range variant. */
    public static double[] fusedSimdMultiSumAllLong(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numSumAggs, long[][] sumLongCols,
            int length) {
        return fusedSimdMultiSumAllLongRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numSumAggs, sumLongCols, 0, length);
    }

    /** Parallel morsel-driven all-long multi-sum. */
    public static double[] fusedSimdMultiSumAllLongParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numSumAggs, long[][] sumLongCols,
            int length) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedSimdMultiSumAllLong(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numSumAggs, sumLongCols,
                    length);
        }

        int nThreads = ColumnOps.effectiveScanThreads();
        int chunkSize = (length + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int tStart = t * chunkSize;
            final int tEnd = Math.min(tStart + chunkSize, length);
            if (tStart >= length) break;
            futures[t] = POOL.submit(() -> {
                double[] partial = new double[numSumAggs + 1];
                int ms = tStart;
                while (ms < tEnd) {
                    int me = Math.min(ms + MORSEL_SIZE, tEnd);
                    double[] morselResult = fusedSimdMultiSumAllLongRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numSumAggs, sumLongCols,
                            ms, me);
                    for (int a = 0; a <= numSumAggs; a++) partial[a] += morselResult[a];
                    ms = me;
                }
                return partial;
            });
        }

        double[] total = new double[numSumAggs + 1];
        for (int t = 0; t < nThreads; t++) {
            if (futures[t] == null) break;
            try {
                double[] partial = futures[t].get();
                for (int a = 0; a <= numSumAggs; a++) total[a] += partial[a];
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return total;
    }

    // ========================================================================
    // Block-skip COUNT: compute block min/max, skip blocks that can't match
    // ========================================================================

    private static final int BLOCK_SIZE = 8192;

    /**
     * Check if a block with given min/max may contain matches for a long predicate.
     */
    private static boolean blockMayMatchLong(int predType, long bmin, long bmax, long lo, long hi) {
        switch (predType) {
            case ColumnOps.PRED_RANGE:     return bmax >= lo && bmin <= hi;
            case ColumnOps.PRED_LT:        return bmin < hi;
            case ColumnOps.PRED_GT:        return bmax > lo;
            case ColumnOps.PRED_EQ:        return bmin <= lo && lo <= bmax;
            case ColumnOps.PRED_LTE:       return bmin <= hi;
            case ColumnOps.PRED_GTE:       return bmax >= lo;
            case ColumnOps.PRED_NEQ:       return !(bmin == lo && bmax == lo);
            case ColumnOps.PRED_NOT_RANGE: return bmin < lo || bmax > hi;
            default: return true;
        }
    }

    /**
     * Check if ALL values in a block satisfy a long predicate (full match → add block size).
     */
    private static boolean blockFullMatchLong(int predType, long bmin, long bmax, long lo, long hi) {
        switch (predType) {
            case ColumnOps.PRED_RANGE:     return bmin >= lo && bmax <= hi;
            case ColumnOps.PRED_LT:        return bmax < hi;
            case ColumnOps.PRED_GT:        return bmin > lo;
            case ColumnOps.PRED_EQ:        return bmin == lo && bmax == lo;
            case ColumnOps.PRED_LTE:       return bmax <= hi;
            case ColumnOps.PRED_GTE:       return bmin >= lo;
            case ColumnOps.PRED_NEQ:       return lo < bmin || lo > bmax;  // all values != lo only if lo is outside [bmin,bmax]
            case ColumnOps.PRED_NOT_RANGE: return bmax < lo || bmin > hi;
            default: return false;
        }
    }

    private static boolean blockMayMatchDouble(int predType, double bmin, double bmax, double lo, double hi) {
        switch (predType) {
            case ColumnOps.PRED_RANGE:     return bmax >= lo && bmin <= hi;
            case ColumnOps.PRED_LT:        return bmin < hi;
            case ColumnOps.PRED_GT:        return bmax > lo;
            case ColumnOps.PRED_EQ:        return bmin <= lo && lo <= bmax;
            case ColumnOps.PRED_LTE:       return bmin <= hi;
            case ColumnOps.PRED_GTE:       return bmax >= lo;
            case ColumnOps.PRED_NEQ:       return !(bmin == lo && bmax == lo);
            case ColumnOps.PRED_NOT_RANGE: return bmin < lo || bmax > hi;
            default: return true;
        }
    }

    private static boolean blockFullMatchDouble(int predType, double bmin, double bmax, double lo, double hi) {
        switch (predType) {
            case ColumnOps.PRED_RANGE:     return bmin >= lo && bmax <= hi;
            case ColumnOps.PRED_LT:        return bmax < hi;
            case ColumnOps.PRED_GT:        return bmin > lo;
            case ColumnOps.PRED_EQ:        return bmin == lo && bmax == lo;
            case ColumnOps.PRED_LTE:       return bmax <= hi;
            case ColumnOps.PRED_GTE:       return bmin >= lo;
            case ColumnOps.PRED_NEQ:       return lo < bmin || lo > bmax;
            case ColumnOps.PRED_NOT_RANGE: return bmax < lo || bmin > hi;
            default: return false;
        }
    }

    /**
     * COUNT with block-level statistics for array inputs.
     * Computes block min/max for each predicate column, classifies blocks as
     * skip (0 matches), full (all match), or partial (needs SIMD).
     * For sparse predicates (e.g., 97% zeros with NEQ 0), skips most blocks.
     */
    public static double[] fusedSimdCountBlockSkipParallel(
            int numLongPreds, int[] longPredTypes, long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes, double[][] dblCols, double[] dblLo, double[] dblHi,
            int length) {

        if (length < BLOCK_SIZE * 2) {
            // Too small for block-skip overhead, fall through to direct SIMD
            return ColumnOps.fusedSimdCountRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                0, length);
        }

        final int numBlocks = (length + BLOCK_SIZE - 1) / BLOCK_SIZE;

        // Quick sampling probe: check 4 evenly-spaced blocks.
        // If none can be skipped or fully matched, block-skip won't help.
        {
            int sampleSkippable = 0;
            final int sampleStep = Math.max(1, numBlocks / 4);
            for (int sb = 0; sb < numBlocks; sb += sampleStep) {
                final int bStart = sb * BLOCK_SIZE;
                final int bEnd = Math.min(bStart + BLOCK_SIZE, length);
                boolean canSkipOrFull = false;
                for (int p = 0; p < numLongPreds && !canSkipOrFull; p++) {
                    final long[] col = longCols[p];
                    long bmin = col[bStart], bmax = col[bStart];
                    for (int i = bStart + 1; i < bEnd; i++) {
                        final long v = col[i];
                        if (v < bmin) bmin = v; else if (v > bmax) bmax = v;
                    }
                    if (!blockMayMatchLong(longPredTypes[p], bmin, bmax, longLo[p], longHi[p])
                        || blockFullMatchLong(longPredTypes[p], bmin, bmax, longLo[p], longHi[p]))
                        canSkipOrFull = true;
                }
                for (int p = 0; p < numDblPreds && !canSkipOrFull; p++) {
                    final double[] col = dblCols[p];
                    double bmin = col[bStart], bmax = col[bStart];
                    for (int i = bStart + 1; i < bEnd; i++) {
                        final double v = col[i];
                        if (v < bmin) bmin = v; else if (v > bmax) bmax = v;
                    }
                    if (!blockMayMatchDouble(dblPredTypes[p], bmin, bmax, dblLo[p], dblHi[p])
                        || blockFullMatchDouble(dblPredTypes[p], bmin, bmax, dblLo[p], dblHi[p]))
                        canSkipOrFull = true;
                }
                if (canSkipOrFull) sampleSkippable++;
            }
            // If no sampled block is skippable/fully-matched, fall back immediately
            if (sampleSkippable == 0) {
                return ColumnOps.fusedSimdCountParallel(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    length);
            }
        }

        // 0 = partial (needs SIMD), 1 = skip, 2 = full match
        final byte[] blockClass = new byte[numBlocks];
        java.util.Arrays.fill(blockClass, (byte) 2);

        // Classify blocks by long predicate columns
        for (int p = 0; p < numLongPreds; p++) {
            final long[] col = longCols[p];
            final int pt = longPredTypes[p];
            final long lo = longLo[p], hi = longHi[p];
            for (int b = 0; b < numBlocks; b++) {
                if (blockClass[b] == 1) continue;
                final int bStart = b * BLOCK_SIZE;
                final int bEnd = Math.min(bStart + BLOCK_SIZE, length);
                long bmin = col[bStart], bmax = col[bStart];
                for (int i = bStart + 1; i < bEnd; i++) {
                    final long v = col[i];
                    if (v < bmin) bmin = v;
                    else if (v > bmax) bmax = v;
                }
                if (!blockMayMatchLong(pt, bmin, bmax, lo, hi)) {
                    blockClass[b] = 1;
                } else if (blockClass[b] == 2 && !blockFullMatchLong(pt, bmin, bmax, lo, hi)) {
                    blockClass[b] = 0;
                }
            }
        }

        // Classify blocks by double predicate columns
        for (int p = 0; p < numDblPreds; p++) {
            final double[] col = dblCols[p];
            final int pt = dblPredTypes[p];
            final double lo = dblLo[p], hi = dblHi[p];
            for (int b = 0; b < numBlocks; b++) {
                if (blockClass[b] == 1) continue;
                final int bStart = b * BLOCK_SIZE;
                final int bEnd = Math.min(bStart + BLOCK_SIZE, length);
                double bmin = col[bStart], bmax = col[bStart];
                for (int i = bStart + 1; i < bEnd; i++) {
                    final double v = col[i];
                    if (v < bmin) bmin = v;
                    else if (v > bmax) bmax = v;
                }
                if (!blockMayMatchDouble(pt, bmin, bmax, lo, hi)) {
                    blockClass[b] = 1;
                } else if (blockClass[b] == 2 && !blockFullMatchDouble(pt, bmin, bmax, lo, hi)) {
                    blockClass[b] = 0;
                }
            }
        }

        // Count blocks by classification
        long fullCount = 0;
        int partialCount = 0;
        int skipCount = 0;
        for (int b = 0; b < numBlocks; b++) {
            if (blockClass[b] == 2) {
                final int bStart = b * BLOCK_SIZE;
                fullCount += Math.min(bStart + BLOCK_SIZE, length) - bStart;
            } else if (blockClass[b] == 0) {
                partialCount++;
            } else {
                skipCount++;
            }
        }

        // Adaptive fallback: if block-skip doesn't help enough (>70% partial),
        // fall back to regular parallel SIMD to avoid min/max scan overhead
        if (partialCount > numBlocks * 7 / 10) {
            return ColumnOps.fusedSimdCountParallel(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                length);
        }

        // SIMD scan only partial blocks
        long simdCount = 0;
        if (partialCount > 0) {
            // Parallelize if enough partial blocks
            if (partialCount * BLOCK_SIZE > ColumnOps.PARALLEL_THRESHOLD) {
                final long[] partialCounts = new long[partialCount];
                int idx = 0;
                java.util.concurrent.Future<?>[] futures = new java.util.concurrent.Future<?>[partialCount];
                for (int b = 0; b < numBlocks; b++) {
                    if (blockClass[b] == 0) {
                        final int bStart = b * BLOCK_SIZE;
                        final int bEnd = Math.min(bStart + BLOCK_SIZE, length);
                        final int fi = idx;
                        futures[idx] = ColumnOps.POOL.submit(() -> {
                            double[] r = ColumnOps.fusedSimdCountRange(
                                numLongPreds, longPredTypes, longCols, longLo, longHi,
                                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                                bStart, bEnd);
                            partialCounts[fi] = (long) r[1];
                        });
                        idx++;
                    }
                }
                for (var f : futures) {
                    try { f.get(); } catch (Exception e) { throw new RuntimeException(e); }
                }
                for (long c : partialCounts) simdCount += c;
            } else {
                for (int b = 0; b < numBlocks; b++) {
                    if (blockClass[b] == 0) {
                        final int bStart = b * BLOCK_SIZE;
                        final int bEnd = Math.min(bStart + BLOCK_SIZE, length);
                        double[] r = ColumnOps.fusedSimdCountRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            bStart, bEnd);
                        simdCount += (long) r[1];
                    }
                }
            }
        }

        return new double[] { 0.0, (double)(fullCount + simdCount) };
    }

    // ========================================================================
    // Multi-key hash for group-by when positional encoding overflows long
    // ========================================================================

    private static int mkNextPow2(int v) {
        return Integer.highestOneBit(Math.max(v - 1, 1)) << 1;
    }

    private static int mkHashSlot(long key, int capacity) {
        return (int)((key * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(capacity)));
    }

    /**
     * Compute multi-column hash keys with collision detection and reverse lookup.
     * Used when composite key space overflows long (e.g., 6 wide columns at 100M rows).
     *
     * Uses splitmix64 finalizer + boost hash_combine for excellent avalanche.
     * Builds an open-addressed hash table to detect collisions and store reverse mapping.
     *
     * Returns Object[4]:
     *   [0] long[] rowHashes - hash per input row (length entries)
     *   [1] long[] distinctHashes - unique hash values (nDistinct entries)
     *   [2] long[] distinctVals - flat group column values (nDistinct * numGroupCols)
     *   [3] Integer - 1 if hash collision detected, 0 otherwise
     */
    public static Object[] computeMultiKeyHashWithLookup(
            long[][] groupCols, int numGroupCols, int length, long seed) {
        long[] hashes = new long[length];

        // Open-addressed hash table for collision detection + reverse lookup
        int lutCap = mkNextPow2(Math.max(1024, Math.min(length / 8, 1 << 22)));
        int lutMask = lutCap - 1;
        long[] lutHash = new long[lutCap];
        long[] lutVals = new long[lutCap * numGroupCols];
        java.util.Arrays.fill(lutHash, ColumnOps.EMPTY_KEY);
        boolean collision = false;
        int lutSize = 0;

        for (int i = 0; i < length; i++) {
            // splitmix64 + boost hash_combine per column
            long h = seed;
            for (int g = 0; g < numGroupCols; g++) {
                long v = groupCols[g][i];
                v = (v ^ (v >>> 30)) * 0xbf58476d1ce4e5b9L;
                v = (v ^ (v >>> 27)) * 0x94d049bb133111ebL;
                v = v ^ (v >>> 31);
                h ^= v + 0x9e3779b97f4a7c15L + (h << 6) + (h >>> 2);
            }
            if (h == ColumnOps.EMPTY_KEY) h = ColumnOps.EMPTY_KEY + 1;
            hashes[i] = h;

            if (!collision) {
                int slot = mkHashSlot(h, lutCap) & lutMask;
                boolean found = false;
                while (lutHash[slot] != ColumnOps.EMPTY_KEY) {
                    if (lutHash[slot] == h) {
                        // Same hash — check if same group columns
                        int base = slot * numGroupCols;
                        boolean match = true;
                        for (int g = 0; g < numGroupCols; g++) {
                            if (lutVals[base + g] != groupCols[g][i]) {
                                match = false;
                                break;
                            }
                        }
                        if (match) { found = true; break; }
                        else { collision = true; break; }
                    }
                    slot = (slot + 1) & lutMask;
                }
                if (!collision && !found) {
                    lutHash[slot] = h;
                    int base = slot * numGroupCols;
                    for (int g = 0; g < numGroupCols; g++) {
                        lutVals[base + g] = groupCols[g][i];
                    }
                    lutSize++;
                    if (lutSize * 10 > lutCap * 7) {
                        // Resize
                        int newCap = lutCap << 1;
                        int newMask = newCap - 1;
                        long[] newHash = new long[newCap];
                        long[] newVals = new long[newCap * numGroupCols];
                        java.util.Arrays.fill(newHash, ColumnOps.EMPTY_KEY);
                        for (int s = 0; s < lutCap; s++) {
                            if (lutHash[s] != ColumnOps.EMPTY_KEY) {
                                int ns = mkHashSlot(lutHash[s], newCap) & newMask;
                                while (newHash[ns] != ColumnOps.EMPTY_KEY) ns = (ns + 1) & newMask;
                                newHash[ns] = lutHash[s];
                                System.arraycopy(lutVals, s * numGroupCols,
                                                 newVals, ns * numGroupCols, numGroupCols);
                            }
                        }
                        lutHash = newHash;
                        lutVals = newVals;
                        lutCap = newCap;
                        lutMask = newMask;
                    }
                }
            }
        }

        // Compact lookup table into dense arrays
        long[] compactHash = new long[lutSize];
        long[] compactVals = new long[lutSize * numGroupCols];
        int ci = 0;
        for (int s = 0; s < lutCap; s++) {
            if (lutHash[s] != ColumnOps.EMPTY_KEY) {
                compactHash[ci] = lutHash[s];
                System.arraycopy(lutVals, s * numGroupCols,
                                 compactVals, ci * numGroupCols, numGroupCols);
                ci++;
            }
        }

        return new Object[] { hashes, compactHash, compactVals,
                              Integer.valueOf(collision ? 1 : 0) };
    }

    // =========================================================================
    // Fused Join + Global Aggregate (JIT-isolated from ColumnOps)
    // =========================================================================

    /** Sentinel for empty slots in join hash table (matches ColumnOps.JOIN_EMPTY). */
    private static final long JOIN_EMPTY = ColumnOps.JOIN_EMPTY;

    /**
     * Encode multi-column composite keys as a single long[] via multiplier encoding.
     * NULL (Long.MIN_VALUE) in any column → NULL composite key.
     */
    public static long[] compositeKeyEncode(long[][] columns, long[] muls, int length) {
        final int nCols = columns.length;
        long[] out = new long[length];

        if (nCols == 2) {
            final long[] c0 = columns[0], c1 = columns[1];
            final long m0 = muls[0], m1 = muls[1];
            for (int i = 0; i < length; i++) {
                long v0 = c0[i], v1 = c1[i];
                if (v0 == Long.MIN_VALUE || v1 == Long.MIN_VALUE) {
                    out[i] = Long.MIN_VALUE;
                } else {
                    out[i] = v0 * m0 + v1 * m1;
                }
            }
        } else {
            for (int i = 0; i < length; i++) {
                boolean hasNull = false;
                long key = 0;
                for (int c = 0; c < nCols; c++) {
                    long v = columns[c][i];
                    if (v == Long.MIN_VALUE) { hasNull = true; break; }
                    key += v * muls[c];
                }
                out[i] = hasNull ? Long.MIN_VALUE : key;
            }
        }

        return out;
    }

    /**
     * Fused hash-join + global aggregate (no GROUP BY) — range variant.
     * Pre-computed probe keys (single-col or pre-encoded composite).
     */
    private static double[] hashJoinGlobalAggregateRange(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft, int start, int end) {

        int mask = capacity - 1;
        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        for (int factIdx = start; factIdx < end; factIdx++) {
            long fk = probeKeys[factIdx];

            if (fk == Long.MIN_VALUE) {
                if (isLeft) {
                    for (int a = 0; a < numAggs; a++) {
                        if (factAggCols[a] != null) {
                            int off = a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT:
                                    accs[off] += factAggCols[a][factIdx]; accs[off + 1] += 1; break;
                                case AGG_COUNT:
                                    accs[off + 1] += 1; break;
                                case AGG_MIN:
                                    accs[off] = Math.min(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                                case AGG_MAX:
                                    accs[off] = Math.max(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                            }
                        }
                    }
                }
                continue;
            }

            int slot = hashSlot(fk, capacity) & mask;
            boolean matched = false;
            while (htKeys[slot] != JOIN_EMPTY) {
                if (htKeys[slot] == fk) {
                    int dimIdx = htFirst[slot];
                    while (dimIdx >= 0) {
                        for (int a = 0; a < numAggs; a++) {
                            int off = a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx]
                                        : dimAggCols[a][dimIdx];
                                    accs[off] += val; accs[off + 1] += 1; break;
                                }
                                case AGG_COUNT:
                                    accs[off + 1] += 1; break;
                                case AGG_MIN: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx]
                                        : dimAggCols[a][dimIdx];
                                    accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                }
                                case AGG_MAX: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx]
                                        : dimAggCols[a][dimIdx];
                                    accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        }
                        dimIdx = htNext[dimIdx];
                    }
                    matched = true;
                    break;
                }
                slot = (slot + 1) & mask;
            }

            if (!matched && isLeft) {
                for (int a = 0; a < numAggs; a++) {
                    if (factAggCols[a] != null) {
                        int off = a * 2;
                        switch (aggTypes[a]) {
                            case AGG_SUM: case AGG_SUM_PRODUCT:
                                accs[off] += factAggCols[a][factIdx]; accs[off + 1] += 1; break;
                            case AGG_COUNT:
                                accs[off + 1] += 1; break;
                            case AGG_MIN:
                                accs[off] = Math.min(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                            case AGG_MAX:
                                accs[off] = Math.max(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                        }
                    }
                }
            }
        }

        return accs;
    }

    /**
     * Parallel fused join + global aggregate with pre-computed probe keys.
     */
    @SuppressWarnings("unchecked")
    public static double[] hashJoinGlobalAggregateParallel(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return hashJoinGlobalAggregateRange(
                    htKeys, htFirst, htNext, capacity,
                    probeKeys,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return hashJoinGlobalAggregateRange(
                        htKeys, htFirst, htNext, capacity,
                        probeKeys,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    // =========================================================================
    // Fused Join + Global Aggregate with Inline Composite Keys
    // =========================================================================

    /**
     * Fused hash-join + global aggregate with inline composite key computation.
     * Eliminates the separate compositeKeyEncode pass + 48MB allocation for multi-col keys.
     * Unrolled 2-column fast path for the common J2/J3 pattern.
     */
    private static double[] hashJoinGlobalAggregateInlineKeyRange(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft, int start, int end) {

        int mask = capacity - 1;
        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        if (numKeyCols == 2) {
            // Unrolled 2-column fast path
            final long[] kc0 = factKeyCols[0], kc1 = factKeyCols[1];
            final long km0 = keyMuls[0], km1 = keyMuls[1];
            for (int factIdx = start; factIdx < end; factIdx++) {
                long v0 = kc0[factIdx], v1 = kc1[factIdx];
                if (v0 == Long.MIN_VALUE || v1 == Long.MIN_VALUE) {
                    if (isLeft) accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                    continue;
                }
                long fk = v0 * km0 + v1 * km1;
                probeAndAccumulate(accs, htKeys, htFirst, htNext, mask, fk,
                        numAggs, aggTypes, factAggCols, dimAggCols, isLeft, factIdx);
            }
        } else {
            // General N-column path
            for (int factIdx = start; factIdx < end; factIdx++) {
                boolean hasNull = false;
                long fk = 0;
                for (int c = 0; c < numKeyCols; c++) {
                    long v = factKeyCols[c][factIdx];
                    if (v == Long.MIN_VALUE) { hasNull = true; break; }
                    fk += v * keyMuls[c];
                }
                if (hasNull) {
                    if (isLeft) accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                    continue;
                }
                probeAndAccumulate(accs, htKeys, htFirst, htNext, mask, fk,
                        numAggs, aggTypes, factAggCols, dimAggCols, isLeft, factIdx);
            }
        }

        return accs;
    }

    /** Accumulate fact-side aggs only (NULL key or unmatched LEFT). */
    private static void accumulateFactOnly(double[] accs, int numAggs, int[] aggTypes,
                                           double[][] factAggCols, int factIdx) {
        for (int a = 0; a < numAggs; a++) {
            if (factAggCols[a] != null) {
                int off = a * 2;
                switch (aggTypes[a]) {
                    case AGG_SUM: case AGG_SUM_PRODUCT:
                        accs[off] += factAggCols[a][factIdx]; accs[off + 1] += 1; break;
                    case AGG_COUNT:
                        accs[off + 1] += 1; break;
                    case AGG_MIN:
                        accs[off] = Math.min(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                    case AGG_MAX:
                        accs[off] = Math.max(accs[off], factAggCols[a][factIdx]); accs[off + 1] += 1; break;
                }
            }
        }
    }

    /** Probe hash table for a single fact row key and accumulate aggs. */
    private static void probeAndAccumulate(double[] accs,
                                           long[] htKeys, int[] htFirst, int[] htNext, int mask,
                                           long fk,
                                           int numAggs, int[] aggTypes,
                                           double[][] factAggCols, double[][] dimAggCols,
                                           boolean isLeft, int factIdx) {
        int slot = hashSlot(fk, htKeys.length) & mask;
        boolean matched = false;
        while (htKeys[slot] != JOIN_EMPTY) {
            if (htKeys[slot] == fk) {
                int dimIdx = htFirst[slot];
                while (dimIdx >= 0) {
                    for (int a = 0; a < numAggs; a++) {
                        int off = a * 2;
                        switch (aggTypes[a]) {
                            case AGG_SUM: case AGG_SUM_PRODUCT: {
                                double val = factAggCols[a] != null
                                    ? factAggCols[a][factIdx]
                                    : dimAggCols[a][dimIdx];
                                accs[off] += val; accs[off + 1] += 1; break;
                            }
                            case AGG_COUNT:
                                accs[off + 1] += 1; break;
                            case AGG_MIN: {
                                double val = factAggCols[a] != null
                                    ? factAggCols[a][factIdx]
                                    : dimAggCols[a][dimIdx];
                                accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                            }
                            case AGG_MAX: {
                                double val = factAggCols[a] != null
                                    ? factAggCols[a][factIdx]
                                    : dimAggCols[a][dimIdx];
                                accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                            }
                        }
                    }
                    dimIdx = htNext[dimIdx];
                }
                matched = true;
                break;
            }
            slot = (slot + 1) & mask;
        }

        if (!matched && isLeft) {
            accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
        }
    }

    /**
     * Parallel fused join + global aggregate with inline composite keys.
     */
    @SuppressWarnings("unchecked")
    public static double[] hashJoinGlobalAggregateInlineKeyParallel(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int probeLength,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return hashJoinGlobalAggregateInlineKeyRange(
                    htKeys, htFirst, htNext, capacity,
                    factKeyCols, keyMuls, numKeyCols,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return hashJoinGlobalAggregateInlineKeyRange(
                        htKeys, htFirst, htNext, capacity,
                        factKeyCols, keyMuls, numKeyCols,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    /** Merge thread-local accumulator arrays for join global aggregate. */
    private static double[] mergeJoinAccumulators(Future<double[]>[] futures,
                                                  int numAggs, int[] aggTypes, int accSize) {
        double[] merged = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) merged[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) merged[a * 2] = Double.NEGATIVE_INFINITY;
        }

        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                for (int a = 0; a < numAggs; a++) {
                    int off = a * 2;
                    switch (aggTypes[a]) {
                        case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                            merged[off] += partial[off]; break;
                        case AGG_MIN:
                            merged[off] = Math.min(merged[off], partial[off]); break;
                        case AGG_MAX:
                            merged[off] = Math.max(merged[off], partial[off]); break;
                    }
                    merged[off + 1] += partial[off + 1];
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel fused join global aggregate failed", e);
        }

        return merged;
    }

    // =========================================================================
    // Perfect Hash Join — Direct Array Indexing
    // =========================================================================

    /**
     * Maximum key range for perfect hash join.
     * At 1M range the int[] is 4MB — fits L3. Adapts to actual hardware via L3_BUDGET.
     */
    public static final int PERFECT_HASH_MAX_RANGE =
        (int) Math.min(1_048_576L, ColumnOps.L3_BUDGET / 4);

    /**
     * Build a perfect hash table for small-range integer keys.
     * Direct array indexing: perfectFirst[key - minKey] = first row index.
     *
     * @param keys     Build-side key column
     * @param length   Number of rows
     * @param minKey   Minimum key value
     * @param keyRange Number of distinct possible keys (max - min + 1)
     * @return Object[] = {int[] perfectFirst, int[] perfectNext, boolean isUnique}
     */
    public static Object[] perfectHashJoinBuild(long[] keys, int length, long minKey, int keyRange) {
        int[] perfectFirst = new int[keyRange];
        int[] perfectNext = new int[length];
        Arrays.fill(perfectFirst, -1);
        Arrays.fill(perfectNext, -1);
        boolean isUnique = true;

        for (int i = 0; i < length; i++) {
            long key = keys[i];
            if (key == Long.MIN_VALUE) continue; // skip NULL

            int slot = (int)(key - minKey);
            if (perfectFirst[slot] == -1) {
                perfectFirst[slot] = i;
            } else {
                // Chain: prepend to existing chain
                perfectNext[i] = perfectFirst[slot];
                perfectFirst[slot] = i;
                isUnique = false;
            }
        }
        return new Object[] { perfectFirst, perfectNext, isUnique };
    }

    /**
     * Perfect hash probe for INNER join.
     * O(1) lookup per probe key — no hash function, no linear probing.
     */
    public static Object[] perfectJoinProbeInner(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long[] probeKeys, int probeLength, long minKey, int keyRange) {

        int outCap = probeLength;
        int[] leftOut = new int[outCap];
        int[] rightOut = new int[outCap];
        int outLen = 0;

        for (int i = 0; i < probeLength; i++) {
            long key = probeKeys[i];
            if (key == Long.MIN_VALUE) continue;

            int slot = (int)(key - minKey);
            if (slot < 0 || slot >= keyRange) continue;

            int dimIdx = perfectFirst[slot];
            if (dimIdx < 0) continue;

            if (isUnique) {
                if (outLen >= outCap) {
                    outCap = outCap * 2;
                    leftOut = Arrays.copyOf(leftOut, outCap);
                    rightOut = Arrays.copyOf(rightOut, outCap);
                }
                leftOut[outLen] = i;
                rightOut[outLen] = dimIdx;
                outLen++;
            } else {
                while (dimIdx >= 0) {
                    if (outLen >= outCap) {
                        outCap = outCap * 2;
                        leftOut = Arrays.copyOf(leftOut, outCap);
                        rightOut = Arrays.copyOf(rightOut, outCap);
                    }
                    leftOut[outLen] = i;
                    rightOut[outLen] = dimIdx;
                    outLen++;
                    dimIdx = perfectNext[dimIdx];
                }
            }
        }
        return new Object[] { leftOut, rightOut, outLen };
    }

    /**
     * Perfect hash probe for LEFT OUTER join.
     * Unmatched probe rows emit rightIdx = -1.
     */
    public static Object[] perfectJoinProbeLeft(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long[] probeKeys, int probeLength, long minKey, int keyRange,
            int buildLength) {

        int outCap = probeLength;
        int[] leftOut = new int[outCap];
        int[] rightOut = new int[outCap];
        boolean[] buildMatched = new boolean[buildLength];
        int outLen = 0;

        for (int i = 0; i < probeLength; i++) {
            long key = probeKeys[i];
            boolean matched = false;

            if (key != Long.MIN_VALUE) {
                int slot = (int)(key - minKey);
                if (slot >= 0 && slot < keyRange) {
                    int dimIdx = perfectFirst[slot];
                    if (dimIdx >= 0) {
                        if (isUnique) {
                            if (outLen >= outCap) {
                                outCap = outCap * 2;
                                leftOut = Arrays.copyOf(leftOut, outCap);
                                rightOut = Arrays.copyOf(rightOut, outCap);
                            }
                            leftOut[outLen] = i;
                            rightOut[outLen] = dimIdx;
                            buildMatched[dimIdx] = true;
                            outLen++;
                            matched = true;
                        } else {
                            while (dimIdx >= 0) {
                                if (outLen >= outCap) {
                                    outCap = outCap * 2;
                                    leftOut = Arrays.copyOf(leftOut, outCap);
                                    rightOut = Arrays.copyOf(rightOut, outCap);
                                }
                                leftOut[outLen] = i;
                                rightOut[outLen] = dimIdx;
                                buildMatched[dimIdx] = true;
                                outLen++;
                                matched = true;
                                dimIdx = perfectNext[dimIdx];
                            }
                        }
                    }
                }
            }

            if (!matched) {
                if (outLen >= outCap) {
                    outCap = outCap * 2;
                    leftOut = Arrays.copyOf(leftOut, outCap);
                    rightOut = Arrays.copyOf(rightOut, outCap);
                }
                leftOut[outLen] = i;
                rightOut[outLen] = -1;
                outLen++;
            }
        }
        return new Object[] { leftOut, rightOut, outLen, buildMatched };
    }

    // =========================================================================
    // Perfect Hash Join + Global Aggregate
    // =========================================================================

    /**
     * Perfect hash probe + global aggregate — range variant.
     * Pre-computed probe keys (single-col or pre-encoded composite).
     * Accumulation is inlined for JIT optimization (no helper method call in hot loop).
     */
    private static double[] perfectJoinGlobalAggregateRange(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft, int start, int end) {

        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        for (int factIdx = start; factIdx < end; factIdx++) {
            long fk = probeKeys[factIdx];

            if (fk == Long.MIN_VALUE) {
                if (isLeft) accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                continue;
            }

            int slot = (int)(fk - minKey);
            boolean matched = false;
            if (slot >= 0 && slot < keyRange) {
                int dimIdx = perfectFirst[slot];
                if (dimIdx >= 0) {
                    matched = true;
                    if (isUnique) {
                        // Inline accumulation — no helper call for JIT
                        for (int a = 0; a < numAggs; a++) {
                            int off = a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                    accs[off] += val; accs[off + 1] += 1; break;
                                }
                                case AGG_COUNT: accs[off + 1] += 1; break;
                                case AGG_MIN: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                    accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                }
                                case AGG_MAX: {
                                    double val = factAggCols[a] != null
                                        ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                    accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        }
                    } else {
                        while (dimIdx >= 0) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] += val; accs[off + 1] += 1; break;
                                    }
                                    case AGG_COUNT: accs[off + 1] += 1; break;
                                    case AGG_MIN: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                    case AGG_MAX: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                }
                            }
                            dimIdx = perfectNext[dimIdx];
                        }
                    }
                }
            }

            if (!matched && isLeft) {
                accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
            }
        }

        return accs;
    }

    /**
     * Parallel perfect hash join + global aggregate with pre-computed probe keys.
     */
    @SuppressWarnings("unchecked")
    public static double[] perfectJoinGlobalAggregateParallel(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys, int probeLength,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return perfectJoinGlobalAggregateRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    probeKeys,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return perfectJoinGlobalAggregateRange(
                        perfectFirst, perfectNext, isUnique,
                        minKey, keyRange,
                        probeKeys,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    /**
     * Perfect hash join + global aggregate with inline composite key computation.
     * Eliminates the separate compositeKeyEncode pass for multi-col keys.
     * Probe + accumulation inlined for JIT optimization.
     */
    private static double[] perfectJoinGlobalAggregateInlineKeyRange(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft, int start, int end) {

        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        if (numKeyCols == 2) {
            final long[] kc0 = factKeyCols[0], kc1 = factKeyCols[1];
            final long km0 = keyMuls[0], km1 = keyMuls[1];
            for (int factIdx = start; factIdx < end; factIdx++) {
                long v0 = kc0[factIdx], v1 = kc1[factIdx];
                if (v0 == Long.MIN_VALUE || v1 == Long.MIN_VALUE) {
                    if (isLeft) accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                    continue;
                }
                long fk = v0 * km0 + v1 * km1;
                // Inline probe + accumulate
                int slot = (int)(fk - minKey);
                boolean matched = false;
                if (slot >= 0 && slot < keyRange) {
                    int dimIdx = perfectFirst[slot];
                    if (dimIdx >= 0) {
                        matched = true;
                        if (isUnique) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] += val; accs[off + 1] += 1; break;
                                    }
                                    case AGG_COUNT: accs[off + 1] += 1; break;
                                    case AGG_MIN: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                    case AGG_MAX: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                }
                            }
                        } else {
                            while (dimIdx >= 0) {
                                for (int a = 0; a < numAggs; a++) {
                                    int off = a * 2;
                                    switch (aggTypes[a]) {
                                        case AGG_SUM: case AGG_SUM_PRODUCT: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] += val; accs[off + 1] += 1; break;
                                        }
                                        case AGG_COUNT: accs[off + 1] += 1; break;
                                        case AGG_MIN: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                        }
                                        case AGG_MAX: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                        }
                                    }
                                }
                                dimIdx = perfectNext[dimIdx];
                            }
                        }
                    }
                }
                if (!matched && isLeft) {
                    accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                }
            }
        } else {
            for (int factIdx = start; factIdx < end; factIdx++) {
                boolean hasNull = false;
                long fk = 0;
                for (int c = 0; c < numKeyCols; c++) {
                    long v = factKeyCols[c][factIdx];
                    if (v == Long.MIN_VALUE) { hasNull = true; break; }
                    fk += v * keyMuls[c];
                }
                if (hasNull) {
                    if (isLeft) accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                    continue;
                }
                // Inline probe + accumulate
                int slot = (int)(fk - minKey);
                boolean matched = false;
                if (slot >= 0 && slot < keyRange) {
                    int dimIdx = perfectFirst[slot];
                    if (dimIdx >= 0) {
                        matched = true;
                        if (isUnique) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] += val; accs[off + 1] += 1; break;
                                    }
                                    case AGG_COUNT: accs[off + 1] += 1; break;
                                    case AGG_MIN: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                    case AGG_MAX: {
                                        double val = factAggCols[a] != null
                                            ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                        accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                }
                            }
                        } else {
                            while (dimIdx >= 0) {
                                for (int a = 0; a < numAggs; a++) {
                                    int off = a * 2;
                                    switch (aggTypes[a]) {
                                        case AGG_SUM: case AGG_SUM_PRODUCT: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] += val; accs[off + 1] += 1; break;
                                        }
                                        case AGG_COUNT: accs[off + 1] += 1; break;
                                        case AGG_MIN: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                        }
                                        case AGG_MAX: {
                                            double val = factAggCols[a] != null
                                                ? factAggCols[a][factIdx] : dimAggCols[a][dimIdx];
                                            accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                        }
                                    }
                                }
                                dimIdx = perfectNext[dimIdx];
                            }
                        }
                    }
                }
                if (!matched && isLeft) {
                    accumulateFactOnly(accs, numAggs, aggTypes, factAggCols, factIdx);
                }
            }
        }

        return accs;
    }

    /**
     * Parallel perfect hash join + global aggregate with inline composite keys.
     */
    @SuppressWarnings("unchecked")
    public static double[] perfectJoinGlobalAggregateInlineKeyParallel(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int probeLength,
            int numAggs, int[] aggTypes,
            double[][] factAggCols, double[][] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return perfectJoinGlobalAggregateInlineKeyRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    factKeyCols, keyMuls, numKeyCols,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return perfectJoinGlobalAggregateInlineKeyRange(
                        perfectFirst, perfectNext, isUnique,
                        minKey, keyRange,
                        factKeyCols, keyMuls, numKeyCols,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    // =========================================================================
    // Perfect Hash Join + Global Aggregate — Mixed Types (skip longToDouble)
    // =========================================================================

    /**
     * Perfect hash probe + global aggregate with mixed-type columns.
     * Accepts Object[] where each entry is long[], double[], or null.
     * Avoids the full-array longToDouble conversion (~7ms for 6M rows).
     * Pre-extracts typed arrays before hot loop for JIT optimization.
     */
    private static double[] perfectJoinGlobalAggregateMixedRange(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys,
            int numAggs, int[] aggTypes,
            Object[] factAggCols, Object[] dimAggCols,
            boolean isLeft, int start, int end) {

        // Pre-extract typed arrays — one-time cost before hot loop
        final long[][] fLong = new long[numAggs][];
        final double[][] fDouble = new double[numAggs][];
        final long[][] dLong = new long[numAggs][];
        final double[][] dDouble = new double[numAggs][];
        for (int a = 0; a < numAggs; a++) {
            if (factAggCols[a] instanceof long[]) fLong[a] = (long[]) factAggCols[a];
            else if (factAggCols[a] instanceof double[]) fDouble[a] = (double[]) factAggCols[a];
            if (dimAggCols[a] instanceof long[]) dLong[a] = (long[]) dimAggCols[a];
            else if (dimAggCols[a] instanceof double[]) dDouble[a] = (double[]) dimAggCols[a];
        }

        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        for (int factIdx = start; factIdx < end; factIdx++) {
            long fk = probeKeys[factIdx];

            if (fk == Long.MIN_VALUE) {
                if (isLeft) {
                    // Inline fact-only accumulation for NULL keys
                    for (int a = 0; a < numAggs; a++) {
                        int off = a * 2;
                        if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                        if (fLong[a] != null || fDouble[a] != null) {
                            double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT:
                                    accs[off] += val; accs[off + 1] += 1; break;
                                case AGG_MIN:
                                    accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                case AGG_MAX:
                                    accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                            }
                        }
                    }
                }
                continue;
            }

            int slot = (int)(fk - minKey);
            boolean matched = false;
            if (slot >= 0 && slot < keyRange) {
                int dimIdx = perfectFirst[slot];
                if (dimIdx >= 0) {
                    matched = true;
                    if (isUnique) {
                        for (int a = 0; a < numAggs; a++) {
                            int off = a * 2;
                            if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                            double val;
                            if (fLong[a] != null) val = (double) fLong[a][factIdx];
                            else if (fDouble[a] != null) val = fDouble[a][factIdx];
                            else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                            else val = dDouble[a][dimIdx];
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT:
                                    accs[off] += val; accs[off + 1] += 1; break;
                                case AGG_MIN:
                                    accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                case AGG_MAX:
                                    accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                            }
                        }
                    } else {
                        while (dimIdx >= 0) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                                double val;
                                if (fLong[a] != null) val = (double) fLong[a][factIdx];
                                else if (fDouble[a] != null) val = fDouble[a][factIdx];
                                else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                                else val = dDouble[a][dimIdx];
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT:
                                        accs[off] += val; accs[off + 1] += 1; break;
                                    case AGG_MIN:
                                        accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    case AGG_MAX:
                                        accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                            dimIdx = perfectNext[dimIdx];
                        }
                    }
                }
            }

            if (!matched && isLeft) {
                for (int a = 0; a < numAggs; a++) {
                    int off = a * 2;
                    if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                    if (fLong[a] != null || fDouble[a] != null) {
                        double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                        switch (aggTypes[a]) {
                            case AGG_SUM: case AGG_SUM_PRODUCT:
                                accs[off] += val; accs[off + 1] += 1; break;
                            case AGG_MIN:
                                accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                            case AGG_MAX:
                                accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                        }
                    }
                }
            }
        }

        return accs;
    }

    /**
     * Parallel perfect hash join + global aggregate with mixed-type columns.
     */
    @SuppressWarnings("unchecked")
    public static double[] perfectJoinGlobalAggregateMixedParallel(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys, int probeLength,
            int numAggs, int[] aggTypes,
            Object[] factAggCols, Object[] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return perfectJoinGlobalAggregateMixedRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    probeKeys,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return perfectJoinGlobalAggregateMixedRange(
                        perfectFirst, perfectNext, isUnique,
                        minKey, keyRange,
                        probeKeys,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    /**
     * Perfect hash join + global aggregate with inline composite keys and mixed-type columns.
     */
    private static double[] perfectJoinGlobalAggregateMixedInlineKeyRange(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int numAggs, int[] aggTypes,
            Object[] factAggCols, Object[] dimAggCols,
            boolean isLeft, int start, int end) {

        // Pre-extract typed arrays
        final long[][] fLong = new long[numAggs][];
        final double[][] fDouble = new double[numAggs][];
        final long[][] dLong = new long[numAggs][];
        final double[][] dDouble = new double[numAggs][];
        for (int a = 0; a < numAggs; a++) {
            if (factAggCols[a] instanceof long[]) fLong[a] = (long[]) factAggCols[a];
            else if (factAggCols[a] instanceof double[]) fDouble[a] = (double[]) factAggCols[a];
            if (dimAggCols[a] instanceof long[]) dLong[a] = (long[]) dimAggCols[a];
            else if (dimAggCols[a] instanceof double[]) dDouble[a] = (double[]) dimAggCols[a];
        }

        final int accSize = numAggs * 2;
        double[] accs = new double[accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }

        if (numKeyCols == 2) {
            final long[] kc0 = factKeyCols[0], kc1 = factKeyCols[1];
            final long km0 = keyMuls[0], km1 = keyMuls[1];
            for (int factIdx = start; factIdx < end; factIdx++) {
                long v0 = kc0[factIdx], v1 = kc1[factIdx];
                if (v0 == Long.MIN_VALUE || v1 == Long.MIN_VALUE) {
                    if (isLeft) {
                        for (int a = 0; a < numAggs; a++) {
                            int off = a * 2;
                            if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                            if (fLong[a] != null || fDouble[a] != null) {
                                double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                    case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        }
                    }
                    continue;
                }
                long fk = v0 * km0 + v1 * km1;
                int slot = (int)(fk - minKey);
                boolean matched = false;
                if (slot >= 0 && slot < keyRange) {
                    int dimIdx = perfectFirst[slot];
                    if (dimIdx >= 0) {
                        matched = true;
                        if (isUnique) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                                double val;
                                if (fLong[a] != null) val = (double) fLong[a][factIdx];
                                else if (fDouble[a] != null) val = fDouble[a][factIdx];
                                else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                                else val = dDouble[a][dimIdx];
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                    case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        } else {
                            while (dimIdx >= 0) {
                                for (int a = 0; a < numAggs; a++) {
                                    int off = a * 2;
                                    if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                                    double val;
                                    if (fLong[a] != null) val = (double) fLong[a][factIdx];
                                    else if (fDouble[a] != null) val = fDouble[a][factIdx];
                                    else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                                    else val = dDouble[a][dimIdx];
                                    switch (aggTypes[a]) {
                                        case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                        case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                        case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                }
                                dimIdx = perfectNext[dimIdx];
                            }
                        }
                    }
                }
                if (!matched && isLeft) {
                    for (int a = 0; a < numAggs; a++) {
                        int off = a * 2;
                        if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                        if (fLong[a] != null || fDouble[a] != null) {
                            double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                            }
                        }
                    }
                }
            }
        } else {
            for (int factIdx = start; factIdx < end; factIdx++) {
                boolean hasNull = false;
                long fk = 0;
                for (int c = 0; c < numKeyCols; c++) {
                    long v = factKeyCols[c][factIdx];
                    if (v == Long.MIN_VALUE) { hasNull = true; break; }
                    fk += v * keyMuls[c];
                }
                if (hasNull) {
                    if (isLeft) {
                        for (int a = 0; a < numAggs; a++) {
                            int off = a * 2;
                            if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                            if (fLong[a] != null || fDouble[a] != null) {
                                double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                    case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        }
                    }
                    continue;
                }
                int slot = (int)(fk - minKey);
                boolean matched = false;
                if (slot >= 0 && slot < keyRange) {
                    int dimIdx = perfectFirst[slot];
                    if (dimIdx >= 0) {
                        matched = true;
                        if (isUnique) {
                            for (int a = 0; a < numAggs; a++) {
                                int off = a * 2;
                                if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                                double val;
                                if (fLong[a] != null) val = (double) fLong[a][factIdx];
                                else if (fDouble[a] != null) val = fDouble[a][factIdx];
                                else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                                else val = dDouble[a][dimIdx];
                                switch (aggTypes[a]) {
                                    case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                    case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                    case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                }
                            }
                        } else {
                            while (dimIdx >= 0) {
                                for (int a = 0; a < numAggs; a++) {
                                    int off = a * 2;
                                    if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                                    double val;
                                    if (fLong[a] != null) val = (double) fLong[a][factIdx];
                                    else if (fDouble[a] != null) val = fDouble[a][factIdx];
                                    else if (dLong[a] != null) val = (double) dLong[a][dimIdx];
                                    else val = dDouble[a][dimIdx];
                                    switch (aggTypes[a]) {
                                        case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                        case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                        case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                                    }
                                }
                                dimIdx = perfectNext[dimIdx];
                            }
                        }
                    }
                }
                if (!matched && isLeft) {
                    for (int a = 0; a < numAggs; a++) {
                        int off = a * 2;
                        if (aggTypes[a] == AGG_COUNT) { accs[off + 1] += 1; continue; }
                        if (fLong[a] != null || fDouble[a] != null) {
                            double val = fLong[a] != null ? (double) fLong[a][factIdx] : fDouble[a][factIdx];
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: accs[off] += val; accs[off + 1] += 1; break;
                                case AGG_MIN: accs[off] = Math.min(accs[off], val); accs[off + 1] += 1; break;
                                case AGG_MAX: accs[off] = Math.max(accs[off], val); accs[off + 1] += 1; break;
                            }
                        }
                    }
                }
            }
        }

        return accs;
    }

    /**
     * Parallel perfect hash join + global aggregate with inline composite keys and mixed types.
     */
    @SuppressWarnings("unchecked")
    public static double[] perfectJoinGlobalAggregateMixedInlineKeyParallel(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[][] factKeyCols, long[] keyMuls, int numKeyCols,
            int probeLength,
            int numAggs, int[] aggTypes,
            Object[] factAggCols, Object[] dimAggCols,
            boolean isLeft) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return perfectJoinGlobalAggregateMixedInlineKeyRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    factKeyCols, keyMuls, numKeyCols,
                    numAggs, aggTypes, factAggCols, dimAggCols,
                    isLeft, 0, probeLength);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return perfectJoinGlobalAggregateMixedInlineKeyRange(
                        perfectFirst, perfectNext, isUnique,
                        minKey, keyRange,
                        factKeyCols, keyMuls, numKeyCols,
                        numAggs, aggTypes, factAggCols, dimAggCols,
                        isLeft, threadStart, threadEnd);
            });
        }

        return mergeJoinAccumulators(futures, numAggs, aggTypes, accSize);
    }

    // =========================================================================
    // Perfect Hash Join + Group-By + Aggregate (Dense)
    // =========================================================================

    /**
     * Fused perfect-hash probe + dim-column read + fact-column accumulation.
     * Same layout as fusedJoinGroupAggregateDenseRange but with O(1) probe.
     */
    private static double[] perfectJoinGroupAggregateDenseRange(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys, int probeLength,
            int numGroupCols, long[][] dimGroupCols, long[] dimGroupMuls,
            int numAggs, int[] aggTypes, double[][] factAggCols,
            int start, int end, int maxKey) {

        final int accSize = numAggs * 2;
        double[] groups = new double[maxKey * accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.NEGATIVE_INFINITY;
            }
        }

        // Extract group column arrays to final locals for JIT
        final long[] gc0 = numGroupCols > 0 ? dimGroupCols[0] : null;
        final long[] gc1 = numGroupCols > 1 ? dimGroupCols[1] : null;
        final long[] gc2 = numGroupCols > 2 ? dimGroupCols[2] : null;
        final long[] gc3 = numGroupCols > 3 ? dimGroupCols[3] : null;
        final long[] gc4 = numGroupCols > 4 ? dimGroupCols[4] : null;
        final long[] gc5 = numGroupCols > 5 ? dimGroupCols[5] : null;
        final long gm0 = numGroupCols > 0 ? dimGroupMuls[0] : 0;
        final long gm1 = numGroupCols > 1 ? dimGroupMuls[1] : 0;
        final long gm2 = numGroupCols > 2 ? dimGroupMuls[2] : 0;
        final long gm3 = numGroupCols > 3 ? dimGroupMuls[3] : 0;
        final long gm4 = numGroupCols > 4 ? dimGroupMuls[4] : 0;
        final long gm5 = numGroupCols > 5 ? dimGroupMuls[5] : 0;

        for (int factIdx = start; factIdx < end; factIdx++) {
            long fk = probeKeys[factIdx];
            if (fk == Long.MIN_VALUE) continue;

            int slot = (int)(fk - minKey);
            if (slot < 0 || slot >= keyRange) continue;

            int dimIdx = perfectFirst[slot];
            if (dimIdx < 0) continue;

            if (isUnique) {
                // Fast path: single dim row per key — no chain walk
                int key = (int)(gc0[dimIdx] * gm0);
                if (numGroupCols > 1) key += (int)(gc1[dimIdx] * gm1);
                if (numGroupCols > 2) key += (int)(gc2[dimIdx] * gm2);
                if (numGroupCols > 3) key += (int)(gc3[dimIdx] * gm3);
                if (numGroupCols > 4) key += (int)(gc4[dimIdx] * gm4);
                if (numGroupCols > 5) key += (int)(gc5[dimIdx] * gm5);
                for (int g = 6; g < numGroupCols; g++)
                    key += (int)(dimGroupCols[g][dimIdx] * dimGroupMuls[g]);

                int base0 = key * accSize;
                for (int a = 0; a < numAggs; a++) {
                    int off = base0 + a * 2;
                    switch (aggTypes[a]) {
                        case AGG_SUM: case AGG_SUM_PRODUCT:
                            groups[off] += factAggCols[a][factIdx];
                            groups[off + 1] += 1; break;
                        case AGG_COUNT:
                            groups[off + 1] += 1; break;
                        case AGG_MIN:
                            groups[off] = Math.min(groups[off], factAggCols[a][factIdx]);
                            groups[off + 1] += 1; break;
                        case AGG_MAX:
                            groups[off] = Math.max(groups[off], factAggCols[a][factIdx]);
                            groups[off + 1] += 1; break;
                    }
                }
            } else {
                while (dimIdx >= 0) {
                    int key = (int)(gc0[dimIdx] * gm0);
                    if (numGroupCols > 1) key += (int)(gc1[dimIdx] * gm1);
                    if (numGroupCols > 2) key += (int)(gc2[dimIdx] * gm2);
                    if (numGroupCols > 3) key += (int)(gc3[dimIdx] * gm3);
                    if (numGroupCols > 4) key += (int)(gc4[dimIdx] * gm4);
                    if (numGroupCols > 5) key += (int)(gc5[dimIdx] * gm5);
                    for (int g = 6; g < numGroupCols; g++)
                        key += (int)(dimGroupCols[g][dimIdx] * dimGroupMuls[g]);

                    int base0 = key * accSize;
                    for (int a = 0; a < numAggs; a++) {
                        int off = base0 + a * 2;
                        switch (aggTypes[a]) {
                            case AGG_SUM: case AGG_SUM_PRODUCT:
                                groups[off] += factAggCols[a][factIdx];
                                groups[off + 1] += 1; break;
                            case AGG_COUNT:
                                groups[off + 1] += 1; break;
                            case AGG_MIN:
                                groups[off] = Math.min(groups[off], factAggCols[a][factIdx]);
                                groups[off + 1] += 1; break;
                            case AGG_MAX:
                                groups[off] = Math.max(groups[off], factAggCols[a][factIdx]);
                                groups[off + 1] += 1; break;
                        }
                    }
                    dimIdx = perfectNext[dimIdx];
                }
            }
        }

        return groups;
    }

    /**
     * Parallel perfect hash join + group-by + aggregate.
     * Same L3-adaptive thread capping as fusedJoinGroupAggregateDenseParallel.
     */
    @SuppressWarnings("unchecked")
    public static double[] perfectJoinGroupAggregateDenseParallel(
            int[] perfectFirst, int[] perfectNext, boolean isUnique,
            long minKey, int keyRange,
            long[] probeKeys, int probeLength,
            int numGroupCols, long[][] dimGroupCols, long[] dimGroupMuls,
            int numAggs, int[] aggTypes, double[][] factAggCols,
            int maxKey) {

        if (probeLength < ColumnOps.PARALLEL_THRESHOLD) {
            return perfectJoinGroupAggregateDenseRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    probeKeys, probeLength,
                    numGroupCols, dimGroupCols, dimGroupMuls,
                    numAggs, aggTypes, factAggCols,
                    0, probeLength, maxKey);
        }

        long perThreadMem = (long) maxKey * numAggs * 2 * 8;
        int nThreads = Math.min(POOL.getParallelism(), ColumnOps.effectiveGroupByThreads(perThreadMem));
        if (nThreads <= 1) {
            return perfectJoinGroupAggregateDenseRange(
                    perfectFirst, perfectNext, isUnique,
                    minKey, keyRange,
                    probeKeys, probeLength,
                    numGroupCols, dimGroupCols, dimGroupMuls,
                    numAggs, aggTypes, factAggCols,
                    0, probeLength, maxKey);
        }
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return perfectJoinGroupAggregateDenseRange(
                        perfectFirst, perfectNext, isUnique,
                        minKey, keyRange,
                        probeKeys, probeLength,
                        numGroupCols, dimGroupCols, dimGroupMuls,
                        numAggs, aggTypes, factAggCols,
                        threadStart, threadEnd, maxKey);
            });
        }

        // Merge thread results
        double[] merged = new double[maxKey * accSize];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) merged[k * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) merged[k * accSize + a * 2] = Double.NEGATIVE_INFINITY;
            }
        }

        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                for (int k = 0; k < maxKey; k++) {
                    int base0 = k * accSize;
                    if (partial[base0 + 1] == 0.0) continue;
                    for (int a = 0; a < numAggs; a++) {
                        int off = base0 + a * 2;
                        switch (aggTypes[a]) {
                            case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                                merged[off] += partial[off]; break;
                            case AGG_MIN:
                                merged[off] = Math.min(merged[off], partial[off]); break;
                            case AGG_MAX:
                                merged[off] = Math.max(merged[off], partial[off]); break;
                        }
                        merged[off + 1] += partial[off + 1];
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel perfect join+group execution failed", e);
        }

        return merged;
    }

}
