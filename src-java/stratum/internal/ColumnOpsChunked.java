package stratum.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Chunked dense group-by operations — JIT-isolated from ColumnOpsExt.
 *
 * Contains streaming chunk-at-a-time GROUP-BY paths that process
 * PersistentColumnIndex data without full materialization:
 * - Dense group-by (fusedGroupAggregateDenseChunkedParallel)
 * - VARIANCE/CORR group-by (fusedGroupAggregateDenseVarChunkedParallel)
 *
 * SIMD filter+aggregate paths (fusedSimdChunkedParallel, fusedSimdChunkedCountParallel)
 * are in ColumnOpsChunkedSimd to prevent cross-method JIT interference:
 * SIMD-heavy methods (130+ lines of broadcasts and switches) degrade
 * JIT compilation of group-by scatter methods in the same class.
 */
public final class ColumnOpsChunked {

    // Re-use ColumnOps pool and thresholds
    private static ForkJoinPool POOL = ColumnOps.POOL;

    // Agg type constants (same values as ColumnOps)
    public static final int AGG_SUM = ColumnOps.AGG_SUM;
    public static final int AGG_COUNT = ColumnOps.AGG_COUNT;
    public static final int AGG_SUM_PRODUCT = ColumnOps.AGG_SUM_PRODUCT;
    public static final int AGG_MIN = ColumnOps.AGG_MIN;
    public static final int AGG_MAX = ColumnOps.AGG_MAX;
    public static final int AGG_VARIANCE = 7;
    public static final int AGG_CORR = 8;

    // Predicate type constants (same values as ColumnOps)
    private static final int PRED_RANGE = ColumnOps.PRED_RANGE;
    private static final int PRED_LT = ColumnOps.PRED_LT;
    private static final int PRED_GT = ColumnOps.PRED_GT;
    private static final int PRED_EQ = ColumnOps.PRED_EQ;
    private static final int PRED_LTE = ColumnOps.PRED_LTE;
    private static final int PRED_GTE = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    // Deliberate duplicate for JIT isolation — not a shared call
    private static int aggSlots(int aggType) {
        switch (aggType) {
            case AGG_VARIANCE: return 3;
            case AGG_CORR: return 6;
            default: return 2;
        }
    }

    /**
     * Local copy of evaluatePredicates for JIT isolation.
     * Cross-class calls to ColumnOps.evaluatePredicates (398 bytes) fail
     * to inline at tier 3, causing 6M non-inlined calls per invocation.
     * This local copy enables same-class inlining.
     */
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

    // =========================================================================
    // Chunked Dense Group-By (streaming over index chunks, no materialization)
    // =========================================================================

    /**
     * Per-thread worker: processes chunks [startChunk, endChunk).
     * Allocates temp arrays once per thread (reused across chunks).
     * Copies heap array data via System.arraycopy, scatters into dense accumulator.
     *
     * @param longPredArrs   Object[numLongPreds][nChunks] long[] chunk arrays
     * @param dblPredArrs    Object[numDblPreds][nChunks] double[] chunk arrays
     * @param groupColArrs   Object[numGroupCols][nChunks] long[] chunk arrays
     * @param groupMuls      multipliers for composite key encoding
     * @param groupOffsets   per-column offset to subtract for normalization (0 if no norm)
     * @param aggColArrs     Object[numAggs][nChunks] long[]/double[] chunk arrays
     * @param aggCol2Arrs    Object[numAggs][nChunks] long[]/double[] chunk arrays for SUM_PRODUCT second col
     * @param aggColIsLong   boolean[numAggs] true if agg source is int64
     * @param aggCol2IsLong  boolean[numAggs] true if agg col2 source is int64
     * @param chunkLengths   int[nChunks] length of each chunk
     * @param maxChunkLen    max chunk length (for temp array sizing)
     * @param maxKey         dense key space size (exclusive)
     * @param nullSlots      per-column NULL slot value (>=0 means NULLs present), or null
     */
    private static double[] fusedGroupAggregateDenseChunkedRange(
            int numLongPreds, int[] longPredTypes,
            Object[][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            Object[][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numGroupCols, Object[][] groupColArrs, long[] groupMuls, long[] groupOffsets,
            int numAggs, int[] aggTypes, Object[][] aggColArrs, Object[][] aggCol2Arrs,
            boolean[] aggColIsLong, boolean[] aggCol2IsLong,
            int[] chunkLengths, int startChunk, int endChunk, int maxChunkLen, int maxKey,
            boolean nanSafe, long[] nullSlots) {

        final int accSize = numAggs * 2;
        double[] groups = new double[maxKey * accSize];
        // Init MIN to +Inf, MAX to -Inf
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.NEGATIVE_INFINITY;
            }
        }

        // Direct chunk refs for group/agg cols (no System.arraycopy for heap arrays)
        double[][] tmpAggCols = new double[numAggs][];
        double[][] tmpAggCol2s = new double[numAggs][];
        double[][] longConvBufs = new double[numAggs][maxChunkLen];
        double[][] longConvBufs2 = new double[numAggs][maxChunkLen];
        long[][] tmpLongPreds = numLongPreds > 0 ? new long[numLongPreds][maxChunkLen] : null;
        double[][] tmpDblPreds = numDblPreds > 0 ? new double[numDblPreds][maxChunkLen] : null;
        int[] rowKeys = new int[maxChunkLen]; // Precomputed dense keys per row (-1 = filtered)

        final boolean hasPredicates = numLongPreds > 0 || numDblPreds > 0;

        for (int c = startChunk; c < endChunk; c++) {
            int len = chunkLengths[c];

            // Direct group col references (heap arrays, no copy)
            final long[] gc0 = numGroupCols > 0 ? (long[]) groupColArrs[0][c] : null;
            final long[] gc1 = numGroupCols > 1 ? (long[]) groupColArrs[1][c] : null;
            final long[] gc2 = numGroupCols > 2 ? (long[]) groupColArrs[2][c] : null;
            final long[] gc3 = numGroupCols > 3 ? (long[]) groupColArrs[3][c] : null;
            final long[] gc4 = numGroupCols > 4 ? (long[]) groupColArrs[4][c] : null;
            final long[] gc5 = numGroupCols > 5 ? (long[]) groupColArrs[5][c] : null;
            final long gm0 = numGroupCols > 0 ? groupMuls[0] : 0;
            final long gm1 = numGroupCols > 1 ? groupMuls[1] : 0;
            final long gm2 = numGroupCols > 2 ? groupMuls[2] : 0;
            final long gm3 = numGroupCols > 3 ? groupMuls[3] : 0;
            final long gm4 = numGroupCols > 4 ? groupMuls[4] : 0;
            final long gm5 = numGroupCols > 5 ? groupMuls[5] : 0;
            final long go0 = numGroupCols > 0 ? groupOffsets[0] : 0;
            final long go1 = numGroupCols > 1 ? groupOffsets[1] : 0;
            final long go2 = numGroupCols > 2 ? groupOffsets[2] : 0;
            final long go3 = numGroupCols > 3 ? groupOffsets[3] : 0;
            final long go4 = numGroupCols > 4 ? groupOffsets[4] : 0;
            final long go5 = numGroupCols > 5 ? groupOffsets[5] : 0;

            // Agg col refs: direct for double[], convert for long[]
            for (int a = 0; a < numAggs; a++) {
                if (aggTypes[a] == AGG_COUNT) { tmpAggCols[a] = longConvBufs[a]; continue; }
                if (aggColIsLong[a]) {
                    long[] src = (long[]) aggColArrs[a][c];
                    double[] buf = longConvBufs[a];
                    for (int i = 0; i < len; i++) buf[i] = (double) src[i];
                    tmpAggCols[a] = buf;
                } else {
                    tmpAggCols[a] = (double[]) aggColArrs[a][c];
                }
                if (aggTypes[a] == AGG_SUM_PRODUCT) {
                    if (aggCol2IsLong[a]) {
                        long[] src2 = (long[]) aggCol2Arrs[a][c];
                        double[] buf2 = longConvBufs2[a];
                        for (int i = 0; i < len; i++) buf2[i] = (double) src2[i];
                        tmpAggCol2s[a] = buf2;
                    } else {
                        tmpAggCol2s[a] = (double[]) aggCol2Arrs[a][c];
                    }
                }
            }

            // Copy pred arrays only when predicates exist
            if (hasPredicates) {
                for (int p = 0; p < numLongPreds; p++)
                    System.arraycopy((long[]) longPredArrs[p][c], 0, tmpLongPreds[p], 0, len);
                for (int p = 0; p < numDblPreds; p++)
                    System.arraycopy((double[]) dblPredArrs[p][c], 0, tmpDblPreds[p], 0, len);
            }

            // === Pass 1: Precompute row keys (pred eval + NULL→slot mapping + key calc) ===
            // Stores dense key per row, or -1 for filtered rows.
            // NULL group keys (Long.MIN_VALUE) are mapped to null slot values.
            int validCount = 0;
            final boolean hasNulls = nullSlots != null;
            for (int i = 0; i < len; i++) {
                if (hasPredicates && !evaluatePredicates(numLongPreds, longPredTypes, tmpLongPreds, longLo, longHi,
                        numDblPreds, dblPredTypes, tmpDblPreds, dblLo, dblHi, i)) {
                    rowKeys[i] = -1; continue;
                }
                // Map NULL group keys to null slot values
                long v0 = gc0[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[0] : -1) : gc0[i];
                if (v0 < 0) { rowKeys[i] = -1; continue; }
                int key = (int)((v0 - go0) * gm0);
                if (numGroupCols > 1) {
                    long v1 = gc1[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[1] : -1) : gc1[i];
                    if (v1 < 0) { rowKeys[i] = -1; continue; }
                    key += (int)((v1 - go1) * gm1);
                }
                if (numGroupCols > 2) {
                    long v2 = gc2[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[2] : -1) : gc2[i];
                    if (v2 < 0) { rowKeys[i] = -1; continue; }
                    key += (int)((v2 - go2) * gm2);
                }
                if (numGroupCols > 3) {
                    long v3 = gc3[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[3] : -1) : gc3[i];
                    if (v3 < 0) { rowKeys[i] = -1; continue; }
                    key += (int)((v3 - go3) * gm3);
                }
                if (numGroupCols > 4) {
                    long v4 = gc4[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[4] : -1) : gc4[i];
                    if (v4 < 0) { rowKeys[i] = -1; continue; }
                    key += (int)((v4 - go4) * gm4);
                }
                if (numGroupCols > 5) {
                    long v5 = gc5[i] == Long.MIN_VALUE ? (hasNulls ? nullSlots[5] : -1) : gc5[i];
                    if (v5 < 0) { rowKeys[i] = -1; continue; }
                    key += (int)((v5 - go5) * gm5);
                }
                boolean nullKey = false;
                for (int g = 6; g < numGroupCols; g++) {
                    long gv = ((long[]) groupColArrs[g][c])[i];
                    if (gv == Long.MIN_VALUE) {
                        if (hasNulls) gv = nullSlots[g]; else { nullKey = true; break; }
                    }
                    key += (int)((gv - groupOffsets[g]) * groupMuls[g]);
                }
                if (nullKey) { rowKeys[i] = -1; continue; }
                assert key >= 0 && key < maxKey : "Dense key out of range: " + key + " (maxKey=" + maxKey + ")";
                rowKeys[i] = key;
                validCount++;
            }

            if (validCount == 0) continue; // Skip chunk entirely

            // === Pass 2+: Per-agg-type accumulation using precomputed keys ===
            // Each inner loop has a single known agg type — no switch.
            // JIT compiles each loop body independently for perfect specialization.
            // Multiple passes over L2-resident 64KB chunks cost ~nothing.
            for (int a = 0; a < numAggs; a++) {
                int aggType = aggTypes[a];
                int aOff = a * 2;
                if (aggType == AGG_COUNT) {
                    for (int i = 0; i < len; i++) {
                        int k = rowKeys[i];
                        if (k >= 0) groups[k * accSize + aOff + 1]++;
                    }
                } else if (aggType == AGG_SUM) {
                    double[] col = tmpAggCols[a];
                    for (int i = 0; i < len; i++) {
                        int k = rowKeys[i];
                        if (k >= 0) { double sv = col[i]; if (!nanSafe || sv == sv) { int off = k * accSize + aOff; groups[off] += sv; groups[off + 1]++; } }
                    }
                } else if (aggType == AGG_SUM_PRODUCT) {
                    double[] col = tmpAggCols[a], col2 = tmpAggCol2s[a];
                    for (int i = 0; i < len; i++) {
                        int k = rowKeys[i];
                        if (k >= 0) { double sv = col[i], sv2 = col2[i]; if (!nanSafe || (sv == sv && sv2 == sv2)) { int off = k * accSize + aOff; groups[off] += sv * sv2; groups[off + 1]++; } }
                    }
                } else if (aggType == AGG_MIN) {
                    double[] col = tmpAggCols[a];
                    for (int i = 0; i < len; i++) {
                        int k = rowKeys[i];
                        if (k >= 0) { double sv = col[i]; if (!nanSafe || sv == sv) { int off = k * accSize + aOff; groups[off] = Math.min(groups[off], sv); groups[off + 1]++; } }
                    }
                } else if (aggType == AGG_MAX) {
                    double[] col = tmpAggCols[a];
                    for (int i = 0; i < len; i++) {
                        int k = rowKeys[i];
                        if (k >= 0) { double sv = col[i]; if (!nanSafe || sv == sv) { int off = k * accSize + aOff; groups[off] = Math.max(groups[off], sv); groups[off + 1]++; } }
                    }
                }
            }
        }
        return groups;
    }

    /**
     * Parallel chunked dense group-by: splits chunks across threads.
     * Each thread processes its chunks via fusedGroupAggregateDenseChunkedRange,
     * then results are merged with MIN/MAX correctness.
     */
    public static double[] fusedGroupAggregateDenseChunkedParallel(
            int numLongPreds, int[] longPredTypes,
            Object[][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            Object[][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numGroupCols, Object[][] groupColArrs, long[] groupMuls, long[] groupOffsets,
            int numAggs, int[] aggTypes, Object[][] aggColArrs, Object[][] aggCol2Arrs,
            boolean[] aggColIsLong, boolean[] aggCol2IsLong,
            int[] chunkLengths, int nChunks, int maxChunkLen, int maxKey,
            boolean nanSafe, long[] nullSlots) {

        final int accSize = numAggs * 2;

        // Compute total row count for PARALLEL_THRESHOLD check
        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        // L3-adaptive thread count (respects PARALLEL_THRESHOLD)
        int nThreads;
        if (totalRows < ColumnOps.PARALLEL_THRESHOLD) {
            nThreads = 1;
        } else {
            long perThreadMem = (long) maxKey * accSize * 8;
            nThreads = Math.min(POOL.getParallelism(), ColumnOps.effectiveGroupByThreads(perThreadMem));
            // At least 4 chunks per thread to amortize overhead
            nThreads = Math.min(nThreads, Math.max(1, nChunks / 4));
        }

        if (nThreads <= 1) {
            return fusedGroupAggregateDenseChunkedRange(
                numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                numGroupCols, groupColArrs, groupMuls, groupOffsets,
                numAggs, aggTypes, aggColArrs, aggCol2Arrs,
                aggColIsLong, aggCol2IsLong,
                chunkLengths, 0, nChunks, maxChunkLen, maxKey, nanSafe, nullSlots);
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedGroupAggregateDenseChunkedRange(
                    numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    numGroupCols, groupColArrs, groupMuls, groupOffsets,
                    numAggs, aggTypes, aggColArrs, aggCol2Arrs,
                    aggColIsLong, aggCol2IsLong,
                    chunkLengths, startChunk, endChunk, maxChunkLen, maxKey, nanSafe, nullSlots));
        }

        // Merge thread results
        final int flatLen = maxKey * accSize;
        boolean hasMinMax = false;
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN || aggTypes[a] == AGG_MAX) { hasMinMax = true; break; }
        }

        try {
            double[] merged = null;
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                if (merged == null) {
                    merged = partial;
                    continue;
                }
                if (!hasMinMax) {
                    for (int j = 0; j < flatLen; j++) merged[j] += partial[j];
                } else {
                    for (int k = 0; k < maxKey; k++) {
                        int base = k * accSize;
                        for (int a = 0; a < numAggs; a++) {
                            int off = base + a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                                    merged[off] += partial[off]; break;
                                case AGG_MIN:
                                    if (partial[off + 1] > 0)
                                        merged[off] = (merged[off + 1] > 0)
                                            ? Math.min(merged[off], partial[off]) : partial[off];
                                    break;
                                case AGG_MAX:
                                    if (partial[off + 1] > 0)
                                        merged[off] = (merged[off + 1] > 0)
                                            ? Math.max(merged[off], partial[off]) : partial[off];
                                    break;
                            }
                            merged[off + 1] += partial[off + 1];
                        }
                    }
                }
            }
            return merged != null ? merged : new double[flatLen];
        } catch (Exception e) {
            throw new RuntimeException("Chunked parallel group-by failed", e);
        }
    }

    // =========================================================================
    // Chunked Dense Group-By with Variable-Width Accumulators (VARIANCE/CORR)
    // =========================================================================

    /**
     * Per-thread chunked group-by with variable-width accumulators.
     * Combines the chunked pattern (System.arraycopy per chunk) with
     * variable-width accumulator layout (VARIANCE=3 slots, CORR=6 slots).
     */
    /**
     * Inner scatter loop for var-width chunked group-by.
     * Extracted as a separate method to avoid competing OSR entries —
     * C2 creates OSR at both the inner (scatter) and outer (chunk) loops
     * in a single large method, causing deoptimization cascades.
     * With the scatter loop in its own method, C2 has a single ~700-byte
     * method to compile with one clean OSR entry point.
     */
    private static void scatterVarChunk(
            double[] groups, int accSize, int[] aggOffsets, int numAggs, int[] aggTypes,
            long[] gc0, long[] gc1, long[] gc2, long[] gc3, long[] gc4, long[] gc5,
            long gm0, long gm1, long gm2, long gm3, long gm4, long gm5,
            long go0, long go1, long go2, long go3, long go4, long go5,
            int numGroupCols, Object[][] groupColArrs, int chunkIdx, long[] groupMuls, long[] groupOffsets,
            double[][] tmpAggCols, double[][] tmpAggCol2s,
            int len, boolean nanSafe, long[] nullSlots) {
        final boolean hasNulls = nullSlots != null;
        rowLoop:
        for (int i = 0; i < len; i++) {
            // Map NULL group keys to null slot values (or skip if no nullSlots)
            long v0 = gc0[i];
            if (v0 == Long.MIN_VALUE) { if (hasNulls) v0 = nullSlots[0]; else continue; }
            int key = (int)((v0 - go0) * gm0);
            if (numGroupCols > 1) {
                long v1 = gc1[i]; if (v1 == Long.MIN_VALUE) { if (hasNulls) v1 = nullSlots[1]; else continue; }
                key += (int)((v1 - go1) * gm1);
            }
            if (numGroupCols > 2) {
                long v2 = gc2[i]; if (v2 == Long.MIN_VALUE) { if (hasNulls) v2 = nullSlots[2]; else continue; }
                key += (int)((v2 - go2) * gm2);
            }
            if (numGroupCols > 3) {
                long v3 = gc3[i]; if (v3 == Long.MIN_VALUE) { if (hasNulls) v3 = nullSlots[3]; else continue; }
                key += (int)((v3 - go3) * gm3);
            }
            if (numGroupCols > 4) {
                long v4 = gc4[i]; if (v4 == Long.MIN_VALUE) { if (hasNulls) v4 = nullSlots[4]; else continue; }
                key += (int)((v4 - go4) * gm4);
            }
            if (numGroupCols > 5) {
                long v5 = gc5[i]; if (v5 == Long.MIN_VALUE) { if (hasNulls) v5 = nullSlots[5]; else continue; }
                key += (int)((v5 - go5) * gm5);
            }
            boolean nullKey = false;
            for (int g = 6; g < numGroupCols; g++) {
                long gv = ((long[]) groupColArrs[g][chunkIdx])[i];
                if (gv == Long.MIN_VALUE) {
                    if (hasNulls) gv = nullSlots[g]; else { nullKey = true; break; }
                }
                key += (int)((gv - groupOffsets[g]) * groupMuls[g]);
            }
            if (nullKey) continue;
            assert key >= 0 && key * accSize < groups.length : "Dense key out of range: " + key;

            int base = key * accSize;
            for (int a = 0; a < numAggs; a++) {
                int off = base + aggOffsets[a];
                switch (aggTypes[a]) {
                    case AGG_SUM: {
                        double sv = tmpAggCols[a][i];
                        if (!nanSafe || sv == sv) { groups[off] += sv; groups[off + 1]++; }
                        break;
                    }
                    case AGG_SUM_PRODUCT: {
                        double sv = tmpAggCols[a][i], sv2 = tmpAggCol2s[a][i];
                        if (!nanSafe || (sv == sv && sv2 == sv2)) { groups[off] += sv * sv2; groups[off + 1]++; }
                        break;
                    }
                    case AGG_COUNT:
                        groups[off + 1]++;
                        break;
                    case AGG_MIN: {
                        double sv = tmpAggCols[a][i];
                        if (!nanSafe || sv == sv) { groups[off] = Math.min(groups[off], sv); groups[off + 1]++; }
                        break;
                    }
                    case AGG_MAX: {
                        double sv = tmpAggCols[a][i];
                        if (!nanSafe || sv == sv) { groups[off] = Math.max(groups[off], sv); groups[off + 1]++; }
                        break;
                    }
                    case AGG_VARIANCE: {
                        double x = tmpAggCols[a][i];
                        if (!nanSafe || x == x) { groups[off] += x; groups[off + 1] += x * x; groups[off + 2]++; }
                        break;
                    }
                    case AGG_CORR: {
                        double cx = tmpAggCols[a][i];
                        double cy = tmpAggCol2s[a][i];
                        if (!nanSafe || (cx == cx && cy == cy)) {
                            groups[off]     += cx;
                            groups[off + 1] += cy;
                            groups[off + 2] += cx * cy;
                            groups[off + 3] += cx * cx;
                            groups[off + 4] += cy * cy;
                            groups[off + 5]++;
                        }
                        break;
                    }
                }
            }
        }
    }

    private static double[] fusedGroupAggregateDenseVarChunkedRange(
            int numLongPreds, int[] longPredTypes,
            Object[][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            Object[][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numGroupCols, Object[][] groupColArrs, long[] groupMuls, long[] groupOffsets,
            int numAggs, int[] aggTypes, Object[][] aggColArrs, Object[][] aggCol2Arrs,
            boolean[] aggColIsLong, boolean[] aggCol2IsLong,
            int[] chunkLengths, int startChunk, int endChunk, int maxChunkLen, int maxKey,
            boolean nanSafe, long[] nullSlots) {

        int accSize = 0;
        int[] aggOffsets = new int[numAggs];
        for (int a = 0; a < numAggs; a++) {
            aggOffsets[a] = accSize;
            accSize += aggSlots(aggTypes[a]);
        }

        double[] groups = new double[maxKey * accSize];
        // Init MIN to +Inf, MAX to -Inf
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                int off = aggOffsets[a];
                for (int k = 0; k < maxKey; k++) groups[k * accSize + off] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                int off = aggOffsets[a];
                for (int k = 0; k < maxKey; k++) groups[k * accSize + off] = Double.NEGATIVE_INFINITY;
            }
        }

        // Direct chunk refs (no System.arraycopy for heap arrays)
        double[][] tmpAggCols = new double[numAggs][];
        double[][] tmpAggCol2s = new double[numAggs][];
        double[][] longConvBufs = new double[numAggs][maxChunkLen];
        double[][] longConvBufs2 = new double[numAggs][maxChunkLen];
        long[][] tmpLongPreds = numLongPreds > 0 ? new long[numLongPreds][maxChunkLen] : null;
        double[][] tmpDblPreds = numDblPreds > 0 ? new double[numDblPreds][maxChunkLen] : null;

        final boolean hasPredicates = numLongPreds > 0 || numDblPreds > 0;
        // Constant group muls/offsets — extracted once before chunk loop
        final long gm0 = numGroupCols > 0 ? groupMuls[0] : 0;
        final long gm1 = numGroupCols > 1 ? groupMuls[1] : 0;
        final long gm2 = numGroupCols > 2 ? groupMuls[2] : 0;
        final long gm3 = numGroupCols > 3 ? groupMuls[3] : 0;
        final long gm4 = numGroupCols > 4 ? groupMuls[4] : 0;
        final long gm5 = numGroupCols > 5 ? groupMuls[5] : 0;
        final long go0 = numGroupCols > 0 ? groupOffsets[0] : 0;
        final long go1 = numGroupCols > 1 ? groupOffsets[1] : 0;
        final long go2 = numGroupCols > 2 ? groupOffsets[2] : 0;
        final long go3 = numGroupCols > 3 ? groupOffsets[3] : 0;
        final long go4 = numGroupCols > 4 ? groupOffsets[4] : 0;
        final long go5 = numGroupCols > 5 ? groupOffsets[5] : 0;

        for (int c = startChunk; c < endChunk; c++) {
            int len = chunkLengths[c];

            // Direct group col references (heap arrays, no copy)
            final long[] gc0 = numGroupCols > 0 ? (long[]) groupColArrs[0][c] : null;
            final long[] gc1 = numGroupCols > 1 ? (long[]) groupColArrs[1][c] : null;
            final long[] gc2 = numGroupCols > 2 ? (long[]) groupColArrs[2][c] : null;
            final long[] gc3 = numGroupCols > 3 ? (long[]) groupColArrs[3][c] : null;
            final long[] gc4 = numGroupCols > 4 ? (long[]) groupColArrs[4][c] : null;
            final long[] gc5 = numGroupCols > 5 ? (long[]) groupColArrs[5][c] : null;

            // Agg col refs: direct for double[], convert for long[]
            for (int a = 0; a < numAggs; a++) {
                if (aggTypes[a] == AGG_COUNT) { tmpAggCols[a] = longConvBufs[a]; continue; }
                if (aggColIsLong[a]) {
                    long[] src = (long[]) aggColArrs[a][c];
                    double[] buf = longConvBufs[a];
                    for (int i = 0; i < len; i++) buf[i] = (double) src[i];
                    tmpAggCols[a] = buf;
                } else {
                    tmpAggCols[a] = (double[]) aggColArrs[a][c];
                }
                if (aggTypes[a] == AGG_SUM_PRODUCT || aggTypes[a] == AGG_CORR) {
                    if (aggCol2IsLong[a]) {
                        long[] src2 = (long[]) aggCol2Arrs[a][c];
                        double[] buf2 = longConvBufs2[a];
                        for (int i = 0; i < len; i++) buf2[i] = (double) src2[i];
                        tmpAggCol2s[a] = buf2;
                    } else {
                        tmpAggCol2s[a] = (double[]) aggCol2Arrs[a][c];
                    }
                }
            }

            if (!hasPredicates) {
                // Fast path: delegate to extracted scatter loop (single OSR target)
                scatterVarChunk(groups, accSize, aggOffsets, numAggs, aggTypes,
                    gc0, gc1, gc2, gc3, gc4, gc5,
                    gm0, gm1, gm2, gm3, gm4, gm5,
                    go0, go1, go2, go3, go4, go5,
                    numGroupCols, groupColArrs, c, groupMuls, groupOffsets,
                    tmpAggCols, tmpAggCol2s, len, nanSafe, nullSlots);
            } else {
                // With predicates: copy pred arrays and inline scatter
                for (int p = 0; p < numLongPreds; p++)
                    System.arraycopy((long[]) longPredArrs[p][c], 0, tmpLongPreds[p], 0, len);
                for (int p = 0; p < numDblPreds; p++)
                    System.arraycopy((double[]) dblPredArrs[p][c], 0, tmpDblPreds[p], 0, len);

                final boolean hasNulls2 = nullSlots != null;
                varPredLoop:
                for (int i = 0; i < len; i++) {
                    if (!evaluatePredicates(numLongPreds, longPredTypes, tmpLongPreds, longLo, longHi,
                            numDblPreds, dblPredTypes, tmpDblPreds, dblLo, dblHi, i))
                        continue;

                    // Map NULL group keys to null slot values (or skip if no nullSlots)
                    long v0 = gc0[i];
                    if (v0 == Long.MIN_VALUE) { if (hasNulls2) v0 = nullSlots[0]; else continue; }
                    int key = (int)((v0 - go0) * gm0);
                    if (numGroupCols > 1) {
                        long v1 = gc1[i]; if (v1 == Long.MIN_VALUE) { if (hasNulls2) v1 = nullSlots[1]; else continue; }
                        key += (int)((v1 - go1) * gm1);
                    }
                    if (numGroupCols > 2) {
                        long v2 = gc2[i]; if (v2 == Long.MIN_VALUE) { if (hasNulls2) v2 = nullSlots[2]; else continue; }
                        key += (int)((v2 - go2) * gm2);
                    }
                    if (numGroupCols > 3) {
                        long v3 = gc3[i]; if (v3 == Long.MIN_VALUE) { if (hasNulls2) v3 = nullSlots[3]; else continue; }
                        key += (int)((v3 - go3) * gm3);
                    }
                    if (numGroupCols > 4) {
                        long v4 = gc4[i]; if (v4 == Long.MIN_VALUE) { if (hasNulls2) v4 = nullSlots[4]; else continue; }
                        key += (int)((v4 - go4) * gm4);
                    }
                    if (numGroupCols > 5) {
                        long v5 = gc5[i]; if (v5 == Long.MIN_VALUE) { if (hasNulls2) v5 = nullSlots[5]; else continue; }
                        key += (int)((v5 - go5) * gm5);
                    }
                    boolean nullKey = false;
                    for (int g = 6; g < numGroupCols; g++) {
                        long gv = ((long[]) groupColArrs[g][c])[i];
                        if (gv == Long.MIN_VALUE) {
                            if (hasNulls2) gv = nullSlots[g]; else { nullKey = true; break; }
                        }
                        key += (int)((gv - groupOffsets[g]) * groupMuls[g]);
                    }
                    if (nullKey) continue;
                    assert key >= 0 && key < maxKey : "Dense key out of range: " + key + " (maxKey=" + maxKey + ")";

                    int base = key * accSize;
                    for (int a = 0; a < numAggs; a++) {
                        int off = base + aggOffsets[a];
                        switch (aggTypes[a]) {
                            case AGG_SUM: {
                                double sv = tmpAggCols[a][i];
                                if (!nanSafe || sv == sv) { groups[off] += sv; groups[off + 1]++; }
                                break;
                            }
                            case AGG_SUM_PRODUCT: {
                                double sv = tmpAggCols[a][i], sv2 = tmpAggCol2s[a][i];
                                if (!nanSafe || (sv == sv && sv2 == sv2)) { groups[off] += sv * sv2; groups[off + 1]++; }
                                break;
                            }
                            case AGG_COUNT:
                                groups[off + 1]++;
                                break;
                            case AGG_MIN: {
                                double sv = tmpAggCols[a][i];
                                if (!nanSafe || sv == sv) { groups[off] = Math.min(groups[off], sv); groups[off + 1]++; }
                                break;
                            }
                            case AGG_MAX: {
                                double sv = tmpAggCols[a][i];
                                if (!nanSafe || sv == sv) { groups[off] = Math.max(groups[off], sv); groups[off + 1]++; }
                                break;
                            }
                            case AGG_VARIANCE: {
                                double x = tmpAggCols[a][i];
                                if (!nanSafe || x == x) { groups[off] += x; groups[off + 1] += x * x; groups[off + 2]++; }
                                break;
                            }
                            case AGG_CORR: {
                                double cx = tmpAggCols[a][i];
                                double cy = tmpAggCol2s[a][i];
                                if (!nanSafe || (cx == cx && cy == cy)) {
                                    groups[off]     += cx;
                                    groups[off + 1] += cy;
                                    groups[off + 2] += cx * cy;
                                    groups[off + 3] += cx * cx;
                                    groups[off + 4] += cy * cy;
                                    groups[off + 5]++;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        return groups;
    }

    /**
     * Parallel chunked dense group-by with variable-width accumulators.
     * Splits chunks across threads, each processes via VarChunkedRange,
     * then merges. All VARIANCE/CORR accumulator slots are additive;
     * MIN/MAX need count-based merge.
     */
    public static double[] fusedGroupAggregateDenseVarChunkedParallel(
            int numLongPreds, int[] longPredTypes,
            Object[][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            Object[][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numGroupCols, Object[][] groupColArrs, long[] groupMuls, long[] groupOffsets,
            int numAggs, int[] aggTypes, Object[][] aggColArrs, Object[][] aggCol2Arrs,
            boolean[] aggColIsLong, boolean[] aggCol2IsLong,
            int[] chunkLengths, int nChunks, int maxChunkLen, int maxKey,
            boolean nanSafe, long[] nullSlots) {

        int accSize = 0;
        int[] aggOffsets = new int[numAggs];
        for (int a = 0; a < numAggs; a++) {
            aggOffsets[a] = accSize;
            accSize += aggSlots(aggTypes[a]);
        }

        // Compute total row count for PARALLEL_THRESHOLD check
        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        // L3-adaptive thread count (respects PARALLEL_THRESHOLD)
        int nThreads;
        if (totalRows < ColumnOps.PARALLEL_THRESHOLD) {
            nThreads = 1;
        } else {
            long perThreadMem = (long) maxKey * accSize * 8;
            nThreads = Math.min(POOL.getParallelism(), ColumnOps.effectiveGroupByThreads(perThreadMem));
            nThreads = Math.min(nThreads, Math.max(1, nChunks / 4));
        }

        if (nThreads <= 1) {
            return fusedGroupAggregateDenseVarChunkedRange(
                numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                numGroupCols, groupColArrs, groupMuls, groupOffsets,
                numAggs, aggTypes, aggColArrs, aggCol2Arrs,
                aggColIsLong, aggCol2IsLong,
                chunkLengths, 0, nChunks, maxChunkLen, maxKey, nanSafe, nullSlots);
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedGroupAggregateDenseVarChunkedRange(
                    numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    numGroupCols, groupColArrs, groupMuls, groupOffsets,
                    numAggs, aggTypes, aggColArrs, aggCol2Arrs,
                    aggColIsLong, aggCol2IsLong,
                    chunkLengths, startChunk, endChunk, maxChunkLen, maxKey, nanSafe, nullSlots));
        }

        // Merge thread results
        final int flatLen = maxKey * accSize;
        boolean hasMinMax = false;
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN || aggTypes[a] == AGG_MAX) { hasMinMax = true; break; }
        }

        try {
            double[] merged = null;
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                if (merged == null) {
                    merged = partial;
                    continue;
                }
                if (!hasMinMax) {
                    for (int j = 0; j < flatLen; j++) merged[j] += partial[j];
                } else {
                    for (int k = 0; k < maxKey; k++) {
                        int base = k * accSize;
                        for (int a = 0; a < numAggs; a++) {
                            int off = base + aggOffsets[a];
                            int slots = aggSlots(aggTypes[a]);
                            switch (aggTypes[a]) {
                                case AGG_MIN:
                                    if (partial[off + 1] > 0)
                                        merged[off] = (merged[off + 1] > 0)
                                            ? Math.min(merged[off], partial[off]) : partial[off];
                                    merged[off + 1] += partial[off + 1];
                                    break;
                                case AGG_MAX:
                                    if (partial[off + 1] > 0)
                                        merged[off] = (merged[off + 1] > 0)
                                            ? Math.max(merged[off], partial[off]) : partial[off];
                                    merged[off + 1] += partial[off + 1];
                                    break;
                                default:
                                    // SUM, COUNT, SUM_PRODUCT, VARIANCE, CORR — all additive
                                    for (int s = 0; s < slots; s++) merged[off + s] += partial[off + s];
                                    break;
                            }
                        }
                    }
                }
            }
            return merged != null ? merged : new double[flatLen];
        } catch (Exception e) {
            throw new RuntimeException("Chunked var parallel group-by failed", e);
        }
    }
}
