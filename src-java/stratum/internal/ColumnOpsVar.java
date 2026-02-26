package stratum.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * VARIANCE/CORR dense group-by — JIT-isolated from ColumnOpsExt.
 *
 * Moved here to avoid class-level JIT interference: ColumnOpsExt (64KB)
 * contains SIMD multi-sum, join, and COUNT DISTINCT methods that get
 * heavily warmed by earlier benchmark tiers. When VARIANCE/CORR methods
 * shared that compilation unit, C2 JIT budget exhaustion caused 2–6x
 * regressions on the array path (H2O-Q6 STDDEV: 28ms→70ms,
 * H2O-Q9 CORR: 53ms→325ms). This small class (~8KB) gives the JIT
 * a clean compilation unit.
 */
public final class ColumnOpsVar {

    private static ForkJoinPool POOL = ColumnOps.POOL;
    private static int MORSEL_SIZE = ColumnOps.MORSEL_SIZE;

    // Agg type constants (same values as ColumnOps/ColumnOpsExt)
    public static final int AGG_SUM = ColumnOps.AGG_SUM;
    public static final int AGG_COUNT = ColumnOps.AGG_COUNT;
    public static final int AGG_SUM_PRODUCT = ColumnOps.AGG_SUM_PRODUCT;
    public static final int AGG_MIN = ColumnOps.AGG_MIN;
    public static final int AGG_MAX = ColumnOps.AGG_MAX;
    public static final int AGG_VARIANCE = 7;  // 3 slots: [sum, sum_sq, count]
    public static final int AGG_CORR = 8;      // 6 slots: [sumx, sumy, sumxy, sumxx, sumyy, count]

    // Predicate type constants
    private static final int PRED_RANGE = ColumnOps.PRED_RANGE;
    private static final int PRED_LT = ColumnOps.PRED_LT;
    private static final int PRED_GT = ColumnOps.PRED_GT;
    private static final int PRED_EQ = ColumnOps.PRED_EQ;
    private static final int PRED_LTE = ColumnOps.PRED_LTE;
    private static final int PRED_GTE = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    // =========================================================================
    // Helpers
    // =========================================================================

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
     * to inline at C2 tier 3, causing 6M non-inlined calls per invocation.
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

    /**
     * Merge variable-width accumulators with agg-type-aware logic.
     * MIN/MAX use Math.min/Math.max with count-based initialization;
     * all other agg types (SUM, COUNT, VARIANCE, CORR) are additive.
     */
    private static void mergeVarAccumulators(double[] dst, double[] src,
            int numAggs, int[] aggTypes, int[] aggOffsets) {
        for (int a = 0; a < numAggs; a++) {
            int off = aggOffsets[a];
            switch (aggTypes[a]) {
                case AGG_MIN:
                    if (src[off + 1] > 0)
                        dst[off] = (dst[off + 1] > 0)
                            ? Math.min(dst[off], src[off]) : src[off];
                    dst[off + 1] += src[off + 1];
                    break;
                case AGG_MAX:
                    if (src[off + 1] > 0)
                        dst[off] = (dst[off + 1] > 0)
                            ? Math.max(dst[off], src[off]) : src[off];
                    dst[off + 1] += src[off + 1];
                    break;
                default: {
                    int slots = aggSlots(aggTypes[a]);
                    for (int s = 0; s < slots; s++) dst[off + s] += src[off + s];
                    break;
                }
            }
        }
    }

    // =========================================================================
    // Dense Group-By with Variable-Width Accumulators
    // =========================================================================

    public static double[][] fusedFilterGroupAggregateDenseVar(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCols2,
            int length, int maxKey, boolean nanSafe) {
        return fusedFilterGroupAggregateDenseVarRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                numAggs, aggTypes, aggCols, aggCols2, 0, length, maxKey, nanSafe);
    }

    private static double[][] fusedFilterGroupAggregateDenseVarRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCols2,
            int start, int end, int maxKey, boolean nanSafe) {

        int accSize = 0;
        int[] aggOffsets = new int[numAggs];
        for (int a = 0; a < numAggs; a++) {
            aggOffsets[a] = accSize;
            accSize += aggSlots(aggTypes[a]);
        }

        double[][] groups = new double[maxKey][];
        for (int k = 0; k < maxKey; k++) {
            groups[k] = new double[accSize];
            for (int a = 0; a < numAggs; a++) {
                if (aggTypes[a] == AGG_MIN) groups[k][aggOffsets[a]] = Double.POSITIVE_INFINITY;
                else if (aggTypes[a] == AGG_MAX) groups[k][aggOffsets[a]] = Double.NEGATIVE_INFINITY;
            }
        }

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

        for (int i = start; i < end; i++) {
            int m = evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                       numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i) ? 1 : 0;

            int key = (int)(gc0[i] * gm0);
            if (numGroupCols > 1) key += (int)(gc1[i] * gm1);
            if (numGroupCols > 2) key += (int)(gc2[i] * gm2);
            if (numGroupCols > 3) key += (int)(gc3[i] * gm3);
            if (numGroupCols > 4) key += (int)(gc4[i] * gm4);
            if (numGroupCols > 5) key += (int)(gc5[i] * gm5);
            for (int g = 6; g < numGroupCols; g++) {
                key += (int)(groupCols[g][i] * groupMuls[g]);
            }
            assert key >= 0 && key < maxKey : "Dense key out of range: " + key + " (maxKey=" + maxKey + ")";

            double[] accs = groups[key];
            for (int a = 0; a < numAggs; a++) {
                int off = aggOffsets[a];
                switch (aggTypes[a]) {
                    case AGG_SUM: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            accs[off] += sv * m;
                            accs[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_SUM_PRODUCT: {
                        double sv = aggCols[a][i];
                        double sv2 = aggCols2[a][i];
                        if (!nanSafe || (sv == sv && sv2 == sv2)) {
                            accs[off] += sv * sv2 * m;
                            accs[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_COUNT:
                        accs[off + 1] += m;
                        break;
                    case AGG_MIN: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            if (m != 0) accs[off] = Math.min(accs[off], sv);
                            accs[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_MAX: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            if (m != 0) accs[off] = Math.max(accs[off], sv);
                            accs[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_VARIANCE: {
                        double x = aggCols[a][i];
                        if (!nanSafe || x == x) {
                            accs[off]     += x * m;             // sum
                            accs[off + 1] += x * x * m;         // sum_sq
                            accs[off + 2] += m;                  // count
                        }
                        break;
                    }
                    case AGG_CORR: {
                        double x = aggCols[a][i];
                        double y = aggCols2[a][i];
                        if (!nanSafe || (x == x && y == y)) {
                            accs[off]     += x * m;         // sumx
                            accs[off + 1] += y * m;         // sumy
                            accs[off + 2] += x * y * m;     // sumxy
                            accs[off + 3] += x * x * m;     // sumxx
                            accs[off + 4] += y * y * m;     // sumyy
                            accs[off + 5] += m;             // count
                        }
                        break;
                    }
                }
            }
        }

        // Null out unused groups
        int firstCountSlot = -1;
        for (int a = 0; a < numAggs; a++) {
            int off = aggOffsets[a];
            int slots = aggSlots(aggTypes[a]);
            firstCountSlot = off + slots - 1;
            break;
        }
        if (firstCountSlot >= 0) {
            for (int k = 0; k < maxKey; k++) {
                if (groups[k][firstCountSlot] == 0.0) {
                    groups[k] = null;
                }
            }
        }

        return groups;
    }

    @SuppressWarnings("unchecked")
    public static double[][] fusedFilterGroupAggregateDenseVarParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCols2,
            int length, int maxKey, boolean nanSafe) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedFilterGroupAggregateDenseVar(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCols2, length, maxKey, nanSafe);
        }

        int accSize = 0;
        int[] aggOffsets = new int[numAggs];
        for (int a = 0; a < numAggs; a++) {
            aggOffsets[a] = accSize;
            accSize += aggSlots(aggTypes[a]);
        }

        long perThreadMem = (long) maxKey * accSize * 8;
        int nThreads = Math.min(POOL.getParallelism(), ColumnOps.effectiveGroupByThreads(perThreadMem));
        if (nThreads <= 1) {
            return fusedFilterGroupAggregateDenseVar(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCols2, length, maxKey, nanSafe);
        }
        int threadRange = (length + nThreads - 1) / nThreads;
        final boolean useMorsels = perThreadMem < 65536;
        final int fAccSize = accSize;

        Future<double[][]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                if (!useMorsels) {
                    return fusedFilterGroupAggregateDenseVarRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            numAggs, aggTypes, aggCols, aggCols2, threadStart, threadEnd, maxKey, nanSafe);
                }
                int firstEnd = Math.min(threadStart + MORSEL_SIZE, threadEnd);
                double[][] accs = fusedFilterGroupAggregateDenseVarRange(
                        numLongPreds, longPredTypes, longCols, longLo, longHi,
                        numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                        numGroupCols, groupCols, groupMuls,
                        numAggs, aggTypes, aggCols, aggCols2, threadStart, firstEnd, maxKey, nanSafe);
                for (int ms = threadStart + MORSEL_SIZE; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[][] partial = fusedFilterGroupAggregateDenseVarRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            numAggs, aggTypes, aggCols, aggCols2, ms, me, maxKey, nanSafe);
                    for (int k = 0; k < maxKey; k++) {
                        if (partial[k] == null) continue;
                        double[] pg = partial[k];
                        double[] ag = accs[k];
                        if (ag == null) { accs[k] = pg; continue; }
                        mergeVarAccumulators(ag, pg, numAggs, aggTypes, aggOffsets);
                    }
                }
                return accs;
            });
        }

        double[][] merged = new double[maxKey][];
        for (int k = 0; k < maxKey; k++) {
            merged[k] = new double[fAccSize];
            for (int a = 0; a < numAggs; a++) {
                if (aggTypes[a] == AGG_MIN) merged[k][aggOffsets[a]] = Double.POSITIVE_INFINITY;
                else if (aggTypes[a] == AGG_MAX) merged[k][aggOffsets[a]] = Double.NEGATIVE_INFINITY;
            }
        }

        try {
            for (Future<double[][]> f : futures) {
                if (f == null) continue;
                double[][] partial = f.get();
                for (int k = 0; k < maxKey; k++) {
                    if (partial[k] == null) continue;
                    double[] pg = partial[k];
                    double[] mg = merged[k];
                    mergeVarAccumulators(mg, pg, numAggs, aggTypes, aggOffsets);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
        return merged;
    }
}
