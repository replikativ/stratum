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
 * Long[] array operations for the native long pipeline.
 *
 * <p>JIT-isolated from ColumnOps/ColumnOpsExt. Contains:
 * <ul>
 *   <li>NULL-aware element-wise long[] arithmetic (add, sub, mul, mod)</li>
 *   <li>Scalar variants of the above</li>
 *   <li>Unary ops (abs, sign) that preserve integer type</li>
 *   <li>Broadcast</li>
 *   <li>Mixed-predicate all-long multi-sum SIMD</li>
 *   <li>Dense group-by with long[] accumulators</li>
 * </ul>
 *
 * <p>No division — integer division has truncation semantics; always use double[].
 * <p>No sqrt/log/exp/pow — inherently produce fractional results; stay double[].
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class ColumnOpsLong {

    private ColumnOpsLong() {}

    // Re-use ColumnOps pool and thresholds
    private static ForkJoinPool POOL = ColumnOps.POOL;
    private static int MORSEL_SIZE = ColumnOps.MORSEL_SIZE;

    // SIMD species
    private static final VectorSpecies<Long> LONG_SPECIES = ColumnOps.LONG_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = ColumnOps.DOUBLE_SPECIES;
    private static final int LONG_LANES = ColumnOps.LONG_LANES;
    private static final int DOUBLE_LANES = (int) DOUBLE_SPECIES.length();

    // Pred type constants (same as ColumnOps)
    private static final int PRED_RANGE = ColumnOps.PRED_RANGE;
    private static final int PRED_LT    = ColumnOps.PRED_LT;
    private static final int PRED_GT    = ColumnOps.PRED_GT;
    private static final int PRED_EQ    = ColumnOps.PRED_EQ;
    private static final int PRED_LTE   = ColumnOps.PRED_LTE;
    private static final int PRED_GTE   = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ   = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    // Agg type constants
    public static final int AGG_SUM         = ColumnOps.AGG_SUM;
    public static final int AGG_COUNT       = ColumnOps.AGG_COUNT;
    public static final int AGG_SUM_PRODUCT = ColumnOps.AGG_SUM_PRODUCT;
    public static final int AGG_MIN         = ColumnOps.AGG_MIN;
    public static final int AGG_MAX         = ColumnOps.AGG_MAX;

    private static final long NULL = Long.MIN_VALUE;

    // =========================================================================
    // Element-wise Binary Array Operations (NULL-aware)
    // =========================================================================

    /** Element-wise multiply: result[i] = a[i] * b[i], NULL-safe. */
    public static long[] arrayMulLong(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            r[i] = (av == NULL || bv == NULL) ? NULL : av * bv;
        }
        return r;
    }

    /** Element-wise add: result[i] = a[i] + b[i], NULL-safe. */
    public static long[] arrayAddLong(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            r[i] = (av == NULL || bv == NULL) ? NULL : av + bv;
        }
        return r;
    }

    /** Element-wise subtract: result[i] = a[i] - b[i], NULL-safe. */
    public static long[] arraySubLong(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            r[i] = (av == NULL || bv == NULL) ? NULL : av - bv;
        }
        return r;
    }

    /** Element-wise modulo: result[i] = a[i] % b[i], NULL-safe. */
    public static long[] arrayModLong(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            r[i] = (av == NULL || bv == NULL || bv == 0) ? NULL : av % bv;
        }
        return r;
    }

    // =========================================================================
    // Scalar-Array Binary Operations (NULL-aware)
    // =========================================================================

    /** Scalar-array multiply: result[i] = scalar * b[i], NULL-safe. */
    public static long[] arrayMulLongScalar(long scalar, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = (b[i] == NULL) ? NULL : scalar * b[i];
        }
        return r;
    }

    /** Scalar-array add: result[i] = scalar + b[i], NULL-safe. */
    public static long[] arrayAddLongScalar(long scalar, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = (b[i] == NULL) ? NULL : scalar + b[i];
        }
        return r;
    }

    /** Scalar-array subtract: result[i] = scalar - b[i], NULL-safe. */
    public static long[] arraySubLongScalar(long scalar, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = (b[i] == NULL) ? NULL : scalar - b[i];
        }
        return r;
    }

    /** Array-scalar modulo: result[i] = a[i] % scalar, NULL-safe. */
    public static long[] arrayModLongScalar(long[] a, long scalar, int length) {
        long[] r = new long[length];
        if (scalar == 0) { Arrays.fill(r, NULL); return r; }
        for (int i = 0; i < length; i++) {
            r[i] = (a[i] == NULL) ? NULL : a[i] % scalar;
        }
        return r;
    }

    // =========================================================================
    // Unary Operations (integer-preserving, NULL-aware)
    // =========================================================================

    /** Element-wise absolute value, NULL-safe. */
    public static long[] arrayAbsLong(long[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = (a[i] == NULL) ? NULL : Math.abs(a[i]);
        }
        return r;
    }

    /** Element-wise signum (-1, 0, 1), NULL-safe. */
    public static long[] arraySignLong(long[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = (a[i] == NULL) ? NULL : Long.signum(a[i]);
        }
        return r;
    }

    // =========================================================================
    // Broadcast
    // =========================================================================

    /** Fill array with scalar value. */
    public static long[] arrayBroadcastLong(long scalar, int length) {
        long[] r = new long[length];
        Arrays.fill(r, scalar);
        return r;
    }

    // =========================================================================
    // Overflow-Checked Multiply (for SUM_PRODUCT pre-multiplication)
    // =========================================================================

    /**
     * Element-wise multiply with overflow detection.
     * Returns null if any product overflows (Math.multiplyHigh != 0 and != -1).
     * NULL-aware: Long.MIN_VALUE propagated.
     */
    public static long[] arrayMulLongChecked(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            if (av == NULL || bv == NULL) { r[i] = NULL; continue; }
            long hi = Math.multiplyHigh(av, bv);
            long lo = av * bv;
            // hi==0 for positive results, hi==-1 for small negative results
            if (hi != 0 && !(hi == -1 && lo < 0)) return null;
            r[i] = lo;
        }
        return r;
    }

    // =========================================================================
    // Mixed-Predicate All-Long Multi-Sum SIMD
    // =========================================================================

    /**
     * All-long multi-sum with both long AND double predicates.
     * Agg columns are all long[] — LongVector accumulators, no longToDouble.
     * Predicates can be mixed (long[] for integer preds, double[] for float preds).
     */
    private static double[] fusedSimdMultiSumAllLongMixedPredsRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
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

        // Double pred broadcasts
        final double[] dc0 = numDblPreds > 0 ? dblCols[0] : null;
        final double[] dc1 = numDblPreds > 1 ? dblCols[1] : null;
        final double[] dc2 = numDblPreds > 2 ? dblCols[2] : null;
        final double[] dc3 = numDblPreds > 3 ? dblCols[3] : null;

        final int dt0 = numDblPreds > 0 ? dblPredTypes[0] : -1;
        final int dt1 = numDblPreds > 1 ? dblPredTypes[1] : -1;
        final int dt2 = numDblPreds > 2 ? dblPredTypes[2] : -1;
        final int dt3 = numDblPreds > 3 ? dblPredTypes[3] : -1;

        final DoubleVector dlo0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[0]) : null;
        final DoubleVector dhi0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[0]) : null;
        final DoubleVector dlo1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[1]) : null;
        final DoubleVector dhi1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[1]) : null;
        final DoubleVector dlo2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[2]) : null;
        final DoubleVector dhi2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[2]) : null;
        final DoubleVector dlo3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[3]) : null;
        final DoubleVector dhi3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[3]) : null;

        // Agg columns
        final long[] sl0 = numSumAggs > 0 ? sumLongCols[0] : null;
        final long[] sl1 = numSumAggs > 1 ? sumLongCols[1] : null;
        final long[] sl2 = numSumAggs > 2 ? sumLongCols[2] : null;
        final long[] sl3 = numSumAggs > 3 ? sumLongCols[3] : null;

        // LongVector accumulators
        LongVector sv0 = LongVector.zero(LONG_SPECIES);
        LongVector sv1 = LongVector.zero(LONG_SPECIES);
        LongVector sv2 = LongVector.zero(LONG_SPECIES);
        LongVector sv3 = LongVector.zero(LONG_SPECIES);
        LongVector nullSentinel = LongVector.broadcast(LONG_SPECIES, NULL);

        for (int i = start; i < upperBound; i += LONG_LANES) {
            // Long predicates
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            // Double predicates — convert long mask to double mask via toLong bitmask
            if (numDblPreds > 0) {
                VectorMask<Double> dm = DOUBLE_SPECIES.maskAll(true);
                // Transfer long mask to double: only process if long preds passed
                // Both species have same lane count, so we can use the same bit pattern
                long bits = lm.toLong();
                dm = VectorMask.fromLong(DOUBLE_SPECIES, bits);
                if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                // Transfer back to long mask for accumulation
                lm = VectorMask.fromLong(LONG_SPECIES, dm.toLong());
            }

            // Accumulate with NULL check
            VectorMask<Long> anyNonNull = LONG_SPECIES.maskAll(false);
            if (numSumAggs > 0) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl0, i); VectorMask<Long> nn = lm.and(v.compare(VectorOperators.NE, nullSentinel)); sv0 = sv0.add(v, nn); anyNonNull = anyNonNull.or(nn); }
            if (numSumAggs > 1) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl1, i); VectorMask<Long> nn = lm.and(v.compare(VectorOperators.NE, nullSentinel)); sv1 = sv1.add(v, nn); anyNonNull = anyNonNull.or(nn); }
            if (numSumAggs > 2) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl2, i); VectorMask<Long> nn = lm.and(v.compare(VectorOperators.NE, nullSentinel)); sv2 = sv2.add(v, nn); anyNonNull = anyNonNull.or(nn); }
            if (numSumAggs > 3) { LongVector v = LongVector.fromArray(LONG_SPECIES, sl3, i); VectorMask<Long> nn = lm.and(v.compare(VectorOperators.NE, nullSentinel)); sv3 = sv3.add(v, nn); anyNonNull = anyNonNull.or(nn); }
            matchCount += anyNonNull.trueCount();
        }

        double[] sums = new double[numSumAggs + 1];
        if (numSumAggs > 0) sums[0] = (double) sv0.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 1) sums[1] = (double) sv1.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 2) sums[2] = (double) sv2.reduceLanes(VectorOperators.ADD);
        if (numSumAggs > 3) sums[3] = (double) sv3.reduceLanes(VectorOperators.ADD);

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            if (ColumnOps.evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                             numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                boolean anyNonNullScalar = false;
                if (numSumAggs > 0 && sl0[i] != NULL) { sums[0] += sl0[i]; anyNonNullScalar = true; }
                if (numSumAggs > 1 && sl1[i] != NULL) { sums[1] += sl1[i]; anyNonNullScalar = true; }
                if (numSumAggs > 2 && sl2[i] != NULL) { sums[2] += sl2[i]; anyNonNullScalar = true; }
                if (numSumAggs > 3 && sl3[i] != NULL) { sums[3] += sl3[i]; anyNonNullScalar = true; }
                if (anyNonNullScalar) matchCount++;
            }
        }
        sums[numSumAggs] = (double) matchCount;
        return sums;
    }

    /** Delegates to range variant. */
    public static double[] fusedSimdMultiSumAllLongMixedPreds(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, long[][] sumLongCols,
            int length) {
        return fusedSimdMultiSumAllLongMixedPredsRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numSumAggs, sumLongCols, 0, length);
    }

    /** Parallel morsel-driven all-long multi-sum with mixed predicates. */
    public static double[] fusedSimdMultiSumAllLongMixedPredsParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, long[][] sumLongCols,
            int length) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedSimdMultiSumAllLongMixedPreds(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numSumAggs, sumLongCols, length);
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
                    double[] morselResult = fusedSimdMultiSumAllLongMixedPredsRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numSumAggs, sumLongCols, ms, me);
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

    // =========================================================================
    // Dense Group-By with long[] Accumulators
    // =========================================================================

    /**
     * Dense group-by with long[] accumulators. Requires ALL agg columns to be long[].
     * Supports SUM, COUNT, MIN, MAX (no AVG — needs division).
     * Returns flat long[maxKey * accSize] where accSize = 2 * numAggs (value + count).
     */
    private static long[] fusedFilterGroupAggregateDenseLongRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, long[][] aggCols,
            int start, int end, int maxKey) {

        int accSize = numAggs * 2; // value + count per agg
        long[] accs = new long[maxKey * accSize];

        // Initialize MIN to MAX_VALUE, MAX to MIN_VALUE+1 (not MIN_VALUE which is NULL)
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) accs[k * accSize + a * 2] = Long.MAX_VALUE;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) accs[k * accSize + a * 2] = Long.MIN_VALUE + 1;
            }
        }

        for (int i = start; i < end; i++) {
            if (!ColumnOps.evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                              numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) continue;

            // Compute group key
            long key = 0;
            for (int g = 0; g < numGroupCols; g++) {
                key += groupCols[g][i] * groupMuls[g];
            }
            if (key < 0 || key >= maxKey) continue;
            int base = (int) key * accSize;

            // Accumulate
            for (int a = 0; a < numAggs; a++) {
                int off = base + a * 2;
                if (aggTypes[a] == AGG_COUNT) {
                    accs[off + 1]++;
                    continue;
                }
                long val = aggCols[a][i];
                if (val == NULL) continue;
                switch (aggTypes[a]) {
                    case AGG_SUM:
                        accs[off] += val;
                        accs[off + 1]++;
                        break;
                    case AGG_MIN:
                        if (val < accs[off]) accs[off] = val;
                        accs[off + 1]++;
                        break;
                    case AGG_MAX:
                        if (val > accs[off]) accs[off] = val;
                        accs[off + 1]++;
                        break;
                }
            }
        }
        return accs;
    }

    /** Delegates to range variant. */
    public static long[] fusedFilterGroupAggregateDenseLong(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, long[][] aggCols,
            int length, int maxKey) {
        return fusedFilterGroupAggregateDenseLongRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                numAggs, aggTypes, aggCols, 0, length, maxKey);
    }

    /** Parallel morsel-driven dense group-by with long[] accumulators. */
    public static long[] fusedFilterGroupAggregateDenseLongParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, long[][] aggCols,
            int length, int maxKey) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedFilterGroupAggregateDenseLong(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, length, maxKey);
        }

        int accSize = numAggs * 2;
        long perThreadMem = (long) maxKey * accSize * 8;
        boolean useMorsels = perThreadMem < 65536;

        int nThreads = ColumnOps.effectiveScanThreads();
        // L3-adaptive thread capping
        if (!useMorsels) {
            long l3 = ColumnOps.L3_BUDGET;
            int maxEffective = Math.max(2, (int)(l3 / perThreadMem));
            if (maxEffective < nThreads) nThreads = maxEffective;
        }

        int chunkSize = (length + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<long[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int tStart = t * chunkSize;
            final int tEnd = Math.min(tStart + chunkSize, length);
            if (tStart >= length) break;
            futures[t] = POOL.submit(() -> {
                if (useMorsels) {
                    long[] threadAccs = new long[maxKey * accSize];
                    // Initialize MIN/MAX
                    for (int a = 0; a < numAggs; a++) {
                        if (aggTypes[a] == AGG_MIN) {
                            for (int k = 0; k < maxKey; k++) threadAccs[k * accSize + a * 2] = Long.MAX_VALUE;
                        } else if (aggTypes[a] == AGG_MAX) {
                            for (int k = 0; k < maxKey; k++) threadAccs[k * accSize + a * 2] = Long.MIN_VALUE + 1;
                        }
                    }
                    int ms = tStart;
                    while (ms < tEnd) {
                        int me = Math.min(ms + MORSEL_SIZE, tEnd);
                        long[] morsel = fusedFilterGroupAggregateDenseLongRange(
                                numLongPreds, longPredTypes, longCols, longLo, longHi,
                                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                                numGroupCols, groupCols, groupMuls,
                                numAggs, aggTypes, aggCols, ms, me, maxKey);
                        // Merge morsel into thread accumulators
                        mergeLongAccs(threadAccs, morsel, maxKey, accSize, numAggs, aggTypes);
                        ms = me;
                    }
                    return threadAccs;
                } else {
                    return fusedFilterGroupAggregateDenseLongRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            numAggs, aggTypes, aggCols, tStart, tEnd, maxKey);
                }
            });
        }

        // Merge all thread results
        long[] total = null;
        for (int t = 0; t < nThreads; t++) {
            if (futures[t] == null) break;
            try {
                long[] partial = futures[t].get();
                if (total == null) {
                    total = partial;
                } else {
                    mergeLongAccs(total, partial, maxKey, accSize, numAggs, aggTypes);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return total;
    }

    /** Merge src long[] accumulators into dst. */
    private static void mergeLongAccs(long[] dst, long[] src, int maxKey, int accSize,
                                       int numAggs, int[] aggTypes) {
        for (int k = 0; k < maxKey; k++) {
            int base = k * accSize;
            for (int a = 0; a < numAggs; a++) {
                int off = base + a * 2;
                long srcCount = src[off + 1];
                if (srcCount == 0) continue;
                switch (aggTypes[a]) {
                    case AGG_SUM:
                        dst[off] += src[off];
                        dst[off + 1] += srcCount;
                        break;
                    case AGG_COUNT:
                        dst[off + 1] += srcCount;
                        break;
                    case AGG_MIN:
                        if (src[off] < dst[off]) dst[off] = src[off];
                        dst[off + 1] += srcCount;
                        break;
                    case AGG_MAX:
                        if (src[off] > dst[off]) dst[off] = src[off];
                        dst[off + 1] += srcCount;
                        break;
                }
            }
        }
    }

    // =========================================================================
    // Single-Agg Long SIMD (SUM/MIN/MAX on long[] column)
    // =========================================================================

    /**
     * Single-agg fused filter+aggregate on a long[] column using LongVector.
     * Supports SUM, MIN, MAX (not COUNT — use fusedSimdCountParallel).
     * Returns double[2] = {result, count} for compatibility with existing API.
     */
    private static double[] fusedSimdLongRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, long[] aggCol,
            int start, int end) {

        int rangeLen = end - start;
        long matchCount = 0;
        int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

        // Extract pred columns as final locals
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

        // Double pred broadcasts
        final double[] dc0 = numDblPreds > 0 ? dblCols[0] : null;
        final double[] dc1 = numDblPreds > 1 ? dblCols[1] : null;
        final double[] dc2 = numDblPreds > 2 ? dblCols[2] : null;
        final double[] dc3 = numDblPreds > 3 ? dblCols[3] : null;

        final int dt0 = numDblPreds > 0 ? dblPredTypes[0] : -1;
        final int dt1 = numDblPreds > 1 ? dblPredTypes[1] : -1;
        final int dt2 = numDblPreds > 2 ? dblPredTypes[2] : -1;
        final int dt3 = numDblPreds > 3 ? dblPredTypes[3] : -1;

        final DoubleVector dlo0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[0]) : null;
        final DoubleVector dhi0 = numDblPreds > 0 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[0]) : null;
        final DoubleVector dlo1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[1]) : null;
        final DoubleVector dhi1 = numDblPreds > 1 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[1]) : null;
        final DoubleVector dlo2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[2]) : null;
        final DoubleVector dhi2 = numDblPreds > 2 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[2]) : null;
        final DoubleVector dlo3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblLo[3]) : null;
        final DoubleVector dhi3 = numDblPreds > 3 ? DoubleVector.broadcast(DOUBLE_SPECIES, dblHi[3]) : null;

        LongVector nullSentinel = LongVector.broadcast(LONG_SPECIES, NULL);
        LongVector acc;
        if (aggType == AGG_MIN) acc = LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE);
        else if (aggType == AGG_MAX) acc = LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE + 1);
        else acc = LongVector.zero(LONG_SPECIES);

        for (int i = start; i < upperBound; i += LONG_LANES) {
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            if (numDblPreds > 0) {
                VectorMask<Double> dm = VectorMask.fromLong(DOUBLE_SPECIES, lm.toLong());
                if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                lm = VectorMask.fromLong(LONG_SPECIES, dm.toLong());
            }

            LongVector v = LongVector.fromArray(LONG_SPECIES, aggCol, i);
            VectorMask<Long> nn = lm.and(v.compare(VectorOperators.NE, nullSentinel));
            if (!nn.anyTrue()) continue;
            matchCount += nn.trueCount();

            switch (aggType) {
                case AGG_SUM: acc = acc.add(v, nn); break;
                case AGG_MIN: acc = acc.blend(acc.min(v), nn); break;
                case AGG_MAX: acc = acc.blend(acc.max(v), nn); break;
            }
        }

        // Reduce SIMD accumulator
        double result;
        switch (aggType) {
            case AGG_SUM: result = (double) acc.reduceLanes(VectorOperators.ADD); break;
            case AGG_MIN: result = (double) acc.reduceLanes(VectorOperators.MIN); break;
            case AGG_MAX: result = (double) acc.reduceLanes(VectorOperators.MAX); break;
            default: result = 0.0;
        }

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            if (ColumnOps.evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                             numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                long val = aggCol[i];
                if (val == NULL) continue;
                matchCount++;
                switch (aggType) {
                    case AGG_SUM: result += val; break;
                    case AGG_MIN: if (val < result) result = val; break;
                    case AGG_MAX: if (val > result) result = val; break;
                }
            }
        }
        return new double[] { result, (double) matchCount };
    }

    /** Parallel morsel-driven single-agg long SIMD. */
    public static double[] fusedSimdLongParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, long[] aggCol,
            int length) {

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedSimdLongRange(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    aggType, aggCol, 0, length);
        }

        int nThreads = ColumnOps.effectiveScanThreads();
        int chunkSize = (length + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int tStart = t * chunkSize;
            final int tEnd = Math.min(tStart + chunkSize, length);
            if (tStart >= length) break;
            final int at = aggType;
            futures[t] = POOL.submit(() -> {
                double revenue = (at == AGG_MIN) ? Double.POSITIVE_INFINITY
                               : (at == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
                long cnt = 0;
                int ms = tStart;
                while (ms < tEnd) {
                    int me = Math.min(ms + MORSEL_SIZE, tEnd);
                    double[] partial = fusedSimdLongRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            at, aggCol, ms, me);
                    cnt += (long) partial[1];
                    switch (at) {
                        case AGG_SUM: revenue += partial[0]; break;
                        case AGG_MIN: revenue = Math.min(revenue, partial[0]); break;
                        case AGG_MAX: revenue = Math.max(revenue, partial[0]); break;
                    }
                    ms = me;
                }
                return new double[] { revenue, (double) cnt };
            });
        }

        double result = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                       : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
        long count = 0;
        for (int t = 0; t < nThreads; t++) {
            if (futures[t] == null) break;
            try {
                double[] partial = futures[t].get();
                count += (long) partial[1];
                switch (aggType) {
                    case AGG_SUM: result += partial[0]; break;
                    case AGG_MIN: result = Math.min(result, partial[0]); break;
                    case AGG_MAX: result = Math.max(result, partial[0]); break;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new double[] { result, (double) count };
    }

    // =========================================================================
    // SIMD Predicate Helpers (deliberate duplicates for JIT isolation)
    // =========================================================================

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
}
