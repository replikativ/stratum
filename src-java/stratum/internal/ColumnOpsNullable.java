package stratum.internal;

import java.util.concurrent.Future;
import jdk.incubator.vector.*;

import static stratum.internal.ColumnOps.PRED_RANGE;
import static stratum.internal.ColumnOps.PRED_LT;
import static stratum.internal.ColumnOps.PRED_GT;
import static stratum.internal.ColumnOps.PRED_EQ;
import static stratum.internal.ColumnOps.PRED_LTE;
import static stratum.internal.ColumnOps.PRED_GTE;
import static stratum.internal.ColumnOps.PRED_NEQ;
import static stratum.internal.ColumnOps.PRED_NOT_RANGE;
import static stratum.internal.ColumnOps.AGG_COUNT;
import static stratum.internal.ColumnOps.AGG_SUM;
import static stratum.internal.ColumnOps.AGG_SUM_PRODUCT;
import static stratum.internal.ColumnOps.AGG_MIN;
import static stratum.internal.ColumnOps.AGG_MAX;
import static stratum.internal.ColumnOps.MORSEL_SIZE;
import static stratum.internal.ColumnOps.PARALLEL_THRESHOLD;
import static stratum.internal.ColumnOps.POOL;
import static stratum.internal.ColumnOps.LONG_SPECIES;
import static stratum.internal.ColumnOps.DOUBLE_SPECIES;
import static stratum.internal.ColumnOps.LONG_LANES;
import static stratum.internal.ColumnOps.DOUBLE_LANES;

/**
 * Validity-bitmap-aware SIMD predicate kernels — phase 1 of the
 * null-handling redesign.
 *
 * <p>These are *siblings* of the sentinel-only kernels in {@link ColumnOps},
 * {@link ColumnOpsExt}, and {@link ColumnOpsChunkedSimd}. The Clojure
 * dispatch layer selects which file to call based on an `any-nullable?`
 * flag computed once per query: when no predicate column carries a
 * validity bitmap, the existing all-valid fast-path methods are called;
 * when any predicate column has nulls, the methods here are called with
 * a combined validity bitmap.
 *
 * <p>Living in a *separate Java file* is load-bearing for JIT health.
 * MEMORY.md ("JIT Optimization Lessons") documents repeated cases
 * where adding a few KB of bytecode to {@link ColumnOps} caused 5-7×
 * cross-section JIT regressions. The pattern that worked previously
 * (ColumnOpsChunked, ColumnOpsChunkedSimd, ColumnOpsString,
 * ColumnOpsVar, ColumnOpsTemporal) was to isolate hot kernels in
 * their own compilation unit.
 *
 * <p>Bitmap layout: packed long[] with 64 rows per entry, LSB-first
 * within each long. Bit set → row is non-NULL; bit clear → row is
 * NULL (the data array's value at that slot is the per-type sentinel,
 * retained for hashing and output but NOT semantically relevant for
 * predicate evaluation).
 *
 * <p>The SIMD bitmap-AND pattern is DuckDB's
 * {@code BinaryExecutor::SelectFlatLoop}: extract LONG_LANES bits from
 * the bitmap into a {@code VectorMask}, AND it into the per-row mask
 * after the comparison runs. Per-block check is one branch per 64
 * rows in the all-valid case (we still pay the bitmap-extraction
 * cost, but that's ~3 long ops + one mask construction). For the
 * common dense-data case the Clojure dispatch picks the
 * non-Nullable kernel and this code is never entered.
 */
public final class ColumnOpsNullable {

    private ColumnOpsNullable() {}

    // ============================================================================
    // Validity bitmap helpers
    // ============================================================================

    /**
     * Extract LONG_LANES bits from a packed long[] validity bitmap,
     * starting at row {@code i}, as a {@code VectorMask<Long>}.
     * Assumes {@code i} is LONG_LANES-aligned (i.e. {@code i & (LONG_LANES-1) == 0})
     * which holds inside the unrolled SIMD loop where {@code i} advances
     * by LONG_LANES. The bits never straddle a 64-bit bitmap entry
     * boundary when LONG_LANES divides 64.
     */
    private static VectorMask<Long> validityLaneMask(long[] validity, int i) {
        long entry = validity[i >>> 6];
        long laneBits = (entry >>> (i & 63)) & ((1L << LONG_LANES) - 1);
        return VectorMask.fromLong(LONG_SPECIES, laneBits);
    }

    /**
     * Test a single bit in a packed long[] validity bitmap. Used by
     * the scalar tail of each kernel.
     */
    private static boolean validityRowValid(long[] validity, int i) {
        return (validity[i >>> 6] & (1L << (i & 63))) != 0L;
    }

    // ============================================================================
    // Predicate kernels — duplicated locally from ColumnOps so this is a
    // self-contained JIT compilation unit (the agent's #1 recommendation).
    // These are *exactly* the same body shapes as ColumnOps; JIT will
    // independently profile and compile them.
    // ============================================================================

    private static VectorMask<Long> applyLongPred(
            VectorMask<Long> in, LongVector v, int predType,
            LongVector lo, LongVector hi) {
        switch (predType) {
            case PRED_RANGE:
                return in.and(v.compare(VectorOperators.GE, lo))
                         .and(v.compare(VectorOperators.LE, hi));
            case PRED_LT:    return in.and(v.compare(VectorOperators.LT, hi));
            case PRED_GT:    return in.and(v.compare(VectorOperators.GT, lo));
            case PRED_EQ:    return in.and(v.compare(VectorOperators.EQ, lo));
            case PRED_LTE:   return in.and(v.compare(VectorOperators.LE, hi));
            case PRED_GTE:   return in.and(v.compare(VectorOperators.GE, lo));
            case PRED_NEQ:   return in.and(v.compare(VectorOperators.NE, lo));
            case PRED_NOT_RANGE:
                return in.and(v.compare(VectorOperators.LT, lo)
                               .or(v.compare(VectorOperators.GT, hi)));
            default: return in;
        }
    }

    private static VectorMask<Double> applyDoublePred(
            VectorMask<Double> in, DoubleVector v, int predType,
            DoubleVector lo, DoubleVector hi) {
        switch (predType) {
            case PRED_RANGE:
                return in.and(v.compare(VectorOperators.GE, lo))
                         .and(v.compare(VectorOperators.LE, hi));
            case PRED_LT:    return in.and(v.compare(VectorOperators.LT, hi));
            case PRED_GT:    return in.and(v.compare(VectorOperators.GT, lo));
            case PRED_EQ:    return in.and(v.compare(VectorOperators.EQ, lo));
            case PRED_LTE:   return in.and(v.compare(VectorOperators.LE, hi));
            case PRED_GTE:   return in.and(v.compare(VectorOperators.GE, lo));
            case PRED_NEQ:   return in.and(v.compare(VectorOperators.NE, lo));
            case PRED_NOT_RANGE:
                return in.and(v.compare(VectorOperators.LT, lo)
                               .or(v.compare(VectorOperators.GT, hi)));
            default: return in;
        }
    }

    /**
     * Scalar predicate evaluation that respects the validity bitmap.
     * Returns true iff (a) row {@code i} is non-null per the bitmap
     * AND (b) every predicate is satisfied. Used by the scalar tail
     * of every kernel below.
     */
    private static boolean evaluatePredicatesWithValidity(
            long[] validity,
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int i) {
        if (!validityRowValid(validity, i)) return false;
        for (int p = 0; p < numLongPreds; p++) {
            long v = longCols[p][i];
            switch (longPredTypes[p]) {
                case PRED_RANGE:     if (v < longLo[p] || v > longHi[p]) return false; break;
                case PRED_LT:        if (v >= longHi[p]) return false; break;
                case PRED_GT:        if (v <= longLo[p]) return false; break;
                case PRED_EQ:        if (v != longLo[p]) return false; break;
                case PRED_LTE:       if (v > longHi[p]) return false; break;
                case PRED_GTE:       if (v < longLo[p]) return false; break;
                case PRED_NEQ:       if (v == longLo[p]) return false; break;
                case PRED_NOT_RANGE: if (v >= longLo[p] && v <= longHi[p]) return false; break;
            }
        }
        for (int p = 0; p < numDblPreds; p++) {
            double v = dblCols[p][i];
            switch (dblPredTypes[p]) {
                case PRED_RANGE:     if (v < dblLo[p] || v > dblHi[p]) return false; break;
                case PRED_LT:        if (v >= dblHi[p]) return false; break;
                case PRED_GT:        if (v <= dblLo[p]) return false; break;
                case PRED_EQ:        if (v != dblLo[p]) return false; break;
                case PRED_LTE:       if (v > dblHi[p]) return false; break;
                case PRED_GTE:       if (v < dblLo[p]) return false; break;
                case PRED_NEQ:       if (v == dblLo[p]) return false; break;
                case PRED_NOT_RANGE: if (v >= dblLo[p] && v <= dblHi[p]) return false; break;
            }
        }
        return true;
    }

    // ============================================================================
    // fusedSimdCountParallel — validity-aware sibling
    // ============================================================================

    /**
     * Parallel fused SIMD COUNT, validity-aware. Identical signature
     * to {@link ColumnOps#fusedSimdCountParallel} plus a single
     * {@code validity} parameter (combined predicate-side bitmap, ANDed
     * across all nullable predicate columns).
     *
     * <p>Callers must pass a non-null validity; the all-valid case
     * should be dispatched to the existing {@code ColumnOps.fusedSimdCountParallel}
     * at the Clojure layer.
     */
    public static double[] fusedSimdCountParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            long[] validity,
            int length) {

        if (validity == null) {
            throw new IllegalArgumentException(
                "ColumnOpsNullable methods require non-null validity; dispatch all-valid to ColumnOps.");
        }

        if (length < PARALLEL_THRESHOLD) {
            return fusedSimdCountRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, validity, 0, length);
        }

        int nThreads = Runtime.getRuntime().availableProcessors();
        // Align morsels to LONG_LANES so the validity-extraction stays
        // lane-aligned. threadRange is rounded up to a multiple of LONG_LANES.
        int threadRange = ((length + nThreads - 1) / nThreads);
        threadRange = ((threadRange + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                long cnt = 0;
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    cnt += (long) fusedSimdCountRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            validity, ms, me)[1];
                }
                return new double[] { 0.0, (double) cnt };
            });
        }

        long count = 0;
        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                count += (long) f.get()[1];
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
        return new double[] { 0.0, (double) count };
    }

    // ============================================================================
    // fusedSimdParallel — validity-aware sibling (filter + aggregate)
    // ============================================================================

    /**
     * Parallel fused filter + aggregate, validity-aware. Identical
     * signature to {@link ColumnOps#fusedSimdParallel} plus a
     * {@code validity} parameter. Used for SUM / SUM_PRODUCT / MIN /
     * MAX (and COUNT, though dedicated {@link #fusedSimdCountParallel}
     * is preferred for COUNT-only).
     *
     * <p>The aggregator side continues to use the existing IEEE-NaN
     * sentinel-skip when {@code nanSafe} is true — that's correct for
     * F-006 era aggregator behavior. The validity bitmap fixes the
     * *predicate* side: SIMD comparisons no longer leak NULL rows
     * through LT/LTE/NEQ/NOT_RANGE etc. (audit F-001/F-002).
     */
    public static double[] fusedSimdParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
            long[] validity,
            int length, boolean nanSafe) {

        if (validity == null) {
            throw new IllegalArgumentException(
                "ColumnOpsNullable methods require non-null validity.");
        }

        if (length < PARALLEL_THRESHOLD) {
            return fusedSimdUnrolledRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    aggType, aggCol1, aggCol2, validity, 0, length, nanSafe);
        }

        int nThreads = Runtime.getRuntime().availableProcessors();
        int threadRange = ((length + nThreads - 1) / nThreads);
        // Align to LONG_LANES so validity-extraction stays lane-aligned.
        threadRange = ((threadRange + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                double revenue = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                               : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
                long cnt = 0;
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[] partial = fusedSimdUnrolledRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            aggType, aggCol1, aggCol2, validity, ms, me, nanSafe);
                    cnt += (long) partial[1];
                    switch (aggType) {
                        case AGG_SUM_PRODUCT: case AGG_SUM: revenue += partial[0]; break;
                        case AGG_MIN: revenue = Math.min(revenue, partial[0]); break;
                        case AGG_MAX: revenue = Math.max(revenue, partial[0]); break;
                    }
                }
                return new double[] { revenue, (double) cnt };
            });
        }

        double result = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                       : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
        long count = 0;
        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                count += (long) partial[1];
                switch (aggType) {
                    case AGG_SUM_PRODUCT: case AGG_SUM: result += partial[0]; break;
                    case AGG_MIN: result = Math.min(result, partial[0]); break;
                    case AGG_MAX: result = Math.max(result, partial[0]); break;
                    case AGG_COUNT: break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
        return new double[] { result, (double) count };
    }

    static double[] fusedSimdUnrolledRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
            long[] validity,
            int start, int end, boolean nanSafe) {

        int rangeLen = end - start;
        double revenue = 0.0;
        int matchCount = 0;
        int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

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

        DoubleVector revenueVec;
        if (aggType == AGG_MIN) {
            revenueVec = DoubleVector.broadcast(DOUBLE_SPECIES, Double.POSITIVE_INFINITY);
            revenue = Double.POSITIVE_INFINITY;
        } else if (aggType == AGG_MAX) {
            revenueVec = DoubleVector.broadcast(DOUBLE_SPECIES, Double.NEGATIVE_INFINITY);
            revenue = Double.NEGATIVE_INFINITY;
        } else {
            revenueVec = DoubleVector.zero(DOUBLE_SPECIES);
        }
        boolean[] maskBits = new boolean[DOUBLE_LANES];

        for (int i = start; i < upperBound; i += LONG_LANES) {
            // Validity-AND at SIMD entry — DuckDB pattern.
            VectorMask<Long> lm = validityLaneMask(validity, i);
            if (!lm.anyTrue()) continue;
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            VectorMask<Double> dm;
            if (numDblPreds > 0 || aggType != AGG_COUNT) {
                for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
                dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);
            } else { matchCount += lm.trueCount(); continue; }

            if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
            if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }

            if (aggType == AGG_SUM_PRODUCT) {
                DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol1, i);
                DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol2, i);
                if (nanSafe) {
                    VectorMask<Double> validDm = dm.andNot(a.test(VectorOperators.IS_NAN)).andNot(b.test(VectorOperators.IS_NAN));
                    revenueVec = revenueVec.add(a.mul(b), validDm);
                    matchCount += validDm.trueCount();
                } else {
                    revenueVec = revenueVec.add(a.mul(b), dm);
                    matchCount += dm.trueCount();
                }
            } else if (aggType == AGG_SUM) {
                DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol1, i);
                if (nanSafe) {
                    VectorMask<Double> validDm = dm.andNot(a.test(VectorOperators.IS_NAN));
                    revenueVec = revenueVec.add(a, validDm);
                    matchCount += validDm.trueCount();
                } else {
                    revenueVec = revenueVec.add(a, dm);
                    matchCount += dm.trueCount();
                }
            } else if (aggType == AGG_MIN) {
                DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol1, i);
                if (nanSafe) {
                    VectorMask<Double> validDm = dm.andNot(a.test(VectorOperators.IS_NAN));
                    revenueVec = revenueVec.min(DoubleVector.broadcast(DOUBLE_SPECIES, Double.POSITIVE_INFINITY).blend(a, validDm));
                    matchCount += validDm.trueCount();
                } else {
                    revenueVec = revenueVec.min(DoubleVector.broadcast(DOUBLE_SPECIES, Double.POSITIVE_INFINITY).blend(a, dm));
                    matchCount += dm.trueCount();
                }
            } else if (aggType == AGG_MAX) {
                DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aggCol1, i);
                if (nanSafe) {
                    VectorMask<Double> validDm = dm.andNot(a.test(VectorOperators.IS_NAN));
                    revenueVec = revenueVec.max(DoubleVector.broadcast(DOUBLE_SPECIES, Double.NEGATIVE_INFINITY).blend(a, validDm));
                    matchCount += validDm.trueCount();
                } else {
                    revenueVec = revenueVec.max(DoubleVector.broadcast(DOUBLE_SPECIES, Double.NEGATIVE_INFINITY).blend(a, dm));
                    matchCount += dm.trueCount();
                }
            } else {
                matchCount += dm.trueCount();
            }
        }

        if (aggType == AGG_MIN) revenue = revenueVec.reduceLanes(VectorOperators.MIN);
        else if (aggType == AGG_MAX) revenue = revenueVec.reduceLanes(VectorOperators.MAX);
        else revenue = revenueVec.reduceLanes(VectorOperators.ADD);

        // Scalar tail — validity-aware predicate eval.
        for (int i = upperBound; i < end; i++) {
            if (evaluatePredicatesWithValidity(validity,
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                if (aggType == AGG_COUNT) {
                    matchCount++;
                } else {
                    double v = aggCol1[i];
                    if (!nanSafe || v == v) {
                        switch (aggType) {
                            case AGG_SUM_PRODUCT:
                                double v2 = aggCol2[i];
                                if (!nanSafe || v2 == v2) { revenue += v * v2; matchCount++; }
                                break;
                            case AGG_SUM: revenue += v; matchCount++; break;
                            case AGG_MIN: revenue = Math.min(revenue, v); matchCount++; break;
                            case AGG_MAX: revenue = Math.max(revenue, v); matchCount++; break;
                        }
                    }
                }
            }
        }
        return new double[] { revenue, (double) matchCount };
    }

    static double[] fusedSimdCountRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            long[] validity,
            int start, int end) {

        int rangeLen = end - start;
        int matchCount = 0;
        int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

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

        boolean[] maskBits = new boolean[DOUBLE_LANES];

        for (int i = start; i < upperBound; i += LONG_LANES) {
            // Start with the validity-masked initial set (DuckDB pattern):
            // any row whose bitmap bit is clear is excluded *before* any
            // predicate runs. The data-array value at a clear bit is the
            // per-type sentinel which would otherwise leak garbage into
            // the LT/NEQ etc. comparisons (audit findings F-001/F-002).
            VectorMask<Long> lm = validityLaneMask(validity, i);
            if (!lm.anyTrue()) continue;
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            if (numDblPreds > 0) {
                VectorMask<Double> dm;
                for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
                dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);
                if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                matchCount += dm.trueCount();
            } else {
                matchCount += lm.trueCount();
            }
        }

        // Scalar tail — validity-aware predicate eval.
        for (int i = upperBound; i < end; i++) {
            if (evaluatePredicatesWithValidity(validity,
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                matchCount++;
            }
        }
        return new double[] { 0.0, (double) matchCount };
    }
}
