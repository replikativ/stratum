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
import static stratum.internal.ColumnOps.MORSEL_SIZE;
import static stratum.internal.ColumnOps.POOL;
import static stratum.internal.ColumnOps.LONG_SPECIES;
import static stratum.internal.ColumnOps.DOUBLE_SPECIES;
import static stratum.internal.ColumnOps.LONG_LANES;
import static stratum.internal.ColumnOps.DOUBLE_LANES;

/**
 * Validity-bitmap-aware sibling of {@link ColumnOpsExt} multi-sum
 * kernels. Phase 1e of the null-handling redesign.
 *
 * <p>Separate file for JIT-class budget isolation (same reason as
 * `ColumnOpsNullable`, `ColumnOpsChunkedSimdNullable`).
 *
 * <p>Currently covers {@code fusedSimdMultiSumParallel} — the
 * single-pass multi-SUM kernel used by B2 (TPC-H Q1) and similar
 * many-SUM workloads. Other multi-sum variants (`AllLong`,
 * `AllLongMixedPreds`) can route through here via Clojure
 * conversion if needed; the dispatch layer falls back to the
 * existing all-valid kernels when no predicate column carries
 * nulls.
 */
public final class ColumnOpsExtNullable {

    private ColumnOpsExtNullable() {}

    private static VectorMask<Long> validityLaneMask(long[] validity, int i) {
        long entry = validity[i >>> 6];
        long laneBits = (entry >>> (i & 63)) & ((1L << LONG_LANES) - 1);
        return VectorMask.fromLong(LONG_SPECIES, laneBits);
    }

    private static boolean validityRowValid(long[] validity, int i) {
        return (validity[i >>> 6] & (1L << (i & 63))) != 0L;
    }

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
     * Validity-aware {@code fusedSimdMultiSumParallel}. Up to 4 SUM /
     * SUM_PRODUCT accumulators in a single pass, with per-row
     * bitmap-AND on the predicate side.
     */
    public static double[] fusedSimdMultiSumParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, double[][] sumCols1, double[][] sumCols2,
            long[] validity,
            int length, boolean nanSafe) {

        if (validity == null) {
            throw new IllegalArgumentException(
                "ColumnOpsExtNullable methods require non-null validity.");
        }

        if (length < ColumnOps.PARALLEL_THRESHOLD) {
            return fusedSimdMultiSumRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numSumAggs, sumCols1, sumCols2, validity, 0, length, nanSafe);
        }

        int nThreads = Runtime.getRuntime().availableProcessors();
        int threadRange = ((length + nThreads - 1) / nThreads);
        threadRange = ((threadRange + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                double[] partial = new double[numSumAggs + 1];
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[] morsel = fusedSimdMultiSumRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numSumAggs, sumCols1, sumCols2, validity, ms, me, nanSafe);
                    for (int s = 0; s < numSumAggs; s++) partial[s] += morsel[s];
                    partial[numSumAggs] += morsel[numSumAggs];
                }
                return partial;
            });
        }

        double[] sums = new double[numSumAggs + 1];
        try {
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                double[] partial = f.get();
                for (int s = 0; s < numSumAggs; s++) sums[s] += partial[s];
                sums[numSumAggs] += partial[numSumAggs];
            }
        } catch (Exception e) {
            throw new RuntimeException("Multi-sum nullable parallel failed", e);
        }
        return sums;
    }

    static double[] fusedSimdMultiSumRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numSumAggs, double[][] sumCols1, double[][] sumCols2,
            long[] validity,
            int start, int end, boolean nanSafe) {

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

        final double[] sc0 = numSumAggs > 0 ? sumCols1[0] : null;
        final double[] sc0b = (numSumAggs > 0 && sumCols2[0] != null) ? sumCols2[0] : null;
        final double[] sc1 = numSumAggs > 1 ? sumCols1[1] : null;
        final double[] sc1b = (numSumAggs > 1 && sumCols2[1] != null) ? sumCols2[1] : null;
        final double[] sc2 = numSumAggs > 2 ? sumCols1[2] : null;
        final double[] sc2b = (numSumAggs > 2 && sumCols2[2] != null) ? sumCols2[2] : null;
        final double[] sc3 = numSumAggs > 3 ? sumCols1[3] : null;
        final double[] sc3b = (numSumAggs > 3 && sumCols2[3] != null) ? sumCols2[3] : null;

        DoubleVector sv0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv2 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sv3 = DoubleVector.zero(DOUBLE_SPECIES);
        boolean[] maskBits = new boolean[DOUBLE_LANES];

        for (int i = start; i < upperBound; i += LONG_LANES) {
            // Validity-AND first.
            VectorMask<Long> lm = validityLaneMask(validity, i);
            if (!lm.anyTrue()) continue;
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            VectorMask<Double> dm;
            for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
            dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);

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
            if (evaluatePredicatesWithValidity(validity,
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                matchCount++;
                if (numSumAggs > 0) {
                    double v = sc0[i];
                    double b = sc0b != null ? sc0b[i] : 1.0;
                    if (!nanSafe || ((v == v) && (sc0b == null || b == b))) sums[0] += v * b;
                }
                if (numSumAggs > 1) {
                    double v = sc1[i];
                    double b = sc1b != null ? sc1b[i] : 1.0;
                    if (!nanSafe || ((v == v) && (sc1b == null || b == b))) sums[1] += v * b;
                }
                if (numSumAggs > 2) {
                    double v = sc2[i];
                    double b = sc2b != null ? sc2b[i] : 1.0;
                    if (!nanSafe || ((v == v) && (sc2b == null || b == b))) sums[2] += v * b;
                }
                if (numSumAggs > 3) {
                    double v = sc3[i];
                    double b = sc3b != null ? sc3b[i] : 1.0;
                    if (!nanSafe || ((v == v) && (sc3b == null || b == b))) sums[3] += v * b;
                }
            }
        }
        sums[numSumAggs] = matchCount;
        return sums;
    }
}
