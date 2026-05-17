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
import static stratum.internal.ColumnOps.POOL;
import static stratum.internal.ColumnOps.LONG_SPECIES;
import static stratum.internal.ColumnOps.DOUBLE_SPECIES;
import static stratum.internal.ColumnOps.LONG_LANES;
import static stratum.internal.ColumnOps.DOUBLE_LANES;

/**
 * Validity-bitmap-aware sibling of {@link ColumnOpsChunkedSimd}.
 * Phase 1d of the null-handling redesign.
 *
 * <p>This file is a separate compilation unit so the validity-aware
 * chunked kernels JIT-compile independently from the all-valid
 * kernels in {@link ColumnOpsChunkedSimd}. MEMORY.md's JIT-Lessons
 * section documents repeated cross-section regressions when hot
 * chunked kernels were placed in the same class as array kernels;
 * the same principle applies here.
 *
 * <p>The chunked-mode validity bitmap is per-chunk (`long[][]` —
 * one bitmap per chunk, or null for an all-valid chunk). Per-chunk
 * fast path: when {@code validity[c] == null} we skip the
 * bitmap-AND for that chunk entirely — full SIMD speed. The
 * Clojure dispatch layer only routes to this file when at least
 * one chunk on at least one predicate column carries nulls; when
 * every chunk is all-valid we go to {@link ColumnOpsChunkedSimd}
 * directly and never enter this code at all.
 */
public final class ColumnOpsChunkedSimdNullable {

    private ColumnOpsChunkedSimdNullable() {}

    // ============================================================================
    // Validity bitmap helpers — duplicated locally for self-containment
    // ============================================================================

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

    /** Scalar predicate evaluation against the chunk-local data + bitmap. */
    private static boolean evalChunkScalar(
            long[] chunkValidity,
            int numLongPreds, int[] longPredTypes,
            long[] la0, long[] la1, long[] la2, long[] la3,
            long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[] da0, double[] da1, double[] da2, double[] da3,
            double[] dblLo, double[] dblHi,
            int i) {
        if (chunkValidity != null && !validityRowValid(chunkValidity, i)) return false;
        if (numLongPreds > 0) { long v = la0[i]; switch(longPredTypes[0]) { case PRED_RANGE: if (v < longLo[0] || v > longHi[0]) return false; break; case PRED_LT: if (v >= longHi[0]) return false; break; case PRED_GT: if (v <= longLo[0]) return false; break; case PRED_EQ: if (v != longLo[0]) return false; break; case PRED_LTE: if (v > longHi[0]) return false; break; case PRED_GTE: if (v < longLo[0]) return false; break; case PRED_NEQ: if (v == longLo[0]) return false; break; case PRED_NOT_RANGE: if (v >= longLo[0] && v <= longHi[0]) return false; break; } }
        if (numLongPreds > 1) { long v = la1[i]; switch(longPredTypes[1]) { case PRED_RANGE: if (v < longLo[1] || v > longHi[1]) return false; break; case PRED_LT: if (v >= longHi[1]) return false; break; case PRED_GT: if (v <= longLo[1]) return false; break; case PRED_EQ: if (v != longLo[1]) return false; break; case PRED_LTE: if (v > longHi[1]) return false; break; case PRED_GTE: if (v < longLo[1]) return false; break; case PRED_NEQ: if (v == longLo[1]) return false; break; case PRED_NOT_RANGE: if (v >= longLo[1] && v <= longHi[1]) return false; break; } }
        if (numLongPreds > 2) { long v = la2[i]; switch(longPredTypes[2]) { case PRED_RANGE: if (v < longLo[2] || v > longHi[2]) return false; break; case PRED_LT: if (v >= longHi[2]) return false; break; case PRED_GT: if (v <= longLo[2]) return false; break; case PRED_EQ: if (v != longLo[2]) return false; break; case PRED_LTE: if (v > longHi[2]) return false; break; case PRED_GTE: if (v < longLo[2]) return false; break; case PRED_NEQ: if (v == longLo[2]) return false; break; case PRED_NOT_RANGE: if (v >= longLo[2] && v <= longHi[2]) return false; break; } }
        if (numLongPreds > 3) { long v = la3[i]; switch(longPredTypes[3]) { case PRED_RANGE: if (v < longLo[3] || v > longHi[3]) return false; break; case PRED_LT: if (v >= longHi[3]) return false; break; case PRED_GT: if (v <= longLo[3]) return false; break; case PRED_EQ: if (v != longLo[3]) return false; break; case PRED_LTE: if (v > longHi[3]) return false; break; case PRED_GTE: if (v < longLo[3]) return false; break; case PRED_NEQ: if (v == longLo[3]) return false; break; case PRED_NOT_RANGE: if (v >= longLo[3] && v <= longHi[3]) return false; break; } }
        if (numDblPreds > 0) { double v = da0[i]; switch(dblPredTypes[0]) { case PRED_RANGE: if (v < dblLo[0] || v > dblHi[0]) return false; break; case PRED_LT: if (v >= dblHi[0]) return false; break; case PRED_GT: if (v <= dblLo[0]) return false; break; case PRED_EQ: if (v != dblLo[0]) return false; break; case PRED_LTE: if (v > dblHi[0]) return false; break; case PRED_GTE: if (v < dblLo[0]) return false; break; case PRED_NEQ: if (v == dblLo[0]) return false; break; case PRED_NOT_RANGE: if (v >= dblLo[0] && v <= dblHi[0]) return false; break; } }
        if (numDblPreds > 1) { double v = da1[i]; switch(dblPredTypes[1]) { case PRED_RANGE: if (v < dblLo[1] || v > dblHi[1]) return false; break; case PRED_LT: if (v >= dblHi[1]) return false; break; case PRED_GT: if (v <= dblLo[1]) return false; break; case PRED_EQ: if (v != dblLo[1]) return false; break; case PRED_LTE: if (v > dblHi[1]) return false; break; case PRED_GTE: if (v < dblLo[1]) return false; break; case PRED_NEQ: if (v == dblLo[1]) return false; break; case PRED_NOT_RANGE: if (v >= dblLo[1] && v <= dblHi[1]) return false; break; } }
        if (numDblPreds > 2) { double v = da2[i]; switch(dblPredTypes[2]) { case PRED_RANGE: if (v < dblLo[2] || v > dblHi[2]) return false; break; case PRED_LT: if (v >= dblHi[2]) return false; break; case PRED_GT: if (v <= dblLo[2]) return false; break; case PRED_EQ: if (v != dblLo[2]) return false; break; case PRED_LTE: if (v > dblHi[2]) return false; break; case PRED_GTE: if (v < dblLo[2]) return false; break; case PRED_NEQ: if (v == dblLo[2]) return false; break; case PRED_NOT_RANGE: if (v >= dblLo[2] && v <= dblHi[2]) return false; break; } }
        if (numDblPreds > 3) { double v = da3[i]; switch(dblPredTypes[3]) { case PRED_RANGE: if (v < dblLo[3] || v > dblHi[3]) return false; break; case PRED_LT: if (v >= dblHi[3]) return false; break; case PRED_GT: if (v <= dblLo[3]) return false; break; case PRED_EQ: if (v != dblLo[3]) return false; break; case PRED_LTE: if (v > dblHi[3]) return false; break; case PRED_GTE: if (v < dblLo[3]) return false; break; case PRED_NEQ: if (v == dblLo[3]) return false; break; case PRED_NOT_RANGE: if (v >= dblLo[3] && v <= dblHi[3]) return false; break; } }
        return true;
    }

    // ============================================================================
    // fusedSimdChunkedCountParallel — validity-aware sibling
    // ============================================================================

    /**
     * Validity-aware chunked filtered COUNT. Caller passes a
     * `long[][] validity` where `validity[c]` is chunk c's per-row
     * packed bitmap, or null when chunk c has no nulls. The whole
     * array may be null when no chunk anywhere has nulls (the
     * Clojure dispatch routes that case to {@link ColumnOpsChunkedSimd}
     * directly — so this method is reached only when at least one
     * chunk needs the bitmap).
     */
    public static double[] fusedSimdChunkedCountParallel(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            long[][] validity,
            int[] chunkLengths, int nChunks) {

        if (nChunks <= 0) return new double[] { 0.0, 0.0 };
        if (validity == null) {
            throw new IllegalArgumentException(
                "ColumnOpsChunkedSimdNullable methods require non-null validity.");
        }

        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        int nThreads = (totalRows < ColumnOps.PARALLEL_THRESHOLD) ? 1
            : Math.min(POOL.getParallelism(), Math.max(1, nChunks / 8));
        if (nThreads <= 1) {
            long cnt = chunkBatchCount(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    validity, chunkLengths, 0, nChunks);
            return new double[] { 0.0, (double) cnt };
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<Long>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                chunkBatchCount(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                        numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                        validity, chunkLengths, startChunk, endChunk));
        }

        long count = 0;
        try {
            for (Future<Long> f : futures) count += f.get();
        } catch (Exception e) {
            throw new RuntimeException("Chunked nullable count failed", e);
        }
        return new double[] { 0.0, (double) count };
    }

    private static long chunkBatchCount(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            long[][] validity,
            int[] chunkLengths, int startChunk, int endChunk) {

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

        long matchCount = 0;
        boolean[] maskBits = new boolean[DOUBLE_LANES];

        for (int c = startChunk; c < endChunk; c++) {
            int length = chunkLengths[c];
            final long[] chunkValidity = validity[c];

            final long[] la0 = numLongPreds > 0 ? longPredArrs[0][c] : null;
            final long[] la1 = numLongPreds > 1 ? longPredArrs[1][c] : null;
            final long[] la2 = numLongPreds > 2 ? longPredArrs[2][c] : null;
            final long[] la3 = numLongPreds > 3 ? longPredArrs[3][c] : null;
            final double[] da0 = numDblPreds > 0 ? dblPredArrs[0][c] : null;
            final double[] da1 = numDblPreds > 1 ? dblPredArrs[1][c] : null;
            final double[] da2 = numDblPreds > 2 ? dblPredArrs[2][c] : null;
            final double[] da3 = numDblPreds > 3 ? dblPredArrs[3][c] : null;

            int upperBound = length - (length % LONG_LANES);

            // Per-chunk fast/slow split: if this chunk has no nulls we
            // skip the bitmap-AND entirely (rest of the kernel runs
            // identically to the non-Nullable version). When the chunk
            // does have nulls we extract the lane mask each iteration.
            if (chunkValidity == null) {
                for (int i = 0; i < upperBound; i += LONG_LANES) {
                    VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
                    if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }
                    if (numDblPreds > 0) {
                        VectorMask<Double> dm;
                        if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
                        else { for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0); }
                        if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                        matchCount += dm.trueCount();
                    } else {
                        matchCount += lm.trueCount();
                    }
                }
            } else {
                for (int i = 0; i < upperBound; i += LONG_LANES) {
                    // Validity-AND first (DuckDB pattern).
                    VectorMask<Long> lm = validityLaneMask(chunkValidity, i);
                    if (!lm.anyTrue()) continue;
                    if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                    if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }
                    if (numDblPreds > 0) {
                        VectorMask<Double> dm;
                        for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
                        dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);
                        if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                        if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                        matchCount += dm.trueCount();
                    } else {
                        matchCount += lm.trueCount();
                    }
                }
            }

            // Scalar tail
            for (int i = upperBound; i < length; i++) {
                if (evalChunkScalar(chunkValidity,
                        numLongPreds, longPredTypes, la0, la1, la2, la3, longLo, longHi,
                        numDblPreds, dblPredTypes, da0, da1, da2, da3, dblLo, dblHi, i)) {
                    matchCount++;
                }
            }
        }
        return matchCount;
    }
}
