package stratum.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import jdk.incubator.vector.*;

/**
 * Chunked SIMD filter+aggregate operations — JIT-isolated from ColumnOpsChunked.
 *
 * Contains streaming chunk-at-a-time SIMD paths that process
 * PersistentColumnIndex data without full materialization:
 * - Fused filter+aggregate (fusedSimdChunkedParallel)
 * - Fused filter+count (fusedSimdChunkedCountParallel)
 *
 * Separated from ColumnOpsChunked (group-by methods) to prevent cross-method
 * JIT interference: SIMD-heavy filter+agg methods (130+ lines of broadcasts
 * and switches) degrade JIT compilation of group-by scatter methods in the
 * same class, even when never called from the same path.
 */
public final class ColumnOpsChunkedSimd {

    // Re-use ColumnOps pool
    private static ForkJoinPool POOL = ColumnOps.POOL;

    // Agg type constants (same values as ColumnOps)
    private static final int AGG_SUM = ColumnOps.AGG_SUM;
    private static final int AGG_COUNT = ColumnOps.AGG_COUNT;
    private static final int AGG_SUM_PRODUCT = ColumnOps.AGG_SUM_PRODUCT;
    private static final int AGG_MIN = ColumnOps.AGG_MIN;
    private static final int AGG_MAX = ColumnOps.AGG_MAX;

    // SIMD species (compile-time constants)
    private static final VectorSpecies<Long> LONG_SPECIES = ColumnOps.LONG_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = ColumnOps.DOUBLE_SPECIES;
    private static final int LONG_LANES = ColumnOps.LONG_LANES;
    private static final int DOUBLE_LANES = ColumnOps.DOUBLE_LANES;

    // Pred type constants (compile-time constants)
    private static final int PRED_RANGE = ColumnOps.PRED_RANGE;
    private static final int PRED_LT = ColumnOps.PRED_LT;
    private static final int PRED_GT = ColumnOps.PRED_GT;
    private static final int PRED_EQ = ColumnOps.PRED_EQ;
    private static final int PRED_LTE = ColumnOps.PRED_LTE;
    private static final int PRED_GTE = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    // JIT-isolated SIMD pred helpers (duplicated for JIT isolation)
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

    // =========================================================================
    // Fused Filter+Aggregate — Chunked SIMD (streaming over index chunks)
    // =========================================================================

    /**
     * Parallel fused filter+aggregate over pre-extracted heap-array chunks.
     * JIT-isolated from ColumnOps array-path methods to prevent cross-method
     * JIT interference (array warmup poisoned this path: 13ms -> 70ms).
     */
    public static double[] fusedSimdChunkedParallel(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int aggType, double[][] aggArr1s, double[][] aggArr2s,
            int[] chunkLengths, int nChunks) {

        if (nChunks <= 0) return new double[] { 0.0, 0.0 };

        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        int nThreads = (totalRows < ColumnOps.PARALLEL_THRESHOLD) ? 1
            : Math.min(POOL.getParallelism(), Math.max(1, nChunks / 8));
        if (nThreads <= 1) {
            return fusedSimdChunkBatch(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    aggType, aggArr1s, aggArr2s, chunkLengths, 0, nChunks);
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedSimdChunkBatch(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                        numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                        aggType, aggArr1s, aggArr2s, chunkLengths, startChunk, endChunk));
        }

        double result = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                       : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
        long count = 0;
        try {
            for (Future<double[]> f : futures) {
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
            throw new RuntimeException("Chunked parallel execution failed", e);
        }
        return new double[] { result, (double) count };
    }

    /**
     * Process a batch of heap-array chunks [startChunk, endChunk).
     * Broadcasts SIMD constants once, then loops over chunks using
     * fromArray for SIMD loads.
     */
    private static double[] fusedSimdChunkBatch(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int aggType, double[][] aggArr1s, double[][] aggArr2s,
            int[] chunkLengths, int startChunk, int endChunk) {

        // Broadcast SIMD constants ONCE for the entire batch
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

        // Cross-chunk accumulators
        double revenue = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                        : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
        long matchCount = 0;
        boolean[] maskBits = new boolean[DOUBLE_LANES];

        // Outer loop: iterate over chunks
        for (int c = startChunk; c < endChunk; c++) {
            int length = chunkLengths[c];

            // Extract heap arrays for this chunk (typed 3D arrays — no checkcast needed)
            final long[] la0 = numLongPreds > 0 ? longPredArrs[0][c] : null;
            final long[] la1 = numLongPreds > 1 ? longPredArrs[1][c] : null;
            final long[] la2 = numLongPreds > 2 ? longPredArrs[2][c] : null;
            final long[] la3 = numLongPreds > 3 ? longPredArrs[3][c] : null;
            final double[] da0 = numDblPreds > 0 ? dblPredArrs[0][c] : null;
            final double[] da1 = numDblPreds > 1 ? dblPredArrs[1][c] : null;
            final double[] da2 = numDblPreds > 2 ? dblPredArrs[2][c] : null;
            final double[] da3 = numDblPreds > 3 ? dblPredArrs[3][c] : null;
            final double[] aa1 = (aggType != AGG_COUNT) ? aggArr1s[c] : null;
            final double[] aa2 = (aggType == AGG_SUM_PRODUCT) ? aggArr2s[c] : null;

            int upperBound = length - (length % LONG_LANES);

            // Per-chunk SIMD accumulator
            DoubleVector revenueVec;
            if (aggType == AGG_MIN) revenueVec = DoubleVector.broadcast(DOUBLE_SPECIES, Double.POSITIVE_INFINITY);
            else if (aggType == AGG_MAX) revenueVec = DoubleVector.broadcast(DOUBLE_SPECIES, Double.NEGATIVE_INFINITY);
            else revenueVec = DoubleVector.zero(DOUBLE_SPECIES);

            // SIMD inner loop for this chunk
            for (int i = 0; i < upperBound; i += LONG_LANES) {
                VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
                if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

                VectorMask<Double> dm;
                if (numDblPreds > 0 || aggType != AGG_COUNT) {
                    if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
                    else { for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0); }
                } else { matchCount += lm.trueCount(); continue; }

                if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }

                matchCount += dm.trueCount();
                if (aggType == AGG_SUM_PRODUCT) { DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aa1, i); DoubleVector b = DoubleVector.fromArray(DOUBLE_SPECIES, aa2, i); revenueVec = revenueVec.add(a.mul(b), dm); }
                else if (aggType == AGG_SUM) { revenueVec = revenueVec.add(DoubleVector.fromArray(DOUBLE_SPECIES, aa1, i), dm); }
                else if (aggType == AGG_MIN) { DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aa1, i); revenueVec = revenueVec.min(DoubleVector.broadcast(DOUBLE_SPECIES, Double.POSITIVE_INFINITY).blend(a, dm)); }
                else if (aggType == AGG_MAX) { DoubleVector a = DoubleVector.fromArray(DOUBLE_SPECIES, aa1, i); revenueVec = revenueVec.max(DoubleVector.broadcast(DOUBLE_SPECIES, Double.NEGATIVE_INFINITY).blend(a, dm)); }
            }

            // Horizontal reduction from this chunk's SIMD accumulator
            double chunkRev;
            if (aggType == AGG_MIN) chunkRev = revenueVec.reduceLanes(VectorOperators.MIN);
            else if (aggType == AGG_MAX) chunkRev = revenueVec.reduceLanes(VectorOperators.MAX);
            else chunkRev = revenueVec.reduceLanes(VectorOperators.ADD);

            // Scalar tail for this chunk (handles non-SIMD-aligned remainder)
            for (int i = upperBound; i < length; i++) {
                boolean match = true;
                if (numLongPreds > 0 && match) { long v = la0[i]; switch(lt0) { case PRED_RANGE: match = v >= longLo[0] && v <= longHi[0]; break; case PRED_LT: match = v < longHi[0]; break; case PRED_GT: match = v > longLo[0]; break; case PRED_EQ: match = v == longLo[0]; break; case PRED_LTE: match = v <= longHi[0]; break; case PRED_GTE: match = v >= longLo[0]; break; case PRED_NEQ: match = v != longLo[0]; break; case PRED_NOT_RANGE: match = v < longLo[0] || v > longHi[0]; break; } }
                if (numLongPreds > 1 && match) { long v = la1[i]; switch(lt1) { case PRED_RANGE: match = v >= longLo[1] && v <= longHi[1]; break; case PRED_LT: match = v < longHi[1]; break; case PRED_GT: match = v > longLo[1]; break; case PRED_EQ: match = v == longLo[1]; break; case PRED_LTE: match = v <= longHi[1]; break; case PRED_GTE: match = v >= longLo[1]; break; case PRED_NEQ: match = v != longLo[1]; break; case PRED_NOT_RANGE: match = v < longLo[1] || v > longHi[1]; break; } }
                if (numLongPreds > 2 && match) { long v = la2[i]; switch(lt2) { case PRED_RANGE: match = v >= longLo[2] && v <= longHi[2]; break; case PRED_LT: match = v < longHi[2]; break; case PRED_GT: match = v > longLo[2]; break; case PRED_EQ: match = v == longLo[2]; break; case PRED_LTE: match = v <= longHi[2]; break; case PRED_GTE: match = v >= longLo[2]; break; case PRED_NEQ: match = v != longLo[2]; break; case PRED_NOT_RANGE: match = v < longLo[2] || v > longHi[2]; break; } }
                if (numLongPreds > 3 && match) { long v = la3[i]; switch(lt3) { case PRED_RANGE: match = v >= longLo[3] && v <= longHi[3]; break; case PRED_LT: match = v < longHi[3]; break; case PRED_GT: match = v > longLo[3]; break; case PRED_EQ: match = v == longLo[3]; break; case PRED_LTE: match = v <= longHi[3]; break; case PRED_GTE: match = v >= longLo[3]; break; case PRED_NEQ: match = v != longLo[3]; break; case PRED_NOT_RANGE: match = v < longLo[3] || v > longHi[3]; break; } }
                if (numDblPreds > 0 && match) { double v = da0[i]; switch(dt0) { case PRED_RANGE: match = v >= dblLo[0] && v <= dblHi[0]; break; case PRED_LT: match = v < dblHi[0]; break; case PRED_GT: match = v > dblLo[0]; break; case PRED_EQ: match = v == dblLo[0]; break; case PRED_LTE: match = v <= dblHi[0]; break; case PRED_GTE: match = v >= dblLo[0]; break; case PRED_NEQ: match = v != dblLo[0]; break; case PRED_NOT_RANGE: match = v < dblLo[0] || v > dblHi[0]; break; } }
                if (numDblPreds > 1 && match) { double v = da1[i]; switch(dt1) { case PRED_RANGE: match = v >= dblLo[1] && v <= dblHi[1]; break; case PRED_LT: match = v < dblHi[1]; break; case PRED_GT: match = v > dblLo[1]; break; case PRED_EQ: match = v == dblLo[1]; break; case PRED_LTE: match = v <= dblHi[1]; break; case PRED_GTE: match = v >= dblLo[1]; break; case PRED_NEQ: match = v != dblLo[1]; break; case PRED_NOT_RANGE: match = v < dblLo[1] || v > dblHi[1]; break; } }
                if (numDblPreds > 2 && match) { double v = da2[i]; switch(dt2) { case PRED_RANGE: match = v >= dblLo[2] && v <= dblHi[2]; break; case PRED_LT: match = v < dblHi[2]; break; case PRED_GT: match = v > dblLo[2]; break; case PRED_EQ: match = v == dblLo[2]; break; case PRED_LTE: match = v <= dblHi[2]; break; case PRED_GTE: match = v >= dblLo[2]; break; case PRED_NEQ: match = v != dblLo[2]; break; case PRED_NOT_RANGE: match = v < dblLo[2] || v > dblHi[2]; break; } }
                if (numDblPreds > 3 && match) { double v = da3[i]; switch(dt3) { case PRED_RANGE: match = v >= dblLo[3] && v <= dblHi[3]; break; case PRED_LT: match = v < dblHi[3]; break; case PRED_GT: match = v > dblLo[3]; break; case PRED_EQ: match = v == dblLo[3]; break; case PRED_LTE: match = v <= dblHi[3]; break; case PRED_GTE: match = v >= dblLo[3]; break; case PRED_NEQ: match = v != dblLo[3]; break; case PRED_NOT_RANGE: match = v < dblLo[3] || v > dblHi[3]; break; } }
                if (match) {
                    switch (aggType) {
                        case AGG_SUM_PRODUCT: chunkRev += aa1[i] * aa2[i]; break;
                        case AGG_SUM: chunkRev += aa1[i]; break;
                        case AGG_MIN: chunkRev = Math.min(chunkRev, aa1[i]); break;
                        case AGG_MAX: chunkRev = Math.max(chunkRev, aa1[i]); break;
                        case AGG_COUNT: break;
                    }
                    matchCount++;
                }
            }

            // Merge chunk result into cross-chunk accumulator
            switch (aggType) {
                case AGG_SUM_PRODUCT: case AGG_SUM: revenue += chunkRev; break;
                case AGG_MIN: revenue = Math.min(revenue, chunkRev); break;
                case AGG_MAX: revenue = Math.max(revenue, chunkRev); break;
            }
        }

        return new double[] { revenue, (double) matchCount };
    }

    // =========================================================================
    // JIT-isolated chunked COUNT (avoids fusedSimdChunkBatch aggType interference)
    // =========================================================================

    /**
     * COUNT-only chunked streaming over index chunks.
     * JIT-isolated from fusedSimdChunkBatch to prevent UnreachedCode deoptimization
     * when SUM_PRODUCT warmup marks COUNT branch as unreached.
     */
    public static double[] fusedSimdChunkedCountParallel(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int[] chunkLengths, int nChunks) {

        if (nChunks <= 0) return new double[] { 0.0, 0.0 };

        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        int nThreads = (totalRows < ColumnOps.PARALLEL_THRESHOLD) ? 1
            : Math.min(POOL.getParallelism(), Math.max(1, nChunks / 8));
        if (nThreads <= 1) {
            long cnt = fusedSimdChunkBatchCount(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    chunkLengths, 0, nChunks);
            return new double[] { 0.0, (double) cnt };
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<Long>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedSimdChunkBatchCount(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                        numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                        chunkLengths, startChunk, endChunk));
        }

        long count = 0;
        try {
            for (Future<Long> f : futures) count += f.get();
        } catch (Exception e) {
            throw new RuntimeException("Chunked count parallel execution failed", e);
        }
        return new double[] { 0.0, (double) count };
    }

    /**
     * COUNT-only batch: evaluate predicates + count matches across chunk range.
     * No agg columns needed, no aggType switch — compact for JIT optimization.
     */
    private static long fusedSimdChunkBatchCount(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
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
        boolean[] maskBits = new boolean[LONG_LANES];

        for (int c = startChunk; c < endChunk; c++) {
            int length = chunkLengths[c];
            final long[] la0 = numLongPreds > 0 ? longPredArrs[0][c] : null;
            final long[] la1 = numLongPreds > 1 ? longPredArrs[1][c] : null;
            final long[] la2 = numLongPreds > 2 ? longPredArrs[2][c] : null;
            final long[] la3 = numLongPreds > 3 ? longPredArrs[3][c] : null;
            final double[] da0 = numDblPreds > 0 ? dblPredArrs[0][c] : null;
            final double[] da1 = numDblPreds > 1 ? dblPredArrs[1][c] : null;
            final double[] da2 = numDblPreds > 2 ? dblPredArrs[2][c] : null;
            final double[] da3 = numDblPreds > 3 ? dblPredArrs[3][c] : null;

            int upperBound = length - (length % LONG_LANES);

            // SIMD inner loop — COUNT only, no agg columns
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

            // Scalar tail — inline predicate evaluation
            for (int i = upperBound; i < length; i++) {
                boolean match = true;
                if (numLongPreds > 0 && match) { long v = la0[i]; match = evalLongPred(v, lt0, longLo[0], longHi[0]); }
                if (numLongPreds > 1 && match) { long v = la1[i]; match = evalLongPred(v, lt1, longLo[1], longHi[1]); }
                if (numLongPreds > 2 && match) { long v = la2[i]; match = evalLongPred(v, lt2, longLo[2], longHi[2]); }
                if (numLongPreds > 3 && match) { long v = la3[i]; match = evalLongPred(v, lt3, longLo[3], longHi[3]); }
                if (numDblPreds > 0 && match) { double v = da0[i]; match = evalDoublePred(v, dt0, dblLo[0], dblHi[0]); }
                if (numDblPreds > 1 && match) { double v = da1[i]; match = evalDoublePred(v, dt1, dblLo[1], dblHi[1]); }
                if (numDblPreds > 2 && match) { double v = da2[i]; match = evalDoublePred(v, dt2, dblLo[2], dblHi[2]); }
                if (numDblPreds > 3 && match) { double v = da3[i]; match = evalDoublePred(v, dt3, dblLo[3], dblHi[3]); }
                if (match) matchCount++;
            }
        }

        return matchCount;
    }

    // Scalar pred helpers for tail processing
    private static boolean evalLongPred(long v, int type, long lo, long hi) {
        switch (type) {
            case PRED_RANGE: return v >= lo && v <= hi;
            case PRED_LT:    return v < hi;
            case PRED_GT:    return v > lo;
            case PRED_EQ:    return v == lo;
            case PRED_LTE:   return v <= hi;
            case PRED_GTE:   return v >= lo;
            case PRED_NEQ:   return v != lo;
            case PRED_NOT_RANGE: return v < lo || v > hi;
            default: return true;
        }
    }

    private static boolean evalDoublePred(double v, int type, double lo, double hi) {
        switch (type) {
            case PRED_RANGE: return v >= lo && v <= hi;
            case PRED_LT:    return v < hi;
            case PRED_GT:    return v > lo;
            case PRED_EQ:    return v == lo;
            case PRED_LTE:   return v <= hi;
            case PRED_GTE:   return v >= lo;
            case PRED_NEQ:   return v != lo;
            case PRED_NOT_RANGE: return v < lo || v > hi;
            default: return true;
        }
    }
}
