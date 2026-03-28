package stratum.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import jdk.incubator.vector.*;

/**
 * Chunked SIMD filter+aggregate for long[] aggregate columns.
 * JIT-isolated from ColumnOpsChunkedSimd (double path) to prevent
 * cross-method JIT interference.
 *
 * Mirrors ColumnOpsChunkedSimd.fusedSimdChunkedParallel but accumulates
 * into long accumulators (LongVector) instead of double. This avoids
 * the longToDouble conversion per chunk and preserves integer precision.
 *
 * Supported agg types: SUM, COUNT, MIN, MAX.
 * SUM_PRODUCT not supported (requires double multiplication).
 */
public final class ColumnOpsChunkedLong {

    private static ForkJoinPool POOL = ColumnOps.POOL;

    private static final int AGG_SUM   = ColumnOps.AGG_SUM;
    private static final int AGG_COUNT = ColumnOps.AGG_COUNT;
    private static final int AGG_MIN   = ColumnOps.AGG_MIN;
    private static final int AGG_MAX   = ColumnOps.AGG_MAX;

    private static final VectorSpecies<Long> LONG_SPECIES = ColumnOps.LONG_SPECIES;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = ColumnOps.DOUBLE_SPECIES;
    private static final int LONG_LANES = ColumnOps.LONG_LANES;

    private static final int PRED_RANGE     = ColumnOps.PRED_RANGE;
    private static final int PRED_LT        = ColumnOps.PRED_LT;
    private static final int PRED_GT        = ColumnOps.PRED_GT;
    private static final int PRED_EQ        = ColumnOps.PRED_EQ;
    private static final int PRED_LTE       = ColumnOps.PRED_LTE;
    private static final int PRED_GTE       = ColumnOps.PRED_GTE;
    private static final int PRED_NEQ       = ColumnOps.PRED_NEQ;
    private static final int PRED_NOT_RANGE = ColumnOps.PRED_NOT_RANGE;

    // JIT-isolated predicate helpers (duplicated for JIT isolation)
    private static VectorMask<Long> applyLongPred(
            VectorMask<Long> mask, LongVector v,
            int type, LongVector loVec, LongVector hiVec) {
        switch (type) {
            case PRED_RANGE:     return mask.and(v.compare(VectorOperators.GE, loVec).and(v.compare(VectorOperators.LE, hiVec)));
            case PRED_LT:        return mask.and(v.compare(VectorOperators.LT, hiVec));
            case PRED_GT:        return mask.and(v.compare(VectorOperators.GT, loVec));
            case PRED_EQ:        return mask.and(v.compare(VectorOperators.EQ, loVec));
            case PRED_LTE:       return mask.and(v.compare(VectorOperators.LT, hiVec).or(v.compare(VectorOperators.EQ, hiVec)));
            case PRED_GTE:       return mask.and(v.compare(VectorOperators.GE, loVec));
            case PRED_NEQ:       return mask.and(v.compare(VectorOperators.NE, loVec));
            case PRED_NOT_RANGE: return mask.and(v.compare(VectorOperators.LT, loVec).or(v.compare(VectorOperators.GT, hiVec)));
            default:             return mask;
        }
    }

    private static VectorMask<Double> applyDoublePred(
            VectorMask<Double> mask, DoubleVector v,
            int type, DoubleVector loVec, DoubleVector hiVec) {
        switch (type) {
            case PRED_RANGE:     return mask.and(v.compare(VectorOperators.GE, loVec).and(v.compare(VectorOperators.LE, hiVec)));
            case PRED_LT:        return mask.and(v.compare(VectorOperators.LT, hiVec));
            case PRED_GT:        return mask.and(v.compare(VectorOperators.GT, loVec));
            case PRED_EQ:        return mask.and(v.compare(VectorOperators.EQ, loVec));
            case PRED_LTE:       return mask.and(v.compare(VectorOperators.LT, hiVec).or(v.compare(VectorOperators.EQ, hiVec)));
            case PRED_GTE:       return mask.and(v.compare(VectorOperators.GE, loVec));
            case PRED_NEQ:       return mask.and(v.compare(VectorOperators.NE, loVec));
            case PRED_NOT_RANGE: return mask.and(v.compare(VectorOperators.LT, loVec).or(v.compare(VectorOperators.GT, hiVec)));
            default:             return mask;
        }
    }

    // =========================================================================
    // Fused Filter+Aggregate — Chunked SIMD with long[] accumulators
    // =========================================================================

    /**
     * Parallel fused filter+aggregate over chunked long[] aggregate arrays.
     * Returns long[2] = {result, count}.
     */
    public static long[] fusedSimdChunkedLongParallel(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int aggType, long[][] aggArr1s,
            int[] chunkLengths, int nChunks) {

        if (nChunks <= 0) return new long[] { 0, 0 };

        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        int nThreads = (totalRows < ColumnOps.PARALLEL_THRESHOLD) ? 1
            : Math.min(POOL.getParallelism(), Math.max(1, nChunks / 8));
        if (nThreads <= 1) {
            return fusedSimdChunkBatchLong(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    aggType, aggArr1s, chunkLengths, 0, nChunks);
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<long[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedSimdChunkBatchLong(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                        numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                        aggType, aggArr1s, chunkLengths, startChunk, endChunk));
        }

        long result = (aggType == AGG_MIN) ? Long.MAX_VALUE
                     : (aggType == AGG_MAX) ? Long.MIN_VALUE : 0;
        long count = 0;
        try {
            for (Future<long[]> f : futures) {
                long[] partial = f.get();
                count += partial[1];
                switch (aggType) {
                    case AGG_SUM: result += partial[0]; break;
                    case AGG_MIN: result = Math.min(result, partial[0]); break;
                    case AGG_MAX: result = Math.max(result, partial[0]); break;
                    case AGG_COUNT: break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Chunked long parallel execution failed", e);
        }
        return new long[] { result, count };
    }

    /**
     * Process a batch of chunks with long[] aggregate accumulation.
     * SIMD predicates use the same LongVector/DoubleVector filter as the double path.
     * Aggregate accumulation uses LongVector for native long arithmetic.
     */
    private static long[] fusedSimdChunkBatchLong(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int aggType, long[][] aggArr1s,
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

        // Cross-chunk long accumulators
        long revenue = (aggType == AGG_MIN) ? Long.MAX_VALUE
                      : (aggType == AGG_MAX) ? Long.MIN_VALUE : 0;
        long matchCount = 0;

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
            final long[] aa1 = (aggType != AGG_COUNT) ? aggArr1s[c] : null;

            int upperBound = length - (length % LONG_LANES);

            // Per-chunk SIMD long accumulator
            LongVector accVec;
            if (aggType == AGG_MIN)      accVec = LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE);
            else if (aggType == AGG_MAX) accVec = LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE);
            else                         accVec = LongVector.zero(LONG_SPECIES);

            // SIMD inner loop — predicates on long/double, aggregate on long
            for (int i = 0; i < upperBound; i += LONG_LANES) {
                VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
                if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

                // Double predicates: need mask conversion Long→Double
                if (numDblPreds > 0) {
                    boolean[] maskBits = new boolean[LONG_LANES];
                    for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
                    VectorMask<Double> dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);
                    if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                    // Convert double mask back to long mask for accumulation
                    for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = dm.laneIsSet(lane);
                    lm = VectorMask.fromArray(LONG_SPECIES, maskBits, 0);
                }

                matchCount += lm.trueCount();
                if (aggType == AGG_SUM)      { accVec = accVec.add(LongVector.fromArray(LONG_SPECIES, aa1, i), lm); }
                else if (aggType == AGG_MIN) { LongVector a = LongVector.fromArray(LONG_SPECIES, aa1, i); accVec = accVec.min(LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE).blend(a, lm)); }
                else if (aggType == AGG_MAX) { LongVector a = LongVector.fromArray(LONG_SPECIES, aa1, i); accVec = accVec.max(LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE).blend(a, lm)); }
            }

            // Horizontal reduction
            long chunkVal;
            if (aggType == AGG_MIN)      chunkVal = accVec.reduceLanes(VectorOperators.MIN);
            else if (aggType == AGG_MAX) chunkVal = accVec.reduceLanes(VectorOperators.MAX);
            else                         chunkVal = accVec.reduceLanes(VectorOperators.ADD);

            // Scalar tail
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
                        case AGG_SUM: chunkVal += aa1[i]; break;
                        case AGG_MIN: chunkVal = Math.min(chunkVal, aa1[i]); break;
                        case AGG_MAX: chunkVal = Math.max(chunkVal, aa1[i]); break;
                        case AGG_COUNT: break;
                    }
                    matchCount++;
                }
            }

            // Merge chunk result into cross-chunk accumulator
            switch (aggType) {
                case AGG_SUM: revenue += chunkVal; break;
                case AGG_MIN: revenue = Math.min(revenue, chunkVal); break;
                case AGG_MAX: revenue = Math.max(revenue, chunkVal); break;
            }
        }

        return new long[] { revenue, matchCount };
    }

    // =========================================================================
    // Multi-Agg Chunked SIMD — process up to 4 aggs in one pass, no materialization
    // =========================================================================

    /**
     * Parallel fused filter + multi-aggregate over chunked long[] arrays.
     * Evaluates predicates once per vector and accumulates into multiple
     * long accumulators (one per agg). Avoids N separate passes and
     * avoids materializing index columns into flat arrays.
     *
     * aggChunkArrays: [numAggs][nChunks] → long[] per chunk per agg.
     *   Multiple aggs on the same column can share the same chunk array.
     * Returns long[numAggs * 2] = {result0, count0, result1, count1, ...}.
     */
    public static long[] fusedSimdChunkedMultiLongParallel(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numAggs, int[] aggTypes, long[][][] aggChunkArrays,
            int[] chunkLengths, int nChunks) {

        if (nChunks <= 0) {
            long[] empty = new long[numAggs * 2];
            return empty;
        }

        int totalRows = 0;
        for (int i = 0; i < nChunks; i++) totalRows += chunkLengths[i];

        int nThreads = (totalRows < ColumnOps.PARALLEL_THRESHOLD) ? 1
            : Math.min(POOL.getParallelism(), Math.max(1, nChunks / 8));
        if (nThreads <= 1) {
            return fusedSimdChunkBatchMultiLong(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                    numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                    numAggs, aggTypes, aggChunkArrays, chunkLengths, 0, nChunks);
        }

        int batchSize = nChunks / nThreads;
        @SuppressWarnings("unchecked")
        Future<long[]>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int startChunk = t * batchSize;
            final int endChunk = (t == nThreads - 1) ? nChunks : startChunk + batchSize;
            futures[t] = POOL.submit(() ->
                fusedSimdChunkBatchMultiLong(numLongPreds, longPredTypes, longPredArrs, longLo, longHi,
                        numDblPreds, dblPredTypes, dblPredArrs, dblLo, dblHi,
                        numAggs, aggTypes, aggChunkArrays, chunkLengths, startChunk, endChunk));
        }

        long[] merged = new long[numAggs * 2];
        // Init MIN/MAX sentinels
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) merged[a * 2] = Long.MAX_VALUE;
            else if (aggTypes[a] == AGG_MAX) merged[a * 2] = Long.MIN_VALUE;
        }
        try {
            for (Future<long[]> f : futures) {
                long[] partial = f.get();
                for (int a = 0; a < numAggs; a++) {
                    int off = a * 2;
                    merged[off + 1] += partial[off + 1]; // count
                    switch (aggTypes[a]) {
                        case AGG_SUM: merged[off] += partial[off]; break;
                        case AGG_MIN: merged[off] = Math.min(merged[off], partial[off]); break;
                        case AGG_MAX: merged[off] = Math.max(merged[off], partial[off]); break;
                        case AGG_COUNT: break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Chunked multi-long parallel execution failed", e);
        }
        return merged;
    }

    private static long[] fusedSimdChunkBatchMultiLong(
            int numLongPreds, int[] longPredTypes,
            long[][][] longPredArrs, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][][] dblPredArrs, double[] dblLo, double[] dblHi,
            int numAggs, int[] aggTypes, long[][][] aggChunkArrays,
            int[] chunkLengths, int startChunk, int endChunk) {

        // Broadcast SIMD pred constants ONCE
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

        // Pre-extract agg types for up to 4 aggs
        final int at0 = numAggs > 0 ? aggTypes[0] : -1;
        final int at1 = numAggs > 1 ? aggTypes[1] : -1;
        final int at2 = numAggs > 2 ? aggTypes[2] : -1;
        final int at3 = numAggs > 3 ? aggTypes[3] : -1;

        // Cross-chunk accumulators (flat: result0, count, result1, count, ...)
        long[] accs = new long[numAggs * 2];
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Long.MAX_VALUE;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Long.MIN_VALUE;
        }

        for (int c = startChunk; c < endChunk; c++) {
            int length = chunkLengths[c];

            // Extract pred chunk arrays
            final long[] la0 = numLongPreds > 0 ? longPredArrs[0][c] : null;
            final long[] la1 = numLongPreds > 1 ? longPredArrs[1][c] : null;
            final long[] la2 = numLongPreds > 2 ? longPredArrs[2][c] : null;
            final long[] la3 = numLongPreds > 3 ? longPredArrs[3][c] : null;
            final double[] da0 = numDblPreds > 0 ? dblPredArrs[0][c] : null;
            final double[] da1 = numDblPreds > 1 ? dblPredArrs[1][c] : null;
            final double[] da2 = numDblPreds > 2 ? dblPredArrs[2][c] : null;
            final double[] da3 = numDblPreds > 3 ? dblPredArrs[3][c] : null;

            // Extract agg chunk arrays (up to 4)
            final long[] aa0 = numAggs > 0 && at0 != AGG_COUNT ? aggChunkArrays[0][c] : null;
            final long[] aa1 = numAggs > 1 && at1 != AGG_COUNT ? aggChunkArrays[1][c] : null;
            final long[] aa2 = numAggs > 2 && at2 != AGG_COUNT ? aggChunkArrays[2][c] : null;
            final long[] aa3 = numAggs > 3 && at3 != AGG_COUNT ? aggChunkArrays[3][c] : null;

            int upperBound = length - (length % LONG_LANES);

            // Per-chunk SIMD accumulators (one per agg)
            LongVector av0 = (at0 == AGG_MIN) ? LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE) : (at0 == AGG_MAX) ? LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE) : LongVector.zero(LONG_SPECIES);
            LongVector av1 = numAggs > 1 ? ((at1 == AGG_MIN) ? LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE) : (at1 == AGG_MAX) ? LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE) : LongVector.zero(LONG_SPECIES)) : null;
            LongVector av2 = numAggs > 2 ? ((at2 == AGG_MIN) ? LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE) : (at2 == AGG_MAX) ? LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE) : LongVector.zero(LONG_SPECIES)) : null;
            LongVector av3 = numAggs > 3 ? ((at3 == AGG_MIN) ? LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE) : (at3 == AGG_MAX) ? LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE) : LongVector.zero(LONG_SPECIES)) : null;
            long chunkCount = 0;

            // SIMD inner loop — predicates evaluated ONCE, all aggs accumulated
            for (int i = 0; i < upperBound; i += LONG_LANES) {
                VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
                if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, la3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

                if (numDblPreds > 0) {
                    boolean[] maskBits = new boolean[LONG_LANES];
                    for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane);
                    VectorMask<Double> dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0);
                    if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, da3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                    for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = dm.laneIsSet(lane);
                    lm = VectorMask.fromArray(LONG_SPECIES, maskBits, 0);
                }

                chunkCount += lm.trueCount();

                // Accumulate all aggs with shared mask
                if (at0 == AGG_SUM)      { av0 = av0.add(LongVector.fromArray(LONG_SPECIES, aa0, i), lm); }
                else if (at0 == AGG_MIN) { av0 = av0.min(LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa0, i), lm)); }
                else if (at0 == AGG_MAX) { av0 = av0.max(LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa0, i), lm)); }

                if (numAggs > 1) {
                    if (at1 == AGG_SUM)      { av1 = av1.add(LongVector.fromArray(LONG_SPECIES, aa1, i), lm); }
                    else if (at1 == AGG_MIN) { av1 = av1.min(LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa1, i), lm)); }
                    else if (at1 == AGG_MAX) { av1 = av1.max(LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa1, i), lm)); }
                }
                if (numAggs > 2) {
                    if (at2 == AGG_SUM)      { av2 = av2.add(LongVector.fromArray(LONG_SPECIES, aa2, i), lm); }
                    else if (at2 == AGG_MIN) { av2 = av2.min(LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa2, i), lm)); }
                    else if (at2 == AGG_MAX) { av2 = av2.max(LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa2, i), lm)); }
                }
                if (numAggs > 3) {
                    if (at3 == AGG_SUM)      { av3 = av3.add(LongVector.fromArray(LONG_SPECIES, aa3, i), lm); }
                    else if (at3 == AGG_MIN) { av3 = av3.min(LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa3, i), lm)); }
                    else if (at3 == AGG_MAX) { av3 = av3.max(LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE).blend(LongVector.fromArray(LONG_SPECIES, aa3, i), lm)); }
                }
            }

            // Horizontal reductions
            long cv0 = (at0 == AGG_MIN) ? av0.reduceLanes(VectorOperators.MIN) : (at0 == AGG_MAX) ? av0.reduceLanes(VectorOperators.MAX) : av0.reduceLanes(VectorOperators.ADD);
            long cv1 = numAggs > 1 ? ((at1 == AGG_MIN) ? av1.reduceLanes(VectorOperators.MIN) : (at1 == AGG_MAX) ? av1.reduceLanes(VectorOperators.MAX) : av1.reduceLanes(VectorOperators.ADD)) : 0;
            long cv2 = numAggs > 2 ? ((at2 == AGG_MIN) ? av2.reduceLanes(VectorOperators.MIN) : (at2 == AGG_MAX) ? av2.reduceLanes(VectorOperators.MAX) : av2.reduceLanes(VectorOperators.ADD)) : 0;
            long cv3 = numAggs > 3 ? ((at3 == AGG_MIN) ? av3.reduceLanes(VectorOperators.MIN) : (at3 == AGG_MAX) ? av3.reduceLanes(VectorOperators.MAX) : av3.reduceLanes(VectorOperators.ADD)) : 0;

            // Scalar tail
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
                    chunkCount++;
                    if (at0 == AGG_SUM) cv0 += aa0[i]; else if (at0 == AGG_MIN) cv0 = Math.min(cv0, aa0[i]); else if (at0 == AGG_MAX) cv0 = Math.max(cv0, aa0[i]);
                    if (numAggs > 1) { if (at1 == AGG_SUM) cv1 += aa1[i]; else if (at1 == AGG_MIN) cv1 = Math.min(cv1, aa1[i]); else if (at1 == AGG_MAX) cv1 = Math.max(cv1, aa1[i]); }
                    if (numAggs > 2) { if (at2 == AGG_SUM) cv2 += aa2[i]; else if (at2 == AGG_MIN) cv2 = Math.min(cv2, aa2[i]); else if (at2 == AGG_MAX) cv2 = Math.max(cv2, aa2[i]); }
                    if (numAggs > 3) { if (at3 == AGG_SUM) cv3 += aa3[i]; else if (at3 == AGG_MIN) cv3 = Math.min(cv3, aa3[i]); else if (at3 == AGG_MAX) cv3 = Math.max(cv3, aa3[i]); }
                }
            }

            // Merge chunk results into cross-chunk accumulators
            accs[1] += chunkCount; // count for agg 0
            if (at0 == AGG_SUM) accs[0] += cv0; else if (at0 == AGG_MIN) accs[0] = Math.min(accs[0], cv0); else if (at0 == AGG_MAX) accs[0] = Math.max(accs[0], cv0);
            if (numAggs > 1) { accs[3] += chunkCount; if (at1 == AGG_SUM) accs[2] += cv1; else if (at1 == AGG_MIN) accs[2] = Math.min(accs[2], cv1); else if (at1 == AGG_MAX) accs[2] = Math.max(accs[2], cv1); }
            if (numAggs > 2) { accs[5] += chunkCount; if (at2 == AGG_SUM) accs[4] += cv2; else if (at2 == AGG_MIN) accs[4] = Math.min(accs[4], cv2); else if (at2 == AGG_MAX) accs[4] = Math.max(accs[4], cv2); }
            if (numAggs > 3) { accs[7] += chunkCount; if (at3 == AGG_SUM) accs[6] += cv3; else if (at3 == AGG_MIN) accs[6] = Math.min(accs[6], cv3); else if (at3 == AGG_MAX) accs[6] = Math.max(accs[6], cv3); }
        }

        return accs;
    }
}
