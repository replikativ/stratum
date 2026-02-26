package stratum.internal;

import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated columnar operations for Stratum.
 *
 * <p>Following the same pattern as Proximum's Distance.java:
 * all methods are static and final for maximum JIT inlining.
 * Uses JDK Vector API (incubator) for SIMD acceleration.
 *
 * <p>Key operations:
 * <ul>
 *   <li>Range filter: scan column, collect matching indices</li>
 *   <li>Comparison filter: &lt;, &gt;, &lt;=, &gt;= with SIMD</li>
 *   <li>Aggregation: SUM, COUNT with SIMD reduction</li>
 *   <li>Combined filter+aggregate: filter and aggregate in one pass</li>
 * </ul>
 *
 * <p><b>Internal API</b> - subject to change without notice.
 */
public final class ColumnOps {

    private ColumnOps() {} // Prevent instantiation

    // =========================================================================
    // SIMD Setup
    // =========================================================================

    static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;
    static final int DOUBLE_LANES = DOUBLE_SPECIES.length();

    static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    static final int LONG_LANES = LONG_SPECIES.length();

    // =========================================================================
    // Range Filter (double) - SIMD accelerated
    // =========================================================================

    /**
     * Filter a double array for values in [lo, hi), returning matching indices.
     *
     * <p>Uses SIMD comparisons to test multiple values simultaneously.
     * On AVX-512: 8 doubles per cycle. On AVX-2: 4 doubles per cycle.
     *
     * @param data    The source data array
     * @param offset  Start offset in data array
     * @param length  Number of elements to scan
     * @param lo      Lower bound (inclusive)
     * @param hi      Upper bound (exclusive)
     * @param result  Output array for matching indices (must be large enough)
     * @param baseIdx Base index to add to result indices
     * @return Number of matching indices written to result
     */
    public static int filterRangeDouble(double[] data, int offset, int length,
                                         double lo, double hi,
                                         int[] result, int baseIdx) {
        int resultCount = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % DOUBLE_LANES));

        // Broadcast constants for SIMD comparison
        DoubleVector loVec = DoubleVector.broadcast(DOUBLE_SPECIES, lo);
        DoubleVector hiVec = DoubleVector.broadcast(DOUBLE_SPECIES, hi);

        // SIMD loop: process DOUBLE_LANES values at once
        for (int i = offset; i < upperBound; i += DOUBLE_LANES) {
            DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, i);

            // SIMD comparison: (v >= lo) AND (v < hi)
            VectorMask<Double> geLo = v.compare(VectorOperators.GE, loVec);
            VectorMask<Double> ltHi = v.compare(VectorOperators.LT, hiVec);
            VectorMask<Double> inRange = geLo.and(ltHi);

            // Extract matching indices
            if (inRange.anyTrue()) {
                for (int lane = 0; lane < DOUBLE_LANES; lane++) {
                    if (inRange.laneIsSet(lane)) {
                        result[resultCount++] = baseIdx + (i - offset) + lane;
                    }
                }
            }
        }

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            double v = data[i];
            if (v >= lo && v < hi) {
                result[resultCount++] = baseIdx + (i - offset);
            }
        }

        return resultCount;
    }

    /**
     * Filter a double array for values < threshold.
     */
    public static int filterLtDouble(double[] data, int offset, int length,
                                      double threshold,
                                      int[] result, int baseIdx) {
        int resultCount = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % DOUBLE_LANES));

        DoubleVector threshVec = DoubleVector.broadcast(DOUBLE_SPECIES, threshold);

        for (int i = offset; i < upperBound; i += DOUBLE_LANES) {
            DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, i);
            VectorMask<Double> lt = v.compare(VectorOperators.LT, threshVec);

            if (lt.anyTrue()) {
                for (int lane = 0; lane < DOUBLE_LANES; lane++) {
                    if (lt.laneIsSet(lane)) {
                        result[resultCount++] = baseIdx + (i - offset) + lane;
                    }
                }
            }
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] < threshold) {
                result[resultCount++] = baseIdx + (i - offset);
            }
        }

        return resultCount;
    }

    /**
     * Filter a double array for values > threshold.
     */
    public static int filterGtDouble(double[] data, int offset, int length,
                                      double threshold,
                                      int[] result, int baseIdx) {
        int resultCount = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % DOUBLE_LANES));

        DoubleVector threshVec = DoubleVector.broadcast(DOUBLE_SPECIES, threshold);

        for (int i = offset; i < upperBound; i += DOUBLE_LANES) {
            DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, i);
            VectorMask<Double> gt = v.compare(VectorOperators.GT, threshVec);

            if (gt.anyTrue()) {
                for (int lane = 0; lane < DOUBLE_LANES; lane++) {
                    if (gt.laneIsSet(lane)) {
                        result[resultCount++] = baseIdx + (i - offset) + lane;
                    }
                }
            }
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] > threshold) {
                result[resultCount++] = baseIdx + (i - offset);
            }
        }

        return resultCount;
    }

    // =========================================================================
    // Long Filter Operations - SIMD accelerated
    // =========================================================================

    /**
     * Filter a long array for values in [lo, hi).
     */
    public static int filterRangeLong(long[] data, int offset, int length,
                                       long lo, long hi,
                                       int[] result, int baseIdx) {
        int resultCount = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % LONG_LANES));

        LongVector loVec = LongVector.broadcast(LONG_SPECIES, lo);
        LongVector hiVec = LongVector.broadcast(LONG_SPECIES, hi);

        for (int i = offset; i < upperBound; i += LONG_LANES) {
            LongVector v = LongVector.fromArray(LONG_SPECIES, data, i);

            VectorMask<Long> geLo = v.compare(VectorOperators.GE, loVec);
            VectorMask<Long> ltHi = v.compare(VectorOperators.LT, hiVec);
            VectorMask<Long> inRange = geLo.and(ltHi);

            if (inRange.anyTrue()) {
                for (int lane = 0; lane < LONG_LANES; lane++) {
                    if (inRange.laneIsSet(lane)) {
                        result[resultCount++] = baseIdx + (i - offset) + lane;
                    }
                }
            }
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] >= lo && data[i] < hi) {
                result[resultCount++] = baseIdx + (i - offset);
            }
        }

        return resultCount;
    }

    /**
     * Filter a long array for values < threshold.
     */
    public static int filterLtLong(long[] data, int offset, int length,
                                    long threshold,
                                    int[] result, int baseIdx) {
        int resultCount = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % LONG_LANES));

        LongVector threshVec = LongVector.broadcast(LONG_SPECIES, threshold);

        for (int i = offset; i < upperBound; i += LONG_LANES) {
            LongVector v = LongVector.fromArray(LONG_SPECIES, data, i);
            VectorMask<Long> lt = v.compare(VectorOperators.LT, threshVec);

            if (lt.anyTrue()) {
                for (int lane = 0; lane < LONG_LANES; lane++) {
                    if (lt.laneIsSet(lane)) {
                        result[resultCount++] = baseIdx + (i - offset) + lane;
                    }
                }
            }
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] < threshold) {
                result[resultCount++] = baseIdx + (i - offset);
            }
        }

        return resultCount;
    }

    // =========================================================================
    // SIMD Aggregation Operations
    // =========================================================================

    /**
     * Sum all doubles in an array using SIMD reduction.
     *
     * <p>Processes DOUBLE_LANES values per cycle, with scalar tail.
     * Horizontal reduction at end.
     */
    public static double sumDouble(double[] data, int offset, int length) {
        int end = offset + length;
        int upperBound = offset + (length - (length % DOUBLE_LANES));

        DoubleVector sumVec = DoubleVector.zero(DOUBLE_SPECIES);
        for (int i = offset; i < upperBound; i += DOUBLE_LANES) {
            DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, i);
            sumVec = sumVec.add(v);
        }

        double sum = sumVec.reduceLanes(VectorOperators.ADD);

        // Scalar tail
        for (int i = upperBound; i < end; i++) {
            sum += data[i];
        }

        return sum;
    }

    /**
     * Sum of (a[i] * b[i]) for matching selection indices - the TPC-H Q06 pattern.
     *
     * <p>This is the critical aggregation: sum(price * discount) for selected rows.
     * Gathers from two arrays using selection indices, multiplies, and sums.
     *
     * @param prices     Price array
     * @param discounts  Discount array
     * @param selection  Array of selected indices
     * @param selCount   Number of valid entries in selection
     * @return sum(prices[sel[i]] * discounts[sel[i]])
     */
    public static double sumProductGather(double[] prices, double[] discounts,
                                           int[] selection, int selCount) {
        double sum = 0.0;
        // Gather is hard to SIMD (non-contiguous access), use scalar loop
        // JIT will handle loop unrolling and strength reduction
        for (int i = 0; i < selCount; i++) {
            int idx = selection[i];
            sum += prices[idx] * discounts[idx];
        }
        return sum;
    }

    // =========================================================================
    // Combined Filter+Count (no allocation)
    // =========================================================================

    /**
     * Count doubles in range [lo, hi) using SIMD.
     * No result array needed - just counts.
     */
    public static int countRangeDouble(double[] data, int offset, int length,
                                        double lo, double hi) {
        int count = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % DOUBLE_LANES));

        DoubleVector loVec = DoubleVector.broadcast(DOUBLE_SPECIES, lo);
        DoubleVector hiVec = DoubleVector.broadcast(DOUBLE_SPECIES, hi);

        for (int i = offset; i < upperBound; i += DOUBLE_LANES) {
            DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, i);
            VectorMask<Double> geLo = v.compare(VectorOperators.GE, loVec);
            VectorMask<Double> ltHi = v.compare(VectorOperators.LT, hiVec);
            count += geLo.and(ltHi).trueCount();
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] >= lo && data[i] < hi) count++;
        }

        return count;
    }

    /**
     * Count longs in range [lo, hi) using SIMD.
     */
    public static int countRangeLong(long[] data, int offset, int length,
                                      long lo, long hi) {
        int count = 0;
        int end = offset + length;
        int upperBound = offset + (length - (length % LONG_LANES));

        LongVector loVec = LongVector.broadcast(LONG_SPECIES, lo);
        LongVector hiVec = LongVector.broadcast(LONG_SPECIES, hi);

        for (int i = offset; i < upperBound; i += LONG_LANES) {
            LongVector v = LongVector.fromArray(LONG_SPECIES, data, i);
            VectorMask<Long> geLo = v.compare(VectorOperators.GE, loVec);
            VectorMask<Long> ltHi = v.compare(VectorOperators.LT, hiVec);
            count += geLo.and(ltHi).trueCount();
        }

        for (int i = upperBound; i < end; i++) {
            if (data[i] >= lo && data[i] < hi) count++;
        }

        return count;
    }

    // =========================================================================
    // General Fused Filter+Aggregate (composable, single-pass)
    // =========================================================================

    // Predicate type constants
    public static final int PRED_RANGE = 0;  // >= lo && <= hi
    public static final int PRED_LT    = 1;  // < hi
    public static final int PRED_GT    = 2;  // > lo
    public static final int PRED_EQ    = 3;  // == lo
    public static final int PRED_LTE   = 4;  // <= hi
    public static final int PRED_GTE   = 5;  // >= lo
    public static final int PRED_NEQ   = 6;  // != lo
    public static final int PRED_NOT_RANGE = 7;  // < lo || > hi (complement of RANGE)

    // Aggregation type constants
    public static final int AGG_SUM_PRODUCT = 0;
    public static final int AGG_SUM         = 1;
    public static final int AGG_COUNT       = 2;
    public static final int AGG_MIN         = 3;
    public static final int AGG_MAX         = 4;

    /**
     * General fused filter + aggregate in a single scalar pass.
     *
     * <p>Evaluates all predicates per row with short-circuit evaluation,
     * then aggregates matching rows immediately. No intermediate allocations.
     *
     * <p>Long predicates are evaluated first (typically more selective),
     * then double predicates — cheaper/more-selective predicates first.
     *
     * @param numLongPreds   Number of long-typed predicates
     * @param longPredTypes  Predicate types (PRED_RANGE, PRED_LT, etc.)
     * @param longCols       Column data arrays for each long predicate
     * @param longLo         Lower bounds for each long predicate
     * @param longHi         Upper bounds for each long predicate
     * @param numDblPreds    Number of double-typed predicates
     * @param dblPredTypes   Predicate types for double columns
     * @param dblCols        Column data arrays for each double predicate
     * @param dblLo          Lower bounds for each double predicate
     * @param dblHi          Upper bounds for each double predicate
     * @param aggType        Aggregation type (AGG_SUM_PRODUCT, AGG_SUM, AGG_COUNT)
     * @param aggCol1        First aggregation column (null for AGG_COUNT)
     * @param aggCol2        Second aggregation column (for AGG_SUM_PRODUCT)
     * @param length         Number of rows
     * @return double[2]: [0] = aggregate result, [1] = match count
     */
    public static double[] fusedFilterAggregate(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
            int length) {

        double result = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                      : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY
                      : 0.0;
        int matchCount = 0;

        for (int i = 0; i < length; i++) {
            if (evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                   numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                switch (aggType) {
                    case AGG_SUM_PRODUCT: result += aggCol1[i] * aggCol2[i]; break;
                    case AGG_SUM:         result += aggCol1[i]; break;
                    case AGG_MIN:         result = Math.min(result, aggCol1[i]); break;
                    case AGG_MAX:         result = Math.max(result, aggCol1[i]); break;
                    case AGG_COUNT:       break;
                }
                matchCount++;
            }
        }

        return new double[] { result, (double) matchCount };
    }

    // =========================================================================
    // Unrolled Fused SIMD (optimized for JIT inlining)
    // =========================================================================

    /**
     * Apply a single long predicate to a SIMD mask.
     * Private static for JIT inlining.
     */
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

    /**
     * Apply a single double predicate to a SIMD mask.
     * Private static for JIT inlining.
     */
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

    /**
     * Evaluate all predicates for a single row using looped switch.
     * Used by scalar tails and group-by methods. Private static for JIT inlining.
     */
    static boolean evaluatePredicates(
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
     * Initialize group-by accumulators with correct starting values.
     * MIN starts at +Infinity, MAX at -Infinity, others at 0.
     */
    private static void initAccumulators(double[] accs, int[] aggTypes, int numAggs) {
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) accs[a * 2] = Double.POSITIVE_INFINITY;
            else if (aggTypes[a] == AGG_MAX) accs[a * 2] = Double.NEGATIVE_INFINITY;
        }
    }

    /**
     * Optimized fused SIMD with unrolled predicates and direct array access.
     *
     * <p>Supports up to 4 long + 4 double predicates. The predicate loops are
     * unrolled so the JIT can:
     * - Eliminate dead branches (numLongPreds is loop-invariant)
     * - Constant-fold switch on pred type (same type every call)
     * - Access column arrays directly (no 2D array indirection)
     *
     * <p>This achieves near hand-written performance for any query shape.
     */
    public static double[] fusedSimdUnrolled(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
            int length, boolean nanSafe) {
        // Delegate to Range variant so both paths share one JIT compilation unit.
        return fusedSimdUnrolledRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                aggType, aggCol1, aggCol2, 0, length, nanSafe);
    }

    /**
     * Fused SIMD COUNT — separate method for JIT isolation.
     * Delegates to fusedSimdCountRange so both paths share one compilation unit.
     */
    public static double[] fusedSimdCount(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int length) {
        return fusedSimdCountRange(numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, 0, length);
    }

    // =========================================================================
    // Fused Filter + Group-By + Aggregate (single-pass, no Clojure overhead)
    // =========================================================================

    /**
     * Fused filter + group-by + multi-aggregate in a single pass.
     *
     * <p>Evaluates predicates per row (short-circuit), computes composite group key
     * from long columns, and accumulates per-group aggregates using HashMap.
     * All in Java to avoid Clojure per-row function call overhead.
     *
     * <p>Accumulator layout per group: double[numAggs * 2] where
     * [agg0_val, agg0_count, agg1_val, agg1_count, ...].
     * For AVG: compute sum in val slot, finalize as val/count on Clojure side.
     *
     * <p>Expression aggregates (e.g., sum(price * (1-discount))) should be
     * pre-computed into a temp double[] on the Clojure side and passed as AGG_SUM.
     *
     * @param numLongPreds  Number of long-typed predicates
     * @param longPredTypes Predicate types (PRED_RANGE, PRED_LT, etc.)
     * @param longCols      Column data arrays for long predicates
     * @param longLo        Lower bounds for long predicates
     * @param longHi        Upper bounds for long predicates
     * @param numDblPreds   Number of double-typed predicates
     * @param dblPredTypes  Predicate types for double columns
     * @param dblCols       Column data arrays for double predicates
     * @param dblLo         Lower bounds for double predicates
     * @param dblHi         Upper bounds for double predicates
     * @param numGroupCols  Number of group-by columns (all long-typed)
     * @param groupCols     Group-by column data arrays
     * @param groupMuls     Multipliers for composite key encoding
     * @param numAggs       Number of aggregations
     * @param aggTypes      Aggregation types per agg (AGG_SUM, AGG_COUNT, etc.)
     * @param aggCols       Source column (double[]) per agg (null for AGG_COUNT)
     * @param length        Number of rows
     * @return HashMap mapping composite group key to double[numAggs*2] accumulators
     */
    // =========================================================================
    // Open-Addressed Hash Table with Flat Accumulators
    // =========================================================================
    // Uses long[] keys + flat double[] accumulators (contiguous memory).
    // Linear probing with Fibonacci hashing. No Long boxing, no per-group alloc.

    // Sentinel for empty hash table slots. Must not collide with legitimate composite
    // group keys. We avoid Long.MIN_VALUE because gc0[i]*gm0 + ... can produce it.
    // Any legitimate key equal to EMPTY_KEY is remapped to EMPTY_KEY^1 on insert.
    static final long EMPTY_KEY = 0xDEADBEEFDEADBEEFL;

    /** Remap a key to avoid collision with the EMPTY_KEY sentinel. */
    static long safeKey(long key) {
        return key == EMPTY_KEY ? (EMPTY_KEY ^ 1) : key;
    }

    private static int nextPow2(int n) {
        int v = Math.max(16, n);
        v--;
        v |= v >> 1; v |= v >> 2; v |= v >> 4; v |= v >> 8; v |= v >> 16;
        return v + 1;
    }

    private static int hashSlot(long key, int capacity) {
        return (int)((key * 0x9E3779B97F4A7C15L) >>> (64 - Integer.numberOfTrailingZeros(capacity)));
    }

    /**
     * Flat hash table: long[] keys + double[] accs (stride = accSize per slot).
     * Returns Object[] = {long[] keys, double[] accs, int[]{size, capacity, accSize}}.
     */
    private static Object[] groupAggregateHashRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int start, int end, int initCapacity) {

        final int accSize = numAggs * 2;
        int capacity = nextPow2(initCapacity);
        int mask = capacity - 1;
        long[] htKeys = new long[capacity];
        double[] htAccs = new double[capacity * accSize];
        java.util.Arrays.fill(htKeys, EMPTY_KEY);
        // Init MIN/MAX sentinel values for all slots
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int s = 0; s < capacity; s++) htAccs[s * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int s = 0; s < capacity; s++) htAccs[s * accSize + a * 2] = Double.NEGATIVE_INFINITY;
            }
        }
        int size = 0;

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

        rowLoop:
        for (int i = start; i < end; i++) {
            if (!evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i))
                continue;

            long key = gc0[i] * gm0;
            if (numGroupCols > 1) key += gc1[i] * gm1;
            if (numGroupCols > 2) key += gc2[i] * gm2;
            if (numGroupCols > 3) key += gc3[i] * gm3;
            if (numGroupCols > 4) key += gc4[i] * gm4;
            if (numGroupCols > 5) key += gc5[i] * gm5;
            for (int g = 6; g < numGroupCols; g++) {
                key += groupCols[g][i] * groupMuls[g];
            }
            key = safeKey(key);

            int slot = hashSlot(key, capacity) & mask;
            while (htKeys[slot] != EMPTY_KEY && htKeys[slot] != key) {
                slot = (slot + 1) & mask;
            }

            if (htKeys[slot] == EMPTY_KEY) {
                htKeys[slot] = key;
                size++;
                if (size * 10 > capacity * 7) {
                    // Resize
                    int newCap = capacity << 1;
                    int newMask = newCap - 1;
                    long[] newKeys = new long[newCap];
                    double[] newAccs = new double[newCap * accSize];
                    java.util.Arrays.fill(newKeys, EMPTY_KEY);
                    for (int a = 0; a < numAggs; a++) {
                        if (aggTypes[a] == AGG_MIN) {
                            for (int s = 0; s < newCap; s++) newAccs[s * accSize + a * 2] = Double.POSITIVE_INFINITY;
                        } else if (aggTypes[a] == AGG_MAX) {
                            for (int s = 0; s < newCap; s++) newAccs[s * accSize + a * 2] = Double.NEGATIVE_INFINITY;
                        }
                    }
                    for (int s = 0; s < capacity; s++) {
                        if (htKeys[s] != EMPTY_KEY) {
                            int ns = hashSlot(htKeys[s], newCap) & newMask;
                            while (newKeys[ns] != EMPTY_KEY) ns = (ns + 1) & newMask;
                            newKeys[ns] = htKeys[s];
                            System.arraycopy(htAccs, s * accSize, newAccs, ns * accSize, accSize);
                        }
                    }
                    htKeys = newKeys;
                    htAccs = newAccs;
                    capacity = newCap;
                    mask = newMask;
                    slot = hashSlot(key, capacity) & mask;
                    while (htKeys[slot] != key) slot = (slot + 1) & mask;
                }
            }

            int base0 = slot * accSize;
            for (int a = 0; a < numAggs; a++) {
                int base = base0 + a * 2;
                switch (aggTypes[a]) {
                    case AGG_SUM:
                        htAccs[base] += aggCols[a][i];
                        htAccs[base + 1]++;
                        break;
                    case AGG_SUM_PRODUCT:
                        htAccs[base] += aggCols[a][i] * aggCol2s[a][i];
                        htAccs[base + 1]++;
                        break;
                    case AGG_COUNT:
                        htAccs[base + 1]++;
                        break;
                    case AGG_MIN:
                        htAccs[base] = Math.min(htAccs[base], aggCols[a][i]);
                        htAccs[base + 1]++;
                        break;
                    case AGG_MAX:
                        htAccs[base] = Math.max(htAccs[base], aggCols[a][i]);
                        htAccs[base + 1]++;
                        break;
                }
            }
        }

        return new Object[] { htKeys, htAccs, new int[]{size, capacity, accSize} };
    }

    /**
     * Compact flat hash table: extract populated keys + flatten accs contiguously.
     * Returns Object[] = {long[] keys, double[] flatAccs} where flatAccs
     * has stride=accSize: [key0_agg0_sum, key0_agg0_cnt, key0_agg1_sum, ...].
     */
    private static Object[] compactFlatHashTable(long[] htKeys, double[] htAccs, int capacity, int size, int accSize) {
        long[] outKeys = new long[size];
        double[] outAccs = new double[size * accSize];
        int idx = 0;
        for (int s = 0; s < capacity; s++) {
            if (htKeys[s] != EMPTY_KEY) {
                outKeys[idx] = htKeys[s];
                System.arraycopy(htAccs, s * accSize, outAccs, idx * accSize, accSize);
                idx++;
            }
        }
        return new Object[] { outKeys, outAccs };
    }

    /**
     * Merge source flat hash table into target flat hash table.
     * Returns Object[] = {long[] keys, double[] accs, int[]{size, capacity, accSize}}.
     */
    private static Object[] mergeFlatHashTables(
            long[] tKeys, double[] tAccs, int tSize, int tCapacity,
            long[] sKeys, double[] sAccs, int sCapacity,
            int[] aggTypes, int numAggs, int accSize) {
        int mask = tCapacity - 1;

        for (int s = 0; s < sCapacity; s++) {
            if (sKeys[s] == EMPTY_KEY) continue;
            long key = sKeys[s];

            int slot = hashSlot(key, tCapacity) & mask;
            while (tKeys[slot] != EMPTY_KEY && tKeys[slot] != key) {
                slot = (slot + 1) & mask;
            }

            int tBase = slot * accSize;
            int sBase = s * accSize;
            if (tKeys[slot] == EMPTY_KEY) {
                tKeys[slot] = key;
                System.arraycopy(sAccs, sBase, tAccs, tBase, accSize);
                tSize++;
                if (tSize * 10 > tCapacity * 7) {
                    int newCap = tCapacity << 1;
                    int newMask = newCap - 1;
                    long[] newKeys = new long[newCap];
                    double[] newAccs = new double[newCap * accSize];
                    java.util.Arrays.fill(newKeys, EMPTY_KEY);
                    for (int a = 0; a < numAggs; a++) {
                        if (aggTypes[a] == AGG_MIN) {
                            for (int j = 0; j < newCap; j++) newAccs[j * accSize + a * 2] = Double.POSITIVE_INFINITY;
                        } else if (aggTypes[a] == AGG_MAX) {
                            for (int j = 0; j < newCap; j++) newAccs[j * accSize + a * 2] = Double.NEGATIVE_INFINITY;
                        }
                    }
                    for (int j = 0; j < tCapacity; j++) {
                        if (tKeys[j] != EMPTY_KEY) {
                            int ns = hashSlot(tKeys[j], newCap) & newMask;
                            while (newKeys[ns] != EMPTY_KEY) ns = (ns + 1) & newMask;
                            newKeys[ns] = tKeys[j];
                            System.arraycopy(tAccs, j * accSize, newAccs, ns * accSize, accSize);
                        }
                    }
                    tKeys = newKeys;
                    tAccs = newAccs;
                    tCapacity = newCap;
                    mask = newMask;
                }
            } else {
                for (int a = 0; a < numAggs; a++) {
                    int b = a * 2;
                    switch (aggTypes[a]) {
                        case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                            tAccs[tBase + b] += sAccs[sBase + b]; break;
                        case AGG_MIN:
                            tAccs[tBase + b] = Math.min(tAccs[tBase + b], sAccs[sBase + b]); break;
                        case AGG_MAX:
                            tAccs[tBase + b] = Math.max(tAccs[tBase + b], sAccs[sBase + b]); break;
                    }
                    tAccs[tBase + b + 1] += sAccs[sBase + b + 1];
                }
            }
        }
        return new Object[] { tKeys, tAccs, new int[]{tSize, tCapacity, accSize} };
    }

    /**
     * Open-addressed hash group-by (single-threaded, full range).
     * Returns Object[] = {long[] keys, double[][] values} (compacted).
     */
    public static Object[] fusedFilterGroupAggregate(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int length) {

        Object[] ht = groupAggregateHashRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                numAggs, aggTypes, aggCols, aggCol2s, 0, length,
                Math.max(16, length / 4));
        long[] htKeys = (long[]) ht[0];
        double[] htAccs = (double[]) ht[1];
        int[] meta = (int[]) ht[2];
        return compactFlatHashTable(htKeys, htAccs, meta[1], meta[0], meta[2]);
    }

    /**
     * Parallel open-addressed hash group-by with morsel-driven execution.
     * Returns Object[] = {long[] keys, double[][] values} (compacted).
     */
    @SuppressWarnings("unchecked")
    public static Object[] fusedFilterGroupAggregateParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int length) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedFilterGroupAggregate(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCol2s, length);
        }

        // Cap thread count to avoid oversaturation on large core counts with small data
        int nThreads = Math.min(POOL.getParallelism(), length / MORSEL_SIZE);
        nThreads = Math.max(nThreads, 2); // at least 2 threads above threshold
        int threadRange = (length + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        Future<Object[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                // Process entire thread range in one call (no morsel splitting for hash path)
                return groupAggregateHashRange(
                        numLongPreds, longPredTypes, longCols, longLo, longHi,
                        numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                        numGroupCols, groupCols, groupMuls,
                        numAggs, aggTypes, aggCols, aggCol2s, threadStart, threadEnd,
                        Math.max(16, threadRange / 4));
            });
        }

        // Hierarchical merge: tree-reduce pairs in parallel (log2(N) rounds instead of N-1)
        try {
            // Collect non-null results
            Object[][] results = new Object[nThreads][];
            int count = 0;
            for (int t = 0; t < nThreads; t++) {
                if (futures[t] == null) continue;
                results[count++] = futures[t].get();
            }
            if (count == 0) return new Object[] { new long[0], new double[0][] };

            // Tree reduction: merge pairs in parallel until one remains
            while (count > 1) {
                int pairs = count / 2;
                int remainder = count % 2;
                if (pairs > 1) {
                    // Parallel merge of pairs
                    Future<Object[]>[] mergeFutures = new Future[pairs];
                    for (int p = 0; p < pairs; p++) {
                        final Object[] left = results[p * 2];
                        final Object[] right = results[p * 2 + 1];
                        final int[] la = (int[]) left[2], ra = (int[]) right[2];
                        mergeFutures[p] = POOL.submit(() ->
                            mergeFlatHashTables((long[]) left[0], (double[]) left[1], la[0], la[1],
                                                (long[]) right[0], (double[]) right[1], ra[1],
                                                aggTypes, numAggs, accSize));
                    }
                    for (int p = 0; p < pairs; p++) {
                        results[p] = mergeFutures[p].get();
                    }
                } else {
                    // Single pair, merge inline
                    Object[] left = results[0], right = results[1];
                    int[] la = (int[]) left[2], ra = (int[]) right[2];
                    results[0] = mergeFlatHashTables((long[]) left[0], (double[]) left[1], la[0], la[1],
                                                     (long[]) right[0], (double[]) right[1], ra[1],
                                                     aggTypes, numAggs, accSize);
                }
                if (remainder > 0) results[pairs] = results[count - 1];
                count = pairs + remainder;
            }

            long[] mKeys = (long[]) results[0][0];
            double[] mAccs = (double[]) results[0][1];
            int[] mMeta = (int[]) results[0][2];
            return compactFlatHashTable(mKeys, mAccs, mMeta[1], mMeta[0], accSize);
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
    }

    // =========================================================================
    // Radix-Partitioned Hash Group-By
    // =========================================================================

    // Adaptive radix partitioning: 256-4096 partitions based on input size.
    // Target: each partition's hash table fits in L2 (~50K entries max).
    private static int computeNumPartitions(int length) {
        // Target max entries per partition for L2 cache sizing
        final int TARGET_PER_PART = 50_000;
        int needed = (length + TARGET_PER_PART - 1) / TARGET_PER_PART;
        // Round up to power of 2, clamped to [256, 4096]
        int numParts = Math.max(256, Integer.highestOneBit(Math.max(1, needed - 1)) << 1);
        return Math.min(numParts, 4096);
    }

    /**
     * Scatter keys + agg values into partition-contiguous buffers.
     * Small static method for JIT to compile independently.
     */
    private static void scatterToPartitions(
            int[][] threadMatched, short[][] threadParts, int nThreads,
            long[] allKeys, double[][] aggCols, double[][] aggCol2s, int[] aggTypes, int numAggs,
            long[] partKeys, double[][] partAggs, double[][] partAggCol2s, int[] scatterPos) {
        for (int t = 0; t < nThreads; t++) {
            final int[] tm = threadMatched[t];
            final short[] tp = threadParts[t];
            final int len = tm.length;
            for (int j = 0; j < len; j++) {
                int idx = tm[j];
                int pos = scatterPos[tp[j] & 0xFFFF]++;
                partKeys[pos] = allKeys[idx];
                for (int a = 0; a < numAggs; a++) {
                    if (aggTypes[a] == AGG_SUM_PRODUCT) {
                        partAggs[a][pos] = aggCols[a][idx];
                        partAggCol2s[a][pos] = aggCol2s[a][idx];
                    } else if (aggTypes[a] != AGG_COUNT) {
                        partAggs[a][pos] = aggCols[a][idx];
                    }
                }
            }
        }
    }

    /**
     * Radix-partitioned parallel hash group-by.
     * Partitions rows by key hash prefix, then aggregates each partition
     * with an L2-sized hash table. No merge needed (partitions are disjoint).
     */
    @SuppressWarnings("unchecked")
    public static Object[] fusedFilterGroupAggregatePartitioned(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int length) {

        // For small inputs, non-partitioned is fine (hash table fits in cache)
        if (length < 200_000) {
            return fusedFilterGroupAggregate(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCol2s, length);
        }

        final int accSize = numAggs * 2;
        // Use radix partitioning even in 1T mode for cache locality
        // (256 partitions × L2-sized tables beats one 300MB+ table)
        // Cap threads to avoid oversaturation on large core counts
        int nThreads;
        if (length < PARALLEL_THRESHOLD) {
            nThreads = 1;
        } else {
            nThreads = Math.min(POOL.getParallelism(), length / MORSEL_SIZE);
            nThreads = Math.max(nThreads, 2);
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

        // Adaptive partition count: 256 for ≤12.5M rows, scales up to 4096 for large inputs
        final int numPartitions = computeNumPartitions(length);
        final int partMask = numPartitions - 1;
        final int partShift = 64 - Integer.numberOfTrailingZeros(numPartitions);

        // === Phase 1: Parallel key computation + predicate filtering ===
        int threadRange = (length + nThreads - 1) / nThreads;
        Future<Object[]>[] p1 = new Future[nThreads];
        final long[] allKeys = new long[length];

        for (int t = 0; t < nThreads; t++) {
            final int tS = t * threadRange, tE = Math.min(tS + threadRange, length);
            if (tS >= length) { p1[t] = null; continue; }
            p1[t] = POOL.submit(() -> {
                int[] matched = new int[tE - tS];
                short[] parts = new short[tE - tS];
                int cnt = 0;
                for (int i = tS; i < tE; i++) {
                    if (!evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i))
                        continue;
                    long key = gc0[i] * gm0;
                    if (numGroupCols > 1) key += gc1[i] * gm1;
                    if (numGroupCols > 2) key += gc2[i] * gm2;
                    if (numGroupCols > 3) key += gc3[i] * gm3;
                    if (numGroupCols > 4) key += gc4[i] * gm4;
                    if (numGroupCols > 5) key += gc5[i] * gm5;
                    for (int g = 6; g < numGroupCols; g++) key += groupCols[g][i] * groupMuls[g];
                    key = safeKey(key);
                    allKeys[i] = key;
                    matched[cnt] = i;
                    // Use a DIFFERENT hash constant from hashSlot to avoid clustering
                    // when groupAggregateHashRange re-hashes partitioned keys
                    parts[cnt] = (short)((int)((key * 0x517CC1B727220A95L) >>> partShift) & partMask);
                    cnt++;
                }
                return new Object[] { java.util.Arrays.copyOf(matched, cnt), java.util.Arrays.copyOf(parts, cnt) };
            });
        }

        // Collect + per-thread histograms for parallel scatter
        int[][] threadMatched = new int[nThreads][];
        short[][] threadParts = new short[nThreads][];
        int[][] threadHistograms = new int[nThreads][numPartitions];
        int totalMatched = 0;
        int[] histogram = new int[numPartitions];
        try {
            for (int t = 0; t < nThreads; t++) {
                if (p1[t] == null) { threadMatched[t] = new int[0]; threadParts[t] = new short[0]; continue; }
                Object[] res = p1[t].get();
                threadMatched[t] = (int[]) res[0];
                threadParts[t] = (short[]) res[1];
                int len = threadMatched[t].length;
                totalMatched += len;
                short[] tp = threadParts[t];
                int[] th = threadHistograms[t];
                for (int j = 0; j < len; j++) th[tp[j] & 0xFFFF]++;
                for (int p = 0; p < numPartitions; p++) histogram[p] += th[p];
            }
        } catch (Exception e) { throw new RuntimeException("Phase 1 failed", e); }

        if (totalMatched == 0) return new Object[] { new long[0], new double[0] };

        // === Phase 2: Parallel Scatter ===
        // Compute global partition offsets
        int[] partOffsets = new int[numPartitions + 1];
        for (int p = 0; p < numPartitions; p++) partOffsets[p + 1] = partOffsets[p] + histogram[p];

        // Compute per-thread, per-partition write offsets via prefix sum
        // threadWritePos[t][p] = partOffsets[p] + sum(threadHistograms[0..t-1][p])
        int[][] threadWritePos = new int[nThreads][numPartitions];
        for (int p = 0; p < numPartitions; p++) {
            int offset = partOffsets[p];
            for (int t = 0; t < nThreads; t++) {
                threadWritePos[t][p] = offset;
                offset += threadHistograms[t][p];
            }
        }

        long[] partKeys = new long[totalMatched];
        double[][] partAggs = new double[numAggs][];
        double[][] partAggCol2s = new double[numAggs][];
        for (int a = 0; a < numAggs; a++) {
            partAggs[a] = new double[totalMatched];
            if (aggTypes[a] == AGG_SUM_PRODUCT) partAggCol2s[a] = new double[totalMatched];
        }

        // Parallel scatter: each thread writes to its own non-overlapping ranges
        if (nThreads > 1) {
            Future<?>[] scatterFutures = new Future[nThreads];
            for (int t = 0; t < nThreads; t++) {
                final int[] tm = threadMatched[t];
                final short[] tp = threadParts[t];
                final int[] twp = threadWritePos[t];
                final int len = tm.length;
                if (len == 0) { scatterFutures[t] = null; continue; }
                scatterFutures[t] = POOL.submit(() -> {
                    int[] localPos = java.util.Arrays.copyOf(twp, numPartitions);
                    for (int j = 0; j < len; j++) {
                        int idx = tm[j];
                        int pos = localPos[tp[j] & 0xFFFF]++;
                        partKeys[pos] = allKeys[idx];
                        for (int a = 0; a < numAggs; a++) {
                            if (aggTypes[a] == AGG_SUM_PRODUCT) {
                                partAggs[a][pos] = aggCols[a][idx];
                                partAggCol2s[a][pos] = aggCol2s[a][idx];
                            } else if (aggTypes[a] != AGG_COUNT) {
                                partAggs[a][pos] = aggCols[a][idx];
                            }
                        }
                    }
                });
            }
            try {
                for (Future<?> f : scatterFutures) if (f != null) f.get();
            } catch (Exception e) { throw new RuntimeException("Parallel scatter failed", e); }
        } else {
            // Single thread: inline scatter
            int[] scatterPos = java.util.Arrays.copyOf(partOffsets, numPartitions);
            scatterToPartitions(threadMatched, threadParts, nThreads, allKeys, aggCols, aggCol2s, aggTypes, numAggs,
                                partKeys, partAggs, partAggCol2s, scatterPos);
        }

        // === Phase 3: Parallel per-partition aggregate ===
        // Reuse the well-JIT-compiled groupAggregateHashRange per partition.
        // partKeys IS the group column (composite key), partAggs ARE the agg columns.
        int partsPerThread = (numPartitions + nThreads - 1) / nThreads;
        Future<Object[]>[] p3 = new Future[nThreads];
        final long[][] singleGroupCol = new long[][] { partKeys };
        final long[] singleGroupMul = new long[] { 1L };
        final int[] noPredTypes = new int[0];
        final long[][] noLongCols = new long[0][];
        final long[] noLo = new long[0], noHi = new long[0];
        final double[][] noDblCols = new double[0][];
        final double[] noDLo = new double[0], noDHi = new double[0];

        for (int t = 0; t < nThreads; t++) {
            final int pS = t * partsPerThread, pE = Math.min(pS + partsPerThread, numPartitions);
            if (pS >= numPartitions) { p3[t] = null; continue; }
            p3[t] = POOL.submit(() -> {
                // Aggregate each partition using the already-JIT-compiled hash range method
                long[] rKeys = new long[0];
                double[] rAccs = new double[0];
                int rIdx = 0;

                for (int p = pS; p < pE; p++) {
                    int begin = partOffsets[p];
                    int end = partOffsets[p + 1];
                    int pSize = end - begin;
                    if (pSize == 0) continue;

                    Object[] ht = groupAggregateHashRange(
                            0, noPredTypes, noLongCols, noLo, noHi,
                            0, noPredTypes, noDblCols, noDLo, noDHi,
                            1, singleGroupCol, singleGroupMul,
                            numAggs, aggTypes, partAggs, partAggCol2s,
                            begin, end, pSize * 10 / 7);
                    long[] htKeys = (long[]) ht[0];
                    double[] htAccs = (double[]) ht[1];
                    int[] meta = (int[]) ht[2];
                    int size = meta[0], cap = meta[1];

                    // Grow result arrays if needed
                    if (rIdx + size > rKeys.length) {
                        int newLen = Math.max(rIdx + size, rKeys.length * 2);
                        rKeys = java.util.Arrays.copyOf(rKeys, newLen);
                        rAccs = java.util.Arrays.copyOf(rAccs, newLen * accSize);
                    }

                    for (int s = 0; s < cap; s++) {
                        if (htKeys[s] != EMPTY_KEY) {
                            rKeys[rIdx] = htKeys[s];
                            System.arraycopy(htAccs, s * accSize, rAccs, rIdx * accSize, accSize);
                            rIdx++;
                        }
                    }
                }

                return new Object[] {
                    java.util.Arrays.copyOf(rKeys, rIdx),
                    java.util.Arrays.copyOf(rAccs, rIdx * accSize)
                };
            });
        }

        // === Phase 4: Wait for Phase 3 + Concatenate ===
        try {
            int totalGroups = 0;
            Object[][] tr = new Object[nThreads][];
            for (int t = 0; t < nThreads; t++) {
                if (p3[t] == null) { tr[t] = null; continue; }
                tr[t] = p3[t].get();
                totalGroups += ((long[]) tr[t][0]).length;
            }
            long[] outK = new long[totalGroups];
            double[] outA = new double[totalGroups * accSize];
            int idx = 0;
            for (int t = 0; t < nThreads; t++) {
                if (tr[t] == null) continue;
                long[] tk = (long[]) tr[t][0]; double[] ta = (double[]) tr[t][1];
                System.arraycopy(tk, 0, outK, idx, tk.length);
                System.arraycopy(ta, 0, outA, idx * accSize, tk.length * accSize);
                idx += tk.length;
            }
            return new Object[] { outK, outA };
        } catch (Exception e) { throw new RuntimeException("Parallel aggregate failed", e); }
    }

    /**
     * Dense fused filter + group-by + multi-aggregate.
     *
     * <p>Same as fusedFilterGroupAggregate but uses a flat double[][] array
     * indexed directly by composite key, instead of HashMap. Much faster for
     * low-cardinality group columns where key values are small non-negative integers.
     *
     * <p>Caller must ensure: composite key values fit in [0, maxKey).
     *
     * @param maxKey   Size of the group array (max possible composite key + 1)
     * @return double[][] of size maxKey, where [key] = accumulator array.
     *         Unused entries are null.
     */
    public static double[] fusedFilterGroupAggregateDense(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int length, int maxKey, boolean nanSafe) {
        // Delegate to Range variant — shared JIT compilation unit
        return fusedFilterGroupAggregateDenseRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                numAggs, aggTypes, aggCols, aggCol2s, 0, length, maxKey, nanSafe);
    }

    // =========================================================================
    // Element-wise Array Operations (for expression pre-computation)
    // =========================================================================

    /** Element-wise multiply: result[i] = a[i] * b[i] */
    public static double[] arrayMul(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] * b[i];
        return r;
    }

    /** Element-wise add: result[i] = a[i] + b[i] */
    public static double[] arrayAdd(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] + b[i];
        return r;
    }

    /** Element-wise subtract: result[i] = a[i] - b[i] */
    public static double[] arraySub(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] - b[i];
        return r;
    }

    /** Element-wise divide: result[i] = a[i] / b[i] */
    public static double[] arrayDiv(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] / b[i];
        return r;
    }

    /** Scalar-array multiply: result[i] = scalar * b[i] */
    public static double[] arrayMulScalar(double scalar, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = scalar * b[i];
        return r;
    }

    /** Scalar-array subtract: result[i] = scalar - b[i] */
    public static double[] arraySubScalar(double scalar, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = scalar - b[i];
        return r;
    }

    /** Scalar-array add: result[i] = scalar + b[i] */
    public static double[] arrayAddScalar(double scalar, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = scalar + b[i];
        return r;
    }

    /** Broadcast scalar to array: result[i] = scalar (for all i) */
    public static double[] arrayBroadcast(double scalar, int length) {
        double[] r = new double[length];
        java.util.Arrays.fill(r, scalar);
        return r;
    }

    // =========================================================================
    // Unary Math Array Operations
    // =========================================================================

    /** Element-wise abs: result[i] = |a[i]| */
    public static double[] arrayAbs(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.abs(a[i]);
        return r;
    }

    /** Element-wise sqrt: result[i] = sqrt(a[i]) */
    public static double[] arraySqrt(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.sqrt(a[i]);
        return r;
    }

    /** Element-wise natural log: result[i] = ln(a[i]) */
    public static double[] arrayLog(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.log(a[i]);
        return r;
    }

    /** Element-wise log base 10: result[i] = log10(a[i]) */
    public static double[] arrayLog10(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.log10(a[i]);
        return r;
    }

    /** Element-wise exp: result[i] = e^(a[i]) */
    public static double[] arrayExp(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.exp(a[i]);
        return r;
    }

    /** Element-wise round: result[i] = round(a[i]) */
    public static double[] arrayRound(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.round(a[i]);
        return r;
    }

    /** Element-wise floor: result[i] = floor(a[i]) */
    public static double[] arrayFloor(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.floor(a[i]);
        return r;
    }

    /** Element-wise ceil: result[i] = ceil(a[i]) */
    public static double[] arrayCeil(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.ceil(a[i]);
        return r;
    }

    /** Element-wise sign: result[i] = signum(a[i]) */
    public static double[] arraySign(double[] a, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.signum(a[i]);
        return r;
    }

    // =========================================================================
    // Binary Math Array Operations
    // =========================================================================

    /** Element-wise mod: result[i] = a[i] % b[i] */
    public static double[] arrayMod(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] % b[i];
        return r;
    }

    /** Scalar-array mod: result[i] = a[i] % scalar */
    public static double[] arrayModScalar(double[] a, double scalar, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = a[i] % scalar;
        return r;
    }

    /** Element-wise pow: result[i] = a[i] ^ b[i] */
    public static double[] arrayPow(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.pow(a[i], b[i]);
        return r;
    }

    /** Scalar-array pow: result[i] = a[i] ^ scalar */
    public static double[] arrayPowScalar(double[] a, double scalar, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Math.pow(a[i], scalar);
        return r;
    }

    // =========================================================================
    // Date Extraction Array Operations (Hinnant civil_from_days algorithm)
    // =========================================================================

    /** Extract year from epoch-days array. Uses Hinnant's algorithm. */
    public static double[] arrayExtractYear(long[] epochDays, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long y = yoe + era * 400;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            long m = mp + (mp < 10 ? 3 : -9);
            r[i] = (double) (y + (m <= 2 ? 1 : 0));
        }
        return r;
    }

    /** Extract month (1-12) from epoch-days array. */
    public static double[] arrayExtractMonth(long[] epochDays, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            r[i] = (double) (mp + (mp < 10 ? 3 : -9));
        }
        return r;
    }

    /** Extract day-of-month (1-31) from epoch-days array. */
    public static double[] arrayExtractDay(long[] epochDays, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long z = epochDays[i] + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long doy = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy + 2) / 153;
            r[i] = (double) (doy - (153*mp + 2)/5 + 1);
        }
        return r;
    }

    /** Extract hour (0-23) from epoch-seconds array. */
    public static double[] arrayExtractHour(long[] epochSeconds, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long s = epochSeconds[i];
            long daySeconds = ((s % 86400) + 86400) % 86400;
            r[i] = (double) (daySeconds / 3600);
        }
        return r;
    }

    /** Extract minute (0-59) from epoch-seconds array. */
    public static double[] arrayExtractMinute(long[] epochSeconds, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long s = epochSeconds[i];
            long daySeconds = ((s % 86400) + 86400) % 86400;
            r[i] = (double) ((daySeconds % 3600) / 60);
        }
        return r;
    }

    /** Extract second (0-59) from epoch-seconds array. */
    public static double[] arrayExtractSecond(long[] epochSeconds, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long s = epochSeconds[i];
            long daySeconds = ((s % 86400) + 86400) % 86400;
            r[i] = (double) (daySeconds % 60);
        }
        return r;
    }

    /** Extract day-of-week (0=Mon, 6=Sun, ISO 8601) from epoch-days array. */
    public static double[] arrayExtractDayOfWeek(long[] epochDays, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            // 1970-01-01 is Thursday (3 in 0=Mon scheme)
            long d = ((epochDays[i] % 7) + 10) % 7; // shift to 0=Mon
            r[i] = (double) d;
        }
        return r;
    }

    /** Extract ISO week-of-year (1-53) from epoch-days array. */
    public static double[] arrayExtractWeekOfYear(long[] epochDays, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long ed = epochDays[i];
            long dow = ((ed % 7) + 10) % 7; // 0=Mon
            // Thursday of this week
            long thu = ed + (3 - dow);
            // Year of the Thursday
            long z = thu + 719468;
            long era = (z >= 0 ? z : z - 146096) / 146097;
            long doe = z - era * 146097;
            long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            long y = yoe + era * 400;
            long doy2 = doe - (365*yoe + yoe/4 - yoe/100);
            long mp = (5*doy2 + 2) / 153;
            long m = mp + (mp < 10 ? 3 : -9);
            long thuYear = y + (m <= 2 ? 1 : 0);
            // Jan 1 of that year as epoch-days
            long yy = thuYear - (m <= 2 ? 1 : 0);
            long eraY = (yy >= 0 ? yy : yy - 399) / 400;
            long yoeY = yy - eraY * 400;
            long doyY = (365*yoeY + yoeY/4 - yoeY/100);
            long jan1 = eraY * 146097 + doyY - 719468;
            // Monday of ISO week 1
            long jan1dow = ((jan1 % 7) + 10) % 7;
            long week1Mon = jan1 + ((jan1dow <= 3) ? -jan1dow : 7 - jan1dow);
            r[i] = (double) ((ed - week1Mon) / 7 + 1);
        }
        return r;
    }

    // =========================================================================
    // String Array Operations
    // =========================================================================

    /** Compute string lengths from dict-encoded column: result[i] = dict[codes[i]].length()
     *  Pre-computes dict entry lengths, then parallel broadcasts for large arrays. */
    public static double[] arrayStringLength(long[] codes, String[] dict, int length) {
        // Pre-compute lengths for dict entries (typically ~100K vs 100M rows)
        double[] dictLens = new double[dict.length];
        for (int d = 0; d < dict.length; d++) dictLens[d] = (double) dict[d].length();
        double[] r = new double[length];
        if (length > PARALLEL_THRESHOLD) {
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
                    for (int i = start; i < end; i++) r[i] = dictLens[(int) codes[i]];
                });
            }
            for (int t = 0; t < nThreads; t++) {
                if (futures[t] == null) break;
                try { futures[t].get(); } catch (Exception e) { throw new RuntimeException(e); }
            }
        } else {
            for (int i = 0; i < length; i++) r[i] = dictLens[(int) codes[i]];
        }
        return r;
    }

    /** LIKE predicate on dict-encoded column. Returns 1/0 mask.
     *  Delegates to ColumnOpsExt for fast-path detection + parallel matching. */
    public static long[] arrayStringLike(long[] codes, String[] dict, String pattern, int length) {
        return ColumnOpsExt.arrayStringLikeFast(codes, dict, pattern, length);
    }

    /** Convert SQL LIKE pattern to Java regex. */
    static String likeToRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else if (".^$|()[]{}\\+*?".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** LIKE predicate on raw String[] column. Returns 1/0 mask.
     *  Delegates to ColumnOpsExt for fast-path detection. */
    public static long[] arrayRawStringLike(String[] strings, String pattern, int length) {
        return ColumnOpsExt.arrayRawStringLikeFast(strings, pattern, length);
    }

    /** String lengths from raw String[] column. */
    public static double[] arrayRawStringLength(String[] strings, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (double) strings[i].length();
        return r;
    }

    // ---- String transform helpers ----

    /**
     * Remap dict-encoded column through a transformed dictionary.
     * Deduplicates the new dict (e.g. UPPER("foo")=UPPER("FOO")="FOO").
     * Returns Object[] { long[] newCodes, String[] newDict }.
     */
    private static Object[] remapDict(long[] codes, String[] newDictRaw, int length) {
        // Deduplicate newDictRaw: string -> new index
        java.util.HashMap<String, Integer> seen = new java.util.HashMap<>();
        String[] unique = new String[newDictRaw.length];
        int[] oldToNew = new int[newDictRaw.length];
        int n = 0;
        for (int i = 0; i < newDictRaw.length; i++) {
            Integer idx = seen.get(newDictRaw[i]);
            if (idx != null) {
                oldToNew[i] = idx;
            } else {
                seen.put(newDictRaw[i], n);
                unique[n] = newDictRaw[i];
                oldToNew[i] = n;
                n++;
            }
        }
        String[] newDict = java.util.Arrays.copyOf(unique, n);
        long[] newCodes = new long[length];
        for (int i = 0; i < length; i++) {
            newCodes[i] = oldToNew[(int) codes[i]];
        }
        return new Object[] { newCodes, newDict };
    }

    /** UPPER on dict-encoded column. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringUpper(long[] codes, String[] dict, int length) {
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) newDict[i] = dict[i].toUpperCase();
        return remapDict(codes, newDict, length);
    }

    /** LOWER on dict-encoded column. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringLower(long[] codes, String[] dict, int length) {
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) newDict[i] = dict[i].toLowerCase();
        return remapDict(codes, newDict, length);
    }

    /** SUBSTRING on dict-encoded column (1-based start, optional length).
     *  Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringSubstr(long[] codes, String[] dict, int start, int len, int length) {
        int s0 = Math.max(start - 1, 0); // SQL is 1-based
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) {
            String s = dict[i];
            int from = Math.min(s0, s.length());
            int to = (len < 0) ? s.length() : Math.min(from + len, s.length());
            newDict[i] = s.substring(from, to);
        }
        return remapDict(codes, newDict, length);
    }

    /** REPLACE on dict-encoded column. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringReplace(long[] codes, String[] dict, String target, String replacement, int length) {
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) newDict[i] = dict[i].replace(target, replacement);
        return remapDict(codes, newDict, length);
    }

    /** TRIM on dict-encoded column. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringTrim(long[] codes, String[] dict, int length) {
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) newDict[i] = dict[i].trim();
        return remapDict(codes, newDict, length);
    }

    /** CONCAT two dict-encoded columns. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringConcat(long[] codes1, String[] dict1, long[] codes2, String[] dict2, int length) {
        // Build concatenated strings per row, then deduplicate
        java.util.HashMap<String, Integer> seen = new java.util.HashMap<>();
        String[] tempDict = new String[length]; // worst case
        long[] newCodes = new long[length];
        int n = 0;
        for (int i = 0; i < length; i++) {
            String s = dict1[(int) codes1[i]] + dict2[(int) codes2[i]];
            Integer idx = seen.get(s);
            if (idx != null) {
                newCodes[i] = idx;
            } else {
                seen.put(s, n);
                tempDict[n] = s;
                newCodes[i] = n;
                n++;
            }
        }
        return new Object[] { newCodes, java.util.Arrays.copyOf(tempDict, n) };
    }

    /** CONCAT dict-encoded column with scalar string. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayStringConcatScalar(long[] codes, String[] dict, String scalar, boolean prepend, int length) {
        String[] newDict = new String[dict.length];
        for (int i = 0; i < dict.length; i++) {
            newDict[i] = prepend ? (scalar + dict[i]) : (dict[i] + scalar);
        }
        return remapDict(codes, newDict, length);
    }

    /** CAST long[] to String[] via dict encoding. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayLongToString(long[] data, int length) {
        java.util.HashMap<String, Integer> seen = new java.util.HashMap<>();
        String[] tempDict = new String[length];
        long[] codes = new long[length];
        int n = 0;
        for (int i = 0; i < length; i++) {
            String s = Long.toString(data[i]);
            Integer idx = seen.get(s);
            if (idx != null) {
                codes[i] = idx;
            } else {
                seen.put(s, n);
                tempDict[n] = s;
                codes[i] = n;
                n++;
            }
        }
        return new Object[] { codes, java.util.Arrays.copyOf(tempDict, n) };
    }

    /** CAST double[] to String[] via dict encoding. Returns Object[] {long[] codes, String[] dict}. */
    public static Object[] arrayDoubleToString(double[] data, int length) {
        java.util.HashMap<String, Integer> seen = new java.util.HashMap<>();
        String[] tempDict = new String[length];
        long[] codes = new long[length];
        int n = 0;
        for (int i = 0; i < length; i++) {
            String s = Double.toString(data[i]);
            Integer idx = seen.get(s);
            if (idx != null) {
                codes[i] = idx;
            } else {
                seen.put(s, n);
                tempDict[n] = s;
                codes[i] = n;
                n++;
            }
        }
        return new Object[] { codes, java.util.Arrays.copyOf(tempDict, n) };
    }

    /** CAST dict-encoded string to long[]. Non-parseable values become Long.MIN_VALUE (NULL). */
    public static long[] arrayStringToLong(long[] codes, String[] dict, int length) {
        long[] parsed = new long[dict.length];
        for (int i = 0; i < dict.length; i++) {
            try { parsed[i] = Long.parseLong(dict[i].trim()); }
            catch (NumberFormatException e) { parsed[i] = Long.MIN_VALUE; }
        }
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = parsed[(int) codes[i]];
        return r;
    }

    /** CAST dict-encoded string to double[]. Non-parseable values become NaN (NULL). */
    public static double[] arrayStringToDouble(long[] codes, String[] dict, int length) {
        double[] parsed = new double[dict.length];
        for (int i = 0; i < dict.length; i++) {
            try { parsed[i] = Double.parseDouble(dict[i].trim()); }
            catch (NumberFormatException e) { parsed[i] = Double.NaN; }
        }
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = parsed[(int) codes[i]];
        return r;
    }

    /** CAST long[] to double[]. */
    public static double[] arrayLongToDouble(long[] data, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (double) data[i];
        return r;
    }

    /** CAST double[] to long[] (truncation). NaN becomes Long.MIN_VALUE (NULL). */
    public static long[] arrayDoubleToLong(double[] data, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = Double.isNaN(data[i]) ? Long.MIN_VALUE : (long) data[i];
        return r;
    }

    // =========================================================================
    // CASE/WHEN Array Blend
    // =========================================================================

    /** Conditional copy: where mask[i]==1, set result[i] = vals[i]. */
    public static void arrayBlend(double[] result, double[] vals, long[] mask, int length) {
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1L) result[i] = vals[i];
        }
    }

    // =========================================================================
    // COUNT DISTINCT
    // =========================================================================

    /**
     * Count distinct values in a long[] using the fastest available strategy:
     * - BitSet for small non-negative ranges (dense integers)
     * - Open-addressed hash table for general case (no boxing overhead)
     */
    public static long countDistinctLong(long[] data, int length) {
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
        // BitSet path: fast for bounded integer ranges (up to 64M)
        if (range > 0 && range <= 67108864L) {
            return countDistinctBitSet(data, length, min);
        }
        // Open-addressed hash table for general case
        return countDistinctHash(data, length);
    }

    /** Count distinct values in a long[] range using the fastest strategy. */
    public static long countDistinctLongRange(long[] data, int start, int end) {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (int i = start; i < end; i++) {
            long v = data[i];
            if (v == Long.MIN_VALUE) continue; // skip NULL
            if (v < min) min = v;
            if (v > max) max = v;
        }
        if (min > max) return 0; // all NULL
        long range = max - min + 1;
        if (range > 0 && range <= 67108864L) {
            java.util.BitSet bs = new java.util.BitSet((int) range);
            for (int i = start; i < end; i++) {
                if (data[i] == Long.MIN_VALUE) continue; // skip NULL
                bs.set((int) (data[i] - min));
            }
            return bs.cardinality();
        }
        return countDistinctHashRange(data, start, end);
    }

    /** Count distinct values in a long[] with a pre-computed mask (1=include, 0=skip). */
    public static long countDistinctLongMasked(long[] data, long[] mask, int length) {
        // First pass: find min/max of included values only, and count included (skip NULL)
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        int included = 0;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1L) {
                long v = data[i];
                if (v == Long.MIN_VALUE) continue; // skip NULL
                if (v < min) min = v;
                if (v > max) max = v;
                included++;
            }
        }
        if (included == 0) return 0;
        long range = max - min + 1;
        if (range > 0 && range <= 67108864L) {
            java.util.BitSet bs = new java.util.BitSet((int) range);
            for (int i = 0; i < length; i++) {
                if (mask[i] == 1L && data[i] != Long.MIN_VALUE) bs.set((int) (data[i] - min));
            }
            return bs.cardinality();
        }
        // Hash path for masked
        int cap = Integer.highestOneBit(Math.max(16, included * 2)) << 1;
        long[] table = new long[cap];
        byte[] occupied = new byte[cap];
        int distinct = 0;
        long MULT = 0x9E3779B97F4A7C15L; // Fibonacci
        for (int i = 0; i < length; i++) {
            if (mask[i] != 1L) continue;
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
        return distinct;
    }

    private static long countDistinctBitSet(long[] data, int length, long min) {
        // Recompute max for BitSet sizing (skip NULL)
        long max = Long.MIN_VALUE;
        for (int i = 0; i < length; i++) {
            long v = data[i];
            if (v == Long.MIN_VALUE) continue; // skip NULL
            if (v > max) max = v;
        }
        if (max == Long.MIN_VALUE) return 0; // all NULL
        java.util.BitSet bs = new java.util.BitSet((int) (max - min + 1));
        for (int i = 0; i < length; i++) {
            if (data[i] == Long.MIN_VALUE) continue; // skip NULL
            bs.set((int) (data[i] - min));
        }
        return bs.cardinality();
    }

    private static long countDistinctHash(long[] data, int length) {
        // Open-addressed hash table with Fibonacci hashing, 50% load factor
        int cap = Integer.highestOneBit(Math.max(16, (int) Math.min((long) length * 2, Integer.MAX_VALUE - 8))) << 1;
        long[] table = new long[cap];
        byte[] occupied = new byte[cap]; // 0=empty, 1=occupied
        int distinct = 0;
        long MULT = 0x9E3779B97F4A7C15L; // Fibonacci constant
        for (int i = 0; i < length; i++) {
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
                if (table[slot] == v) break; // already seen
                slot = (slot + 1) & (cap - 1); // linear probe
            }
        }
        return distinct;
    }

    private static long countDistinctHashRange(long[] data, int start, int end) {
        int len = end - start;
        int cap = Integer.highestOneBit(Math.max(16, len * 2)) << 1;
        long[] table = new long[cap];
        byte[] occupied = new byte[cap];
        int distinct = 0;
        long MULT = 0x9E3779B97F4A7C15L;
        for (int i = start; i < end; i++) {
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
        return distinct;
    }

    /** Convert long[] to double[] */
    public static double[] longToDouble(long[] src, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (double) src[i];
        return r;
    }

    // =========================================================================
    // NULL Handling Array Operations
    // =========================================================================

    /** IS NULL mask for double[]: NaN → 1, else → 0 */
    public static long[] arrayIsNull(double[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = Double.isNaN(a[i]) ? 1L : 0L;
        return r;
    }

    /** IS NULL mask for long[]: Long.MIN_VALUE → 1, else → 0 */
    public static long[] arrayIsNullLong(long[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = (a[i] == Long.MIN_VALUE) ? 1L : 0L;
        return r;
    }

    /** IS NOT NULL mask for double[]: NaN → 0, else → 1 */
    public static long[] arrayIsNotNull(double[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = Double.isNaN(a[i]) ? 0L : 1L;
        return r;
    }

    /** IS NOT NULL mask for long[]: Long.MIN_VALUE → 0, else → 1 */
    public static long[] arrayIsNotNullLong(long[] a, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = (a[i] == Long.MIN_VALUE) ? 0L : 1L;
        return r;
    }

    /** COALESCE(a, b): first non-NaN value element-wise */
    public static double[] arrayCoalesce(double[] a, double[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Double.isNaN(a[i]) ? b[i] : a[i];
        return r;
    }

    /** COALESCE(a, scalar): replace NaN with scalar */
    public static double[] arrayCoalesceScalar(double[] a, double scalar, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = Double.isNaN(a[i]) ? scalar : a[i];
        return r;
    }

    /** COALESCE(a, b) for long[]: first non-NULL value element-wise */
    public static long[] arrayCoalesceLong(long[] a, long[] b, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = (a[i] == Long.MIN_VALUE) ? b[i] : a[i];
        return r;
    }

    /** COALESCE(a, scalar) for long[]: replace NULL sentinel with scalar */
    public static long[] arrayCoalesceLongScalar(long[] a, long scalar, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = (a[i] == Long.MIN_VALUE) ? scalar : a[i];
        return r;
    }

    /** NULLIF(a, val): set to NaN where a[i] == val */
    public static double[] arrayNullif(double[] a, double val, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (a[i] == val) ? Double.NaN : a[i];
        return r;
    }

    // =========================================================================
    // Date/Time Arithmetic Array Operations
    // =========================================================================

    /** Inverse Hinnant: convert (year, month, day) to epoch-days.
     *  Private helper used by date-trunc and date-add. */
    private static long civilToDays(long y, long m, long d) {
        y -= (m <= 2) ? 1 : 0;
        long era = (y >= 0 ? y : y - 399) / 400;
        long yoe = y - era * 400;
        long doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    /** Extract (year, month, day) from epoch-days using Hinnant algorithm.
     *  Returns [year, month, day] in the provided array. */
    private static void civilFromDays(long epochDays, long[] ymd) {
        long z = epochDays + 719468;
        long era = (z >= 0 ? z : z - 146096) / 146097;
        long doe = z - era * 146097;
        long yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
        long y = yoe + era * 400;
        long doy = doe - (365*yoe + yoe/4 - yoe/100);
        long mp = (5*doy + 2) / 153;
        long d = doy - (153*mp + 2)/5 + 1;
        long m = mp + (mp < 10 ? 3 : -9);
        y += (m <= 2) ? 1 : 0;
        ymd[0] = y;
        ymd[1] = m;
        ymd[2] = d;
    }

    /** DATE_TRUNC to year: zero month/day, keep as epoch-seconds */
    public static long[] arrayDateTruncYear(long[] epochSeconds, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long epochDays = Math.floorDiv(epochSeconds[i], 86400L);
            civilFromDays(epochDays, ymd);
            r[i] = civilToDays(ymd[0], 1, 1) * 86400L;
        }
        return r;
    }

    /** DATE_TRUNC to month: zero day, keep as epoch-seconds */
    public static long[] arrayDateTruncMonth(long[] epochSeconds, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long epochDays = Math.floorDiv(epochSeconds[i], 86400L);
            civilFromDays(epochDays, ymd);
            r[i] = civilToDays(ymd[0], ymd[1], 1) * 86400L;
        }
        return r;
    }

    /** DATE_TRUNC to day: zero time component */
    public static long[] arrayDateTruncDay(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorDiv(epochSeconds[i], 86400L) * 86400L;
        }
        return r;
    }

    /** DATE_TRUNC to hour: zero minutes/seconds */
    public static long[] arrayDateTruncHour(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorDiv(epochSeconds[i], 3600L) * 3600L;
        }
        return r;
    }

    /** DATE_TRUNC to minute: zero seconds */
    public static long[] arrayDateTruncMinute(long[] epochSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            r[i] = Math.floorDiv(epochSeconds[i], 60L) * 60L;
        }
        return r;
    }

    /** DATE_ADD days: add N days (as seconds) */
    public static long[] arrayDateAddDays(long[] epochSeconds, long nDays, int length) {
        long[] r = new long[length];
        long delta = nDays * 86400L;
        for (int i = 0; i < length; i++) r[i] = epochSeconds[i] + delta;
        return r;
    }

    /** DATE_ADD seconds: add N seconds */
    public static long[] arrayDateAddSeconds(long[] epochSeconds, long nSeconds, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = epochSeconds[i] + nSeconds;
        return r;
    }

    /** DATE_ADD months: add N months using Hinnant civil date arithmetic */
    public static long[] arrayDateAddMonths(long[] epochSeconds, int nMonths, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long s = epochSeconds[i];
            long epochDays = Math.floorDiv(s, 86400L);
            long timeOfDay = s - epochDays * 86400L;
            civilFromDays(epochDays, ymd);
            long totalMonths = ymd[0] * 12 + (ymd[1] - 1) + nMonths;
            long newYear = Math.floorDiv(totalMonths, 12);
            long newMonth = Math.floorMod(totalMonths, 12) + 1;
            // Clamp day to valid range for target month
            long maxDay = 28; // safe default
            if (newMonth == 2) {
                boolean leap = (newYear % 4 == 0 && newYear % 100 != 0) || (newYear % 400 == 0);
                maxDay = leap ? 29 : 28;
            } else if (newMonth == 4 || newMonth == 6 || newMonth == 9 || newMonth == 11) {
                maxDay = 30;
            } else {
                maxDay = 31;
            }
            long day = Math.min(ymd[2], maxDay);
            r[i] = civilToDays(newYear, newMonth, day) * 86400L + timeOfDay;
        }
        return r;
    }

    /** DATE_DIFF: difference in fractional days between two epoch-second columns */
    public static double[] arrayDateDiffDays(long[] a, long[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (double)(a[i] - b[i]) / 86400.0;
        return r;
    }

    /** DATE_DIFF: difference in seconds between two epoch-second columns */
    public static double[] arrayDateDiffSeconds(long[] a, long[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) r[i] = (double)(a[i] - b[i]);
        return r;
    }

    // =========================================================================
    // Parallel Execution
    // =========================================================================

    /** JVM-singleton thread pool sized to available cores. */
    static final ForkJoinPool POOL =
            new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    /** Morsel size for cache-friendly parallel execution.
     *  64K rows × ~4 columns × 8B = ~2MB → fits in L2 cache per core. */
    static final int MORSEL_SIZE = 64 * 1024;

    private static final int DEFAULT_PARALLEL_THRESHOLD = 200_000;

    /** Threshold below which we stay single-threaded. Set to Integer.MAX_VALUE to force 1T. */
    static volatile int PARALLEL_THRESHOLD = DEFAULT_PARALLEL_THRESHOLD;

    /**
     * L3 cache budget for parallel group-by thread capping.
     * When total accumulator memory across threads exceeds this, reduce thread count
     * to avoid L3 bandwidth saturation from random scatter access patterns.
     * Detected from sysfs on Linux, falls back to conservative 16MB.
     */
    static final long L3_BUDGET = detectL3Budget();

    private static long detectL3Budget() {
        // Try Linux sysfs for accurate L3 size
        try {
            java.io.File f = new java.io.File("/sys/devices/system/cpu/cpu0/cache/index3/size");
            if (f.exists()) {
                String s = new String(java.nio.file.Files.readAllBytes(f.toPath())).trim();
                long multiplier = 1;
                if (s.endsWith("K")) { multiplier = 1024; s = s.substring(0, s.length()-1); }
                else if (s.endsWith("M")) { multiplier = 1024*1024; s = s.substring(0, s.length()-1); }
                long l3 = Long.parseLong(s) * multiplier;
                // Use half of L3 as budget (leave room for input data streaming)
                return Math.max(l3 / 2, 8_000_000L);
            }
        } catch (Exception ignored) {}
        // Conservative default for unknown platforms
        return 16_000_000L;
    }

    /**
     * Compute effective thread count for dense group-by with random scatter.
     * Limits threads so total accumulator footprint fits in L3 budget.
     * For small accumulators (low cardinality), L3_BUDGET/perThreadMem >> pool size,
     * so call sites cap at POOL.getParallelism() for full parallelism.
     * Returns 1 when L3 budget only allows 2 threads (overhead outweighs benefit).
     */
    static int effectiveGroupByThreads(long perThreadMem) {
        int threads = (int) (L3_BUDGET / Math.max(1, perThreadMem));
        // With only 2 threads, ForkJoin overhead + L3 contention > parallel benefit
        return threads < 3 ? 1 : threads;
    }

    /** Set the parallel threshold. Use Integer.MAX_VALUE to force single-threaded execution. */
    public static void setParallelThreshold(int threshold) { PARALLEL_THRESHOLD = threshold; }

    /** Get the current parallel threshold. */
    public static int getParallelThreshold() { return PARALLEL_THRESHOLD; }

    /** Get the number of threads in the pool. */
    public static int getPoolSize() { return POOL.getParallelism(); }

    /**
     * Effective thread count for bandwidth-bound scan operations.
     * On multi-NUMA systems, using all cores for pure memory scanning can cause
     * cross-NUMA traffic that degrades performance. Cap to half the pool size
     * (roughly one NUMA node's worth of cores) with a minimum of 4 threads.
     * For high core counts (>12), this avoids negative scaling on pure scan queries
     * like COUNT WHERE, LIKE, and single-aggregate operations.
     */
    static int effectiveScanThreads() {
        int pool = POOL.getParallelism();
        if (pool <= 12) return pool;  // single NUMA node likely, use all
        return Math.max(4, pool / 2);
    }


    /**
     * Parallel fused SIMD filter+aggregate.
     * Uses nThreads futures, each processing its range in MORSEL_SIZE steps
     * for cache-friendly access (L2-sized working set per morsel).
     */
    public static double[] fusedSimdParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
            int length, boolean nanSafe) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedSimdUnrolled(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    aggType, aggCol1, aggCol2, length, nanSafe);
        }

        int nThreads = effectiveScanThreads();
        int threadRange = (length + nThreads - 1) / nThreads;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                // Process in morsel-sized chunks for cache friendliness
                double revenue = (aggType == AGG_MIN) ? Double.POSITIVE_INFINITY
                               : (aggType == AGG_MAX) ? Double.NEGATIVE_INFINITY : 0.0;
                long cnt = 0;
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[] partial = fusedSimdUnrolledRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            aggType, aggCol1, aggCol2, ms, me, nanSafe);
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

        // Combine thread results
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

    /**
     * Parallel fused SIMD COUNT.
     * Uses nThreads futures with morsel-sized steps for cache friendliness.
     */
    public static double[] fusedSimdCountParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int length) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedSimdCount(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, length);
        }

        int nThreads = effectiveScanThreads();
        int threadRange = (length + nThreads - 1) / nThreads;

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
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, ms, me)[1];
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

    // Multi-Sum methods moved to ColumnOpsExt for JIT isolation.

    /**
     * Parallel fused dense group-by.
     * Each thread processes morsel-sized chunks, building local accumulators,
     * then merged element-wise across threads.
     */
    public static double[] fusedFilterGroupAggregateDenseParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int length, int maxKey, boolean nanSafe) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedFilterGroupAggregateDense(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCol2s, length, maxKey, nanSafe);
        }

        final int accSize = numAggs * 2;
        final int flatLen = maxKey * accSize;

        // Limit threads so total accumulators fit in L3.
        long perThreadMem = (long) maxKey * accSize * 8;
        int nThreads = Math.min(POOL.getParallelism(), effectiveGroupByThreads(perThreadMem));
        if (nThreads <= 1) {
            // L3 budget too tight for parallelism — fall back to sequential
            return fusedFilterGroupAggregateDense(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, aggTypes, aggCols, aggCol2s, length, maxKey, nanSafe);
        }
        int threadRange = (length + nThreads - 1) / nThreads;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        // Per-morsel accumulator allocation cost: maxKey × accSize × 8 bytes per morsel.
        // For small groups (e.g. 6 groups × 14 accSize = 672B) morsel splitting helps
        // data column L2 locality. For large groups (e.g. 60K × 4 = 1.9MB) the
        // allocation overwhelms any cache benefit. Threshold: 64KB per morsel.
        final boolean useMorsels = (long) maxKey * accSize * 8 < 65536;

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                if (!useMorsels) {
                    return fusedFilterGroupAggregateDenseRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            numAggs, aggTypes, aggCols, aggCol2s, threadStart, threadEnd, maxKey, nanSafe);
                }
                // Morsel-driven: accumulate into thread-local flat array across all morsels
                double[] accs = fusedFilterGroupAggregateDenseRange(
                        numLongPreds, longPredTypes, longCols, longLo, longHi,
                        numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                        numGroupCols, groupCols, groupMuls,
                        numAggs, aggTypes, aggCols, aggCol2s, threadStart,
                        Math.min(threadStart + MORSEL_SIZE, threadEnd), maxKey, nanSafe);
                for (int ms = threadStart + MORSEL_SIZE; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    double[] partial = fusedFilterGroupAggregateDenseRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            numAggs, aggTypes, aggCols, aggCol2s, ms, me, maxKey, nanSafe);
                    // Merge morsel result — flat contiguous, per-agg for MIN/MAX correctness
                    for (int k = 0; k < maxKey; k++) {
                        int base = k * accSize;
                        for (int a = 0; a < numAggs; a++) {
                            int off = base + a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                                    accs[off] += partial[off]; break;
                                case AGG_MIN:
                                    if (partial[off + 1] > 0)
                                        accs[off] = (accs[off + 1] > 0)
                                            ? Math.min(accs[off], partial[off]) : partial[off];
                                    break;
                                case AGG_MAX:
                                    if (partial[off + 1] > 0)
                                        accs[off] = (accs[off + 1] > 0)
                                            ? Math.max(accs[off], partial[off]) : partial[off];
                                    break;
                            }
                            accs[off + 1] += partial[off + 1];
                        }
                    }
                }
                return accs;
            });
        }

        // Hierarchical merge: tree-reduce pairs in parallel (log2(N) rounds)
        try {
            boolean hasMinMax = false;
            for (int a = 0; a < numAggs; a++) {
                if (aggTypes[a] == AGG_MIN || aggTypes[a] == AGG_MAX) { hasMinMax = true; break; }
            }

            // Collect non-null results
            double[][] results = new double[nThreads][];
            int count = 0;
            for (Future<double[]> f : futures) {
                if (f == null) continue;
                results[count++] = f.get();
            }
            if (count == 0) return new double[flatLen];

            final boolean fHasMinMax = hasMinMax;
            // Tree reduction
            while (count > 1) {
                int pairs = count / 2;
                int remainder = count % 2;
                if (pairs > 1 && !hasMinMax) {
                    // Parallel merge of pairs (only for SUM/COUNT — embarrassingly parallel)
                    Future<double[]>[] mergeFutures = new Future[pairs];
                    for (int p = 0; p < pairs; p++) {
                        final double[] left = results[p * 2];
                        final double[] right = results[p * 2 + 1];
                        mergeFutures[p] = POOL.submit(() -> {
                            for (int j = 0; j < flatLen; j++) left[j] += right[j];
                            return left;
                        });
                    }
                    for (int p = 0; p < pairs; p++) results[p] = mergeFutures[p].get();
                } else {
                    // Sequential merge (with MIN/MAX handling or small count)
                    for (int p = 0; p < pairs; p++) {
                        double[] left = results[p * 2];
                        double[] right = results[p * 2 + 1];
                        if (!fHasMinMax) {
                            for (int j = 0; j < flatLen; j++) left[j] += right[j];
                        } else {
                            for (int k = 0; k < maxKey; k++) {
                                int base = k * accSize;
                                for (int a = 0; a < numAggs; a++) {
                                    int off = base + a * 2;
                                    switch (aggTypes[a]) {
                                        case AGG_SUM: case AGG_SUM_PRODUCT: case AGG_COUNT:
                                            left[off] += right[off]; break;
                                        case AGG_MIN:
                                            if (right[off + 1] > 0)
                                                left[off] = (left[off + 1] > 0)
                                                    ? Math.min(left[off], right[off]) : right[off];
                                            break;
                                        case AGG_MAX:
                                            if (right[off + 1] > 0)
                                                left[off] = (left[off + 1] > 0)
                                                    ? Math.max(left[off], right[off]) : right[off];
                                            break;
                                    }
                                    left[off + 1] += right[off + 1];
                                }
                            }
                        }
                        results[p] = left;
                    }
                }
                if (remainder > 0) results[pairs] = results[count - 1];
                count = pairs + remainder;
            }
            return results[0];
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
    }

    // =========================================================================
    // Range-Based Variants (for parallel partitioning)
    // =========================================================================

    /**
     * fusedSimdUnrolled operating on [start, end) range of the same arrays.
     * Same logic, just shifted loop bounds.
     */
    private static double[] fusedSimdUnrolledRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int aggType, double[] aggCol1, double[] aggCol2,
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
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            VectorMask<Double> dm;
            if (numDblPreds > 0 || aggType != AGG_COUNT) {
                if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
                else { for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0); }
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
                // AGG_COUNT — count all matching rows regardless of NaN
                matchCount += dm.trueCount();
            }
        }

        if (aggType == AGG_MIN) revenue = revenueVec.reduceLanes(VectorOperators.MIN);
        else if (aggType == AGG_MAX) revenue = revenueVec.reduceLanes(VectorOperators.MAX);
        else revenue = revenueVec.reduceLanes(VectorOperators.ADD);

        // Scalar tail — use shared helper (runs < LONG_LANES times)
        for (int i = upperBound; i < end; i++) {
            if (evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                   numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                if (aggType == AGG_COUNT) {
                    matchCount++;
                } else {
                    double v = aggCol1[i];
                    if (!nanSafe || v == v) { // skip NaN check when not nan-safe
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

    /**
     * fusedSimdCount operating on [start, end) range.
     */
    static double[] fusedSimdCountRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
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
            VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
            if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
            if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

            if (numDblPreds > 0) {
                VectorMask<Double> dm;
                if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
                else { for (int lane = 0; lane < LONG_LANES; lane++) maskBits[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, maskBits, 0); }
                if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                matchCount += dm.trueCount();
            } else {
                matchCount += lm.trueCount();
            }
        }

        // Scalar tail — use shared helper (runs < LONG_LANES times)
        for (int i = upperBound; i < end; i++) {
            if (evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                   numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) {
                matchCount++;
            }
        }
        return new double[] { 0.0, (double) matchCount };
    }

    /**
     * fusedFilterGroupAggregateDense operating on [start, end) range.
     */
    private static double[] fusedFilterGroupAggregateDenseRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int[] aggTypes, double[][] aggCols, double[][] aggCol2s,
            int start, int end, int maxKey, boolean nanSafe) {

        final int accSize = numAggs * 2;
        // Flat contiguous array: groups[key * accSize + a*2] = value, [key * accSize + a*2 + 1] = count
        double[] groups = new double[maxKey * accSize];
        // Init MIN/MAX accumulators (SUM/COUNT default to 0.0 which is correct)
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.NEGATIVE_INFINITY;
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

        rowLoop:
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

            int base = key * accSize;
            for (int a = 0; a < numAggs; a++) {
                int off = base + a * 2;
                switch (aggTypes[a]) {
                    case AGG_SUM: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            groups[off] += sv * m;
                            groups[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_SUM_PRODUCT: {
                        double sv = aggCols[a][i];
                        double sv2 = aggCol2s[a][i];
                        if (!nanSafe || (sv == sv && sv2 == sv2)) {
                            groups[off] += sv * sv2 * m;
                            groups[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_COUNT:
                        groups[off + 1] += m;
                        break;
                    case AGG_MIN: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            if (m != 0) groups[off] = Math.min(groups[off], sv);
                            groups[off + 1] += m;
                        }
                        break;
                    }
                    case AGG_MAX: {
                        double sv = aggCols[a][i];
                        if (!nanSafe || sv == sv) {
                            if (m != 0) groups[off] = Math.max(groups[off], sv);
                            groups[off + 1] += m;
                        }
                        break;
                    }
                }
            }
        }

        return groups;
    }

    // =========================================================================
    // COUNT-Only Dense Group-By (JIT-isolated from general agg path)
    // =========================================================================

    /**
     * Dense group-by for COUNT-only aggregates.
     * Delegates to Range variant for shared JIT compilation unit.
     */
    public static double[][] fusedFilterGroupCountDense(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int length, int maxKey) {
        return fusedFilterGroupCountDenseRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                numAggs, 0, length, maxKey);
    }

    /**
     * Accumulate COUNT into pre-allocated long[] counts array.
     * Inline SIMD predicates with early-exit + scalar scatter to counts.
     * No allocation, no agg switch — compact for JIT.
     */
    private static void accumulateCountDenseRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int start, int end, long[] counts) {

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

        if (numLongPreds == 0 && numDblPreds == 0) {
            // No predicates — count all rows directly
            countLoop:
            for (int i = start; i < end; i++) {
                int key = (int)(gc0[i] * gm0);
                if (numGroupCols > 1) key += (int)(gc1[i] * gm1);
                if (numGroupCols > 2) key += (int)(gc2[i] * gm2);
                if (numGroupCols > 3) key += (int)(gc3[i] * gm3);
                if (numGroupCols > 4) key += (int)(gc4[i] * gm4);
                if (numGroupCols > 5) key += (int)(gc5[i] * gm5);
                for (int g = 6; g < numGroupCols; g++) {
                    key += (int)(groupCols[g][i] * groupMuls[g]);
                }
                counts[key]++;
            }
        } else {
            // Inline SIMD predicates with early-exit + scalar scatter to counts
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

            int rangeLen = end - start;
            int upperBound = start + (rangeLen - (rangeLen % LONG_LANES));

            for (int i = start; i < upperBound; i += LONG_LANES) {
                VectorMask<Long> lm = LONG_SPECIES.maskAll(true);
                if (numLongPreds > 0) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc0, i), lt0, llo0, lhi0); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 1) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc1, i), lt1, llo1, lhi1); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 2) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc2, i), lt2, llo2, lhi2); if (!lm.anyTrue()) continue; }
                if (numLongPreds > 3) { lm = applyLongPred(lm, LongVector.fromArray(LONG_SPECIES, lc3, i), lt3, llo3, lhi3); if (!lm.anyTrue()) continue; }

                // Convert mask to bits for scatter
                long maskBits;
                if (numDblPreds > 0) {
                    boolean[] boolMask = new boolean[LONG_LANES];
                    VectorMask<Double> dm;
                    if (numLongPreds == 0) { dm = DOUBLE_SPECIES.maskAll(true); }
                    else { for (int lane = 0; lane < LONG_LANES; lane++) boolMask[lane] = lm.laneIsSet(lane); dm = VectorMask.fromArray(DOUBLE_SPECIES, boolMask, 0); }
                    if (numDblPreds > 0) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc0, i), dt0, dlo0, dhi0); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 1) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc1, i), dt1, dlo1, dhi1); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 2) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc2, i), dt2, dlo2, dhi2); if (!dm.anyTrue()) continue; }
                    if (numDblPreds > 3) { dm = applyDoublePred(dm, DoubleVector.fromArray(DOUBLE_SPECIES, dc3, i), dt3, dlo3, dhi3); if (!dm.anyTrue()) continue; }
                    maskBits = dm.toLong();
                } else {
                    maskBits = lm.toLong();
                }

                // Scatter matching lanes to counts using bit manipulation
                while (maskBits != 0) {
                    int lane = Long.numberOfTrailingZeros(maskBits);
                    int idx = i + lane;
                    maskBits &= maskBits - 1; // clear lowest set bit
                    int key = (int)(gc0[idx] * gm0);
                    if (numGroupCols > 1) key += (int)(gc1[idx] * gm1);
                    if (numGroupCols > 2) key += (int)(gc2[idx] * gm2);
                    if (numGroupCols > 3) key += (int)(gc3[idx] * gm3);
                    if (numGroupCols > 4) key += (int)(gc4[idx] * gm4);
                    if (numGroupCols > 5) key += (int)(gc5[idx] * gm5);
                    for (int g = 6; g < numGroupCols; g++) key += (int)(groupCols[g][idx] * groupMuls[g]);
                    counts[key]++;
                }
            }

            // Scalar tail
            scalarTail:
            for (int i = upperBound; i < end; i++) {
                if (!evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                        numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) continue;
                int key = (int)(gc0[i] * gm0);
                if (numGroupCols > 1) key += (int)(gc1[i] * gm1);
                if (numGroupCols > 2) key += (int)(gc2[i] * gm2);
                if (numGroupCols > 3) key += (int)(gc3[i] * gm3);
                if (numGroupCols > 4) key += (int)(gc4[i] * gm4);
                if (numGroupCols > 5) key += (int)(gc5[i] * gm5);
                for (int g = 6; g < numGroupCols; g++) {
                    key += (int)(groupCols[g][i] * groupMuls[g]);
                }
                counts[key]++;
            }
        }
    }

    /**
     * COUNT-only dense group-by on [start, end) range.
     * Allocates counts, calls SIMD accumulate, converts to double[][].
     */
    private static double[][] fusedFilterGroupCountDenseRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int start, int end, int maxKey) {

        long[] counts = new long[maxKey];
        accumulateCountDenseRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                start, end, counts);

        // Convert to double[][] format
        int accSize = numAggs * 2;
        double[][] groups = new double[maxKey][];
        for (int k = 0; k < maxKey; k++) {
            if (counts[k] == 0) continue;
            groups[k] = new double[accSize];
            for (int a = 0; a < numAggs; a++) {
                groups[k][a * 2 + 1] = (double) counts[k];
            }
        }
        return groups;
    }

    /**
     * Parallel COUNT-only dense group-by.
     * Thread-local long[] counts: 1 allocation per thread, accumulate across
     * all morsels, then merge long[] arrays and convert to double[][].
     * JIT-isolated from the general agg parallel path.
     */
    public static double[][] fusedFilterGroupCountDenseParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            int numAggs, int length, int maxKey) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedFilterGroupCountDense(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, length, maxKey);
        }

        // Cap thread count based on cache hierarchy
        long perThreadMem = (long) maxKey * 8;
        int nThreads = Math.min(POOL.getParallelism(), effectiveGroupByThreads(perThreadMem));
        if (nThreads <= 1) {
            return fusedFilterGroupCountDense(numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    numAggs, length, maxKey);
        }
        int threadRange = (length + nThreads - 1) / nThreads;

        @SuppressWarnings("unchecked")
        Future<long[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                long[] counts = new long[maxKey]; // 1 alloc per thread
                // Accumulate across all morsels (no per-morsel alloc/merge)
                for (int ms = threadStart; ms < threadEnd; ms += MORSEL_SIZE) {
                    int me = Math.min(ms + MORSEL_SIZE, threadEnd);
                    accumulateCountDenseRange(
                            numLongPreds, longPredTypes, longCols, longLo, longHi,
                            numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                            numGroupCols, groupCols, groupMuls,
                            ms, me, counts);
                }
                return counts;
            });
        }

        // Merge thread long[] counts
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
            throw new RuntimeException("Parallel execution failed", e);
        }

        // Convert to double[][] format
        int accSize = numAggs * 2;
        double[][] groups = new double[maxKey][];
        for (int k = 0; k < maxKey; k++) {
            if (merged[k] == 0) continue;
            groups[k] = new double[accSize];
            for (int a = 0; a < numAggs; a++) {
                groups[k][a * 2 + 1] = (double) merged[k];
            }
        }
        return groups;
    }

    // =========================================================================
    // Hash Join
    // =========================================================================
    // Open-addressed hash table for equi-joins.
    // Build side: insert all keys into hash table with chained row indices.
    // Probe side: look up each key, emit (leftIdx, rightIdx) pairs.

    /** Sentinel for empty slots in join hash table (can't use Long.MIN_VALUE — that's NULL). */
    static final long JOIN_EMPTY = Long.MIN_VALUE + 1;

    /**
     * Build a hash table for the build side of a hash join.
     *
     * @param keys    Build-side key column (long[])
     * @param length  Number of rows
     * @return Object[] = {long[] htKeys, int[] htFirst, int[] htNext, int capacity}
     *         htKeys[slot]  = key value (JOIN_EMPTY for empty)
     *         htFirst[slot] = first row index with that key (-1 = no row)
     *         htNext[row]   = next row with same key (-1 = end of chain)
     */
    public static Object[] hashJoinBuild(long[] keys, int length) {
        int capacity = nextPow2((int) Math.min((long) length * 2, Integer.MAX_VALUE - 8)); // 50% load factor
        int mask = capacity - 1;
        long[] htKeys = new long[capacity];
        int[] htFirst = new int[capacity];
        int[] htNext = new int[length];
        java.util.Arrays.fill(htKeys, JOIN_EMPTY);
        java.util.Arrays.fill(htFirst, -1);
        java.util.Arrays.fill(htNext, -1);

        for (int i = 0; i < length; i++) {
            long key = keys[i];
            // Skip NULL keys (SQL: NULL ≠ NULL)
            if (key == Long.MIN_VALUE) continue;

            int slot = hashSlot(key, capacity) & mask;
            while (htKeys[slot] != JOIN_EMPTY && htKeys[slot] != key) {
                slot = (slot + 1) & mask;
            }
            if (htKeys[slot] == JOIN_EMPTY) {
                htKeys[slot] = key;
                htFirst[slot] = i;
            } else {
                // Chain: prepend to existing chain
                htNext[i] = htFirst[slot];
                htFirst[slot] = i;
            }
        }
        return new Object[] { htKeys, htFirst, htNext, capacity };
    }

    /**
     * Probe hash table for INNER join.
     *
     * @return Object[] = {int[] leftIndices, int[] rightIndices, int resultLength}
     *         leftIndices[i]  = probe-side (left) row index
     *         rightIndices[i] = build-side (right) row index
     */
    public static Object[] hashJoinProbeInner(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength) {
        int mask = capacity - 1;
        int outCap = probeLength;
        int[] leftOut = new int[outCap];
        int[] rightOut = new int[outCap];
        int outLen = 0;

        for (int i = 0; i < probeLength; i++) {
            long key = probeKeys[i];
            if (key == Long.MIN_VALUE) continue; // skip NULL

            int slot = hashSlot(key, capacity) & mask;
            while (htKeys[slot] != JOIN_EMPTY) {
                if (htKeys[slot] == key) {
                    // Walk chain
                    int row = htFirst[slot];
                    while (row >= 0) {
                        if (outLen >= outCap) {
                            outCap = outCap * 2;
                            leftOut = java.util.Arrays.copyOf(leftOut, outCap);
                            rightOut = java.util.Arrays.copyOf(rightOut, outCap);
                        }
                        leftOut[outLen] = i;
                        rightOut[outLen] = row;
                        outLen++;
                        row = htNext[row];
                    }
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }
        return new Object[] { leftOut, rightOut, outLen };
    }

    /**
     * Probe hash table for LEFT OUTER join.
     * Emits unmatched probe rows with rightIdx = -1.
     *
     * @return Object[] = {int[] leftIndices, int[] rightIndices, int resultLength, boolean[] buildMatched}
     *         buildMatched[row] = true if build row was matched (for FULL OUTER support)
     */
    public static Object[] hashJoinProbeLeft(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength, int buildLength) {
        int mask = capacity - 1;
        int outCap = probeLength;
        int[] leftOut = new int[outCap];
        int[] rightOut = new int[outCap];
        boolean[] buildMatched = new boolean[buildLength];
        int outLen = 0;

        for (int i = 0; i < probeLength; i++) {
            long key = probeKeys[i];
            boolean matched = false;

            if (key != Long.MIN_VALUE) {
                int slot = hashSlot(key, capacity) & mask;
                while (htKeys[slot] != JOIN_EMPTY) {
                    if (htKeys[slot] == key) {
                        int row = htFirst[slot];
                        while (row >= 0) {
                            if (outLen >= outCap) {
                                outCap = outCap * 2;
                                leftOut = java.util.Arrays.copyOf(leftOut, outCap);
                                rightOut = java.util.Arrays.copyOf(rightOut, outCap);
                            }
                            leftOut[outLen] = i;
                            rightOut[outLen] = row;
                            buildMatched[row] = true;
                            outLen++;
                            matched = true;
                            row = htNext[row];
                        }
                        break;
                    }
                    slot = (slot + 1) & mask;
                }
            }

            if (!matched) {
                if (outLen >= outCap) {
                    outCap = outCap * 2;
                    leftOut = java.util.Arrays.copyOf(leftOut, outCap);
                    rightOut = java.util.Arrays.copyOf(rightOut, outCap);
                }
                leftOut[outLen] = i;
                rightOut[outLen] = -1;
                outLen++;
            }
        }
        return new Object[] { leftOut, rightOut, outLen, buildMatched };
    }

    /**
     * Gather long[] using index array. indices[i] < 0 → NULL sentinel (Long.MIN_VALUE).
     */
    public static long[] gatherLong(long[] src, int[] indices, int length) {
        long[] out = new long[length];
        for (int i = 0; i < length; i++) {
            int idx = indices[i];
            out[i] = idx >= 0 ? src[idx] : Long.MIN_VALUE;
        }
        return out;
    }

    /**
     * Gather double[] using index array. indices[i] < 0 → NaN (NULL sentinel).
     */
    public static double[] gatherDouble(double[] src, int[] indices, int length) {
        double[] out = new double[length];
        for (int i = 0; i < length; i++) {
            int idx = indices[i];
            out[i] = idx >= 0 ? src[idx] : Double.NaN;
        }
        return out;
    }

    // =========================================================================
    // Fused Join + Group-By + Aggregate (Dense)
    // =========================================================================

    /**
     * Fused probe + dim-column read + fact-column accumulation in a single pass.
     * Eliminates intermediate array allocations and Clojure orchestration overhead.
     *
     * <p>For each fact row: hash-probe fk → dimIdx, read dim group cols,
     * read fact agg cols, accumulate into dense group array.
     *
     * @param htKeys     Hash table keys (from hashJoinBuild)
     * @param htFirst    Hash table first-row index per slot
     * @param htNext     Hash table chain next-row pointers
     * @param capacity   Hash table capacity (power of 2)
     * @param probeKeys  Fact-side foreign key column
     * @param probeLength Number of fact rows
     * @param numGroupCols Number of dimension group columns
     * @param dimGroupCols Dimension group column arrays (read via dimIdx)
     * @param dimGroupMuls Group column multipliers for composite key
     * @param numAggs    Number of aggregations
     * @param aggTypes   Aggregation types (AGG_SUM, AGG_COUNT, etc.)
     * @param factAggCols Fact-side aggregation column arrays (read via factIdx)
     * @param maxKey     Dense output array size
     * @return double[maxKey * numAggs*2] flat accumulator array (same layout as dense group-by)
     */
    public static double[] fusedJoinGroupAggregateDense(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength,
            int numGroupCols, long[][] dimGroupCols, long[] dimGroupMuls,
            int numAggs, int[] aggTypes, double[][] factAggCols,
            int maxKey) {
        return fusedJoinGroupAggregateDenseRange(
                htKeys, htFirst, htNext, capacity,
                probeKeys, probeLength,
                numGroupCols, dimGroupCols, dimGroupMuls,
                numAggs, aggTypes, factAggCols,
                0, probeLength, maxKey);
    }

    /**
     * Range variant of fused join+group+aggregate for morsel-driven parallelism.
     * Returns flat double[maxKey * accSize] (same layout as fusedFilterGroupAggregateDenseRange).
     */
    private static double[] fusedJoinGroupAggregateDenseRange(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength,
            int numGroupCols, long[][] dimGroupCols, long[] dimGroupMuls,
            int numAggs, int[] aggTypes, double[][] factAggCols,
            int start, int end, int maxKey) {

        int mask = capacity - 1;
        final int accSize = numAggs * 2;
        double[] groups = new double[maxKey * accSize];
        // Init MIN/MAX accumulators
        for (int a = 0; a < numAggs; a++) {
            if (aggTypes[a] == AGG_MIN) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.POSITIVE_INFINITY;
            } else if (aggTypes[a] == AGG_MAX) {
                for (int k = 0; k < maxKey; k++) groups[k * accSize + a * 2] = Double.NEGATIVE_INFINITY;
            }
        }

        // Extract group column arrays to final locals for JIT (up to 6 unrolled)
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
            if (fk == Long.MIN_VALUE) continue; // NULL FK

            // Hash probe
            int slot = hashSlot(fk, capacity) & mask;
            while (htKeys[slot] != JOIN_EMPTY) {
                if (htKeys[slot] == fk) {
                    // Walk chain of matching dim rows
                    int dimIdx = htFirst[slot];
                    while (dimIdx >= 0) {
                        // Compute group key from dimension columns
                        int key = (int)(gc0[dimIdx] * gm0);
                        if (numGroupCols > 1) key += (int)(gc1[dimIdx] * gm1);
                        if (numGroupCols > 2) key += (int)(gc2[dimIdx] * gm2);
                        if (numGroupCols > 3) key += (int)(gc3[dimIdx] * gm3);
                        if (numGroupCols > 4) key += (int)(gc4[dimIdx] * gm4);
                        if (numGroupCols > 5) key += (int)(gc5[dimIdx] * gm5);
                        for (int g = 6; g < numGroupCols; g++)
                            key += (int)(dimGroupCols[g][dimIdx] * dimGroupMuls[g]);
                        assert key >= 0 && key < maxKey : "Dense key out of range: " + key + " (maxKey=" + maxKey + ")";

                        // Accumulate fact-side values into flat array
                        int base0 = key * accSize;
                        for (int a = 0; a < numAggs; a++) {
                            int off = base0 + a * 2;
                            switch (aggTypes[a]) {
                                case AGG_SUM:
                                case AGG_SUM_PRODUCT:
                                    groups[off] += factAggCols[a][factIdx];
                                    groups[off + 1] += 1;
                                    break;
                                case AGG_COUNT:
                                    groups[off + 1] += 1;
                                    break;
                                case AGG_MIN:
                                    groups[off] = Math.min(groups[off], factAggCols[a][factIdx]);
                                    groups[off + 1] += 1;
                                    break;
                                case AGG_MAX:
                                    groups[off] = Math.max(groups[off], factAggCols[a][factIdx]);
                                    groups[off + 1] += 1;
                                    break;
                            }
                        }
                        dimIdx = htNext[dimIdx];
                    }
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }

        return groups;
    }

    /**
     * Parallel version of fused join+group+aggregate using morsel-driven execution.
     * Each thread processes a range of fact rows with its own flat accumulators, merged at end.
     * Returns flat double[maxKey * accSize] (same layout as dense group-by).
     */
    public static double[] fusedJoinGroupAggregateDenseParallel(
            long[] htKeys, int[] htFirst, int[] htNext, int capacity,
            long[] probeKeys, int probeLength,
            int numGroupCols, long[][] dimGroupCols, long[] dimGroupMuls,
            int numAggs, int[] aggTypes, double[][] factAggCols,
            int maxKey) {

        if (probeLength < PARALLEL_THRESHOLD) {
            return fusedJoinGroupAggregateDense(
                    htKeys, htFirst, htNext, capacity,
                    probeKeys, probeLength,
                    numGroupCols, dimGroupCols, dimGroupMuls,
                    numAggs, aggTypes, factAggCols, maxKey);
        }

        // Cap thread count based on cache hierarchy
        long perThreadMem = (long) maxKey * numAggs * 2 * 8;
        int nThreads = Math.min(POOL.getParallelism(), effectiveGroupByThreads(perThreadMem));
        if (nThreads <= 1) {
            return fusedJoinGroupAggregateDense(
                    htKeys, htFirst, htNext, capacity,
                    probeKeys, probeLength,
                    numGroupCols, dimGroupCols, dimGroupMuls,
                    numAggs, aggTypes, factAggCols, maxKey);
        }
        int threadRange = (probeLength + nThreads - 1) / nThreads;
        final int accSize = numAggs * 2;

        @SuppressWarnings("unchecked")
        Future<double[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, probeLength);
            if (threadStart >= probeLength) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return fusedJoinGroupAggregateDenseRange(
                        htKeys, htFirst, htNext, capacity,
                        probeKeys, probeLength,
                        numGroupCols, dimGroupCols, dimGroupMuls,
                        numAggs, aggTypes, factAggCols,
                        threadStart, threadEnd, maxKey);
            });
        }

        // Merge thread results into flat array
        double[] merged = new double[maxKey * accSize];
        // Init MIN/MAX accumulators
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
                    // Skip empty groups (count in slot 1 is zero)
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
            throw new RuntimeException("Parallel fused join+group execution failed", e);
        }

        return merged;
    }

    // =========================================================================
    // Array Max (for group-by dense sizing)
    // =========================================================================

    /** Find the maximum value in a long[] array. Used for dense group-by key space sizing. */
    public static long arrayMaxLong(long[] data, int length) {
        long mx = Long.MIN_VALUE;
        for (int i = 0; i < length; i++) if (data[i] > mx) mx = data[i];
        return mx;
    }

    /** Find minimum value in a long array. */
    public static long arrayMinLong(long[] data, int length) {
        long mn = Long.MAX_VALUE;
        for (int i = 0; i < length; i++) if (data[i] < mn) mn = data[i];
        return mn;
    }

    /** Check if a double array contains any NaN values. Short-circuits on first NaN. */
    public static boolean arrayHasNaN(double[] data, int length) {
        for (int i = 0; i < length; i++) if (Double.isNaN(data[i])) return true;
        return false;
    }

    /** Check if a long array contains Long.MIN_VALUE (NULL sentinel). Short-circuits. */
    public static boolean arrayHasLongNull(long[] data, int length) {
        for (int i = 0; i < length; i++) if (data[i] == Long.MIN_VALUE) return true;
        return false;
    }

    /** Subtract scalar from each element, returning new long array. */
    public static long[] arraySubLongScalar(long[] data, long scalar, int length) {
        long[] result = new long[length];
        for (int i = 0; i < length; i++) result[i] = data[i] - scalar;
        return result;
    }

    // =========================================================================
    // COUNT DISTINCT Dense Group-By (JIT-isolated)
    // =========================================================================

    /**
     * Dense group-by for COUNT DISTINCT aggregate.
     * Each group has its own open-addressed hash table for tracking distinct values.
     * Returns int[maxKey] of distinct counts per group.
     * Delegates to Range variant for shared JIT compilation unit.
     */
    public static int[] fusedFilterGroupCountDistinctDense(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            long[] distinctCol,
            int length, int maxKey) {
        return fusedFilterGroupCountDistinctDenseRange(
                numLongPreds, longPredTypes, longCols, longLo, longHi,
                numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                numGroupCols, groupCols, groupMuls,
                distinctCol, 0, length, maxKey);
    }

    /**
     * Range variant of COUNT DISTINCT dense group-by.
     * Per-group open-addressed hash tables, lazy-initialized.
     */
    private static int[] fusedFilterGroupCountDistinctDenseRange(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            long[] distinctCol,
            int start, int end, int maxKey) {

        // Per-group hash tables: lazy-initialized
        long[][] groupTables = new long[maxKey][];  // hash table keys
        byte[][] groupOccupied = new byte[maxKey][]; // occupancy flags
        int[] groupCapacities = new int[maxKey];
        int[] groupSizes = new int[maxKey]; // count of distinct values per group
        int[] groupDistinct = new int[maxKey]; // result: distinct count per group

        final long MULT = 0x9E3779B97F4A7C15L; // Fibonacci hashing constant

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
            if (!evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) continue;

            int key = (int)(gc0[i] * gm0);
            if (numGroupCols > 1) key += (int)(gc1[i] * gm1);
            if (numGroupCols > 2) key += (int)(gc2[i] * gm2);
            if (numGroupCols > 3) key += (int)(gc3[i] * gm3);
            if (numGroupCols > 4) key += (int)(gc4[i] * gm4);
            if (numGroupCols > 5) key += (int)(gc5[i] * gm5);
            for (int g = 6; g < numGroupCols; g++) key += (int)(groupCols[g][i] * groupMuls[g]);

            long val = distinctCol[i];

            // Lazy-init hash table for this group
            if (groupTables[key] == null) {
                int cap = 16;
                groupTables[key] = new long[cap];
                groupOccupied[key] = new byte[cap];
                groupCapacities[key] = cap;
            }

            int cap = groupCapacities[key];
            long[] table = groupTables[key];
            byte[] occupied = groupOccupied[key];
            int capMask = cap - 1;
            int shift = 64 - Integer.numberOfTrailingZeros(cap);
            int slot = (int)((val * MULT) >>> shift) & capMask;

            boolean found = false;
            while (true) {
                if (occupied[slot] == 0) {
                    // New distinct value
                    table[slot] = val;
                    occupied[slot] = 1;
                    groupSizes[key]++;
                    groupDistinct[key]++;
                    found = true;
                    break;
                }
                if (table[slot] == val) { found = true; break; } // already seen
                slot = (slot + 1) & capMask; // linear probe
            }

            // Resize if > 70% load
            if (found && groupSizes[key] * 10 > cap * 7) {
                int newCap = cap << 1;
                int newMask = newCap - 1;
                int newShift = 64 - Integer.numberOfTrailingZeros(newCap);
                long[] newTable = new long[newCap];
                byte[] newOccupied = new byte[newCap];
                for (int j = 0; j < cap; j++) {
                    if (occupied[j] != 0) {
                        long v = table[j];
                        int ns = (int)((v * MULT) >>> newShift) & newMask;
                        while (newOccupied[ns] != 0) ns = (ns + 1) & newMask;
                        newTable[ns] = v;
                        newOccupied[ns] = 1;
                    }
                }
                groupTables[key] = newTable;
                groupOccupied[key] = newOccupied;
                groupCapacities[key] = newCap;
            }
        }

        return groupDistinct;
    }

    /**
     * Parallel COUNT DISTINCT dense group-by.
     * Per-thread independent hash tables, merged by re-inserting into target tables.
     */
    @SuppressWarnings("unchecked")
    public static int[] fusedFilterGroupCountDistinctDenseParallel(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            long[] distinctCol,
            int length, int maxKey) {

        if (length < PARALLEL_THRESHOLD) {
            return fusedFilterGroupCountDistinctDense(
                    numLongPreds, longPredTypes, longCols, longLo, longHi,
                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                    numGroupCols, groupCols, groupMuls,
                    distinctCol, length, maxKey);
        }

        int nThreads = POOL.getParallelism();
        int threadRange = (length + nThreads - 1) / nThreads;

        // Each thread returns: {long[][] tables, byte[][] occupied, int[] capacities}
        // for per-group hash table state (needed for set union merge)
        Future<Object[]>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int threadStart = t * threadRange;
            final int threadEnd = Math.min(threadStart + threadRange, length);
            if (threadStart >= length) { futures[t] = null; continue; }
            futures[t] = POOL.submit(() -> {
                return countDistinctDenseRangeWithState(
                        numLongPreds, longPredTypes, longCols, longLo, longHi,
                        numDblPreds, dblPredTypes, dblCols, dblLo, dblHi,
                        numGroupCols, groupCols, groupMuls,
                        distinctCol, threadStart, threadEnd, maxKey);
            });
        }

        try {
            // Merge: collect all per-group hash tables from all threads, union them
            long[][] mergedTables = new long[maxKey][];
            byte[][] mergedOccupied = new byte[maxKey][];
            int[] mergedCapacities = new int[maxKey];
            int[] mergedDistinct = new int[maxKey];
            long FMULT = 0x9E3779B97F4A7C15L;

            for (Future<Object[]> f : futures) {
                if (f == null) continue;
                Object[] state = f.get();
                long[][] tTables = (long[][]) state[0];
                byte[][] tOccupied = (byte[][]) state[1];
                int[] tCapacities = (int[]) state[2];

                for (int k = 0; k < maxKey; k++) {
                    if (tTables[k] == null) continue;
                    if (mergedTables[k] == null) {
                        // First thread with data for this group — take ownership
                        mergedTables[k] = tTables[k];
                        mergedOccupied[k] = tOccupied[k];
                        mergedCapacities[k] = tCapacities[k];
                        // Count entries
                        int cnt = 0;
                        byte[] occ = tOccupied[k];
                        for (int j = 0; j < tCapacities[k]; j++) if (occ[j] != 0) cnt++;
                        mergedDistinct[k] = cnt;
                    } else {
                        // Merge: re-insert thread's entries into merged table
                        byte[] tOcc = tOccupied[k];
                        long[] tTab = tTables[k];
                        int tCap = tCapacities[k];
                        for (int j = 0; j < tCap; j++) {
                            if (tOcc[j] == 0) continue;
                            long val = tTab[j];
                            // Insert into merged table for group k
                            int mCap = mergedCapacities[k];
                            long[] mTab = mergedTables[k];
                            byte[] mOcc = mergedOccupied[k];
                            int mMask = mCap - 1;
                            int mShift = 64 - Integer.numberOfTrailingZeros(mCap);
                            int slot = (int)((val * FMULT) >>> mShift) & mMask;
                            boolean isNew = false;
                            while (true) {
                                if (mOcc[slot] == 0) {
                                    mTab[slot] = val;
                                    mOcc[slot] = 1;
                                    mergedDistinct[k]++;
                                    isNew = true;
                                    break;
                                }
                                if (mTab[slot] == val) break; // already in merged
                                slot = (slot + 1) & mMask;
                            }
                            // Resize if needed
                            if (isNew && mergedDistinct[k] * 10 > mCap * 7) {
                                int newCap = mCap << 1;
                                int newMask = newCap - 1;
                                int newShift = 64 - Integer.numberOfTrailingZeros(newCap);
                                long[] newTab = new long[newCap];
                                byte[] newOcc = new byte[newCap];
                                for (int x = 0; x < mCap; x++) {
                                    if (mOcc[x] != 0) {
                                        int ns = (int)((mTab[x] * FMULT) >>> newShift) & newMask;
                                        while (newOcc[ns] != 0) ns = (ns + 1) & newMask;
                                        newTab[ns] = mTab[x];
                                        newOcc[ns] = 1;
                                    }
                                }
                                mergedTables[k] = newTab;
                                mergedOccupied[k] = newOcc;
                                mergedCapacities[k] = newCap;
                            }
                        }
                    }
                }
            }
            return mergedDistinct;
        } catch (Exception e) {
            throw new RuntimeException("Parallel execution failed", e);
        }
    }

    /**
     * COUNT DISTINCT range with full hash table state returned for parallel merge.
     * Returns Object[] = {long[][] tables, byte[][] occupied, int[] capacities}.
     */
    private static Object[] countDistinctDenseRangeWithState(
            int numLongPreds, int[] longPredTypes,
            long[][] longCols, long[] longLo, long[] longHi,
            int numDblPreds, int[] dblPredTypes,
            double[][] dblCols, double[] dblLo, double[] dblHi,
            int numGroupCols, long[][] groupCols, long[] groupMuls,
            long[] distinctCol,
            int start, int end, int maxKey) {

        long[][] groupTables = new long[maxKey][];
        byte[][] groupOccupied = new byte[maxKey][];
        int[] groupCapacities = new int[maxKey];
        int[] groupSizes = new int[maxKey];

        final long MULT = 0x9E3779B97F4A7C15L;

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
            if (!evaluatePredicates(numLongPreds, longPredTypes, longCols, longLo, longHi,
                                    numDblPreds, dblPredTypes, dblCols, dblLo, dblHi, i)) continue;

            int key = (int)(gc0[i] * gm0);
            if (numGroupCols > 1) key += (int)(gc1[i] * gm1);
            if (numGroupCols > 2) key += (int)(gc2[i] * gm2);
            if (numGroupCols > 3) key += (int)(gc3[i] * gm3);
            if (numGroupCols > 4) key += (int)(gc4[i] * gm4);
            if (numGroupCols > 5) key += (int)(gc5[i] * gm5);
            for (int g = 6; g < numGroupCols; g++) key += (int)(groupCols[g][i] * groupMuls[g]);

            long val = distinctCol[i];

            if (groupTables[key] == null) {
                int cap = 16;
                groupTables[key] = new long[cap];
                groupOccupied[key] = new byte[cap];
                groupCapacities[key] = cap;
            }

            int cap = groupCapacities[key];
            long[] table = groupTables[key];
            byte[] occupied = groupOccupied[key];
            int capMask = cap - 1;
            int shift = 64 - Integer.numberOfTrailingZeros(cap);
            int slot = (int)((val * MULT) >>> shift) & capMask;

            boolean isNew = false;
            while (true) {
                if (occupied[slot] == 0) {
                    table[slot] = val;
                    occupied[slot] = 1;
                    groupSizes[key]++;
                    isNew = true;
                    break;
                }
                if (table[slot] == val) break;
                slot = (slot + 1) & capMask;
            }

            if (isNew && groupSizes[key] * 10 > cap * 7) {
                int newCap = cap << 1;
                int newMask = newCap - 1;
                int newShift = 64 - Integer.numberOfTrailingZeros(newCap);
                long[] newTable = new long[newCap];
                byte[] newOccupied = new byte[newCap];
                for (int j = 0; j < cap; j++) {
                    if (occupied[j] != 0) {
                        long v = table[j];
                        int ns = (int)((v * MULT) >>> newShift) & newMask;
                        while (newOccupied[ns] != 0) ns = (ns + 1) & newMask;
                        newTable[ns] = v;
                        newOccupied[ns] = 1;
                    }
                }
                groupTables[key] = newTable;
                groupOccupied[key] = newOccupied;
                groupCapacities[key] = newCap;
            }
        }

        return new Object[] { groupTables, groupOccupied, groupCapacities };
    }

    // =========================================================================
    // Utility
    // =========================================================================

    /**
     * Get SIMD lane count for doubles.
     * On AVX-512: 8, on AVX-2: 4, on SSE: 2.
     */
    public static int getDoubleLanes() {
        return DOUBLE_LANES;
    }

    /**
     * Get SIMD lane count for longs.
     */
    public static int getLongLanes() {
        return LONG_LANES;
    }

    // =========================================================================
    // QuickSelect (O(N) percentile)
    // =========================================================================

    /**
     * Hoare's QuickSelect: find k-th smallest element in work[0..n-1].
     * Modifies the work array in-place. Average O(N), worst O(N^2).
     * For PERCENTILE_CONT interpolation, caller can call twice for k and k+1.
     */
    public static double quickSelect(double[] work, int n, int k) {
        if (n == 0) return Double.NaN;
        if (n == 1) return work[0];
        if (k < 0) k = 0;
        if (k >= n) k = n - 1;
        int lo = 0, hi = n - 1;
        java.util.Random rng = new java.util.Random(42);
        while (lo < hi) {
            // Median-of-3 pivot for better partitioning
            int mid = lo + rng.nextInt(hi - lo + 1);
            double pivot = work[mid];
            work[mid] = work[lo];
            work[lo] = pivot;
            int i = lo, j = hi + 1;
            while (true) {
                do { i++; } while (i <= hi && work[i] < pivot);
                do { j--; } while (work[j] > pivot);
                if (i >= j) break;
                double tmp = work[i]; work[i] = work[j]; work[j] = tmp;
            }
            work[lo] = work[j]; work[j] = pivot;
            if (j == k) return work[k];
            if (j < k) lo = j + 1;
            else hi = j - 1;
        }
        return work[lo];
    }

    /**
     * Compute percentile using QuickSelect with PERCENTILE_CONT interpolation.
     * Copies n elements from src to internal work array.
     * @param src source array
     * @param n number of elements to consider
     * @param percentile value in [0.0, 1.0]
     * @return interpolated percentile value
     */
    public static double percentile(double[] src, int n, double percentile) {
        if (n == 0) return Double.NaN;
        if (n == 1) return src[0];
        double[] work = new double[n];
        System.arraycopy(src, 0, work, 0, n);
        return percentileInPlace(work, n, percentile);
    }

    /** Percentile on a work array that may be modified in-place. */
    private static double percentileInPlace(double[] work, int n, double percentile) {
        if (n == 0) return Double.NaN;
        if (n == 1) return work[0];
        double idx = percentile * (n - 1);
        int lo = (int) Math.floor(idx);
        int hi = (int) Math.ceil(idx);
        if (lo == hi) {
            return quickSelect(work, n, lo);
        }
        double vLo = quickSelect(work, n, lo);
        // After quickSelect for lo, work[lo+1..n-1] are all >= vLo
        double vHi = Double.POSITIVE_INFINITY;
        for (int i = lo + 1; i < n; i++) {
            if (work[i] < vHi) vHi = work[i];
        }
        double frac = idx - lo;
        return vLo + frac * (vHi - vLo);
    }

    /**
     * Compute percentile with predicate mask filtering.
     * Gathers matching values from src where mask[i] == 1, then runs quickSelect.
     */
    public static double percentileFiltered(double[] src, long[] mask, int length, double pct) {
        // Count matches
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1) count++;
        }
        if (count == 0) return Double.NaN;
        // Gather matching values
        double[] work = new double[count];
        int pos = 0;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1) work[pos++] = src[i];
        }
        return percentileInPlace(work, count, pct);
    }

    /**
     * Compute percentile with long[] source and predicate mask filtering.
     */
    public static double percentileFilteredLong(long[] src, long[] mask, int length, double pct) {
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1) count++;
        }
        if (count == 0) return Double.NaN;
        double[] work = new double[count];
        int pos = 0;
        for (int i = 0; i < length; i++) {
            if (mask[i] == 1) work[pos++] = (double) src[i];
        }
        return percentileInPlace(work, count, pct);
    }

    /**
     * Two-pass grouped percentile for dense (low-cardinality) groups.
     * Pass 1: count per-group sizes.
     * Pass 2: scatter values into flat contiguous array.
     * Pass 3: per-group quickSelect.
     *
     * @param groupKeys  long[] group key per row (0-based dense)
     * @param values     double[] values to aggregate
     * @param mask       long[] predicate mask (1=include, null=all)
     * @param length     number of rows
     * @param maxKey     max group key + 1
     * @param pct        percentile fraction [0,1]
     * @return double[maxKey] with percentile per group (NaN for empty groups)
     */
    public static double[] groupPercentileDense(long[] groupKeys, double[] values,
                                                 long[] mask, int length, int maxKey, double pct) {
        // Pass 1: count per-group sizes
        int[] counts = new int[maxKey];
        int totalCount = 0;
        for (int i = 0; i < length; i++) {
            if (mask == null || mask[i] == 1) {
                int g = (int) groupKeys[i];
                if (g >= 0 && g < maxKey) {
                    counts[g]++;
                    totalCount++;
                }
            }
        }

        // Compute offsets (exclusive prefix sum)
        int[] offsets = new int[maxKey];
        int offset = 0;
        for (int g = 0; g < maxKey; g++) {
            offsets[g] = offset;
            offset += counts[g];
        }

        // Pass 2: scatter values into flat array
        double[] flat = new double[totalCount];
        int[] writePos = new int[maxKey];
        System.arraycopy(offsets, 0, writePos, 0, maxKey);
        for (int i = 0; i < length; i++) {
            if (mask == null || mask[i] == 1) {
                int g = (int) groupKeys[i];
                if (g >= 0 && g < maxKey) {
                    flat[writePos[g]++] = values[i];
                }
            }
        }

        // Pass 3: per-group quickSelect
        double[] results = new double[maxKey];
        for (int g = 0; g < maxKey; g++) {
            int n = counts[g];
            if (n == 0) {
                results[g] = Double.NaN;
            } else {
                // percentileInPlace works on the slice [offsets[g]..offsets[g]+n)
                // We need a sub-array view, but quickSelect modifies in-place, which is fine
                // since each group's region in flat[] is independent
                double[] sub = new double[n];
                System.arraycopy(flat, offsets[g], sub, 0, n);
                results[g] = percentileInPlace(sub, n, pct);
            }
        }
        return results;
    }

    /**
     * Two-pass grouped percentile for dense groups with long[] values.
     */
    public static double[] groupPercentileDenseLong(long[] groupKeys, long[] values,
                                                     long[] mask, int length, int maxKey, double pct) {
        int[] counts = new int[maxKey];
        int totalCount = 0;
        for (int i = 0; i < length; i++) {
            if (mask == null || mask[i] == 1) {
                int g = (int) groupKeys[i];
                if (g >= 0 && g < maxKey) {
                    counts[g]++;
                    totalCount++;
                }
            }
        }
        int[] offsets = new int[maxKey];
        int offset = 0;
        for (int g = 0; g < maxKey; g++) {
            offsets[g] = offset;
            offset += counts[g];
        }
        double[] flat = new double[totalCount];
        int[] writePos = new int[maxKey];
        System.arraycopy(offsets, 0, writePos, 0, maxKey);
        for (int i = 0; i < length; i++) {
            if (mask == null || mask[i] == 1) {
                int g = (int) groupKeys[i];
                if (g >= 0 && g < maxKey) {
                    flat[writePos[g]++] = (double) values[i];
                }
            }
        }
        double[] results = new double[maxKey];
        for (int g = 0; g < maxKey; g++) {
            int n = counts[g];
            if (n == 0) {
                results[g] = Double.NaN;
            } else {
                double[] sub = new double[n];
                System.arraycopy(flat, offsets[g], sub, 0, n);
                results[g] = percentileInPlace(sub, n, pct);
            }
        }
        return results;
    }
}
