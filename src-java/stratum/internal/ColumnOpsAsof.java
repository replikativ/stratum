package stratum.internal;

import java.util.Arrays;
import java.util.concurrent.Future;

/**
 * ASOF join hot path.
 *
 * <p>Algorithm (DuckDB-style, in-memory variant):
 * <ol>
 *   <li>Hash-partition both sides on equality keys (single partition when no eq keys).</li>
 *   <li>Sort each partition by the inequality column.</li>
 *   <li>Two-pointer merge with a monotonic build cursor — at most one match per probe row.</li>
 * </ol>
 *
 * <p>Kept in its own class to avoid JIT cross-section interference with the equi-join
 * hot path (see ColumnOps notes in MEMORY.md).
 */
public final class ColumnOpsAsof {
    private ColumnOpsAsof() {}

    /** NULL sentinel for long timestamp columns (matches stratum convention). */
    public static final long NULL_TS = Long.MIN_VALUE;

    /** Match-condition operator codes. */
    public static final int OP_GTE = 0;  // probe.ts >= build.ts
    public static final int OP_GT  = 1;  // probe.ts >  build.ts
    public static final int OP_LTE = 2;  // probe.ts <= build.ts
    public static final int OP_LT  = 3;  // probe.ts <  build.ts

    /**
     * Filter (mask + non-NULL) and sort by the inequality column, ascending.
     * Returns the original-row indices in sort order; length = number of valid rows.
     *
     * <p>Fast path: packed-long sort with adaptive idx-bits. Ascending sort by
     * {@code Arrays.parallelSort(long[])} on {@code (ts << idxBits) | localIdx}.
     * Falls back to boxed-Integer indirect sort when ts overflows the available bits.
     */
    static int[] sortValidByTsAsc(long[] ts, int len, long[] mask) {
        int[] tmp = new int[len];
        int valid = 0;
        for (int i = 0; i < len; i++) {
            if (mask != null && mask[i] == 0) continue;
            if (ts[i] == NULL_TS) continue;
            tmp[valid++] = i;
        }
        if (valid <= 1) {
            int[] out = new int[valid];
            if (valid == 1) out[0] = tmp[0];
            return out;
        }
        // idxBits = ceil(log2(valid)); reserve at least 16 bits for headroom.
        int idxBits = Math.max(16, 32 - Integer.numberOfLeadingZeros(valid - 1));
        // Packed format: (ts << idxBits) | localIdx. Requires ts ≥ 0 and ts < 2^(64-idxBits).
        long tsLimit = (idxBits >= 63) ? 0L : (1L << (64 - idxBits));
        boolean canPack = (idxBits <= 48);
        if (canPack) {
            for (int i = 0; i < valid; i++) {
                long t = ts[tmp[i]];
                if (t < 0L || t >= tsLimit) { canPack = false; break; }
            }
        }
        if (canPack) {
            long idxMask = (1L << idxBits) - 1L;
            long[] packed = new long[valid];
            for (int i = 0; i < valid; i++) {
                packed[i] = (ts[tmp[i]] << idxBits) | (i & idxMask);
            }
            Arrays.parallelSort(packed);
            int[] out = new int[valid];
            for (int k = 0; k < valid; k++) {
                int origLocal = (int) (packed[k] & idxMask);
                out[k] = tmp[origLocal];
            }
            return out;
        } else {
            Integer[] perm = new Integer[valid];
            for (int i = 0; i < valid; i++) perm[i] = tmp[i];
            final long[] keys = ts;
            Arrays.sort(perm, (a, b) -> Long.compare(keys[a], keys[b]));
            int[] out = new int[valid];
            for (int i = 0; i < valid; i++) out[i] = perm[i];
            return out;
        }
    }

    // Two-pointer merge functions. Both sides must be pre-sorted ascending by ts.
    // Each returns int[probeValidLen] — best build sorted-position per probe sorted-
    // position, or -1 if none.
    //
    // The four operators only differ in the inner-loop predicate and which
    // cursor position becomes the match. The build cursor monotonically advances
    // because probe is sorted ascending, so the search is amortized O(B+P).

    /** probe.ts >= build.ts — latest build at-or-before probe. */
    static int[] mergeGteSorted(long[] bTs, int bn, long[] pTs, int pn) {
        int[] match = new int[pn];
        int b = 0;
        for (int p = 0; p < pn; p++) {
            long t = pTs[p];
            while (b < bn && bTs[b] <= t) b++;
            match[p] = (b > 0) ? (b - 1) : -1;
        }
        return match;
    }

    /** probe.ts > build.ts — latest build strictly before probe. */
    static int[] mergeGtSorted(long[] bTs, int bn, long[] pTs, int pn) {
        int[] match = new int[pn];
        int b = 0;
        for (int p = 0; p < pn; p++) {
            long t = pTs[p];
            while (b < bn && bTs[b] < t) b++;
            match[p] = (b > 0) ? (b - 1) : -1;
        }
        return match;
    }

    /** probe.ts <= build.ts — earliest build at-or-after probe. */
    static int[] mergeLteSorted(long[] bTs, int bn, long[] pTs, int pn) {
        int[] match = new int[pn];
        int b = 0;
        for (int p = 0; p < pn; p++) {
            long t = pTs[p];
            while (b < bn && bTs[b] < t) b++;
            match[p] = (b < bn) ? b : -1;
        }
        return match;
    }

    /** probe.ts < build.ts — earliest build strictly after probe. */
    static int[] mergeLtSorted(long[] bTs, int bn, long[] pTs, int pn) {
        int[] match = new int[pn];
        int b = 0;
        for (int p = 0; p < pn; p++) {
            long t = pTs[p];
            while (b < bn && bTs[b] <= t) b++;
            match[p] = (b < bn) ? b : -1;
        }
        return match;
    }

    /**
     * Single-partition ASOF join. No equality keys — the entire build/probe
     * form one partition. Use {@link #asofJoinPartitioned} when equality keys
     * are present.
     *
     * @param buildTs    build inequality column (long timestamps); Long.MIN_VALUE = NULL
     * @param buildLen   build row count
     * @param buildMask  optional long[] mask (1=keep, 0=drop), or null
     * @param probeTs    probe inequality column
     * @param probeLen   probe row count
     * @param probeMask  optional long[] mask, or null
     * @param op         one of OP_GTE / OP_GT / OP_LTE / OP_LT
     * @param isLeft     true = LEFT outer (emit unmatched probes with -1), false = INNER
     * @return Object[] = {int[] leftIndices, int[] rightIndices, Integer resultLength}
     *         with rightIndices == -1 marking unmatched (LEFT) rows.
     */
    public static Object[] asofJoinSinglePartition(
            long[] buildTs, int buildLen, long[] buildMask,
            long[] probeTs, int probeLen, long[] probeMask,
            int op, boolean isLeft) {

        int[] bSortedIdx = sortValidByTsAsc(buildTs, buildLen, buildMask);
        int bValid = bSortedIdx.length;
        long[] bSortedTs = new long[bValid];
        for (int i = 0; i < bValid; i++) bSortedTs[i] = buildTs[bSortedIdx[i]];

        int[] pSortedIdx = sortValidByTsAsc(probeTs, probeLen, probeMask);
        int pValid = pSortedIdx.length;
        long[] pSortedTs = new long[pValid];
        for (int i = 0; i < pValid; i++) pSortedTs[i] = probeTs[pSortedIdx[i]];

        int[] matchSortedPos;
        switch (op) {
            case OP_GTE: matchSortedPos = mergeGteSorted(bSortedTs, bValid, pSortedTs, pValid); break;
            case OP_GT:  matchSortedPos = mergeGtSorted (bSortedTs, bValid, pSortedTs, pValid); break;
            case OP_LTE: matchSortedPos = mergeLteSorted(bSortedTs, bValid, pSortedTs, pValid); break;
            case OP_LT:  matchSortedPos = mergeLtSorted (bSortedTs, bValid, pSortedTs, pValid); break;
            default: throw new IllegalArgumentException("Unknown ASOF op: " + op);
        }

        // Inverse perm: probeOrigIdx → sortedPos (or -1 if filtered)
        int[] probeOrigToSortedPos = new int[probeLen];
        Arrays.fill(probeOrigToSortedPos, -1);
        for (int k = 0; k < pValid; k++) probeOrigToSortedPos[pSortedIdx[k]] = k;

        // Materialize in probe input order
        int[] leftOut = new int[probeLen];
        int[] rightOut = new int[probeLen];
        int outLen = 0;
        for (int p = 0; p < probeLen; p++) {
            int sp = probeOrigToSortedPos[p];
            if (sp < 0) continue; // probe row filtered (mask=0 or NULL ts) — never emitted
            int matchSp = matchSortedPos[sp];
            int buildOrig = (matchSp < 0) ? -1 : bSortedIdx[matchSp];
            if (!isLeft && buildOrig < 0) continue; // INNER drops unmatched
            leftOut[outLen] = p;
            rightOut[outLen] = buildOrig;
            outLen++;
        }
        return new Object[] { leftOut, rightOut, Integer.valueOf(outLen) };
    }

    // ============================================================================
    // Partitioned ASOF: radix-partition by equality key, sort per partition,
    // two-pointer merge per partition, parallel across partitions.
    // ============================================================================

    /**
     * Partitioning hash multiplier — distinct from the per-partition multiplier
     * used by hash group-by paths to prevent the radix-clustering footgun
     * (see MEMORY.md: "Radix partitioning hash constant clash").
     */
    private static final long PART_HASH_MUL = 0x517CC1B727220A95L;

    /** Default radix bit count for ASOF partitioning. 256 buckets matches hash group-by. */
    public static final int DEFAULT_PART_BITS = 8;

    /**
     * Partitioned ASOF join.
     *
     * <p>Both sides must already have composite equality keys encoded as a single
     * long per row (use {@link ColumnOpsExt#compositeKeyEncode} on the Clojure side
     * with shared multipliers). NULL composite keys (Long.MIN_VALUE) are filtered
     * (or LEFT-NULL-padded for asof-left).
     *
     * @param buildKey   composite equality key per build row
     * @param probeKey   composite equality key per probe row
     * @param buildTs    build inequality column
     * @param buildLen   build row count
     * @param buildMask  optional build mask (1=keep)
     * @param probeTs    probe inequality column
     * @param probeLen   probe row count
     * @param probeMask  optional probe mask
     * @param op         OP_GTE / OP_GT / OP_LTE / OP_LT
     * @param isLeft     true → LEFT outer; false → INNER
     * @param partBits   number of partition bits (e.g. 8 → 256 partitions)
     */
    public static Object[] asofJoinPartitioned(
            long[] buildKey, long[] buildTs, int buildLen, long[] buildMask,
            long[] probeKey, long[] probeTs, int probeLen, long[] probeMask,
            int op, boolean isLeft, int partBits) {

        if (buildKey == null || probeKey == null) {
            throw new IllegalArgumentException("asofJoinPartitioned requires non-null composite keys; "
                + "use asofJoinSinglePartition for the no-equality-keys case.");
        }
        if (partBits < 0 || partBits > 16) {
            throw new IllegalArgumentException("partBits must be in [0, 16], got " + partBits);
        }

        final int numParts = 1 << partBits;
        final int partMask = numParts - 1;
        final int hashShift = 64 - partBits;

        // ---- Pass 1: count valid rows per partition ----
        int[] buildCount = new int[numParts];
        for (int i = 0; i < buildLen; i++) {
            if (buildMask != null && buildMask[i] == 0) continue;
            if (buildTs[i] == NULL_TS) continue;
            long k = buildKey[i];
            if (k == Long.MIN_VALUE) continue;
            buildCount[partOf(k, partMask, hashShift)]++;
        }
        int[] probeCount = new int[numParts];
        for (int i = 0; i < probeLen; i++) {
            if (probeMask != null && probeMask[i] == 0) continue;
            if (probeTs[i] == NULL_TS) continue;
            long k = probeKey[i];
            if (k == Long.MIN_VALUE) continue;
            probeCount[partOf(k, partMask, hashShift)]++;
        }

        // Prefix sums into offsets
        int[] buildOffsets = new int[numParts + 1];
        int[] probeOffsets = new int[numParts + 1];
        for (int p = 0; p < numParts; p++) {
            buildOffsets[p + 1] = buildOffsets[p] + buildCount[p];
            probeOffsets[p + 1] = probeOffsets[p] + probeCount[p];
        }
        final int totalBuildValid = buildOffsets[numParts];
        final int totalProbeValid = probeOffsets[numParts];

        // ---- Pass 2: scatter into partition-contiguous buffers ----
        final int[] buildFlatIdx = new int[totalBuildValid];
        final long[] buildFlatTs = new long[totalBuildValid];
        final int[] probeFlatIdx = new int[totalProbeValid];
        final long[] probeFlatTs = new long[totalProbeValid];

        int[] bCur = buildOffsets.clone();
        int[] pCur = probeOffsets.clone();
        for (int i = 0; i < buildLen; i++) {
            if (buildMask != null && buildMask[i] == 0) continue;
            long t = buildTs[i];
            if (t == NULL_TS) continue;
            long k = buildKey[i];
            if (k == Long.MIN_VALUE) continue;
            int part = partOf(k, partMask, hashShift);
            int pos = bCur[part]++;
            buildFlatIdx[pos] = i;
            buildFlatTs[pos] = t;
        }
        for (int i = 0; i < probeLen; i++) {
            if (probeMask != null && probeMask[i] == 0) continue;
            long t = probeTs[i];
            if (t == NULL_TS) continue;
            long k = probeKey[i];
            if (k == Long.MIN_VALUE) continue;
            int part = partOf(k, partMask, hashShift);
            int pos = pCur[part]++;
            probeFlatIdx[pos] = i;
            probeFlatTs[pos] = t;
        }

        // ---- Pass 3: per-partition sort + merge (parallel across partitions) ----
        final int[] probeOrigToBuild = new int[probeLen];
        Arrays.fill(probeOrigToBuild, -1);

        final int opFinal = op;
        Future<?>[] futures = new Future<?>[numParts];
        for (int p = 0; p < numParts; p++) {
            final int bLo = buildOffsets[p], bHi = buildOffsets[p + 1];
            final int pLo = probeOffsets[p], pHi = probeOffsets[p + 1];
            if (pLo == pHi) { futures[p] = null; continue; }
            futures[p] = ColumnOps.POOL.submit(() -> {
                sortPartitionInPlace(buildFlatTs, buildFlatIdx, bLo, bHi);
                sortPartitionInPlace(probeFlatTs, probeFlatIdx, pLo, pHi);
                mergeAndStorePartition(buildFlatTs, buildFlatIdx, bLo, bHi,
                                       probeFlatTs, probeFlatIdx, pLo, pHi,
                                       probeOrigToBuild, opFinal);
            });
        }
        for (Future<?> f : futures) {
            if (f != null) {
                try { f.get(); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
                catch (java.util.concurrent.ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                    throw new RuntimeException(cause);
                }
            }
        }

        // ---- Pass 4: materialize in probe input order ----
        int[] leftOut = new int[probeLen];
        int[] rightOut = new int[probeLen];
        int outLen = 0;
        for (int i = 0; i < probeLen; i++) {
            if (probeMask != null && probeMask[i] == 0) continue;
            if (probeTs[i] == NULL_TS) continue;
            long k = probeKey[i];
            if (k == Long.MIN_VALUE) {
                // NULL eq key never matches anything. INNER drops, LEFT keeps with -1.
                if (isLeft) {
                    leftOut[outLen] = i;
                    rightOut[outLen] = -1;
                    outLen++;
                }
                continue;
            }
            int b = probeOrigToBuild[i];
            if (!isLeft && b < 0) continue;
            leftOut[outLen] = i;
            rightOut[outLen] = b;
            outLen++;
        }
        return new Object[] { leftOut, rightOut, Integer.valueOf(outLen) };
    }

    private static int partOf(long key, int partMask, int hashShift) {
        long h = key * PART_HASH_MUL;
        return (int) ((h >>> hashShift) & partMask);
    }

    /**
     * In-place tandem sort of (ts[lo..hi), idx[lo..hi)) ascending by ts.
     *
     * <p>Fast path: pack (ts, localIdx) into a long with idxBits chosen
     * adaptively (≥ ceil(log2(n))) and sort the packed array. Falls back to
     * boxed Integer indirect sort when ts overflows the available bits.
     */
    static void sortPartitionInPlace(long[] ts, int[] idx, int lo, int hi) {
        int n = hi - lo;
        if (n <= 1) return;

        int idxBits = Math.max(16, 32 - Integer.numberOfLeadingZeros(n - 1));
        long tsLimit = (idxBits >= 63) ? 0L : (1L << (64 - idxBits));
        boolean canPack = (idxBits <= 48);
        if (canPack) {
            for (int i = lo; i < hi; i++) {
                long t = ts[i];
                if (t < 0L || t >= tsLimit) { canPack = false; break; }
            }
        }

        if (canPack) {
            long idxMask = (1L << idxBits) - 1L;
            long[] packed = new long[n];
            for (int i = 0; i < n; i++) {
                packed[i] = (ts[lo + i] << idxBits) | ((long) (i & idxMask));
            }
            Arrays.sort(packed);
            long[] tsTmp = new long[n];
            int[] idxTmp = new int[n];
            for (int k = 0; k < n; k++) {
                int origLocal = (int) (packed[k] & idxMask);
                tsTmp[k] = ts[lo + origLocal];
                idxTmp[k] = idx[lo + origLocal];
            }
            System.arraycopy(tsTmp, 0, ts, lo, n);
            System.arraycopy(idxTmp, 0, idx, lo, n);
        } else {
            Integer[] perm = new Integer[n];
            for (int i = 0; i < n; i++) perm[i] = i;
            final long[] tsRef = ts;
            final int loFinal = lo;
            Arrays.sort(perm, (a, b) -> Long.compare(tsRef[loFinal + a], tsRef[loFinal + b]));
            long[] tsTmp = new long[n];
            int[] idxTmp = new int[n];
            for (int i = 0; i < n; i++) {
                tsTmp[i] = ts[lo + perm[i]];
                idxTmp[i] = idx[lo + perm[i]];
            }
            System.arraycopy(tsTmp, 0, ts, lo, n);
            System.arraycopy(idxTmp, 0, idx, lo, n);
        }
    }

    /**
     * Merge a single partition: for each probe row in [pLo, pHi), find the best
     * matching build row in [bLo, bHi) under the given op. Writes results into
     * {@code probeOrigToBuild} indexed by the probe's original row index.
     *
     * <p>Both sides must be pre-sorted ascending by ts.
     *
     * <p>Inner loops are duplicated per op (no switch in hot loop) to keep JIT happy.
     */
    static void mergeAndStorePartition(
            long[] bTs, int[] bIdx, int bLo, int bHi,
            long[] pTs, int[] pIdx, int pLo, int pHi,
            int[] probeOrigToBuild, int op) {
        int b = bLo;
        if (op == OP_GTE) {
            for (int p = pLo; p < pHi; p++) {
                long t = pTs[p];
                while (b < bHi && bTs[b] <= t) b++;
                int m = (b > bLo) ? (b - 1) : -1;
                probeOrigToBuild[pIdx[p]] = (m >= 0) ? bIdx[m] : -1;
            }
        } else if (op == OP_GT) {
            for (int p = pLo; p < pHi; p++) {
                long t = pTs[p];
                while (b < bHi && bTs[b] < t) b++;
                int m = (b > bLo) ? (b - 1) : -1;
                probeOrigToBuild[pIdx[p]] = (m >= 0) ? bIdx[m] : -1;
            }
        } else if (op == OP_LTE) {
            for (int p = pLo; p < pHi; p++) {
                long t = pTs[p];
                while (b < bHi && bTs[b] < t) b++;
                int m = (b < bHi) ? b : -1;
                probeOrigToBuild[pIdx[p]] = (m >= 0) ? bIdx[m] : -1;
            }
        } else { // OP_LT
            for (int p = pLo; p < pHi; p++) {
                long t = pTs[p];
                while (b < bHi && bTs[b] <= t) b++;
                int m = (b < bHi) ? b : -1;
                probeOrigToBuild[pIdx[p]] = (m >= 0) ? bIdx[m] : -1;
            }
        }
    }
}
