package stratum.internal;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD lane-wise reductions over EXACT long monoids.
 *
 * A lane-wise reduction keeps one partial per SIMD lane and combines the lane-partials at the end in an
 * unspecified order. For long (integer) ops that order is irrelevant — the ops are associative, so the
 * result is identical regardless of vector width. This is the property that makes SIMD reduction
 * DETERMINISTIC here. It is NOT true for floating-point ADD/MUL (not associative — the result varies with
 * vector width), which is exactly why this kernel is long-only and why the caller must gate float
 * reductions on a proof of associativity (wandler) rather than vectorizing them silently.
 *
 * All methods are static for maximum JIT inlining; species is fixed final (SPECIES_PREFERRED).
 */
public final class SimdReduce {
    private SimdReduce() {}

    static final VectorSpecies<Long> SP = LongVector.SPECIES_PREFERRED;
    static final int LANES = SP.length();

    public static final int OP_ADD = 0, OP_OR = 1, OP_AND = 2, OP_XOR = 3, OP_MIN = 4, OP_MAX = 5;

    // Constant-op specialized reductions. Vector API intrinsics require the op to be a COMPILE-TIME
    // CONSTANT, so each op gets its own method (a runtime `switch` over the op defeats intrinsification).
    // All of ADD/OR/AND/XOR/MIN/MAX are AVX2-supported for longs (unlike 64-bit MUL, which is not).
    // Deterministic: the lane-partials combine under an associative long op, so vector width is irrelevant.

    public static long sumLong(long[] a, int n) {
        LongVector acc = LongVector.broadcast(SP, 0L); int b = SP.loopBound(n), i = 0;
        for (; i < b; i += LANES) acc = acc.add(LongVector.fromArray(SP, a, i));
        long r = acc.reduceLanes(VectorOperators.ADD);
        for (; i < n; i++) r += a[i];
        return r;
    }
    public static long orLong(long[] a, int n) {
        LongVector acc = LongVector.broadcast(SP, 0L); int b = SP.loopBound(n), i = 0;
        for (; i < b; i += LANES) acc = acc.lanewise(VectorOperators.OR, LongVector.fromArray(SP, a, i));
        long r = acc.reduceLanes(VectorOperators.OR);
        for (; i < n; i++) r |= a[i];
        return r;
    }
    public static long minLong(long[] a, int n) {
        LongVector acc = LongVector.broadcast(SP, Long.MAX_VALUE); int b = SP.loopBound(n), i = 0;
        for (; i < b; i += LANES) acc = acc.lanewise(VectorOperators.MIN, LongVector.fromArray(SP, a, i));
        long r = acc.reduceLanes(VectorOperators.MIN);
        for (; i < n; i++) r = Math.min(r, a[i]);
        return r;
    }
    public static long maxLong(long[] a, int n) {
        LongVector acc = LongVector.broadcast(SP, Long.MIN_VALUE); int b = SP.loopBound(n), i = 0;
        for (; i < b; i += LANES) acc = acc.lanewise(VectorOperators.MAX, LongVector.fromArray(SP, a, i));
        long r = acc.reduceLanes(VectorOperators.MAX);
        for (; i < n; i++) r = Math.max(r, a[i]);
        return r;
    }

    /** Dispatch wrapper (correctness/fallback). NOTE: the runtime `op` defeats vector intrinsification —
        prefer the constant-op methods above for speed. Kept for the gate's generic path. */
    public static long reduceLong(long[] a, int n, int op, long identity) {
        switch (op) {
            case OP_ADD: return sumLong(a, n);
            case OP_OR:  return orLong(a, n);
            case OP_MIN: return minLong(a, n);
            case OP_MAX: return maxLong(a, n);
            default: throw new IllegalArgumentException("op " + op);
        }
    }

    /** Compute-heavy fused map+sum over longs using ONLY AVX2-vectorizable ops (xor/shift/add — NO 64-bit
        multiply, which AVX2 lacks and which deopts the vector loop to scalar). `mix` = a multiply-free
        xorshift bit-mixer. SIMD-vectorized AND deterministic: the per-element work is heavy enough to be
        compute-bound (so SIMD wins) and the long sum is associative (so the lane-combine is reproducible)
        — the fast-AND-deterministic intersection. The double analogue vectorizes too (and faster, with
        SIMD multiply) but is non-deterministic — exactly the case the wandler gate refuses. */
    public static long sumMixLong(long[] a, int n) {
        LongVector acc = LongVector.broadcast(SP, 0L); int b = SP.loopBound(n), i = 0;
        for (; i < b; i += LANES) {
            LongVector x = LongVector.fromArray(SP, a, i);
            for (int r = 0; r < 6; r++) {                              // 6 xorshift rounds = compute-heavy
                x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHL, 13));
                x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHR, 7));
                x = x.lanewise(VectorOperators.XOR, x.lanewise(VectorOperators.LSHL, 17));
            }
            acc = acc.add(x);
        }
        long r = acc.reduceLanes(VectorOperators.ADD);
        for (; i < n; i++) {
            long x = a[i];
            for (int k = 0; k < 6; k++) { x ^= (x << 13); x ^= (x >>> 7); x ^= (x << 17); }
            r += x;
        }
        return r;
    }
}
