package stratum.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * String/LIKE operations — separated from ColumnOpsExt to avoid JIT interference.
 *
 * <p>Contains:
 * <ul>
 *   <li>Bitmask pre-filtering (alpha + bigram masks) for LIKE</li>
 *   <li>Dict-encoded LIKE matching (contains/startsWith/endsWith fast-paths)</li>
 *   <li>Raw string LIKE matching</li>
 *   <li>Parallel dict matching for large dictionaries</li>
 * </ul>
 *
 * <p>Moved from ColumnOpsExt to keep its bytecode under the JIT budget.
 * Adding ~180 lines of LIKE code to the 59KB ColumnOpsExt caused 2-6x
 * regressions on unrelated group-by methods (H2O-Q1: 23ms→46ms).
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class ColumnOpsString {

    private ColumnOpsString() {}

    private static ForkJoinPool POOL = ColumnOps.POOL;
    private static int MORSEL_SIZE = ColumnOps.MORSEL_SIZE;

    // =========================================================================
    // Bitmask pre-filtering for LIKE
    // =========================================================================

    /** Compute 26-bit alpha mask: one bit per letter a-z (case-insensitive). */
    public static int alphaMask(String s) {
        int mask = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 'a' && c <= 'z') mask |= (1 << (c - 'a'));
            else if (c >= 'A' && c <= 'Z') mask |= (1 << (c - 'A'));
        }
        return mask;
    }

    /** Compute 64-bit bigram mask: hashed consecutive character pairs (case-insensitive). */
    public static long bigramMask(String s) {
        long mask = 0;
        for (int i = 0; i < s.length() - 1; i++) {
            char c0 = Character.toLowerCase(s.charAt(i));
            char c1 = Character.toLowerCase(s.charAt(i + 1));
            int hash = ((c0 * 131) + c1) & 63;
            mask |= (1L << hash);
        }
        return mask;
    }

    /** Build alpha masks for entire dictionary. */
    public static int[] buildDictAlphaMasks(String[] dict) {
        int[] masks = new int[dict.length];
        for (int i = 0; i < dict.length; i++) masks[i] = alphaMask(dict[i]);
        return masks;
    }

    /** Build bigram masks for entire dictionary. */
    public static long[] buildDictBigramMasks(String[] dict) {
        long[] masks = new long[dict.length];
        for (int i = 0; i < dict.length; i++) masks[i] = bigramMask(dict[i]);
        return masks;
    }

    // =========================================================================
    // Dict-encoded LIKE matching
    // =========================================================================

    /** LIKE predicate on dict-encoded column with fast-path detection + parallel matching. */
    public static long[] arrayStringLikeFast(long[] codes, String[] dict, String pattern, int length) {
        return arrayStringLikeFastMasked(codes, dict, pattern, length, null, null);
    }

    /** LIKE predicate with optional bitmask pre-filtering for large dictionaries. */
    public static long[] arrayStringLikeFastMasked(long[] codes, String[] dict, String pattern,
                                                    int length, int[] alphaMasks, long[] bigramMasks) {
        boolean[] dictMatch = new boolean[dict.length];
        if (dict.length > 100000) {
            matchDictLikeParallel(dict, pattern, dictMatch, alphaMasks, bigramMasks);
        } else {
            matchDictLike(dict, pattern, dictMatch, alphaMasks, bigramMasks);
        }
        long[] r = new long[length];
        for (int i = 0; i < length; i++) r[i] = dictMatch[(int) codes[i]] ? 1L : 0L;
        return r;
    }

    // =========================================================================
    // Raw string LIKE matching
    // =========================================================================

    /** LIKE predicate on raw String[] column with fast-path detection + parallel scan. */
    public static long[] arrayRawStringLikeFast(String[] strings, String pattern, int length) {
        long[] r = new long[length];
        if (length > ColumnOps.PARALLEL_THRESHOLD) {
            int nThreads = Math.min(POOL.getParallelism(), length / MORSEL_SIZE);
            nThreads = Math.max(nThreads, 2);
            int range = (length + nThreads - 1) / nThreads;
            @SuppressWarnings("unchecked")
            Future<?>[] futures = new Future[nThreads];
            for (int t = 0; t < nThreads; t++) {
                final int start = t * range;
                final int end = Math.min(start + range, length);
                if (start >= length) break;
                futures[t] = POOL.submit(() -> {
                    rawStringLikeRange(strings, pattern, r, start, end);
                });
            }
            for (int t = 0; t < nThreads; t++) {
                if (futures[t] == null) break;
                try { futures[t].get(); } catch (Exception e) { throw new RuntimeException(e); }
            }
        } else {
            rawStringLikeRange(strings, pattern, r, 0, length);
        }
        return r;
    }

    /** Apply LIKE pattern to a range of raw strings with fast-path detection. */
    private static void rawStringLikeRange(String[] strings, String pattern, long[] r, int start, int end) {
        int pLen = pattern.length();
        boolean startsPercent = pLen > 0 && pattern.charAt(0) == '%';
        boolean endsPercent = pLen > 0 && pattern.charAt(pLen - 1) == '%';
        String inner = pattern.substring(startsPercent ? 1 : 0, endsPercent ? pLen - 1 : pLen);
        boolean innerHasWild = inner.indexOf('%') >= 0 || inner.indexOf('_') >= 0;
        if (!innerHasWild && startsPercent && endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].contains(inner) ? 1L : 0L;
        } else if (!innerHasWild && !startsPercent && endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].startsWith(inner) ? 1L : 0L;
        } else if (!innerHasWild && startsPercent && !endsPercent && inner.length() > 0) {
            for (int i = start; i < end; i++) r[i] = strings[i].endsWith(inner) ? 1L : 0L;
        } else {
            String regex = ColumnOps.likeToRegex(pattern);
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (int i = start; i < end; i++) r[i] = p.matcher(strings[i]).matches() ? 1L : 0L;
        }
    }

    // =========================================================================
    // Dict LIKE matching internals
    // =========================================================================

    /** Parallel dict matching for large dictionaries (>100K entries). */
    private static void matchDictLikeParallel(String[] dict, String pattern, boolean[] dictMatch,
                                               int[] alphaMasks, long[] bigramMasks) {
        int nThreads = POOL.getParallelism();
        int chunkSize = (dict.length + nThreads - 1) / nThreads;
        @SuppressWarnings("unchecked")
        Future<?>[] futures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, dict.length);
            if (start >= dict.length) break;
            futures[t] = POOL.submit(() -> {
                matchDictLikeRange(dict, pattern, dictMatch, start, end, alphaMasks, bigramMasks);
            });
        }
        for (int t = 0; t < nThreads; t++) {
            if (futures[t] == null) break;
            try { futures[t].get(); } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    /** Match a range of dictionary entries with fast-path detection and optional bitmask pre-filtering. */
    private static void matchDictLikeRange(String[] dict, String pattern, boolean[] dictMatch,
                                            int start, int end, int[] alphaMasks, long[] bigramMasks) {
        int pLen = pattern.length();
        boolean startsPercent = pLen > 0 && pattern.charAt(0) == '%';
        boolean endsPercent = pLen > 0 && pattern.charAt(pLen - 1) == '%';
        String inner = pattern.substring(startsPercent ? 1 : 0, endsPercent ? pLen - 1 : pLen);
        boolean innerHasWild = inner.indexOf('%') >= 0 || inner.indexOf('_') >= 0;

        boolean useMasks = alphaMasks != null && bigramMasks != null
                           && !innerHasWild && inner.length() >= 2;
        int patAlpha = 0;
        long patBigram = 0;
        if (useMasks) {
            patAlpha = alphaMask(inner);
            patBigram = bigramMask(inner);
        }

        if (!innerHasWild && startsPercent && endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) {
                if (useMasks && ((alphaMasks[d] & patAlpha) != patAlpha
                                 || (bigramMasks[d] & patBigram) != patBigram)) continue;
                dictMatch[d] = dict[d].contains(inner);
            }
        } else if (!innerHasWild && !startsPercent && endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) {
                if (useMasks && ((alphaMasks[d] & patAlpha) != patAlpha
                                 || (bigramMasks[d] & patBigram) != patBigram)) continue;
                dictMatch[d] = dict[d].startsWith(inner);
            }
        } else if (!innerHasWild && startsPercent && !endsPercent && inner.length() > 0) {
            for (int d = start; d < end; d++) {
                if (useMasks && ((alphaMasks[d] & patAlpha) != patAlpha
                                 || (bigramMasks[d] & patBigram) != patBigram)) continue;
                dictMatch[d] = dict[d].endsWith(inner);
            }
        } else {
            String regex = ColumnOps.likeToRegex(pattern);
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (int d = start; d < end; d++) dictMatch[d] = p.matcher(dict[d]).matches();
        }
    }

    /** Match dictionary entries against LIKE pattern with fast-path for common patterns. */
    private static void matchDictLike(String[] dict, String pattern, boolean[] dictMatch,
                                       int[] alphaMasks, long[] bigramMasks) {
        matchDictLikeRange(dict, pattern, dictMatch, 0, dict.length, alphaMasks, bigramMasks);
    }
}
