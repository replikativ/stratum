package stratum.internal;

/**
 * Micros-precision temporal kernels for the time-series query engine.
 *
 * Extracted from ColumnOps to keep ColumnOps.class under the JIT compilation
 * budget threshold (~72KB bytecode). See MEMORY.md "JIT Optimization Lessons"
 * — large ColumnOps classes regress chunked SIMD filter+aggregate paths
 * (B1/B2/B3 idx) by 5-7x because methods not even called by those paths
 * still consume JIT budget within the same class.
 *
 * Sibling classes follow the same JIT-isolation pattern: ColumnOpsVar,
 * ColumnOpsString, ColumnOpsChunkedSimd were all split from larger parents
 * to fix analogous regressions.
 *
 * civilToDays/civilFromDays are duplicated here (not shared with ColumnOps)
 * intentionally — separate compilation units profile separately and inline
 * independently.
 */
public final class ColumnOpsTemporal {

    private ColumnOpsTemporal() {}

    static final long MICROS_PER_MILLI  = 1_000L;
    static final long MICROS_PER_SECOND = 1_000_000L;
    static final long MICROS_PER_MINUTE = 60L * MICROS_PER_SECOND;
    static final long MICROS_PER_HOUR   = 60L * MICROS_PER_MINUTE;
    static final long MICROS_PER_DAY    = 24L * MICROS_PER_HOUR;

    private static long civilToDays(long y, long m, long d) {
        y -= (m <= 2) ? 1 : 0;
        long era = (y >= 0 ? y : y - 399) / 400;
        long yoe = y - era * 400;
        long doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

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

    private static final long NULL_L = Long.MIN_VALUE;

    /** DATE_TRUNC to micro (identity): pass-through, no rounding needed.
     *  F-017: copyOf preserves the NULL sentinel verbatim. */
    public static long[] arrayDateTruncMicroMicros(long[] em, int length) {
        return java.util.Arrays.copyOf(em, length);
    }

    /** DATE_TRUNC to millisecond on epoch-micros column. */
    public static long[] arrayDateTruncMilliMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, MICROS_PER_MILLI) * MICROS_PER_MILLI;
        }
        return r;
    }

    /** DATE_TRUNC to second on epoch-micros column. */
    public static long[] arrayDateTruncSecondMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, MICROS_PER_SECOND) * MICROS_PER_SECOND;
        }
        return r;
    }

    /** DATE_TRUNC to minute on epoch-micros column. */
    public static long[] arrayDateTruncMinuteMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, MICROS_PER_MINUTE) * MICROS_PER_MINUTE;
        }
        return r;
    }

    /** DATE_TRUNC to hour on epoch-micros column. */
    public static long[] arrayDateTruncHourMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, MICROS_PER_HOUR) * MICROS_PER_HOUR;
        }
        return r;
    }

    /** DATE_TRUNC to day on epoch-micros column. */
    public static long[] arrayDateTruncDayMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, MICROS_PER_DAY) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to month on epoch-micros column. Uses Hinnant civil arithmetic. */
    public static long[] arrayDateTruncMonthMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            r[i] = civilToDays(ymd[0], ymd[1], 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to year on epoch-micros column. */
    public static long[] arrayDateTruncYearMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            r[i] = civilToDays(ymd[0], 1, 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to week (ISO 8601: Monday) on epoch-micros column. Step 4b. */
    public static long[] arrayDateTruncWeekMicros(long[] em, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            long dow = ((epochDays % 7) + 10) % 7;  // 0=Mon..6=Sun
            r[i] = (epochDays - dow) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to quarter on epoch-micros column. Step 4b. */
    public static long[] arrayDateTruncQuarterMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            long qStartMonth = ((ymd[1] - 1) / 3) * 3 + 1;
            r[i] = civilToDays(ymd[0], qStartMonth, 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to decade on epoch-micros column. Step 4b. */
    public static long[] arrayDateTruncDecadeMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            long decYear = ymd[0] - Math.floorMod(ymd[0], 10);
            r[i] = civilToDays(decYear, 1, 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to century on epoch-micros column. Step 4b. */
    public static long[] arrayDateTruncCenturyMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            long centStart = ((ymd[0] - 1) / 100) * 100 + 1;
            r[i] = civilToDays(centStart, 1, 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** DATE_TRUNC to millennium on epoch-micros column. Step 4b. */
    public static long[] arrayDateTruncMillenniumMicros(long[] em, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(v, MICROS_PER_DAY);
            civilFromDays(epochDays, ymd);
            long milStart = ((ymd[0] - 1) / 1000) * 1000 + 1;
            r[i] = civilToDays(milStart, 1, 1) * MICROS_PER_DAY;
        }
        return r;
    }

    /** Extract hour (0-23) from epoch-micros array. */
    public static double[] arrayExtractHourMicros(long[] em, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = Double.NaN; continue; }
            long t = Math.floorMod(v, MICROS_PER_DAY);
            r[i] = (double) (t / MICROS_PER_HOUR);
        }
        return r;
    }

    /** Extract minute (0-59) from epoch-micros array. */
    public static double[] arrayExtractMinuteMicros(long[] em, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = Double.NaN; continue; }
            long t = Math.floorMod(v, MICROS_PER_HOUR);
            r[i] = (double) (t / MICROS_PER_MINUTE);
        }
        return r;
    }

    /** Extract second (0-59) from epoch-micros array. */
    public static double[] arrayExtractSecondMicros(long[] em, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = Double.NaN; continue; }
            long t = Math.floorMod(v, MICROS_PER_MINUTE);
            r[i] = (double) (t / MICROS_PER_SECOND);
        }
        return r;
    }

    /** Extract millisecond-of-second (0-999) from epoch-micros array. */
    public static double[] arrayExtractMillisecondMicros(long[] em, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = Double.NaN; continue; }
            long t = Math.floorMod(v, MICROS_PER_SECOND);
            r[i] = (double) (t / MICROS_PER_MILLI);
        }
        return r;
    }

    /** Extract microsecond-of-second (0-999999) from epoch-micros array. */
    public static double[] arrayExtractMicrosecondMicros(long[] em, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? Double.NaN : (double) Math.floorMod(v, MICROS_PER_SECOND);
        }
        return r;
    }

    /** DATE_ADD on epoch-micros: add N micros. */
    public static long[] arrayDateAddMicrosMicros(long[] em, long n, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : v + n;
        }
        return r;
    }

    /** DATE_ADD months on epoch-micros column. */
    public static long[] arrayDateAddMonthsMicros(long[] em, int nMonths, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long s = em[i];
            if (s == NULL_L) { r[i] = NULL_L; continue; }
            long epochDays = Math.floorDiv(s, MICROS_PER_DAY);
            long timeOfDay = s - epochDays * MICROS_PER_DAY;
            civilFromDays(epochDays, ymd);
            long totalMonths = ymd[0] * 12 + (ymd[1] - 1) + nMonths;
            long newYear = Math.floorDiv(totalMonths, 12);
            long newMonth = Math.floorMod(totalMonths, 12) + 1;
            long maxDay;
            if (newMonth == 2) {
                boolean leap = (newYear % 4 == 0 && newYear % 100 != 0) || (newYear % 400 == 0);
                maxDay = leap ? 29 : 28;
            } else if (newMonth == 4 || newMonth == 6 || newMonth == 9 || newMonth == 11) {
                maxDay = 30;
            } else {
                maxDay = 31;
            }
            long day = Math.min(ymd[2], maxDay);
            r[i] = civilToDays(newYear, newMonth, day) * MICROS_PER_DAY + timeOfDay;
        }
        return r;
    }

    /** DATE_DIFF in micros between two epoch-micros columns. NULL on either side → NaN. */
    public static double[] arrayDateDiffMicros(long[] a, long[] b, int length) {
        double[] r = new double[length];
        for (int i = 0; i < length; i++) {
            long av = a[i], bv = b[i];
            r[i] = (av == NULL_L || bv == NULL_L) ? Double.NaN : (double)(av - bv);
        }
        return r;
    }

    /** TIME_BUCKET on epoch-micros column with arbitrary micro-width.
     *  Bucket boundaries are aligned to epoch (origin = 0). For each row:
     *    bucket = floor(em[i] / width) * width
     *  Width must be > 0. */
    public static long[] arrayTimeBucketMicros(long[] em, long widthMicros, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, widthMicros) * widthMicros;
        }
        return r;
    }

    /** TIME_BUCKET on epoch-micros column with origin offset.
     *    bucket = floor((em[i] - origin) / width) * width + origin */
    public static long[] arrayTimeBucketMicrosOrigin(long[] em, long widthMicros, long originMicros, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = em[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            long shifted = v - originMicros;
            r[i] = Math.floorDiv(shifted, widthMicros) * widthMicros + originMicros;
        }
        return r;
    }

    /** TIME_BUCKET on epoch-days column (DATE) with day-width. */
    public static long[] arrayTimeBucketDays(long[] ed, long widthDays, int length) {
        long[] r = new long[length];
        for (int i = 0; i < length; i++) {
            long v = ed[i];
            r[i] = (v == NULL_L) ? NULL_L : Math.floorDiv(v, widthDays) * widthDays;
        }
        return r;
    }

    /** TIME_BUCKET on epoch-days column with month-width. Aligned to month boundaries.
     *  Each input is converted to (year, month) total months since epoch and bucketed. */
    public static long[] arrayTimeBucketMonths(long[] ed, int widthMonths, int length) {
        long[] r = new long[length];
        long[] ymd = new long[3];
        for (int i = 0; i < length; i++) {
            long v = ed[i];
            if (v == NULL_L) { r[i] = NULL_L; continue; }
            civilFromDays(v, ymd);
            // months since 1970-01: year*12 + (month-1), but adjusted so 1970-01 = 0
            long totalMonths = (ymd[0] - 1970) * 12 + (ymd[1] - 1);
            long bucket = Math.floorDiv(totalMonths, widthMonths) * widthMonths;
            long bucketYear = 1970 + Math.floorDiv(bucket, 12);
            long bucketMonth = Math.floorMod(bucket, 12) + 1;
            r[i] = civilToDays(bucketYear, bucketMonth, 1);
        }
        return r;
    }
}
