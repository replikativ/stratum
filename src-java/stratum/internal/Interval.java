package stratum.internal;

import java.util.Objects;

/**
 * Stratum's INTERVAL value — a fixed-size three-component duration matching
 * both PostgreSQL ({@code timestamp.h:47-53} {@code Interval}: time/day/month)
 * and DuckDB ({@code interval.hpp:25-29} {@code interval_t}: months/days/micros).
 *
 * <p>The three components are deliberately separate because calendar
 * arithmetic is ambiguous otherwise: a month is not a fixed number of days
 * (28..31), and a day is not a fixed number of microseconds (DST creates
 * 23h and 25h days). {@code TIMESTAMP '2026-01-31' + INTERVAL '1 month'}
 * must produce {@code 2026-02-28}, which is impossible if the month had
 * already been normalised to micros.
 *
 * <p>Wire format on OID 1186 ({@link PgWireServer#OID_INTERVAL}) is 16 bytes
 * big-endian: {@code (int64 micros, int32 days, int32 months)} —
 * matching PG's {@code interval_send} at {@code timestamp.c:997-1032}.
 *
 * <p>Instances are immutable.
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class Interval {

    /**
     * Microseconds component — non-calendar time (hours/minutes/seconds/
     * fractional seconds normalised to micros). Independent of day/month.
     */
    public final long micros;

    /** Days component — calendar-aware (24h day, but DST-sensitive). */
    public final int days;

    /** Months component — calendar-aware (28..31 days, varies by month). */
    public final int months;

    public Interval(long micros, int days, int months) {
        this.micros = micros;
        this.days = days;
        this.months = months;
    }

    /** Construct from years + months. */
    public static Interval ofMonths(int totalMonths) {
        return new Interval(0L, 0, totalMonths);
    }

    /** Construct an all-zero interval. */
    public static final Interval ZERO = new Interval(0L, 0, 0);

    public static Interval ofDays(int d)    { return new Interval(0L, d, 0); }
    public static Interval ofMicros(long m) { return new Interval(m, 0, 0); }

    /**
     * Render the interval in PostgreSQL's "postgres" output format —
     * something like {@code 1 year 2 mons 3 days 04:05:06.789}. Zero
     * components are omitted; an all-zero interval renders as
     * {@code 00:00:00}.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int years  = months / 12;
        int monRem = months % 12;
        if (years != 0)  { append(sb, years,  "year",  "years"); }
        if (monRem != 0) { append(sb, monRem, "mon",   "mons"); }
        if (days != 0)   { append(sb, days,   "day",   "days"); }
        if (micros != 0 || (years == 0 && monRem == 0 && days == 0)) {
            if (sb.length() > 0) sb.append(' ');
            appendTime(sb, micros);
        }
        return sb.toString();
    }

    private static void append(StringBuilder sb, int n, String sing, String plur) {
        if (sb.length() > 0) sb.append(' ');
        sb.append(n).append(' ').append(Math.abs(n) == 1 ? sing : plur);
    }

    private static void appendTime(StringBuilder sb, long micros) {
        long abs = Math.abs(micros);
        long secs  = abs / 1_000_000L;
        long usRem = abs % 1_000_000L;
        long hours = secs / 3600L;
        long mins  = (secs % 3600L) / 60L;
        long secsR = secs % 60L;
        if (micros < 0) sb.append('-');
        sb.append(String.format("%02d:%02d:%02d", hours, mins, secsR));
        if (usRem != 0) sb.append(String.format(".%06d", usRem));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Interval i)) return false;
        return micros == i.micros && days == i.days && months == i.months;
    }

    @Override
    public int hashCode() {
        return Objects.hash(micros, days, months);
    }
}
