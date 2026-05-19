package stratum.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL binary wire-format codecs (format code 1).
 *
 * <p>Symmetric encoders + decoders for every OID we tag on the wire. All
 * multi-byte values are big-endian; date/time values use the PostgreSQL
 * epoch (2000-01-01) on the wire, with helpers shifting to/from the Java
 * epoch (1970-01-01) used internally by Stratum.
 *
 * <p>Caller contract: encoders return a byte[] payload (excluding the int32
 * length prefix that DataRow / Bind wrap around it) — null in / null out
 * means "use the wire NULL sentinel". Decoders take the payload bytes
 * (with the prefix already stripped).
 *
 * <p><b>Internal API</b> — subject to change without notice.
 */
public final class PgBinaryCodec {

    private PgBinaryCodec() {}

    // -------------------------------------------------------------------
    // Epoch shifts. PostgreSQL counts DATE/TIMESTAMP from 2000-01-01.
    // -------------------------------------------------------------------

    /** Days from 1970-01-01 to 2000-01-01. */
    public static final long PG_EPOCH_DAYS = 10957L;

    /** Microseconds from 1970-01-01T00:00:00Z to 2000-01-01T00:00:00Z. */
    public static final long PG_EPOCH_MICROS = 946684800000000L;

    // -------------------------------------------------------------------
    // Output-capability gate. Used by the wire layer to decide whether a
    // requested binary format can be honoured for a column's OID.
    // -------------------------------------------------------------------

    /**
     * Whether this codec can produce a binary encoding for the given OID.
     * Returning false causes the wire layer to refuse with ErrorResponse
     * 0A000 (feature_not_supported).
     */
    public static boolean supportsBinaryOutput(int oid) {
        return switch (oid) {
            case PgWireServer.OID_BOOL,
                 PgWireServer.OID_INT2,
                 PgWireServer.OID_INT4,
                 PgWireServer.OID_INT8,
                 PgWireServer.OID_OID,
                 PgWireServer.OID_FLOAT4,
                 PgWireServer.OID_FLOAT8,
                 PgWireServer.OID_TEXT,
                 PgWireServer.OID_NAME,
                 PgWireServer.OID_VARCHAR,
                 PgWireServer.OID_DATE,
                 PgWireServer.OID_TIMESTAMP,
                 PgWireServer.OID_TIMESTAMPTZ,
                 PgWireServer.OID_NUMERIC,
                 PgWireServer.OID_INTERVAL,
                 PgWireServer.OID_UUID,
                 PgWireServer.OID_JSONB -> true;
            default -> false;
        };
    }

    /** Whether this codec can decode a binary-format input for the given OID. */
    public static boolean supportsBinaryInput(int oid) {
        // Step W3 mirrors the output set. Kept as a separate method so we can
        // narrow it later if any OID turns out to be output-only.
        return supportsBinaryOutput(oid);
    }

    // -------------------------------------------------------------------
    // Encoders — produce raw payload bytes (no length prefix).
    // -------------------------------------------------------------------

    /**
     * Encode a typed value for {@code oid} into PG binary bytes. {@code value}
     * may be any reasonable Java representation — {@code Number} for numeric
     * OIDs, {@code String} / {@code byte[]} / {@code UUID} for the appropriate
     * text/byte OIDs. Returns null on null input.
     *
     * @throws IllegalArgumentException if {@code value} can't be coerced to
     *         the OID's expected type.
     */
    public static byte[] encode(int oid, Object value) {
        if (value == null) return null;
        return switch (oid) {
            case PgWireServer.OID_BOOL      -> encodeBool(asBool(value));
            case PgWireServer.OID_INT2      -> encodeInt2(((Number) value).shortValue());
            case PgWireServer.OID_INT4,
                 PgWireServer.OID_OID       -> encodeInt4(((Number) value).intValue());
            case PgWireServer.OID_INT8      -> encodeInt8(((Number) value).longValue());
            case PgWireServer.OID_FLOAT4    -> encodeFloat4(((Number) value).floatValue());
            case PgWireServer.OID_FLOAT8    -> encodeFloat8(((Number) value).doubleValue());
            case PgWireServer.OID_TEXT,
                 PgWireServer.OID_NAME,
                 PgWireServer.OID_VARCHAR   -> encodeText(value.toString());
            case PgWireServer.OID_DATE      -> encodeDate(((Number) value).longValue());
            case PgWireServer.OID_TIMESTAMP,
                 PgWireServer.OID_TIMESTAMPTZ -> encodeTimestamp(((Number) value).longValue());
            case PgWireServer.OID_NUMERIC   -> encodeNumeric(asBigDecimal(value));
            case PgWireServer.OID_INTERVAL  -> encodeInterval((Interval) value);
            case PgWireServer.OID_UUID      -> encodeUuid(value);
            case PgWireServer.OID_JSONB     -> encodeJsonb(value.toString());
            default -> throw new IllegalArgumentException(
                "No binary encoder for OID " + oid);
        };
    }

    public static byte[] encodeBool(boolean b) {
        return new byte[] { (byte) (b ? 1 : 0) };
    }

    public static byte[] encodeInt2(short v) {
        return new byte[] { (byte) (v >>> 8), (byte) v };
    }

    public static byte[] encodeInt4(int v) {
        return new byte[] {
            (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v
        };
    }

    public static byte[] encodeInt8(long v) {
        byte[] out = new byte[8];
        for (int i = 7; i >= 0; i--) { out[i] = (byte) v; v >>>= 8; }
        return out;
    }

    public static byte[] encodeFloat4(float v) {
        return encodeInt4(Float.floatToRawIntBits(v));
    }

    public static byte[] encodeFloat8(double v) {
        return encodeInt8(Double.doubleToRawLongBits(v));
    }

    public static byte[] encodeText(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Encode a DATE — input is days since 1970-01-01 (Stratum's storage),
     * output is int32 days since 2000-01-01 (PG wire).
     */
    public static byte[] encodeDate(long unixDays) {
        return encodeInt4((int) (unixDays - PG_EPOCH_DAYS));
    }

    /**
     * Encode a TIMESTAMP — input is microseconds since 1970-01-01 UTC,
     * output is int64 microseconds since 2000-01-01.
     */
    public static byte[] encodeTimestamp(long unixMicros) {
        return encodeInt8(unixMicros - PG_EPOCH_MICROS);
    }

    /**
     * Encode a UUID. Accepts {@link UUID}, {@code byte[16]}, or a
     * canonical-string form.
     */
    public static byte[] encodeUuid(Object value) {
        if (value instanceof UUID u) {
            byte[] out = new byte[16];
            long hi = u.getMostSignificantBits();
            long lo = u.getLeastSignificantBits();
            for (int i = 7; i >= 0; i--) { out[i] = (byte) hi; hi >>>= 8; }
            for (int i = 15; i >= 8; i--) { out[i] = (byte) lo; lo >>>= 8; }
            return out;
        }
        if (value instanceof byte[] b && b.length == 16) {
            return b.clone();
        }
        if (value instanceof String s) {
            return encodeUuid(UUID.fromString(s));
        }
        throw new IllegalArgumentException("Cannot encode UUID from " + value.getClass().getName());
    }

    /**
     * Encode JSONB. Output is one version byte (always 0x01) followed by
     * the UTF-8 bytes of the JSON text.
     */
    public static byte[] encodeJsonb(String json) {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[1 + body.length];
        out[0] = 0x01;
        System.arraycopy(body, 0, out, 1, body.length);
        return out;
    }

    // -------------------------------------------------------------------
    // NUMERIC — PG's variable-length base-10000 encoding.
    //
    //   int16 ndigits
    //   int16 weight   — index of the most significant digit (in base-10000
    //                    units, counting from the implied decimal point);
    //                    -1 means the digit lies in the 10^-4..10^-1 slot.
    //   int16 sign     — 0x0000 POS, 0x4000 NEG, 0xC000 NaN
    //   int16 dscale   — display scale (base-10 digits after the decimal point)
    //   int16[]        — ndigits base-10000 digits, MSB first
    //
    // Reference: postgres src/backend/utils/adt/numeric.c (numeric_send /
    // numeric_recv). Cross-checked against pgjdbc ByteConverter.numeric.
    // -------------------------------------------------------------------

    private static final short NUMERIC_POS = (short) 0x0000;
    private static final short NUMERIC_NEG = (short) 0x4000;
    private static final short NUMERIC_NAN = (short) 0xC000;
    private static final BigInteger BASE_10000 = BigInteger.valueOf(10000L);

    public static byte[] encodeNumeric(BigDecimal bd) {
        if (bd == null) throw new IllegalArgumentException("encodeNumeric(null)");

        short sign;
        if (bd.signum() < 0) {
            sign = NUMERIC_NEG;
            bd = bd.negate();
        } else {
            sign = NUMERIC_POS;
        }

        short dscale = (short) Math.max(0, bd.scale());

        // Zero — short-circuit. ndigits = 0, weight = 0.
        if (bd.signum() == 0) {
            return numericHeader((short) 0, (short) 0, NUMERIC_POS, dscale);
        }

        // Pad the unscaled value with trailing zeros so its scale is a
        // multiple of 4 (one base-10000 digit = 4 base-10 digits).
        BigInteger unscaled = bd.unscaledValue();
        int scale = bd.scale();
        if (scale < 0) {
            unscaled = unscaled.multiply(BigInteger.TEN.pow(-scale));
            scale = 0;
        }
        int pad = (4 - (scale % 4)) % 4;
        if (pad > 0) {
            unscaled = unscaled.multiply(BigInteger.TEN.pow(pad));
            scale += pad;
        }
        int fracDigits = scale / 4;

        // Split into base-10000 digits, LSB first.
        List<Short> lsbFirst = new ArrayList<>();
        while (unscaled.signum() != 0) {
            BigInteger[] qr = unscaled.divideAndRemainder(BASE_10000);
            lsbFirst.add((short) qr[1].intValueExact());
            unscaled = qr[0];
        }
        int totalDigits = lsbFirst.size();
        int integerDigits = totalDigits - fracDigits;
        int weight = integerDigits - 1;

        // Reverse to MSB-first, trim leading zeros (decreasing weight), then
        // trim trailing zeros (leaving weight alone).
        java.util.Collections.reverse(lsbFirst);
        int leadingZeros = 0;
        for (Short d : lsbFirst) { if (d == 0) leadingZeros++; else break; }
        if (leadingZeros == lsbFirst.size()) {
            return numericHeader((short) 0, (short) 0, NUMERIC_POS, dscale);
        }
        lsbFirst = lsbFirst.subList(leadingZeros, lsbFirst.size());
        weight -= leadingZeros;
        int trailingZeros = 0;
        for (int i = lsbFirst.size() - 1; i >= 0; i--) {
            if (lsbFirst.get(i) == 0) trailingZeros++; else break;
        }
        lsbFirst = lsbFirst.subList(0, lsbFirst.size() - trailingZeros);

        short ndigits = (short) lsbFirst.size();
        ByteBuffer bb = ByteBuffer.allocate(8 + 2 * ndigits);
        bb.putShort(ndigits);
        bb.putShort((short) weight);
        bb.putShort(sign);
        bb.putShort(dscale);
        for (Short d : lsbFirst) bb.putShort(d);
        return bb.array();
    }

    private static byte[] numericHeader(short ndigits, short weight, short sign, short dscale) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putShort(ndigits);
        bb.putShort(weight);
        bb.putShort(sign);
        bb.putShort(dscale);
        return bb.array();
    }

    // -------------------------------------------------------------------
    // Decoders — consume raw payload bytes (no length prefix).
    // -------------------------------------------------------------------

    public static boolean decodeBool(byte[] b) {
        if (b.length != 1) throw new IllegalArgumentException("BOOL length " + b.length);
        return b[0] != 0;
    }

    public static short decodeInt2(byte[] b) {
        if (b.length != 2) throw new IllegalArgumentException("INT2 length " + b.length);
        return (short) (((b[0] & 0xff) << 8) | (b[1] & 0xff));
    }

    public static int decodeInt4(byte[] b) {
        if (b.length != 4) throw new IllegalArgumentException("INT4 length " + b.length);
        return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16)
             | ((b[2] & 0xff) << 8)  |  (b[3] & 0xff);
    }

    public static long decodeInt8(byte[] b) {
        if (b.length != 8) throw new IllegalArgumentException("INT8 length " + b.length);
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (b[i] & 0xff);
        return v;
    }

    public static float decodeFloat4(byte[] b) {
        return Float.intBitsToFloat(decodeInt4(b));
    }

    public static double decodeFloat8(byte[] b) {
        return Double.longBitsToDouble(decodeInt8(b));
    }

    public static String decodeText(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    /** Decode a DATE — output is days since 1970-01-01 (Stratum storage). */
    public static long decodeDate(byte[] b) {
        return decodeInt4(b) + PG_EPOCH_DAYS;
    }

    /** Decode a TIMESTAMP — output is microseconds since 1970-01-01 UTC. */
    public static long decodeTimestamp(byte[] b) {
        return decodeInt8(b) + PG_EPOCH_MICROS;
    }

    public static UUID decodeUuid(byte[] b) {
        if (b.length != 16) throw new IllegalArgumentException("UUID length " + b.length);
        long hi = 0, lo = 0;
        for (int i = 0; i < 8; i++) hi = (hi << 8) | (b[i] & 0xff);
        for (int i = 8; i < 16; i++) lo = (lo << 8) | (b[i] & 0xff);
        return new UUID(hi, lo);
    }

    public static String decodeJsonb(byte[] b) {
        if (b.length < 1) throw new IllegalArgumentException("JSONB empty payload");
        int version = b[0] & 0xff;
        if (version != 1) {
            throw new IllegalArgumentException("Unsupported JSONB version " + version);
        }
        return new String(b, 1, b.length - 1, StandardCharsets.UTF_8);
    }

    public static BigDecimal decodeNumeric(byte[] b) {
        ByteBuffer bb = ByteBuffer.wrap(b);
        short ndigits = bb.getShort();
        short weight  = bb.getShort();
        short sign    = bb.getShort();
        short dscale  = bb.getShort();
        if (sign == NUMERIC_NAN) {
            throw new IllegalArgumentException("NUMERIC NaN is not representable as BigDecimal");
        }
        if (ndigits == 0) {
            return BigDecimal.ZERO.setScale(Math.max(0, dscale), RoundingMode.UNNECESSARY);
        }
        BigInteger unscaled = BigInteger.ZERO;
        for (int i = 0; i < ndigits; i++) {
            unscaled = unscaled.multiply(BASE_10000)
                               .add(BigInteger.valueOf(bb.getShort() & 0xffff));
        }
        // integerDigits = weight + 1 ; fracDigits = ndigits - integerDigits.
        int fracDigits = ndigits - (weight + 1);
        BigDecimal out;
        if (fracDigits >= 0) {
            out = new BigDecimal(unscaled, 4 * fracDigits);
        } else {
            // Trailing implicit zeros: shift left into the integer part.
            BigInteger shifted = unscaled.multiply(BigInteger.TEN.pow(-4 * fracDigits));
            out = new BigDecimal(shifted, 0);
        }
        if (sign == NUMERIC_NEG) out = out.negate();
        return out.setScale(Math.max(0, dscale), RoundingMode.HALF_EVEN);
    }

    /**
     * Decode a binary input by OID. Inverse of {@link #encode(int, Object)}.
     * @return a Java representation suitable to feed back into the engine
     *         as a parameter (Long, Double, BigDecimal, String, Boolean, UUID).
     */
    public static Object decode(int oid, byte[] payload) {
        return switch (oid) {
            case PgWireServer.OID_BOOL      -> decodeBool(payload);
            case PgWireServer.OID_INT2      -> decodeInt2(payload);
            case PgWireServer.OID_INT4,
                 PgWireServer.OID_OID       -> decodeInt4(payload);
            case PgWireServer.OID_INT8      -> decodeInt8(payload);
            case PgWireServer.OID_FLOAT4    -> decodeFloat4(payload);
            case PgWireServer.OID_FLOAT8    -> decodeFloat8(payload);
            case PgWireServer.OID_TEXT,
                 PgWireServer.OID_NAME,
                 PgWireServer.OID_VARCHAR   -> decodeText(payload);
            case PgWireServer.OID_DATE      -> decodeDate(payload);
            case PgWireServer.OID_TIMESTAMP,
                 PgWireServer.OID_TIMESTAMPTZ -> decodeTimestamp(payload);
            case PgWireServer.OID_NUMERIC   -> decodeNumeric(payload);
            case PgWireServer.OID_INTERVAL  -> decodeInterval(payload);
            case PgWireServer.OID_UUID      -> decodeUuid(payload);
            case PgWireServer.OID_JSONB     -> decodeJsonb(payload);
            default -> throw new IllegalArgumentException("No binary decoder for OID " + oid);
        };
    }

    // -------------------------------------------------------------------
    // INTERVAL — PG 16-byte fixed layout: int64 micros, int32 days, int32 months
    // (matches src/backend/utils/adt/timestamp.c interval_send/recv and
    // src/include/datatype/timestamp.h Interval struct).
    // -------------------------------------------------------------------

    public static byte[] encodeInterval(Interval iv) {
        if (iv == null) throw new IllegalArgumentException("encodeInterval(null)");
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(iv.micros);
        bb.putInt(iv.days);
        bb.putInt(iv.months);
        return bb.array();
    }

    public static Interval decodeInterval(byte[] b) {
        if (b.length != 16) {
            throw new IllegalArgumentException("INTERVAL length " + b.length + " (expected 16)");
        }
        ByteBuffer bb = ByteBuffer.wrap(b);
        long micros = bb.getLong();
        int days    = bb.getInt();
        int months  = bb.getInt();
        return new Interval(micros, days, months);
    }

    // -------------------------------------------------------------------
    // Coercion helpers
    // -------------------------------------------------------------------

    private static boolean asBool(Object v) {
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.longValue() != 0;
        if (v instanceof String s) {
            return s.equalsIgnoreCase("t") || s.equalsIgnoreCase("true")
                || s.equals("1") || s.equalsIgnoreCase("yes");
        }
        throw new IllegalArgumentException("Cannot coerce to bool: " + v.getClass().getName());
    }

    private static BigDecimal asBigDecimal(Object v) {
        if (v instanceof BigDecimal bd) return bd;
        if (v instanceof BigInteger bi) return new BigDecimal(bi);
        if (v instanceof Number n) return new BigDecimal(n.toString());
        if (v instanceof String s) return new BigDecimal(s);
        throw new IllegalArgumentException("Cannot coerce to BigDecimal: " + v.getClass().getName());
    }
}
