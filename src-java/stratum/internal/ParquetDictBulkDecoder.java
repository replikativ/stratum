package stratum.internal;

import java.util.Arrays;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;

/**
 * Bulk decoder for parquet PLAIN_DICTIONARY / RLE_DICTIONARY data pages
 * that bypasses parquet-mr's per-value {@code ColumnReader}/{@code Dictionary}
 * indirection. Walks the RLE+bit-packed run grammar directly on a byte[]
 * and gathers straight into a typed output array against a pre-decoded
 * dictionary.
 *
 * Supports DataPageV1 with {@code max-rep-level == 0} (top-level columns)
 * and {@code max-def-level == 0} (required columns). Nullable columns
 * fall back to the slow path in the caller.
 */
public final class ParquetDictBulkDecoder {

    private ParquetDictBulkDecoder() {}

    private static int readUnsignedVarInt(byte[] page, int[] cursor) {
        int value = 0;
        int i = 0;
        int b;
        int p = cursor[0];
        while (((b = page[p++] & 0xff) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
        }
        cursor[0] = p;
        return value | (b << i);
    }

    private static int readPaddedLE(byte[] page, int[] cursor, int bitWidth) {
        int bytesWidth = (bitWidth + 7) >>> 3;
        int p = cursor[0];
        int v = 0;
        for (int j = 0; j < bytesWidth; j++) {
            v |= (page[p + j] & 0xff) << (j * 8);
        }
        cursor[0] = p + bytesWidth;
        return v;
    }

    public static void decodeV1Double(byte[] page, int pageOff, int pageLen,
                                      int valueCount,
                                      double[] dict, double[] out, int outOffset) {
        int bitWidth = page[pageOff] & 0xff;
        if (bitWidth == 0) {
            Arrays.fill(out, outOffset, outOffset + valueCount, dict[0]);
            return;
        }
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        int[] buf = new int[8];
        int[] cursor = new int[]{ pageOff + 1 };
        int written = 0;
        while (written < valueCount) {
            int header = readUnsignedVarInt(page, cursor);
            int count = header >>> 1;
            if ((header & 1) == 0) {
                int value = readPaddedLE(page, cursor, bitWidth);
                double dv = dict[value];
                int end = written + count;
                if (end > valueCount) end = valueCount;
                Arrays.fill(out, outOffset + written, outOffset + end, dv);
                written = end;
            } else {
                int numGroups = count;
                int groupValues = numGroups * 8;
                int byteBase = cursor[0];
                cursor[0] = byteBase + numGroups * bitWidth;
                int outBase = outOffset + written;
                int remaining = valueCount - written;
                int fullGroups = (remaining < groupValues) ? (remaining >>> 3) : numGroups;
                for (int g = 0; g < fullGroups; g++) {
                    packer.unpack8Values(page, byteBase + g * bitWidth, buf, 0);
                    int base = outBase + g * 8;
                    out[base    ] = dict[buf[0]];
                    out[base + 1] = dict[buf[1]];
                    out[base + 2] = dict[buf[2]];
                    out[base + 3] = dict[buf[3]];
                    out[base + 4] = dict[buf[4]];
                    out[base + 5] = dict[buf[5]];
                    out[base + 6] = dict[buf[6]];
                    out[base + 7] = dict[buf[7]];
                }
                int produced = fullGroups * 8;
                if (produced < remaining && fullGroups < numGroups) {
                    packer.unpack8Values(page, byteBase + fullGroups * bitWidth, buf, 0);
                    int tail = remaining - produced;
                    int base = outBase + produced;
                    for (int j = 0; j < tail; j++) {
                        out[base + j] = dict[buf[j]];
                    }
                    produced += tail;
                }
                written += Math.min(produced, remaining);
            }
        }
    }

    public static void decodeV1Long(byte[] page, int pageOff, int pageLen,
                                    int valueCount,
                                    long[] dict, long[] out, int outOffset) {
        int bitWidth = page[pageOff] & 0xff;
        if (bitWidth == 0) {
            Arrays.fill(out, outOffset, outOffset + valueCount, dict[0]);
            return;
        }
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        int[] buf = new int[8];
        int[] cursor = new int[]{ pageOff + 1 };
        int written = 0;
        while (written < valueCount) {
            int header = readUnsignedVarInt(page, cursor);
            int count = header >>> 1;
            if ((header & 1) == 0) {
                int value = readPaddedLE(page, cursor, bitWidth);
                long dv = dict[value];
                int end = written + count;
                if (end > valueCount) end = valueCount;
                Arrays.fill(out, outOffset + written, outOffset + end, dv);
                written = end;
            } else {
                int numGroups = count;
                int groupValues = numGroups * 8;
                int byteBase = cursor[0];
                cursor[0] = byteBase + numGroups * bitWidth;
                int outBase = outOffset + written;
                int remaining = valueCount - written;
                int fullGroups = (remaining < groupValues) ? (remaining >>> 3) : numGroups;
                for (int g = 0; g < fullGroups; g++) {
                    packer.unpack8Values(page, byteBase + g * bitWidth, buf, 0);
                    int base = outBase + g * 8;
                    out[base    ] = dict[buf[0]];
                    out[base + 1] = dict[buf[1]];
                    out[base + 2] = dict[buf[2]];
                    out[base + 3] = dict[buf[3]];
                    out[base + 4] = dict[buf[4]];
                    out[base + 5] = dict[buf[5]];
                    out[base + 6] = dict[buf[6]];
                    out[base + 7] = dict[buf[7]];
                }
                int produced = fullGroups * 8;
                if (produced < remaining && fullGroups < numGroups) {
                    packer.unpack8Values(page, byteBase + fullGroups * bitWidth, buf, 0);
                    int tail = remaining - produced;
                    int base = outBase + produced;
                    for (int j = 0; j < tail; j++) {
                        out[base + j] = dict[buf[j]];
                    }
                    produced += tail;
                }
                written += Math.min(produced, remaining);
            }
        }
    }
}
