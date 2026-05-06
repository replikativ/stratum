package stratum.internal;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Memory-mapped {@link InputFile} for parquet-mr. Drop-in replacement for
 * {@code LocalInputFile} that avoids the per-call {@code byte[]} allocations
 * in its {@code read(ByteBuffer)} path and the {@code RandomAccessFile}
 * read syscalls — backing data lives in a single shared
 * {@link MemorySegment} mapped from the underlying file.
 *
 * Supports files larger than 2 GiB (foreign-memory API has no signed-int
 * length limit on offsets/sizes). Per-call reads are still bounded by
 * {@code int} since they target {@code byte[]}/{@link ByteBuffer}, which
 * is fine for parquet column-chunk reads.
 *
 * Not safe for concurrent use of a single stream (parquet-mr uses one
 * stream per {@code ParquetFileReader}, single-threaded).
 */
public final class MmapInputFile implements InputFile, AutoCloseable {

    private final long length;
    private final FileChannel channel;
    private final Arena arena;
    private final MemorySegment segment;

    public MmapInputFile(Path path) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        this.length = channel.size();
        this.arena = Arena.ofShared();
        this.segment = channel.map(FileChannel.MapMode.READ_ONLY, 0L, length, arena);
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new MmapStream();
    }

    @Override
    public void close() throws IOException {
        try {
            arena.close();
        } finally {
            channel.close();
        }
    }

    private final class MmapStream extends SeekableInputStream {
        private long pos = 0L;

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            this.pos = newPos;
        }

        @Override
        public int read() throws IOException {
            if (pos >= length) return -1;
            int b = segment.get(ValueLayout.JAVA_BYTE, pos) & 0xff;
            pos++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (pos >= length) return -1;
            int n = (int) Math.min((long) len, length - pos);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            if (pos + (long) len > length) throw new EOFException();
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, bytes, start, len);
            pos += len;
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            if (pos >= length) return -1;
            int n = (int) Math.min((long) buf.remaining(), length - pos);
            MemorySegment src = segment.asSlice(pos, (long) n);
            buf.put(src.asByteBuffer());
            pos += n;
            return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int len = buf.remaining();
            if (pos + (long) len > length) throw new EOFException();
            MemorySegment src = segment.asSlice(pos, (long) len);
            buf.put(src.asByteBuffer());
            pos += len;
        }
    }
}
