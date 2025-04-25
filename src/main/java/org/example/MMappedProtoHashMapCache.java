package org.example;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.function.Function;
import com.google.protobuf.InvalidProtocolBufferException;

public class MMappedProtoHashMapCache<P extends com.google.protobuf.GeneratedMessageV3>
        implements AutoCloseable {

    private final int SEGMENT_COUNT;
    private final int SEGMENT_ENTRY_SIZE = 24;
    private final int SEGMENT_MAX_ENTRIES;
    private final int CACHE_SIZE;
    private final int bufferCacheSize;
    private static final long INITIAL_VALUE_SIZE = 4L * 1024 * 1024 * 1024; // 4GB
    private static final double GROWTH_FACTOR = 1.5;
    private static final long MIN_MAPPING_SIZE = 1024L * 1024 * 1024; // 1GB

    // Instead of fixed-size mappings via bit-shifting, we now record each mapping's starting offset.
    private List<MappedByteBuffer> valueBuffers;
    private List<Long> valueBufferOffsets;
    private long currentPosition;
    private long currentValueSize;
    private final Path valuePath;
    private final MappedByteBuffer[] indexSegments;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ThreadLocal<ByteBuffer> threadReadBuffer;
    private final ThreadLocal<ByteBuffer> threadWriteBuffer;
    private final Function<byte[], P> protoFactory;
    private final Map<String, P> cache;
    private long protoFactoryApplyCount = 0;
    private long protoFactoryApplyTotalNanos = 0;

    // Statistics on collisions and probes.
    private long totalProbes = 0;
    private long totalCollisions = 0;

    public MMappedProtoHashMapCache(Path mapDir, Function<byte[], P> protoFactory,
                               int indexSegmentCount, int indexSegmentMaxEntries, int onHeapValueCacheSize,
                               int deserializeBufferCacheSizeBytes) throws IOException {
        this.SEGMENT_COUNT = indexSegmentCount;
        this.SEGMENT_MAX_ENTRIES = indexSegmentMaxEntries;
        this.CACHE_SIZE = onHeapValueCacheSize;
        this.protoFactory = protoFactory;
        this.indexSegments = new MappedByteBuffer[SEGMENT_COUNT];
        this.bufferCacheSize = deserializeBufferCacheSizeBytes;

        this.threadReadBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(bufferCacheSize));
        this.threadWriteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(bufferCacheSize));

        if (CACHE_SIZE > 0) {
            this.cache = Collections.synchronizedMap(
                    new LinkedHashMap<String, P>(CACHE_SIZE, 0.75f, true) {
                        protected boolean removeEldestEntry(Map.Entry<String, P> eldest) {
                            return size() > CACHE_SIZE;
                        }
                    }
            );
        } else {
            this.cache = null;
        }

        Files.createDirectories(mapDir);
        initIndexSegments(mapDir);
        this.valuePath = mapDir.resolve("values.dat");
        if (!Files.exists(valuePath)) {
            Files.createFile(valuePath);
        }
        initValueBuffer(INITIAL_VALUE_SIZE);
    }

    public MMappedProtoHashMapCache(Path mapDir, Function<byte[], P> protoFactory) throws IOException {
        this(mapDir, protoFactory, 1, 500000, 0, 200 * 1024);
    }

    private void initIndexSegments(Path mapDir) throws IOException {
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentFile = String.format("index-segment-%02d.idx", i);
            Path segmentPath = mapDir.resolve(segmentFile);
            if (!Files.exists(segmentPath)) {
                Files.createFile(segmentPath);
            }
            try (RandomAccessFile raf = new RandomAccessFile(segmentPath.toFile(), "rw")) {
                raf.setLength((long) SEGMENT_ENTRY_SIZE * SEGMENT_MAX_ENTRIES);
                indexSegments[i] = raf.getChannel()
                        .map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
            }
        }
    }

    private void initValueBuffer(long size) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(valuePath.toFile(), "rw")) {
            if (Files.exists(valuePath) && Files.size(valuePath) > 0) {
                size = Files.size(valuePath);
            }
            raf.setLength(size);

            valueBuffers = new ArrayList<>();
            valueBufferOffsets = new ArrayList<>();
            FileChannel channel = raf.getChannel();

            long remaining = size;
            long position = 0;
            while (remaining > 0) {
                long mappingSize = Math.max(Math.min(remaining, Integer.MAX_VALUE), MIN_MAPPING_SIZE);
                MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE, position, mappingSize);
                valueBuffers.add(buffer);
                valueBufferOffsets.add(position);
                position += mappingSize;
                remaining -= mappingSize;
            }
            this.currentValueSize = size;
            this.currentPosition = 0;
        }
    }

    // FIX: When growing, we add new mappings (with their starting offset) without altering old mappings.
    private void growValueBuffer() throws IOException {
        long newSize = (long) (currentValueSize * GROWTH_FACTOR);
        try (RandomAccessFile raf = new RandomAccessFile(valuePath.toFile(), "rw")) {
            for (MappedByteBuffer buffer : valueBuffers) {
                buffer.force();
            }
            raf.setLength(newSize);

            long position = currentValueSize;
            long remaining = newSize - currentValueSize;
            FileChannel channel = raf.getChannel();

            while (remaining > 0) {
                long mappingSize = Math.max(Math.min(remaining, Integer.MAX_VALUE), MIN_MAPPING_SIZE);
                MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE, position, mappingSize);
                valueBuffers.add(buffer);
                valueBufferOffsets.add(position);
                position += mappingSize;
                remaining -= mappingSize;
            }
            this.currentValueSize = newSize;
        }
    }

    // Helper method: given an absolute offset, find the corresponding buffer and relative offset.
    private BufferLocation locateBuffer(long absoluteOffset) {
        for (int i = 0; i < valueBuffers.size(); i++) {
            long start = valueBufferOffsets.get(i);
            int capacity = valueBuffers.get(i).capacity();
            if (absoluteOffset >= start && absoluteOffset < start + capacity) {
                return new BufferLocation(i, (int) (absoluteOffset - start));
            }
        }
        throw new IllegalStateException("Offset out of bounds: " + absoluteOffset);
    }

    private static class BufferLocation {
        int index;
        int offset;
        BufferLocation(int index, int offset) {
            this.index = index;
            this.offset = offset;
        }
    }

    private void putInValueBuffer(ByteBuffer src) {
        long destOffset = currentPosition;
        int remaining = src.remaining();
        // Write 'remaining' bytes starting at currentPosition.
        while (remaining > 0) {
            BufferLocation loc = locateBuffer(currentPosition);
            MappedByteBuffer currentBuffer = valueBuffers.get(loc.index);
            int capacity = currentBuffer.capacity();
            int space = capacity - loc.offset;
            int toWrite = Math.min(space, remaining);
            currentBuffer.position(loc.offset);
            byte[] temp = new byte[toWrite];
            src.get(temp);
            currentBuffer.put(temp);
            currentPosition += toWrite;
            remaining -= toWrite;
        }
    }

    private void getFromValueBuffer(byte[] dest, long absoluteOffset, int length) {
        int copied = 0;
        long offset = absoluteOffset;
        while (copied < length) {
            BufferLocation loc = locateBuffer(offset);
            MappedByteBuffer buffer = valueBuffers.get(loc.index);
            buffer.position(loc.offset);
            int capacity = buffer.capacity();
            int available = capacity - loc.offset;
            int toCopy = Math.min(available, length - copied);
            buffer.get(dest, copied, toCopy);
            copied += toCopy;
            offset += toCopy;
        }
    }

    private int hash(byte[] key) {
        return murmur3Hash32(key, 0);
    }

    public static int murmur3Hash32(byte[] data, int seed) {
        int length = data.length;
        int h1 = seed;
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int i = 0;
        while (i + 4 <= length) {
            int k1 = (data[i] & 0xff) |
                     ((data[i + 1] & 0xff) << 8) |
                     ((data[i + 2] & 0xff) << 16) |
                     ((data[i + 3] & 0xff) << 24);
            i += 4;

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int rem = length & 3;
        if (rem == 3) k1 ^= (data[i + 2] & 0xff) << 16;
        if (rem >= 2) k1 ^= (data[i + 1] & 0xff) << 8;
        if (rem >= 1) {
            k1 ^= (data[i] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }

        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }

    private int segmentForHash(int hash) {
        return Math.floorMod(hash, SEGMENT_COUNT);
    }

    // In findSlotOffset, we count each probe and each collision (when a matching hash but non‐matching key is encountered)
    private long findSlotOffset(MappedByteBuffer segment, int hash, byte[] key, boolean forWrite) throws IOException {
        long start = Math.floorMod(hash, SEGMENT_MAX_ENTRIES);
        for (long i = 0; i < SEGMENT_MAX_ENTRIES; i++) {
            totalProbes++;
            long idx = (start + i) % SEGMENT_MAX_ENTRIES;
            long offset = idx * SEGMENT_ENTRY_SIZE;
            segment.position((int) offset);
            long storedHash = segment.getLong();
            long valOffset = segment.getLong();
            int len = segment.getInt();
            byte flags = segment.get();
            if (flags == 0) {
                if (!forWrite) return -1;
                return offset;
            }
            if (storedHash == hash) {
                if (len <= 0) continue;
                ByteBuffer buf = threadReadBuffer.get();
                if (buf.capacity() < len) {
                    buf = ByteBuffer.allocate(len);
                    threadReadBuffer.set(buf);
                } else {
                    buf.clear();
                }
                buf.limit(len);
                getFromValueBuffer(buf.array(), valOffset, len);
                buf.position(0);
                if (buf.remaining() < 4) continue;
                int keyLen = buf.getInt();
                if (keyLen < 0 || keyLen > buf.remaining()) continue;
                byte[] storedKey = new byte[keyLen];
                buf.get(storedKey);
                if (Arrays.equals(storedKey, key)) {
                    return offset;
                } else {
                    totalCollisions++;
                }
            }
        }
        throw new IOException("Segment full");
    }

    public void put(byte[] key, P value) throws IOException {
        lock.writeLock().lock();
        try {
            int hash = hash(key);
            int segmentIndex = segmentForHash(hash);
            MappedByteBuffer segment = indexSegments[segmentIndex];
            long slotOffset = findSlotOffset(segment, hash, key, true);
            byte[] valueBytes = value.toByteArray();
            int totalLen = 4 + key.length + 4 + valueBytes.length;
            long requiredPosition = currentPosition + totalLen;
            if (requiredPosition > currentValueSize) {
                growValueBuffer();
            }
            ByteBuffer buf = threadWriteBuffer.get();
            if (buf.capacity() < totalLen) {
                buf = ByteBuffer.allocate(totalLen);
                threadWriteBuffer.set(buf);
            } else {
                buf.clear();
            }
            long valOffset = currentPosition;
            buf.putInt(key.length).put(key);
            buf.putInt(valueBytes.length).put(valueBytes);
            buf.flip();
            putInValueBuffer(buf);
            segment.position((int) slotOffset);
            segment.putLong(hash);
            segment.putLong(valOffset);
            segment.putInt(totalLen);
            segment.put((byte) 1); // valid flag
            if (cache != null) {
                cache.put(Base64.getEncoder().encodeToString(key), value);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public double getAverageDeserializeMicros() {
        return protoFactoryApplyCount == 0 ? 0.0 : (protoFactoryApplyTotalNanos / 1000.0) / protoFactoryApplyCount;
    }

    public P get(byte[] key) throws IOException {
        if (cache != null) {
            String cacheKey = Base64.getEncoder().encodeToString(key);
            P cached = cache.get(cacheKey);
            if (cached != null) return cached;
        }
        lock.readLock().lock();
        try {
            int hash = hash(key);
            int segmentIndex = segmentForHash(hash);
            MappedByteBuffer segment = indexSegments[segmentIndex];
            long slotOffset = findSlotOffset(segment, hash, key, false);
            if (slotOffset == -1) return null;
            segment.position((int) slotOffset);
            long storedHash = segment.getLong();
            long valOffset = segment.getLong();
            int len = segment.getInt();
            byte flags = segment.get();
            if (flags == 0 || storedHash != hash) return null;
            ByteBuffer buf = threadReadBuffer.get();
            if (buf.capacity() < len) {
                buf = ByteBuffer.allocate(len);
                threadReadBuffer.set(buf);
            } else {
                buf.clear();
            }
            buf.limit(len);
            getFromValueBuffer(buf.array(), valOffset, len);
            buf.position(0);
            int keyLen = buf.getInt();
            byte[] storedKey = new byte[keyLen];
            buf.get(storedKey);
            if (!Arrays.equals(storedKey, key)) return null;
            int valueLen = buf.getInt();
            byte[] valBytes = new byte[valueLen];
            buf.get(valBytes);
            long start = System.nanoTime();
            P value = protoFactory.apply(valBytes);
            long end = System.nanoTime();
            protoFactoryApplyCount++;
            protoFactoryApplyTotalNanos += (end - start);
            if (cache != null) {
                String cacheKey = Base64.getEncoder().encodeToString(key);
                cache.put(cacheKey, value);
            }
            return value;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Accessor methods for collision statistics.
    public long getTotalProbes() {
        return totalProbes;
    }

    public long getTotalCollisions() {
        return totalCollisions;
    }

    @Override
    public void close() throws IOException {
        if (valueBuffers != null) {
            for (MappedByteBuffer buffer : valueBuffers) {
                buffer.force();
            }
        }
        for (MappedByteBuffer segment : indexSegments) {
            segment.force();
        }
    }
}