/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.pool.BufferPool;
import org.xerial.snappy.pool.DefaultPoolFactory;
import org.xerial.snappy.pool.QuiescentBufferPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

class SnappyCompressor extends Compressor {
    // Covers the worst-case Snappy output for a standard 48,000,000-byte wire message.
    private static final int MAX_CACHED_SCRATCH_SIZE = 64 * 1024 * 1024;

    @Override
    public String getName() {
        return "snappy";
    }

    @Override
    public byte getId() {
        return 1;
    }

    // OP_COMPRESSED carries a single raw Snappy block.
    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        try {
            if (source.size() == 1) {
                ByteBuf buffer = source.get(0);
                if (buffer.isBackedByArray()) {
                    int offset = buffer.arrayOffset() + buffer.position();
                    int length = buffer.remaining();
                    buffer.position(buffer.limit());
                    compress(buffer.array(), offset, length, target);
                    return;
                }
                if (target instanceof ByteBufferBsonOutput && tryCompressDirect(buffer, (ByteBufferBsonOutput) target)) {
                    buffer.position(buffer.limit());
                    return;
                }
            }
            int size = getUncompressedSize(source);
            BufferPool pool = scratchPool(size);
            byte[] input = pool.allocateArray(size);
            try {
                consumeIntoArray(source, input);
                compress(input, 0, size, target);
            } finally {
                pool.releaseArray(input);
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
    }

    private static boolean tryCompressDirect(final ByteBuf source, final ByteBufferBsonOutput target) throws IOException {
        ByteBuffer input = source.asNIO();
        if (!input.isDirect()) {
            return false;
        }
        int bound = Snappy.maxCompressedLength(input.remaining());
        return target.tryWriteContiguous(bound, false, dst -> {
            if (dst instanceof NettyByteBuf && ((NettyByteBuf) dst).asByteBuf().nioBufferCount() != 1) {
                return -1;
            }
            ByteBuffer output = dst.asNIO().duplicate();
            if (!output.isDirect() || output.isReadOnly() || output.remaining() < bound) {
                return -1;
            }
            // Xerial sets the output view's limit. The BSON writer commits the returned byte count.
            return Snappy.compress(input.duplicate(), output);
        });
    }

    private static void compress(final byte[] input, final int offset, final int length, final BsonOutput target) throws IOException {
        int bound = Snappy.maxCompressedLength(length);
        if (target instanceof ByteBufferBsonOutput && ((ByteBufferBsonOutput) target).tryWriteContiguous(bound, true,
                dst -> Snappy.compress(input, offset, length, dst.array(), dst.arrayOffset() + dst.position()))) {
            return;
        }
        BufferPool pool = scratchPool(bound);
        byte[] output = pool.allocateArray(bound);
        try {
            int written = Snappy.compress(input, offset, length, output, 0);
            target.writeBytes(output, 0, written);
        } finally {
            pool.releaseArray(output);
        }
    }

    // Reuse bulk-sized scratch across active connections. Keep small and oversized arrays out of the cache.
    private static BufferPool scratchPool(final int size) {
        return size >= 4096 && size <= MAX_CACHED_SCRATCH_SIZE ? DefaultPoolFactory.getDefaultPool() : QuiescentBufferPool.getInstance();
    }

    private static int getUncompressedSize(final List<ByteBuf> source) {
        int uncompressedSize = 0;
        for (ByteBuf cur : source) {
            uncompressedSize += cur.remaining();
        }
        return uncompressedSize;
    }

    @Override
    void uncompress(final ByteBuf source, final ByteBuf target) {
        int expected = target.remaining();
        int written;
        try {
            if (source.isBackedByArray() && target.isBackedByArray()) {
                byte[] input = source.array();
                int offset = source.arrayOffset() + source.position();
                int length = source.remaining();
                checkSize(Snappy.uncompressedLength(input, offset, length), expected);
                written = Snappy.uncompress(input, offset, length, target.array(), target.arrayOffset() + target.position());
            } else {
                // A Netty composite buffer can materialize a detached copy when asked for one NIO view.
                boolean composite = target instanceof NettyByteBuf && ((NettyByteBuf) target).asByteBuf().nioBufferCount() != 1;
                ByteBuffer output = composite ? null : target.asNIO();
                ByteBuffer input = source.asNIO();
                if (output == null || !output.isDirect() || output.isReadOnly() || !input.isDirect()) {
                    super.uncompress(source, target);
                    checkSize(expected - target.remaining(), expected);
                    return;
                }
                checkSize(Snappy.uncompressedLength(input), expected);
                // Xerial updates the destination limit. Isolate those changes from the driver's buffer state.
                written = Snappy.uncompress(input.duplicate(), output.duplicate());
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unable to decompress Snappy response", e);
        }
        checkSize(written, expected);
        source.position(source.limit());
        target.position(target.position() + written);
    }

    private static void checkSize(final int actual, final int expected) {
        if (actual != expected) {
            throw new MongoInternalException("Expected a " + expected + "-byte Snappy response, got " + actual + " bytes");
        }
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new SnappyInputStream(source);
    }
}
