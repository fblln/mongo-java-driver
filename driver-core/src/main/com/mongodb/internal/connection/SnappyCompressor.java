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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

class SnappyCompressor extends Compressor {
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
            byte[] input = new byte[getUncompressedSize(source)];
            copy(source, input);
            compress(input, 0, input.length, target);
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
        byte[] output = new byte[bound];
        int written = Snappy.compress(input, offset, length, output, 0);
        target.writeBytes(output, 0, written);
    }

    private int getUncompressedSize(final List<ByteBuf> source) {
        int uncompressedSize = 0;
        for (ByteBuf cur : source) {
            uncompressedSize += cur.remaining();
        }
        return uncompressedSize;
    }

    private void copy(final List<ByteBuf> source, final byte[] in) {
        int offset = 0;
        for (ByteBuf cur : source) {
            int remaining = cur.remaining();
            cur.get(in, offset, remaining);
            offset += remaining;
        }
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new SnappyInputStream(source);
    }
}
