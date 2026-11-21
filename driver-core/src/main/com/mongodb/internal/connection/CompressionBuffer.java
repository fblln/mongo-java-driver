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

import com.mongodb.lang.Nullable;
import org.bson.ByteBuf;

import java.nio.ByteBuffer;
import java.util.List;

// A contiguous input or output range for a one-shot codec invocation.
final class CompressionBuffer implements AutoCloseable {
    private static final int MIN_POOLED_SIZE = 8 * 1024;

    private final byte[] bytes;
    private final int offset;
    private final int length;
    @Nullable
    private final ByteBuffer pooled;

    private CompressionBuffer(final byte[] bytes, final int offset, final int length, @Nullable final ByteBuffer pooled) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.pooled = pooled;
    }

    byte[] array() {
        return bytes;
    }

    int offset() {
        return offset;
    }

    int length() {
        return length;
    }

    static CompressionBuffer allocate(final int length) {
        if (length < MIN_POOLED_SIZE) {
            return new CompressionBuffer(new byte[length], 0, length, null);
        }
        ByteBuffer buffer = PowerOfTwoBufferPool.DEFAULT.getByteBuffer(length);
        return new CompressionBuffer(buffer.array(), buffer.arrayOffset(), length, buffer);
    }

    static CompressionBuffer consume(final List<ByteBuf> source) {
        if (source.size() == 1 && source.get(0).isBackedByArray()) {
            ByteBuf buffer = source.get(0);
            CompressionBuffer input = new CompressionBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining(), null);
            buffer.position(buffer.limit());
            return input;
        }

        int length = 0;
        for (ByteBuf buffer : source) {
            length += buffer.remaining();
        }
        CompressionBuffer input = allocate(length);
        try {
            int position = input.offset;
            for (ByteBuf buffer : source) {
                int count = buffer.remaining();
                buffer.get(input.bytes, position, count);
                position += count;
            }
            return input;
        } catch (RuntimeException | Error e) {
            input.close();
            throw e;
        }
    }

    @Override
    public void close() {
        if (pooled != null) {
            PowerOfTwoBufferPool.DEFAULT.release(pooled);
        }
    }
}
