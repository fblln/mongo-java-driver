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

import com.mongodb.internal.connection.netty.NettyByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnappySendBufferTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void twoOutstandingMessagesOwnTheirCompressedBytes(final boolean direct) throws IOException {
        BufferProvider provider = direct ? n -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.directBuffer(n, n))
                : PowerOfTwoBufferPool.DEFAULT;
        SnappyCompressor compressor = new SnappyCompressor();
        byte[] large = new byte[100003];
        new Random(17).nextBytes(large);
        byte[] small = {3, 4, 5};
        try (ByteBufferBsonOutput first = new ByteBufferBsonOutput(provider);
             ByteBufferBsonOutput second = new ByteBufferBsonOutput(provider)) {
            encode(compressor, large, first, direct);
            encode(compressor, small, second, direct);
            assertFrame(large, first);
            assertFrame(small, second);
        }
    }

    private static void encode(final SnappyCompressor compressor, final byte[] data, final ByteBufferBsonOutput target,
                               final boolean direct) {
        ByteBuffer bytes = direct ? ByteBuffer.allocateDirect(data.length + 3) : ByteBuffer.allocate(data.length + 3);
        ((Buffer) bytes).position(3);
        bytes.put(data);
        ((Buffer) bytes).position(3);
        ByteBuf input = new ByteBufNIO(bytes);
        try {
            target.writeInt32(0);
            compressor.compress(Collections.singletonList(input), target);
            assertEquals(input.limit(), input.position());
            target.writeInt32(0, target.getSize());
        } finally {
            input.release();
        }
    }

    private static void assertFrame(final byte[] expected, final ByteBufferBsonOutput target) throws IOException {
        byte[] message = target.toByteArray();
        assertEquals(message.length, ByteBuffer.wrap(message).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt());
        assertArrayEquals(expected, Snappy.uncompress(Arrays.copyOfRange(message, 4, message.length)));
    }

    @Test
    void discardsNewBufferWhenEncodingFails() {
        List<ByteBuf> allocated = new ArrayList<>();
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(size -> {
            ByteBuf buffer = new ByteBufNIO(ByteBuffer.allocate(size));
            allocated.add(buffer);
            return buffer;
        })) {
            target.writeByte(17);
            assertThrows(IOException.class, () -> target.tryWriteContiguous(8192, true, dst -> {
                throw new IOException("writer failed");
            }));
            assertEquals(0, allocated.get(1).getReferenceCount());
            target.writeByte(23);
            assertArrayEquals(new byte[]{17, 23}, target.toByteArray());
        }
        assertEquals(0, allocated.get(0).getReferenceCount());
    }
}
