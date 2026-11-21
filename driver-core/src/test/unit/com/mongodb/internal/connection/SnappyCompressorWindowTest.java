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

import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SnappyCompressorWindowTest {
    @ParameterizedTest
    @ValueSource(ints = {4095, 4096, 65535, 65536, 65537, 65663, 65664, 81919, 81920, 4096000})
    void shouldProduceOneRawBlockAcrossBufferBoundaries(final int length) throws IOException {
        byte[] expected = new byte[length];
        new Random(length).nextBytes(expected);
        ByteBuffer direct = ByteBuffer.allocateDirect(37);
        direct.put(expected, 0, 37);
        ((Buffer) direct).flip();
        List<ByteBuf> source = Arrays.asList(new ByteBufNIO(direct),
                new ByteBufNIO(ByteBuffer.allocate(0)), new ByteBufNIO(ByteBuffer.wrap(expected, 37, length - 37)));
        try (BasicOutputBuffer target = new BasicOutputBuffer()) {
            new SnappyCompressor().compress(source, target);
            assertArrayEquals(expected, Snappy.uncompress(target.toByteArray()));
            source.forEach(buffer -> assertFalse(buffer.hasRemaining()));
        } finally {
            source.forEach(ByteBuf::release);
        }
    }

    @Test
    void shouldLeaseDifferentScratchWhileAnotherWriteIsInProgress() throws IOException {
        byte[] outer = new byte[16384];
        byte[] inner = new byte[16384];
        new Random(31).nextBytes(outer);
        new Random(42).nextBytes(inner);
        SnappyCompressor compressor = new SnappyCompressor();
        ByteBuf first = new ByteBufNIO(ByteBuffer.wrap(outer));
        ByteBuf second = new ByteBufNIO(ByteBuffer.wrap(inner));
        try (BasicOutputBuffer nested = new BasicOutputBuffer(); BasicOutputBuffer target = new BasicOutputBuffer() {
            @Override
            public void writeBytes(final byte[] bytes, final int offset, final int length) {
                compressor.compress(Collections.singletonList(second), nested);
                super.writeBytes(bytes, offset, length);
            }
        }) {
            compressor.compress(Collections.singletonList(first), target);
            assertArrayEquals(outer, Snappy.uncompress(target.toByteArray()));
            assertArrayEquals(inner, Snappy.uncompress(nested.toByteArray()));
        } finally {
            first.release();
            second.release();
        }
    }
}
