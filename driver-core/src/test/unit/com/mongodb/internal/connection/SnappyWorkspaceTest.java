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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class SnappyWorkspaceTest {
    private final SnappyCompressor compressor = new SnappyCompressor();

    @Test
    void overlappingCallsDoNotOverwriteAnOutstandingResult() throws IOException {
        byte[] first = new byte[50000];
        byte[] second = new byte[50000];
        new Random(31).nextBytes(first);
        new Random(47).nextBytes(second);
        try (BasicOutputBuffer inner = new BasicOutputBuffer();
             BasicOutputBuffer outer = new BasicOutputBuffer() {
                 @Override
                 public void writeBytes(final byte[] data, final int offset, final int length) {
                     encode(second, inner);
                     super.writeBytes(data, offset, length);
                 }
             }) {
            encode(first, outer);
            assertArrayEquals(first, Snappy.uncompress(outer.toByteArray()));
            assertArrayEquals(second, Snappy.uncompress(inner.toByteArray()));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 4095, 4096, 4097, 65535, 65536, 65537, 4194304})
    void compressesOnlyThePopulatedPrefixAfterALargerOperation(final int size) throws IOException {
        byte[] data = new byte[size];
        new Random(11).nextBytes(data);
        try (BasicOutputBuffer warmup = new BasicOutputBuffer(); BasicOutputBuffer out = new BasicOutputBuffer()) {
            encode(new byte[65536], warmup);
            encode(data, out);
            assertArrayEquals(data, Snappy.uncompress(out.toByteArray()));
        }
    }

    private void encode(final byte[] bytes, final BasicOutputBuffer out) {
        int half = bytes.length / 2;
        List<ByteBuf> input = Arrays.asList(new ByteBufNIO(ByteBuffer.wrap(bytes, 0, half)),
                new ByteBufNIO(ByteBuffer.wrap(bytes, half, bytes.length - half)));
        try {
            compressor.compress(input, out);
        } finally {
            input.forEach(ByteBuf::release);
        }
    }
}
