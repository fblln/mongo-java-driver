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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class SnappyScratchTest {
    private final SnappyCompressor compressor = new SnappyCompressor();

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 1008, 1009, 4095, 4096, 65535, 65536, 409601, 4194304})
    void roundTripsAGatheredCommand(final int size) throws IOException {
        assertRoundTrip(random(size, 11));
    }

    @Test
    void roundTripsACommandThatNeedsNoCopy() throws IOException {
        byte[] data = random(500, 13);
        ByteBuf source = new ByteBufNIO(ByteBuffer.wrap(data));
        try (BasicOutputBuffer out = new BasicOutputBuffer()) {
            compressor.compress(singletonList(source), out);
            assertArrayEquals(data, Snappy.uncompress(out.toByteArray()));
            assertFalse(source.hasRemaining(), "source was not consumed");
        } finally {
            source.release();
        }
    }

    @Test
    void compressesOnlyThePopulatedPrefixOfBorrowedScratch() throws IOException {
        // The pool rounds up to a power of two and does not clear, so the scratch arrives oversized and
        // still holding the previous command's bytes.
        assertRoundTrip(random(65536, 17));
        assertRoundTrip(random(4095, 19));
        assertRoundTrip(random(1009, 23));
    }

    @Test
    void returnsBorrowedScratchToThePool() throws IOException {
        byte[] data = random(65536, 29);
        // Put both size classes the next command needs into the pool, and keep what it handed out.
        byte[] gathered = pooledArray(data.length);
        byte[] out = pooledArray(Snappy.maxCompressedLength(data.length));

        assertRoundTrip(data);

        // A pool that got its buffers back hands the same arrays out again.
        assertSame(gathered, pooledArray(data.length), "the gathered input array was not returned");
        assertSame(out, pooledArray(Snappy.maxCompressedLength(data.length)), "the output array was not returned");
    }

    @Test
    void aNestedCommandDoesNotOverwriteAnOutstandingResult() throws IOException {
        byte[] first = random(50000, 31);
        byte[] second = random(50000, 37);
        try (BasicOutputBuffer inner = new BasicOutputBuffer();
             BasicOutputBuffer outer = new BasicOutputBuffer() {
                 @Override
                 public void writeBytes(final byte[] bytes, final int offset, final int length) {
                     compress(second, inner);
                     super.writeBytes(bytes, offset, length);
                 }
             }) {
            compress(first, outer);
            assertArrayEquals(first, Snappy.uncompress(outer.toByteArray()));
            assertArrayEquals(second, Snappy.uncompress(inner.toByteArray()));
        }
    }

    private void assertRoundTrip(final byte[] data) throws IOException {
        try (BasicOutputBuffer out = new BasicOutputBuffer()) {
            out.writeByte(123);
            compress(data, out);
            byte[] written = out.toByteArray();
            assertEquals(123, written[0]);
            assertArrayEquals(data, Snappy.uncompress(Arrays.copyOfRange(written, 1, written.length)));
        }
    }

    /** Hand the compressor a command split across two segments, as a fragmented OP_MSG arrives. */
    private void compress(final byte[] data, final BasicOutputBuffer out) {
        int half = data.length / 2;
        List<ByteBuf> source = asList(new ByteBufNIO(ByteBuffer.wrap(data, 0, half)),
                new ByteBufNIO(ByteBuffer.wrap(data, half, data.length - half)));
        try {
            compressor.compress(source, out);
            source.forEach(buffer -> assertFalse(buffer.hasRemaining(), "source was not consumed"));
        } finally {
            source.forEach(ByteBuf::release);
        }
    }

    private static byte[] pooledArray(final int size) {
        ByteBuf buffer = PowerOfTwoBufferPool.DEFAULT.getBuffer(size);
        try {
            return buffer.array();
        } finally {
            buffer.release();
        }
    }

    private static byte[] random(final int size, final int seed) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }
}
