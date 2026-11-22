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

import com.mongodb.MongoCompressor;
import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompressionOutputTest {
    static Stream<Compressor> compressors() {
        return Stream.of(new SnappyCompressor(), new ZlibCompressor(MongoCompressor.createZlibCompressor()), new ZstdCompressor());
    }

    @ParameterizedTest
    @MethodSource("compressors")
    void shouldFinishOneMessageAcrossHeapAndDirectSegments(final Compressor compressor) {
        byte[] expected = new byte[200003];
        new Random(17).nextBytes(expected);
        ByteBuffer direct = ByteBuffer.allocateDirect(170000);
        direct.put(expected, 41, direct.capacity());
        ((Buffer) direct).flip();
        List<ByteBuf> source = Arrays.asList(new ByteBufNIO(ByteBuffer.wrap(expected, 0, 41)),
                new ByteBufNIO(ByteBuffer.allocate(0)), new ByteBufNIO(direct),
                new ByteBufNIO(ByteBuffer.wrap(expected, 170041, expected.length - 170041).slice().asReadOnlyBuffer()));
        checkRoundTrip(compressor, source, expected);
    }

    @ParameterizedTest
    @MethodSource("compressors")
    void shouldRespectPositionAndArrayOffset(final Compressor compressor) {
        byte[] bytes = new byte[83];
        new Random(23).nextBytes(bytes);
        ByteBuffer slice = ByteBuffer.wrap(bytes, 7, 63).slice();
        ((Buffer) slice).position(5).limit(48);
        checkRoundTrip(compressor, Collections.singletonList(new ByteBufNIO(slice)), Arrays.copyOfRange(bytes, 12, 55));
    }

    @ParameterizedTest
    @MethodSource("compressors")
    void shouldFinishAnEmptyMessage(final Compressor compressor) {
        checkRoundTrip(compressor, Collections.emptyList(), new byte[0]);
    }

    @ParameterizedTest
    @ValueSource(ints = {8191, 8192, 8193})
    void shouldHandleSegmentedMessagesAtZstdThreshold(final int size) {
        byte[] bytes = new byte[size];
        new Random(37).nextBytes(bytes);
        List<ByteBuf> source = Arrays.asList(new ByteBufNIO(ByteBuffer.wrap(bytes, 0, 13)),
                new ByteBufNIO(ByteBuffer.wrap(bytes, 13, size - 13)));
        checkRoundTrip(new ZstdCompressor(), source, bytes);
    }

    private static void checkRoundTrip(final Compressor compressor, final List<ByteBuf> source, final byte[] expected) {
        try (BasicOutputBuffer out = new BasicOutputBuffer()) {
            out.writeByte(31);
            compressor.compress(source, out);
            int end = out.getPosition();
            out.writeByte(32);
            assertEquals(31, out.getInternalBuffer()[0]);
            assertEquals(32, out.getInternalBuffer()[end]);
            ByteBuf encoded = new ByteBufNIO(ByteBuffer.wrap(out.getInternalBuffer(), 1, end - 1).slice());
            ByteBuf decoded = new ByteBufNIO(ByteBuffer.allocate(expected.length));
            try {
                compressor.uncompress(encoded, decoded);
                assertEquals(expected.length, decoded.position());
                assertArrayEquals(expected, decoded.array());
            } finally {
                encoded.release();
                decoded.release();
            }
            source.forEach(buffer -> assertEquals(buffer.limit(), buffer.position()));
        } finally {
            source.forEach(ByteBuf::release);
        }
    }

    @Test
    void shouldDiscardPartialOutputIfFinishingFails() {
        Compressor compressor = new ZlibCompressor(MongoCompressor.createZlibCompressor()) {
            @Override
            OutputStream getOutputStream(final OutputStream target) {
                return new FilterOutputStream(target) {
                    @Override
                    public void close() throws IOException {
                        throw new IOException("could not finish message");
                    }
                };
            }
        };
        ByteBuf input = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        try (BasicOutputBuffer out = new BasicOutputBuffer()) {
            out.writeByte(47);
            assertThrows(MongoInternalException.class, () -> compressor.compress(Collections.singletonList(input), out));
            assertArrayEquals(new byte[]{47}, out.toByteArray());
        } finally {
            input.release();
        }
    }
}
