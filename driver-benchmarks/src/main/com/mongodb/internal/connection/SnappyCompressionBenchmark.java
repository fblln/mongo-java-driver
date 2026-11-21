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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Compresses the complete list of wire-message segments into the driver's pooled output. */
@State(Scope.Thread)
public class SnappyCompressionBenchmark {
    @Param({"0", "1", "7", "31", "127", "128", "512", "4096", "16384", "65535", "65536", "65537",
            "262144", "1048576", "4194304", "8388608"})
    private int size;
    @Param({"heap", "direct", "two", "eight", "thirtyTwo", "oneTwentyEight", "mixed"})
    private String layout;
    @Param({"structured", "binary"})
    private String content;

    private final SnappyCompressor compressor = new SnappyCompressor();
    private final List<ByteBuf> input = new ArrayList<>();

    @Setup
    public void setup() throws IOException {
        int segments;
        switch (layout) {
            case "heap":
            case "direct":
                segments = 1;
                break;
            case "two":
                segments = 2;
                break;
            case "eight":
            case "mixed":
                segments = 8;
                break;
            case "thirtyTwo":
                segments = 32;
                break;
            case "oneTwentyEight":
                segments = 128;
                break;
            default:
                throw new IllegalArgumentException(layout);
        }
        byte[] bytes = new byte[size];
        new Random(47).nextBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
            if (content.equals("structured") && i % 64 < 48) {
                bytes[i] = (byte) (i % 13);
            }
        }
        int offset = 0;
        for (int i = 0; i < segments; i++) {
            int length = size / segments + (i < size % segments ? 1 : 0);
            boolean direct = layout.equals("direct") || (layout.equals("mixed") && i % 2 == 1);
            ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
            buffer.put(bytes, offset, length);
            offset += length;
            ((Buffer) buffer).flip();
            input.add(new ByteBufNIO(buffer));
        }
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            compressor.compress(input, target);
            if (!Arrays.equals(bytes, Snappy.uncompress(target.toByteArray()))) {
                throw new IllegalStateException("Compression fixture did not round-trip");
            }
        }
    }

    @Benchmark
    public int compress() {
        for (ByteBuf buffer : input) {
            buffer.position(0);
        }
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            compressor.compress(input, target);
            return target.getPosition();
        }
    }

    @TearDown
    public void close() {
        input.forEach(ByteBuf::release);
    }
}
