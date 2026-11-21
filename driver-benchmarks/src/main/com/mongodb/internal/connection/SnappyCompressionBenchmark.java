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
import com.mongodb.lang.Nullable;
import io.netty.buffer.PooledByteBufAllocator;
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
    @Param({"heap", "direct", "two", "eight", "thirtyTwo", "oneTwentyEight", "mixed", "driverSegments", "nettyDirect"})
    private String layout;
    @Param({"structured", "binary"})
    private String content;

    private final SnappyCompressor compressor = new SnappyCompressor();
    private final List<ByteBuf> input = new ArrayList<>();

    private BufferProvider provider;
    @Nullable
    private ByteBufferBsonOutput segmentedSource;
    private int startPosition;

    @Setup
    public void setup() throws IOException {
        byte[] bytes = new byte[size];
        new Random(47).nextBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
            if (content.equals("structured") && i % 64 < 48) {
                bytes[i] = (byte) (i % 13);
            }
        }
        provider = layout.equals("nettyDirect")
                ? n -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.directBuffer(n, n))
                : PowerOfTwoBufferPool.DEFAULT;
        if (layout.equals("driverSegments")) {
            segmentedSource = new ByteBufferBsonOutput(provider);
            segmentedSource.writeBytes(new byte[MessageHeader.MESSAGE_HEADER_LENGTH]);
            segmentedSource.writeBytes(bytes);
            input.addAll(segmentedSource.getByteBuffers());
            startPosition = MessageHeader.MESSAGE_HEADER_LENGTH;
        } else {
            addSegments(bytes);
        }
        resetInput();
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(provider)) {
            compressor.compress(input, target);
            if (!Arrays.equals(bytes, Snappy.uncompress(target.toByteArray()))) {
                throw new IllegalStateException("Compression fixture did not round-trip");
            }
        }
    }

    private void addSegments(final byte[] bytes) {
        int segments;
        switch (layout) {
            case "heap":
            case "direct":
            case "nettyDirect":
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
        startPosition = segments == 1 ? MessageHeader.MESSAGE_HEADER_LENGTH : 0;
        int offset = 0;
        for (int i = 0; i < segments; i++) {
            int length = bytes.length / segments + (i < bytes.length % segments ? 1 : 0);
            boolean direct = layout.equals("direct") || layout.equals("nettyDirect") || (layout.equals("mixed") && i % 2 == 1);
            int prefix = i == 0 ? startPosition : 0;
            ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(prefix + length) : ByteBuffer.allocate(prefix + length);
            ((Buffer) buffer).position(prefix);
            buffer.put(bytes, offset, length);
            offset += length;
            ((Buffer) buffer).flip();
            input.add(new ByteBufNIO(buffer));
        }
    }

    private void resetInput() {
        for (int i = 0; i < input.size(); i++) {
            input.get(i).position(i == 0 ? startPosition : 0);
        }
    }

    @Benchmark
    public int compress() {
        resetInput();
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(provider)) {
            compressor.compress(input, target);
            return target.getPosition();
        }
    }

    /** Includes the wire header, length backpatch and outgoing buffer handoff. */
    @Benchmark
    public int encodeForSend() {
        resetInput();
        try (ByteBufferBsonOutput target = new ByteBufferBsonOutput(provider)) {
            target.writeInt32(0);
            target.writeInt32(42);
            target.writeInt32(0);
            target.writeInt32(OpCode.OP_COMPRESSED.getValue());
            target.writeInt32(OpCode.OP_MSG.getValue());
            target.writeInt32(size);
            target.writeByte(compressor.getId());
            compressor.compress(input, target);
            target.writeInt32(0, target.getPosition());
            int count = 0;
            List<ByteBuf> outgoing = target.getByteBuffers();
            try {
                for (ByteBuf segment : outgoing) {
                    count += segment.remaining();
                }
            } finally {
                outgoing.forEach(ByteBuf::release);
            }
            return count;
        }
    }

    @TearDown
    public void close() {
        input.forEach(ByteBuf::release);
        if (segmentedSource != null) {
            segmentedSource.close();
        }
    }
}
