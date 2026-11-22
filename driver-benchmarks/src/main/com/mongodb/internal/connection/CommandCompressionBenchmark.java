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
import org.bson.ByteBuf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Measures outbound compression of a pre-encoded OP_MSG body with the driver's normal segment sizes. */
@State(Scope.Thread)
public class CommandCompressionBenchmark {
    private static final byte[] COMPRESSED_HEADER = new byte[25];

    @Param({"zlib", "zstd", "snappy"})
    private String codec;

    @Param({"1", "100", "1000"})
    private int documents;

    private Compressor compressor;
    private ByteBufferBsonOutput message;
    private List<ByteBuf> source;

    @Setup
    public void setup() {
        switch (codec) {
            case "zlib":
                compressor = new ZlibCompressor(MongoCompressor.createZlibCompressor());
                break;
            case "zstd":
                compressor = new ZstdCompressor();
                break;
            case "snappy":
                compressor = new SnappyCompressor();
                break;
            default:
                throw new IllegalArgumentException(codec);
        }
        message = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT);
        message.writeBytes(new byte[16]);
        message.writeInt32(0);
        message.writeByte(0);
        int commandStart = message.getPosition();
        message.writeInt32(0);
        message.writeByte(2);
        message.writeCString("insert");
        message.writeString("events");
        message.writeByte(2);
        message.writeCString("$db");
        message.writeString("bench");
        message.writeByte(0);
        message.writeInt32(commandStart, message.getPosition() - commandStart);
        message.writeByte(1);
        int sequenceStart = message.getPosition();
        message.writeInt32(0);
        message.writeCString("documents");
        int documentSize = documents == 1 ? 512 : 4096;
        byte[] payload = new byte[documentSize - 28];
        Random random = new Random(351);
        for (int id = 0; id < documents; id++) {
            random.nextBytes(payload);
            for (int j = 0; j < payload.length; j += 128) {
                Arrays.fill(payload, j, Math.min(j + 96, payload.length), (byte) (j % 13));
            }
            message.writeInt32(documentSize);
            message.writeByte(16);
            message.writeCString("_id");
            message.writeInt32(id);
            message.writeByte(5);
            message.writeCString("payload");
            message.writeInt32(payload.length);
            message.writeByte(0);
            message.writeBytes(payload);
            message.writeByte(0);
        }
        message.writeInt32(sequenceStart, message.getPosition() - sequenceStart);
        source = message.getByteBuffers();
    }

    @Benchmark
    public int compress() {
        for (int i = 0; i < source.size(); i++) {
            source.get(i).position(i == 0 ? 16 : 0);
        }
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(PowerOfTwoBufferPool.DEFAULT)) {
            output.writeBytes(COMPRESSED_HEADER);
            compressor.compress(source, output);
            return output.getPosition();
        }
    }

    @TearDown
    public void tearDown() {
        source.forEach(ByteBuf::release);
        message.close();
    }
}
