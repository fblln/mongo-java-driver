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
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class SnappyCompressor extends Compressor {
    private static final int WINDOW_SIZE = 64 * 1024;
    private static final int MIN_SCRATCH_MESSAGE_SIZE = 4 * 1024;

    // One idle scratch pair shared across connections. Concurrent calls own separate arrays.
    private static final AtomicReference<Scratch> IDLE_SCRATCH = new AtomicReference<>();

    @Override
    public String getName() {
        return "snappy";
    }

    @Override
    public byte getId() {
        return 1;
    }

    // The server expects a raw Snappy block, so the framed streaming compressor cannot be used.
    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        int messageSize = source.size() == 1 ? source.get(0).remaining() : getUncompressedSize(source);
        if (messageSize >= MIN_SCRATCH_MESSAGE_SIZE) {
            compressWithScratch(source, target, messageSize);
            return;
        }

        byte[] input;
        int offset;
        int length;
        if (source.size() == 1 && source.get(0).isBackedByArray()) {
            ByteBuf buffer = source.get(0);
            input = buffer.array();
            offset = buffer.arrayOffset() + buffer.position();
            length = buffer.remaining();
            buffer.position(buffer.limit());
        } else {
            length = messageSize;
            input = new byte[length];
            offset = 0;
            copy(source, input);
        }

        try {
            byte[] out = new byte[Snappy.maxCompressedLength(length)];
            int compressedSize = Snappy.compress(input, offset, length, out, 0);
            target.writeBytes(out, 0, compressedSize);
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
    }

    private static void compressWithScratch(final List<ByteBuf> source, final BsonOutput target, final int size) {
        int windowSize = Math.min(size, WINDOW_SIZE);
        Scratch scratch = IDLE_SCRATCH.getAndSet(null);
        if (scratch == null || scratch.input.length < windowSize) {
            scratch = new Scratch(windowSize);
        }
        try {
            // Raw Snappy has one varint length followed by literals and relative backreferences.
            // Each window's tags can follow the preceding window's tags unchanged.
            if (size > WINDOW_SIZE) {
                writeLength(target, size);
            }
            int index = 0;
            for (int remaining = size; remaining > 0;) {
                int length = Math.min(remaining, WINDOW_SIZE);
                while (!source.get(index).hasRemaining()) {
                    index++;
                }
                ByteBuf buffer = source.get(index);
                byte[] input;
                int offset;
                if (buffer.isBackedByArray() && buffer.remaining() >= length) {
                    input = buffer.array();
                    offset = buffer.arrayOffset() + buffer.position();
                    buffer.position(buffer.position() + length);
                } else {
                    input = scratch.input;
                    offset = 0;
                    int copied = 0;
                    while (copied < length) {
                        buffer = source.get(index);
                        int count = Math.min(length - copied, buffer.remaining());
                        buffer.get(input, copied, count);
                        copied += count;
                        if (copied < length) {
                            index++;
                        }
                    }
                }
                int written = Snappy.compress(input, offset, length, scratch.output, 0);
                int prefix = size > WINDOW_SIZE ? varintLength(length) : 0;
                target.writeBytes(scratch.output, prefix, written - prefix);
                remaining -= length;
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        } finally {
            IDLE_SCRATCH.compareAndSet(null, scratch);
        }
    }

    private static void writeLength(final BsonOutput target, final int length) {
        int value = length;
        while (value >= 128) {
            target.writeByte((value & 127) | 128);
            value >>>= 7;
        }
        target.writeByte(value);
    }

    private static int varintLength(final int value) {
        // A window of at most 64 KiB needs at most three varint bytes.
        return value < 128 ? 1 : value < 16384 ? 2 : 3;
    }

    private static final class Scratch {
        private final byte[] input;
        private final byte[] output;

        Scratch(final int length) {
            input = new byte[length];
            output = new byte[Snappy.maxCompressedLength(length)];
        }
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
