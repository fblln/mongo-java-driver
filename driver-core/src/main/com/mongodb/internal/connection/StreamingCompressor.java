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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

abstract class StreamingCompressor extends Compressor {
    private static final int COPY_BUFFER_SIZE = 8192;

    abstract OutputStream getOutputStream(OutputStream target) throws IOException;

    @Override
    void compress(final List<ByteBuf> source, final BsonOutput target) {
        int start = target.getPosition();
        boolean finished = false;
        try {
            try (OutputStream encoder = getOutputStream(new BsonOutputStream(target))) {
                byte[] scratch = null;
                for (ByteBuf buffer : source) {
                    if (!buffer.hasRemaining()) {
                        continue;
                    }
                    if (buffer.isBackedByArray()) {
                        encoder.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                        buffer.position(buffer.limit());
                    } else {
                        if (scratch == null) {
                            scratch = new byte[COPY_BUFFER_SIZE];
                        }
                        while (buffer.hasRemaining()) {
                            int count = Math.min(buffer.remaining(), scratch.length);
                            buffer.get(scratch, 0, count);
                            encoder.write(scratch, 0, count);
                        }
                    }
                }
            }
            finished = true;
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        } finally {
            if (!finished && target.getPosition() > start) {
                target.truncateToPosition(start);
            }
        }
    }

    // Finishing the encoder must leave the caller's BSON output open.
    private static final class BsonOutputStream extends OutputStream {
        private final BsonOutput target;

        BsonOutputStream(final BsonOutput target) {
            this.target = target;
        }

        @Override
        public void write(final int value) {
            target.writeByte(value);
        }

        @Override
        public void write(final byte[] bytes, final int offset, final int length) {
            target.writeBytes(bytes, offset, length);
        }
    }
}
