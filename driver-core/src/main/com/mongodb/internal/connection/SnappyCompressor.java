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
    private static final int MIN_WORKSPACE_INPUT = 4 * 1024;
    private static final int MAX_WORKSPACE_INPUT = 64 * 1024;
    private final AtomicReference<Workspace> idleWorkspace = new AtomicReference<>();

    private static final class Workspace {
        private final byte[] input = new byte[MAX_WORKSPACE_INPUT];
        private final byte[] output = new byte[Snappy.maxCompressedLength(MAX_WORKSPACE_INPUT)];
    }

    @Override
    public String getName() {
        return "snappy";
    }

    @Override
    public byte getId() {
        return 1;
    }

    // OP_COMPRESSED carries a single raw Snappy block.
    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        int length = source.size() == 1 ? source.get(0).remaining() : getUncompressedSize(source);
        Workspace workspace = null;
        if (length >= MIN_WORKSPACE_INPUT && length <= MAX_WORKSPACE_INPUT) {
            workspace = idleWorkspace.getAndSet(null);
            if (workspace == null) {
                workspace = new Workspace();
            }
        }
        try {
            byte[] input;
            int offset;
            if (source.size() == 1 && source.get(0).isBackedByArray()) {
                ByteBuf buffer = source.get(0);
                input = buffer.array();
                offset = buffer.arrayOffset() + buffer.position();
                buffer.position(buffer.limit());
            } else {
                input = workspace == null ? new byte[length] : workspace.input;
                offset = 0;
                copy(source, input);
            }
            byte[] output = workspace == null ? new byte[Snappy.maxCompressedLength(length)] : workspace.output;
            int written = Snappy.compress(input, offset, length, output, 0);
            target.writeBytes(output, 0, written);
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        } finally {
            if (workspace != null) {
                // writeBytes has copied the result; another operation can now claim these arrays.
                idleWorkspace.compareAndSet(null, workspace);
            }
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
