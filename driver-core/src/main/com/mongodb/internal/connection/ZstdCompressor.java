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

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

class ZstdCompressor extends StreamingCompressor {
    private static final int MAX_ONE_SHOT_SIZE = 8 * 1024;

    @Override
    public String getName() {
        return "zstd";
    }

    @Override
    public byte getId() {
        return 3;
    }

    @Override
    void compress(final List<ByteBuf> source, final BsonOutput target) {
        // Stream setup is expensive for short commands. Keep the temporary output small on the one-shot path.
        int remainingBudget = MAX_ONE_SHOT_SIZE;
        for (ByteBuf buffer : source) {
            remainingBudget -= buffer.remaining();
            if (remainingBudget < 0) {
                super.compress(source, target);
                return;
            }
        }
        int length = MAX_ONE_SHOT_SIZE - remainingBudget;
        byte[] input;
        int offset = 0;
        if (source.size() == 1 && source.get(0).isBackedByArray()) {
            ByteBuf buffer = source.get(0);
            input = buffer.array();
            offset = buffer.arrayOffset() + buffer.position();
            buffer.position(buffer.limit());
        } else {
            input = new byte[length];
            consumeIntoArray(source, input);
        }
        try {
            byte[] output = new byte[(int) Zstd.compressBound(length)];
            int written = (int) Zstd.compressByteArray(output, 0, output.length, input, offset, length, Zstd.defaultCompressionLevel());
            target.writeBytes(output, 0, written);
        } catch (Exception e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    @Override
    OutputStream getOutputStream(final OutputStream target) throws IOException {
        return new ZstdOutputStreamNoFinalizer(target, RecyclingBufferPool.INSTANCE);
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new ZstdInputStreamNoFinalizer(source);
    }
}
