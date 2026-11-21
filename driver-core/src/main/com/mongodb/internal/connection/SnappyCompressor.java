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

class SnappyCompressor extends Compressor {
    @Override
    public String getName() {
        return "snappy";
    }

    @Override
    public byte getId() {
        return 1;
    }

    // OP_COMPRESSED carries a raw Snappy block. Its encoder takes a contiguous range of bytes.
    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        try (CompressionBuffer input = CompressionBuffer.consume(source);
                CompressionBuffer output = CompressionBuffer.allocate(Snappy.maxCompressedLength(input.length()))) {
            int written = Snappy.compress(input.array(), input.offset(), input.length(), output.array(), output.offset());
            target.writeBytes(output.array(), output.offset(), written);
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new SnappyInputStream(source);
    }
}
