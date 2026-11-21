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

import org.bson.ByteBufNIO;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class BsonLengthBackpatchTest {
    @Test
    void shouldPatchLengthsOnEitherSideOfASegmentBoundaryAfterTruncation() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(size -> new ByteBufNIO(ByteBuffer.allocate(size)))) {
            output.writeBytes(new byte[4096]);
            output.truncateToPosition(1030);
            output.writeBytes(new byte[16]);
            byte[] expected = new byte[1046];
            ByteBuffer bytes = ByteBuffer.wrap(expected).order(ByteOrder.LITTLE_ENDIAN);
            for (int offset : new int[]{1034, 1021, 1011, 1024}) {
                output.writeInt32(offset, offset + 1);
                bytes.putInt(offset, offset + 1);
                assertArrayEquals(expected, output.toByteArray());
            }
        }
    }
}
