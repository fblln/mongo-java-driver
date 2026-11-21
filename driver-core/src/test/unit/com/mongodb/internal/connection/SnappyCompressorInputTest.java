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
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SnappyCompressorInputTest {
    @Test
    void shouldCompressOnlyRemainingBytesOfArraySlice() throws IOException {
        byte[] bytes = new byte[96];
        new Random(13).nextBytes(bytes);
        ByteBuffer slice = ByteBuffer.wrap(bytes, 12, 64).slice();
        ((Buffer) slice).position(7).limit(41);
        ByteBuf input = new ByteBufNIO(slice);
        try (BasicOutputBuffer output = new BasicOutputBuffer()) {
            new SnappyCompressor().compress(Collections.singletonList(input), output);
            assertArrayEquals(Snappy.compress(Arrays.copyOfRange(bytes, 19, 53)), output.toByteArray());
            assertEquals(input.limit(), input.position());
        } finally {
            input.release();
        }
    }
}
