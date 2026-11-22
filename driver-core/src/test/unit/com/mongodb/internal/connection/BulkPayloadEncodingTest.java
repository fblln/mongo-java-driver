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

import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BulkPayloadEncodingTest {
    @Test
    void shouldPreserveGeneratedIdOnRetryAndEncodeRemainingInsertOnSplit() {
        SplittablePayload payload = new SplittablePayload(SplittablePayload.Type.INSERT, Arrays.asList(
                new WriteRequestWithIndex(new InsertRequest(new BsonDocument()), 3),
                new WriteRequestWithIndex(new InsertRequest(new BsonDocument()), 7)), true, NoOpFieldNameValidator.INSTANCE);

        BsonValue firstId = encodeOne(payload).get("_id");
        assertNotNull(firstId);
        assertEquals(firstId, payload.getInsertedIds().get(3));
        assertEquals(firstId, encodeOne(payload).get("_id"));

        SplittablePayload remaining = payload.getNextSplit();
        BsonValue secondId = encodeOne(remaining).get("_id");
        assertNotNull(secondId);
        assertEquals(secondId, remaining.getInsertedIds().get(7));
        assertNotEquals(firstId, secondId);
    }

    @Test
    void preservesAnExplicitNullIdOnRetry() {
        SplittablePayload payload = new SplittablePayload(SplittablePayload.Type.INSERT, Collections.singletonList(
                new WriteRequestWithIndex(new InsertRequest(new BsonDocument("_id", BsonNull.VALUE)), 2)),
                true, NoOpFieldNameValidator.INSTANCE);
        assertEquals(BsonNull.VALUE, encodeOne(payload).get("_id"));
        assertEquals(BsonNull.VALUE, encodeOne(payload).get("_id"));
        assertEquals(BsonNull.VALUE, payload.getInsertedIds().get(2));
    }

    @Test
    void allowsAnEmptyReplacementAndRejectsAnEmptyUpdate() {
        BsonDocument filter = BsonDocument.parse("{key: 1}");
        BsonDocument empty = new BsonDocument();
        for (WriteRequest.Type type : Arrays.asList(WriteRequest.Type.REPLACE, WriteRequest.Type.UPDATE)) {
            SplittablePayload payload = new SplittablePayload(SplittablePayload.Type.valueOf(type.name()), Collections.singletonList(
                    new WriteRequestWithIndex(new UpdateRequest(filter, empty, type), 0)), true, NoOpFieldNameValidator.INSTANCE);
            if (type == WriteRequest.Type.REPLACE) {
                BsonDocument command = encodeOne(payload);
                assertEquals(filter, command.getDocument("q"));
                assertEquals(empty, command.getDocument("u"));
            } else {
                assertThrows(IllegalArgumentException.class, () -> encodeOne(payload));
            }
        }
    }

    private static BsonDocument encodeOne(final SplittablePayload payload) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            BsonWriterHelper.writePayload(writer, output, MessageSettings.builder().maxBatchCount(1).build(),
                    0, payload, Integer.MAX_VALUE);
            assertEquals(1, payload.getPosition());
            RawBsonDocument document = new RawBsonDocument(output.toByteArray());
            assertEquals(ByteBuffer.wrap(output.getInternalBuffer()).order(ByteOrder.LITTLE_ENDIAN).getInt(0), output.getPosition());
            return document;
        }
    }
}
