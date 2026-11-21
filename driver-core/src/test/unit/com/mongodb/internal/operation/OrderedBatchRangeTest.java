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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.thread.AsyncClientExecutor;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OrderedBatchRangeTest {
    @Test
    void shouldKeepOriginalErrorIndexAfterTypeChangeAndPayloadSplit() {
        InsertRequest insert = new InsertRequest(new BsonDocument());
        DeleteRequest delete = new DeleteRequest(new BsonDocument());
        List<WriteRequest> requests = Arrays.asList(insert, delete, delete, insert);
        BulkWriteBatch batch = BulkWriteBatch.createBulkWriteBatch(new MongoNamespace("db.coll"),
                new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())), true, WriteConcern.ACKNOWLEDGED,
                null, false, requests, OperationContext.simpleOperationContext(TimeoutSettings.DEFAULT, null, AsyncClientExecutor.NO_OP),
                null, null);

        for (int expectedSize : new int[]{1, 2, 1}) {
            assertEquals(expectedSize, batch.getPayload().size());
            batch.getPayload().setPosition(1);
            batch = batch.getNextBatch();
        }
        assertEquals(1, batch.getPayload().size());
        assertFalse(batch.hasAnotherBatch());
        batch.addResult(BsonDocument.parse("{n: 0, writeErrors: [{index: 0, code: 11000, errmsg: 'duplicate'}]}"));
        assertEquals(3, batch.getError().getWriteErrors().get(0).getIndex());
        batch.getPayload().setPosition(1);
        assertFalse(batch.getNextBatch().shouldProcessBatch());
    }
}
