package org.mongodb.devrel.pod.wfchangestreamsample;

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
 *
 */

import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


//
//Sample code showing how to connect to database, establish a changeStream cursor
//on a collection and write the event information to an Audit collection
//
//https://www.mongodb.com/docs/v5.0/changeStreams/#change-streams
//
//An example change stream document is shown in ChangeStreamDocuments.json
//

public class Main {

    static Logger logger;

    public static void main(String[] args) {

        logger = LoggerFactory.getLogger(Main.class);

        //Connect to your MongoDB Instance
        String uri = "mongodb+srv://mscsuser:OAnEgOls9HKB3qZd@wfchangestreamaudit.pvttaro.mongodb.net/test";

        MongoCursor<ChangeStreamDocument<Document>> changeStreamCursor = null;
        try (MongoClient mongoClient = MongoClients.create(uri)) {

            //Get a reference to the MongoDB database and collection you wish to monitor
            MongoDatabase database = mongoClient.getDatabase("WFAudit");
            MongoCollection<Document> btRequestsCol = database.getCollection("BTRequests");
            MongoCollection<Document> btAuditCol = database.getCollection("BTAudit");

            //Create an aggregation pipeline to say we only want "UPDATE", "INSERT" or "DELETE" events.
            //As well as filtering events, you can also use this to shape the returned event documents.
            //https://www.mongodb.com/docs/v5.0/changeStreams/#modify-change-stream-output
            List<Document> filterPipeline = Arrays.asList(
                    new Document("$match",
                            new Document("$expr",
                                    new Document("$or", Arrays.asList(
                                            new Document("$eq", Arrays.asList("$operationType", "insert")),
                                            new Document("$gte", Arrays.asList("$operationType", "update")),
                                            new Document("$gte", Arrays.asList("$operationType", "delete"))
                                    ))
                            )
                    )
            );


            //Send a request to watch the collection and use the returned cursor to respond to
            //events. Include in the watch request that we want a full copy of the document for
            //update events as well as the delta fields. The full document is included by
            //default in insert events. We also request the 'before' image of the document
            //for updates and deletes.
            changeStreamCursor = btRequestsCol.watch(filterPipeline).fullDocument(FullDocument.UPDATE_LOOKUP).fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE).iterator();
            while(changeStreamCursor.hasNext()){
                ChangeStreamDocument<Document> nextEvent = changeStreamCursor.next();
                //Shape the returned change event date here if needed
                logger.info((nextEvent.toString()));
                Document auditDoc = new Document();
                auditDoc.append("operationType", nextEvent.getOperationTypeString());
                auditDoc.append("fullDocument", nextEvent.getFullDocument());
                auditDoc.append("fullDocumentBefore", nextEvent.getFullDocumentBeforeChange());
                auditDoc.append("updatedFields", nextEvent.getUpdateDescription().getUpdatedFields());
                auditDoc.append("changeDate", Date.from(Instant.ofEpochSecond(nextEvent.getClusterTime().getTime())));
                auditDoc.append("documentPath", nextEvent.getNamespace().getFullName());
                auditDoc.append("documentID", nextEvent.getDocumentKey());
                btAuditCol.insertOne(auditDoc);
            }
        }finally {
            if(changeStreamCursor != null) {
                changeStreamCursor.close();
            }
        }
    }
}
