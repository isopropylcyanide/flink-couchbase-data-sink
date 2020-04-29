package com.github.isopropylcyanide.flinkcouchbasesink;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.util.List;
import java.util.Properties;

/**
 * A custom sink that dumps the incoming json documents to couchbase asynchronously
 */
class CouchbaseJsonDocumentSink implements SinkFunction<List<SinkJsonDocument>> {

    private final Properties properties;

    CouchbaseJsonDocumentSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void invoke(List<SinkJsonDocument> starterJsonDocuments, Context context) {
        CouchbaseManager cbManager = new CouchbaseManager();

        starterJsonDocuments
                .forEach(doc -> {
                    final String docId = doc.getId();
                    final JsonObject jsonObject = JsonObject.from(doc.getJsonMap());
                    cbManager.upsertDocument(docId, jsonObject, properties).subscribe(new ResponseSubscriber());
                });
    }

    public static class ResponseSubscriber extends Subscriber<Document> {

        private static final Logger log = LoggerFactory.getLogger(ResponseSubscriber.class);

        @Override
        public void onCompleted() {
            log.info("Successfully completed observable");
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error occurred from observable ", throwable);
        }

        @Override
        public void onNext(Document document) {
            log.info("Successfully processed document {}", document.id());
        }
    }
}
