package com.github.isopropylcyanide.flinkcouchbasesink.job;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.json.JsonObject;
import com.github.isopropylcyanide.flinkcouchbasesink.datasource.CouchbaseDataSource;
import com.github.isopropylcyanide.flinkcouchbasesink.datasource.SinkJsonDocument;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.util.List;
import java.util.Properties;

/**
 * A custom sink that dumps the incoming json documents to couchbase asynchronously
 */
public class CouchbaseDumpSinkFunction implements SinkFunction<List<SinkJsonDocument>> {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseDumpSinkFunction.class);
    private final Properties properties;

    public CouchbaseDumpSinkFunction(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void invoke(List<SinkJsonDocument> sinkDocuments, Context context) {
        log.info("Processing event at [{}]", context.timestamp());
        CouchbaseDataSource dataSource = new CouchbaseDataSource(properties);

        sinkDocuments.forEach(doc -> {
            final String docId = doc.getId();
            final JsonObject jsonObject = JsonObject.from(doc.getJsonMap());
            dataSource.upsertDocument(docId, jsonObject).subscribe(new ResponseSubscriber());
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
