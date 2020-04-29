/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.isopropylcyanide.flinkcouchbasesink.datasource;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CouchbaseDataSource {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseDataSource.class);
    private final Map<String, AsyncBucket> bucketsMap = new ConcurrentHashMap<>();
    private final Cluster cluster;

    public CouchbaseDataSource(Properties properties) {
        String nodeIP = properties.getProperty(DataSourceProperties.CONFIG_COUCH_NODE_IP);
        String nodeUser = properties.getProperty(DataSourceProperties.CONFIG_COUCH_USER_NAME);
        String nodePassword = properties.getProperty(DataSourceProperties.CONFIG_COUCH_PASSWORD);
        this.cluster = setCluster(nodeIP, nodeUser, nodePassword);
    }

    public Observable<Document> upsertDocument(String docId, JsonObject payload) {
        JsonDocument couchbaseDocument = JsonDocument.create(docId, payload);
        AsyncBucket asyncBucket = getBucket(cluster);
        return asyncBucket.upsert(couchbaseDocument);
    }

    private Cluster setCluster(String nodes, String user, String pass) {
        CouchbaseCluster cluster;
        log.info("Attempting to connect to couchbase cluster [{}]", nodes);
        String[] nodeList = nodes.split(";");
        cluster = CouchbaseCluster.create(nodeList);
        cluster.authenticate(user, pass);
        log.info("Connected to couchbase cluster [{}]", nodes);
        return cluster;
    }

    private AsyncBucket getBucket(Cluster cluster) {
        String bucketName = DataSourceProperties.BUCKET_DATA;
        bucketsMap.computeIfAbsent(bucketName, n -> cluster.openBucket(n).async());
        return bucketsMap.get(bucketName);
    }
}
