/*
 * Licensed under the Apache License, Version 2.0 (the "License");	 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.	 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at	 * You may obtain a copy of the License at
 *	 *
 * http://www.apache.org/licenses/LICENSE-2.0	 * http://www.apache.org/licenses/LICENSE-2.0
 *	 *
 * Unless required by applicable law or agreed to in writing, software	 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,	 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.	 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and	 * See the License for the specific language governing permissions and
 * limitations under the License.	 * limitations under the License.
 */
package com.github.isopropylcyanide.flinkcouchbasesink;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class CouchbaseManager {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseManager.class);
    private static final HashMap<String, AsyncBucket> bucketsMap = new HashMap<>();
    private static final Map<String, CouchbaseCluster> clusters = new HashMap<>();

    private static Cluster getCluster(String nodes, String user, String pass) {
        CouchbaseCluster cluster = clusters.get(nodes);
        if (cluster == null) {
            synchronized (clusters) {
                cluster = clusters.get(nodes);
                if (cluster == null) {
                    log.info("Attempting to connect to couchbase cluster [{}]", nodes);
                    String[] nodeList = nodes.split(";");
                    cluster = CouchbaseCluster.create(nodeList);
                    cluster.authenticate(user, pass);
                    log.info("Connected to couchbase cluster [{}]", nodes);
                    clusters.put(nodes, cluster);
                }
            }
        }
        return cluster;
    }

    /**
     * Gets a bucket with the given name from the cluster
     */
    private static AsyncBucket getBucket(Cluster cluster, String bucketName) {
        AsyncBucket bucket = bucketsMap.get(bucketName);
        if (bucket != null)
            return bucket;
        else {
            synchronized (bucketsMap) {
                bucket = bucketsMap.get(bucketName);
                if (bucket == null) {
                    bucket = Objects.requireNonNull(cluster).openBucket(bucketName).async();
                    bucketsMap.put(bucketName, bucket);
                }
            }
        }
        return bucket;
    }

    /**
     * Opens a connection with the correct config properties
     */
    private Cluster openConnection(Properties properties) {
        String nodeIP = properties.getProperty(JobProperties.CONFIG_COUCH_NODE_IP);
        String nodeUser = properties.getProperty(JobProperties.CONFIG_COUCH_USER_NAME);
        String nodePassword = properties.getProperty(JobProperties.CONFIG_COUCH_PASSWORD);
        return CouchbaseManager.getCluster(nodeIP, nodeUser, nodePassword);
    }

    /**
     * Upsert document in bucket with the given object
     */
    Observable<Document> upsertDocument(String docId, JsonObject payload, Properties properties) {
        Cluster cluster = this.openConnection(properties);
        Objects.requireNonNull(cluster);
        JsonDocument couchbaseDocument = JsonDocument.create(docId, payload);
        AsyncBucket asyncBucket = CouchbaseManager.getBucket(cluster, JobProperties.BUCKET_DATA);
        return asyncBucket.upsert(couchbaseDocument);
    }
}
