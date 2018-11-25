package com.aman.flink.utility;

import com.aman.flink.constants.Constant;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CouchbaseManager {

	private static final Logger LOG = LoggerFactory.getLogger(CouchbaseManager.class);

	private static final HashMap<String, Bucket> mapOFBuckets = new HashMap<>();

	private static final Map<String, CouchbaseCluster> clusters = new HashMap<>();

	private static Cluster getCluster(String nodes, String user, String pass) {
		CouchbaseCluster cluster = clusters.get(nodes);
		if (cluster == null) {
			synchronized (clusters) {
				cluster = clusters.get(nodes);
				if (cluster == null) {
					LOG.info("Attempting to connect to couchbase cluster [{0}]", nodes);
					String[] nodeList = nodes.split(";");
					cluster = CouchbaseCluster.create(nodeList);
					cluster.authenticate(user, pass);
					LOG.info("Connected to couchbase cluster [{0}]", nodes);
					clusters.put(nodes, cluster);
				}
			}
		}
		return cluster;
	}

	/**
	 * Gets a bucket with the given name from the cluster
	 */
	private static Bucket getBucket(Cluster cluster, String bucketName) {
		Bucket bucket = mapOFBuckets.get(bucketName);
		if (bucket != null)
			return bucket;
		else {
			synchronized (mapOFBuckets) {
				bucket = mapOFBuckets.get(bucketName);
				if (bucket == null) {
					bucket = Objects.requireNonNull(cluster).openBucket(bucketName);
					mapOFBuckets.put(bucketName, bucket);
				}
			}
		}
		return bucket;
	}

	/**
	 * Opens a connection with the correct config properties
	 */
	private Cluster openConnection() {
		ParameterTool applicationProperties = Util.getApplicationProperties();
		String nodeIP = applicationProperties.getRequired(Constant.CONFIG_COUCH_NODE_IP);
		String nodeUser = applicationProperties.getRequired(Constant.CONFIG_COUCH_USER_NAME);
		String nodePassword = applicationProperties.getRequired(Constant.CONFIG_COUCH_PASSWORD);
		return CouchbaseManager.getCluster(nodeIP, nodeUser, nodePassword);
	}

	/**
	 * Upsert document in bucket with the given object
	 */
	public void upsertDocument(String docId, String bucketName, JsonObject payload) {
		Cluster cluster = this.openConnection();
		Objects.requireNonNull(cluster);
		JsonDocument jsonDocument = JsonDocument.create(docId, payload);
		Bucket bucket = getBucket(cluster, bucketName);
		bucket.upsert(jsonDocument);
	}

}
