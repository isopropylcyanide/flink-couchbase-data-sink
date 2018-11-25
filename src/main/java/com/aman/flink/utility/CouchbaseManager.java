package com.prudential.prutopia.utility;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.prudential.platform.data.couchbase.CouchbaseClusterMgr;
import com.prudential.prutopia.exception.DBTransactionException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;

public class SynchronousDBUtil {

	private static final Logger LOG = LoggerFactory.getLogger(SynchronousDBUtil.class);

	private Cluster cluster;

	static HashMap<String, Bucket> mapOFBuckets = new HashMap<String, Bucket>();

	static Bucket getBucket(Cluster cluster, String bucketName) {
		Bucket bucket = mapOFBuckets.get(bucketName);
		if (bucket != null)
			return bucket;
		else {
			synchronized (mapOFBuckets) {
				bucket = mapOFBuckets.get(bucketName);
				if (bucket == null) {
					bucket = cluster.openBucket(bucketName);
					mapOFBuckets.put(bucketName, bucket);
				}
			}
		}
		return bucket;
	}

	/**
	 * Get synchronous db util
	 */
	public static SynchronousDBUtil getDBUtil() {
		try {
			return SynchronousDBUtil.class.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.warn(e.getMessage());
			return null;
		}
	}

	/**
	 * Opens a connection with the correct config properties
	 */
	public void openConnection() {
		/*CouchbaseEnvironment couchbaseEnvironment = DefaultCouchbaseEnvironment.builder()
				.connectTimeout(10000)
				.build();*/
		ParameterTool applicationProperties = Util.getApplicationProperties();
		String nodeIP = applicationProperties.getRequired(PruConstants.CONFIG_COUCH_NODE_IP);
		String nodeUser = applicationProperties.getRequired(PruConstants.CONFIG_COUCH_USER_NAME);
		String nodePassword = applicationProperties.getRequired(PruConstants.CONFIG_COUCH_PASSWORD);
		//this.cluster = CouchbaseCluster.create(couchbaseEnvironment, nodeIP);
		//this.cluster.authenticate(nodeUser, nodePassword);
		this.cluster = CouchbaseClusterMgr.getCluster(nodeIP, nodeUser, nodePassword);
	}

	/**
	 * Perform a database operation with the given operation type and json object
	 */
	public Optional<JsonDocument> doTransaction(String docId, String bucketName, JsonObject jsonObject,
			String dbOperation) throws DBTransactionException {
		openConnection();

		Optional<JsonDocument> respJsonDoc = Optional.empty();
		switch (dbOperation) {
			case PruConstants.CREATE:
				respJsonDoc = insertDocument(docId, bucketName, jsonObject);
				break;

			case PruConstants.UPSERT:
				respJsonDoc = upsertDocument(docId, bucketName, jsonObject);
				break;

			case PruConstants.REMOVE:
				respJsonDoc = deleteByDocumentId(docId, bucketName, jsonObject);
				break;

			case PruConstants.GET:
				respJsonDoc = get(docId, bucketName);
				break;
		}
		/*if (Util.isFalse(closeConnection()))
			throw new DBTransactionException("Could not disconnect the node : [" + PruConstants.COUCH_NODE + "]");*/
		return respJsonDoc;
	}

	/**
	 * Get document by id from the specified bucket name
	 */
	private Optional<JsonDocument> get(String docId, String bucketName) throws DBTransactionException {
		Optional<Bucket> bucketOpt = Optional.ofNullable(getBucket(cluster, bucketName));
		bucketOpt.orElseThrow(() -> new DBTransactionException("Could not open bucket : [" + bucketName + " ]"));

		Optional<JsonDocument> jsonDocumentOpt = bucketOpt.map(bucket -> bucket.get(docId));
		/*bucketOpt.map().filter(Util::isFalse)
				.ifPresent(ex -> new DBTransactionException("Could not close bucket : [" + bucketName + "]"));*/
		return jsonDocumentOpt;
	}


	/**
	 * Inserts document in the bucket with a specific id
	 */
	private Optional<JsonDocument> insertDocument(String docId, String bucketName, JsonObject payload) throws DBTransactionException {
		Optional<JsonDocument> jsonDoc = Optional.empty();
		Optional<Bucket> bucket = Optional.ofNullable(getBucket(cluster, bucketName));
		bucket.orElseThrow(() -> new DBTransactionException("Could not open bucket : [" + bucketName + " ]"));
		if (bucket.isPresent()) {
			jsonDoc = Optional.of(bucket.get().insert(JsonDocument.create(docId, payload)));
			/*if (Util.isFalse(bucket.get().close()))
				throw new DBTransactionException("Could not close bucket : [" + bucketName + "]");*/
		}

		return jsonDoc;
	}

	/**
	 * Delete document from the bucket either by id or the object
	 */
	private Optional<JsonDocument> deleteByDocumentId(String docId, String bucketName, JsonObject payload) {

		Optional<JsonDocument> jsonDoc = Optional.empty();
		Optional<Bucket> bucket = Optional.ofNullable(getBucket(cluster, bucketName));
		bucket.orElseThrow(() -> new DBTransactionException("Could not open bucket : [" + bucketName + " ]"));
		if (bucket.isPresent()) {

			jsonDoc = Optional.ofNullable(bucket.get().remove((JsonDocument.create(docId, payload))));
		}

		return jsonDoc;
	}

	/**
	 * Upsert document in bucket with the given object
	 */
	private Optional<JsonDocument> upsertDocument(String docId, String bucketName, JsonObject payload) {
		Optional<JsonDocument> jsonDoc = Optional.empty();
		Optional<Bucket> bucket = Optional.ofNullable(getBucket(cluster, bucketName));
		bucket.orElseThrow(() -> new DBTransactionException("Could not open bucket : [" + bucketName + " ]"));
		if (bucket.isPresent()) {
			jsonDoc = Optional.of(bucket.get().upsert(JsonDocument.create(docId, payload)));
			/*if (Util.isFalse(bucket.get().close()))
				throw new DBTransactionException("Couldn't close bucket : [" + bucketName + "]");*/
		}
		return jsonDoc;
	}

	/**
	 * Close connection gracefully
	 */
	/*public Boolean closeConnection() {
		try {
			return this.cluster.disconnect();
		} catch (Exception e) {
			LOG.info("Exception occurred while closing cluster");
			LOG.warn(e.getMessage());
		}
		return false;
	}*/
}
