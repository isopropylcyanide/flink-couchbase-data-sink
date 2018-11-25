package com.aman.flink.constants;

/**
 *
 */
public class Constant {


	/**
	 * Binding resource to the job at runtime
	 */
	public static final String CONFIG_PROP_PARAM_NAME = "config";

	/**
	 * Couchbase user password
	 */
	public static final String CONFIG_COUCH_PASSWORD = "couchbase.password";

	/**
	 * Couchbase user name
	 */
	public static final String CONFIG_COUCH_USER_NAME = "couchbase.username";

	/**
	 * Couchbase cluster location
	 */
	public static final String CONFIG_COUCH_NODE_IP = "couchbase.node";

	/**
	 * The couchbase bucket to write to
	 */
	public static final String BUCKET_DATA = "data";

	/**
	 * The path for the list of documents to be inserted to couchbase
	 */
	public static final String STARTUP_DOCUMENT_PATH = "startup.documents.path";

	/**
	 * Flag to denote a polling behaviour vs a one time lookup
	 */
	public static final String STARTUP_DOCUMENT_POLL_CONTINOUS = "startup.documents.poll.continuous";

	/**
	 * Duration (in ms) after which the file would be polled for changes if polling is enabled
	 */
	public static final String STARTUP_DOCUMENTS_POLL_DURATION = "startup.documents.poll.duration";

	/**
	 * Database Create
	 */
	public static final String CREATE = "CREATE";

	/**
	 * Database Upsert
	 */
	public static final String UPSERT = "UPSERT";

	/**
	 * Database Remove
	 */
	public static final String REMOVE = "REMOVE";

	/**
	 * Database Get
	 */
	public static final String GET = "GET";

}
