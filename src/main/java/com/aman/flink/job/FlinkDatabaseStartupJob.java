package com.aman.flink.job;

import com.aman.flink.Env;
import com.aman.flink.constants.Constant;
import com.aman.flink.model.StarterJsonDocument;
import com.aman.flink.utility.CouchbaseManager;
import com.aman.flink.utility.Util;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class FlinkDatabaseStartupJob {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDatabaseStartupJob.class);
	private static CouchbaseManager cbManager = null;

	public static void main(String[] args) {
		try {
			final StreamExecutionEnvironment env = Env.instance.getExecutionEnv();
			Util.setApplicationProperties(args);

			final String jsonStarterPath = Util.getApplicationProperties().get(Constant.STARTUP_DOCUMENT_PATH);
			Path path = new Path(jsonStarterPath);

			FileProcessingMode fileProcessingMode = FileProcessingMode.PROCESS_ONCE;
			long fileReadPollDuration = 0;
			Boolean isFilePollEnabled =
					Boolean.valueOf(Util.getApplicationProperties().get(Constant.STARTUP_DOCUMENT_POLL_CONTINOUS));

			if (isFilePollEnabled) {
				fileReadPollDuration =
						Long.parseLong(Util.getApplicationProperties().get(Constant.STARTUP_DOCUMENTS_POLL_DURATION));
				fileProcessingMode = FileProcessingMode.PROCESS_CONTINUOUSLY;
			}

			SingleOutputStreamOperator<List<StarterJsonDocument>> jsonDocumentStream =
					env.readFile(new UntilEOFTextInputFormat(path), jsonStarterPath, fileProcessingMode,
							fileReadPollDuration)
							.map(Util::acceptJsonStringAsDocument)
							.filter(Objects::nonNull);

			jsonDocumentStream.addSink(new FlinkDatabaseStartupJob.CouchbaseSink(env, args)).name("Datalake sink");
			env.execute("Database Starter");

		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * A custom sink that dumps the incoming records to couchbase
	 */
	private static class CouchbaseSink implements SinkFunction<List<StarterJsonDocument>> {

		private String[] args;

		public CouchbaseSink(StreamExecutionEnvironment env, String[] args) {
			this.args = args;
		}

		@Override
		public void invoke(List<StarterJsonDocument> starterJsonDocuments) throws Exception {
			cbManager = new CouchbaseManager();
			Util.setApplicationProperties(this.args);

			starterJsonDocuments.stream()
					.forEach(doc -> {
						final String docId = doc.getId();
						final JsonObject jsonObject = JsonObject.from(doc.getJsonMap());
						cbManager.doTransaction(docId, Constant.BUCKET_DATA, jsonObject, Constant.UPSERT);
					});
		}
	}

	/**
	 * A custom implementation of text input that delimits by EOF and not the default line feed
	 */
	private static class UntilEOFTextInputFormat extends TextInputFormat {

		public UntilEOFTextInputFormat(Path filePath) {
			super(filePath);
			super.setDelimiter("0xff");
		}

		@Override
		public String readRecord(String reusable, byte[] bytes, int offset, int numBytes) throws IOException {
			LOG.debug("Scheduled read [Database starter]: " + Date.from(Instant.now()));
			return new String(bytes, offset, numBytes);
		}
	}
}
