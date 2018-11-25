package com.aman.flink;

import com.aman.flink.model.StarterJsonDocument;
import com.aman.flink.utility.Util;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class FlinkDatabaseStartupJob {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDatabaseStartupJob.class);
	private static SynchronousDBUtil dbUtil = null;

	public static void main(String[] args) {
		try {
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			Util.setApplicationProperties(args);

			final String jsonStarterPath = Util.getApplicationProperties().get(PruConstants.STARTUP_DOCUMENT_PATH);
			Path path = new Path(jsonStarterPath);

			FileProcessingMode fileProcessingMode = FileProcessingMode.PROCESS_ONCE;
			long fileReadPollDuration = 0;
			Boolean isFilePollEnabled =
					Boolean.valueOf(Util.getApplicationProperties().get(PruConstants.STARTUP_DOCUMENT_POLL_CONTINOUS));

			if (isFilePollEnabled) {
				fileReadPollDuration =
						Long.parseLong(Util.getApplicationProperties().get(PruConstants.STARTUP_DOCUMENTS_POLL_DURATION));
				fileProcessingMode = FileProcessingMode.PROCESS_CONTINUOUSLY;
			}

			SingleOutputStreamOperator<List<StarterJsonDocument>> jsonDocumentStream =
					env.readFile(new UntilEOFTextInputFormat(path), jsonStarterPath, fileProcessingMode,
							fileReadPollDuration)
							.map(Util::acceptJsonStringAsDocument)
							.filter(Objects::nonNull);

			jsonDocumentStream.addSink(new DatalakeStartupJob.CouchbaseSink(args)).name("Datalake sink");
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

		public CouchbaseSink(String[] args) {
			this.args = args;
		}

		@Override
		public void invoke(List<StarterJsonDocument> value, Context context) {
			dbUtil = new SynchronousDBUtil();
			Util.setApplicationProperties(this.args);

			value.stream()
					.forEach(doc -> {
						final String docId = doc.getId();
						final JsonObject jsonObject = JsonObject.from(doc.getJsonMap());
						dbUtil.doTransaction(docId, PruConstants.BUCKET_CUSTOMER_REGISTRATION, jsonObject
								, PruConstants.UPSERT);
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
