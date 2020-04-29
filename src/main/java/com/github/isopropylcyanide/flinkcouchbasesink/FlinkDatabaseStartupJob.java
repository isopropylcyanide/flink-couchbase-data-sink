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

import com.couchbase.client.java.document.json.JsonObject;
import com.github.isopropylcyanide.flinkcouchbasesink.model.StarterJsonDocument;
import com.github.isopropylcyanide.flinkcouchbasesink.utility.CouchbaseManager;
import com.github.isopropylcyanide.flinkcouchbasesink.utility.Util;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class FlinkDatabaseStartupJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkDatabaseStartupJob.class);

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment env = Env.instance.getExecutionEnv();
            Util.setApplicationProperties(args);

            final String jsonStarterPath = Util.getApplicationProperties().get(Constant.STARTUP_DOCUMENT_PATH);
            Path path = new Path(jsonStarterPath);

            FileProcessingMode fileProcessingMode = FileProcessingMode.PROCESS_ONCE;
            long fileReadPollDuration = 0;
            Boolean isFilePollEnabled =
                    Boolean.valueOf(Util.getApplicationProperties().get(Constant.STARTUP_DOCUMENT_POLL_CONTINUOUS));

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

            jsonDocumentStream.addSink(new FlinkDatabaseStartupJob.CouchbaseSink(args)).name("Couchbase sink");
            env.execute("Database Starter");

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * A custom sink that dumps the incoming records to couchbase
     */
    private static class CouchbaseSink implements SinkFunction<List<StarterJsonDocument>> {

        private String[] args;

        private CouchbaseSink(String[] args) {
            this.args = args;
        }

        @Override
        public void invoke(List<StarterJsonDocument> starterJsonDocuments, Context context) {
            CouchbaseManager cbManager = new CouchbaseManager();
            Util.setApplicationProperties(this.args);

            starterJsonDocuments
                    .forEach(doc -> {
                        final String docId = doc.getId();
                        final JsonObject jsonObject = JsonObject.from(doc.getJsonMap());
                        cbManager
                                .upsertDocument(docId, jsonObject)
                                .subscribe(new ResponseSubscriber());
                    });
        }
    }

    /**
     * A custom implementation of text input that delimits by EOF and not the default line feed
     */
    private static class UntilEOFTextInputFormat extends TextInputFormat {

        private UntilEOFTextInputFormat(Path filePath) {
            super(filePath);
            super.setDelimiter("0xff");
        }

        @Override
        public String readRecord(String reusable, byte[] bytes, int offset, int numBytes) {
            log.info("Scheduled read [Database starter]: {} ", Date.from(Instant.now()));
            return new String(bytes, offset, numBytes);
        }
    }
}
