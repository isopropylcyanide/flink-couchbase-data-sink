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

public class Constant {

    private Constant() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

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
    public static final String STARTUP_DOCUMENT_POLL_CONTINUOUS = "startup.documents.poll.continuous";

    /**
     * Duration (in ms) after which the file would be polled for changes if polling is enabled
     */
    public static final String STARTUP_DOCUMENTS_POLL_DURATION = "startup.documents.poll.duration";
}
