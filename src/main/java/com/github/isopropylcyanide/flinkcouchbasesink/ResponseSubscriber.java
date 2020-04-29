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

import com.couchbase.client.java.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

public class ResponseSubscriber extends Subscriber<Document> {

	private static final Logger LOG = LoggerFactory.getLogger(ResponseSubscriber.class);

	@Override
	public void onCompleted() {
		LOG.info("Successfully completed observable");
	}

	@Override
	public void onError(Throwable throwable) {
		LOG.error("Error occurred from observable ", throwable);
	}

	@Override
	public void onNext(Document document) {
		if (LOG.isInfoEnabled()) {
			LOG.info("Successfully processed document {}", document.id());
		}
	}
}
