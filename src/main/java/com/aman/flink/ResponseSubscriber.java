package com.aman.flink;

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
