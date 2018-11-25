package com.aman.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Env {

	public static Env instance = new Env();

	private StreamExecutionEnvironment streamingEnv;

	private Env() {
		initializeStreamingEnvironment();
	}

	public StreamExecutionEnvironment getExecutionEnv() {
		return streamingEnv;
	}

	/**
	 * Initialize the streaming env
	 */
	private void initializeStreamingEnvironment() {
		this.streamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		this.streamingEnv.setBufferTimeout(0);
	}

	/**
	 * Execute the job with the given jobname through the streaming env
	 */
	public void execute(String jobName) throws Exception {
		this.streamingEnv.execute(jobName);
	}

}
