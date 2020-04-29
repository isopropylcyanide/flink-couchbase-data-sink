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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Env {

	public static Env instance = new Env();

	private StreamExecutionEnvironment streamingEnv;

	private Env() {
		this.initializeStreamingEnvironment();
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
