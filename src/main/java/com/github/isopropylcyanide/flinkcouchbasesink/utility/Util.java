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
package com.github.isopropylcyanide.flinkcouchbasesink.utility;

import com.github.isopropylcyanide.flinkcouchbasesink.Env;
import com.github.isopropylcyanide.flinkcouchbasesink.constants.Constant;
import com.github.isopropylcyanide.flinkcouchbasesink.model.StarterJsonDocument;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class Util {

	private static final Logger LOG = LoggerFactory.getLogger(Util.class);

	private static ParameterTool parameter = null;

	private Util() {

	}

	/**
	 * sets application properties to global env properties
	 */
	public static synchronized void setApplicationProperties(String... args) {
		if (null == parameter) {
			ParameterTool params = ParameterTool.fromArgs(args);
			try {
				String filePath = params.get(Constant.CONFIG_PROP_PARAM_NAME);
				parameter = ParameterTool.fromPropertiesFile(filePath);
			} catch (IOException e) {
				LOG.warn(e.getMessage());
			}
			Env.instance.getExecutionEnv().getConfig().setGlobalJobParameters(parameter);
		}
	}

	/**
	 * gets application properties from global env properties
	 */
	public static synchronized ParameterTool getApplicationProperties() {
		return (ParameterTool) Env.instance.getExecutionEnv().getConfig().getGlobalJobParameters();
	}

	/**
	 * Try deserializing json doc list string to list of @StarterJsonDocument if possible
	 */
	public static List<StarterJsonDocument> acceptJsonStringAsDocument(String jsonDocString) {
		try {
			return new ObjectMapper().readValue(jsonDocString,
					StarterJsonDocument.getStarterJsonDocumentTypeReference());
		} catch (Exception ex) {
			LOG.warn(ex.getMessage());
			return Collections.emptyList();
		}
	}
}
