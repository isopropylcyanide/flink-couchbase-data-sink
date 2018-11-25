package com.aman.flink.utility;

import com.aman.flink.Env;
import com.aman.flink.constants.Constant;
import com.aman.flink.model.StarterJsonDocument;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.BitField;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Util {

	private static final Logger LOG = LoggerFactory.getLogger(Util.class);

	private static ParameterTool parameter = null;

	private static BitField parameterSetBitField = new BitField(0);

	/**
	 * sets application properties to global env properties
	 */
	public static void setApplicationProperties(String... args) {
		if (null == parameter) {
			synchronized (parameterSetBitField) {
				if (null == parameter) {
					ParameterTool params = ParameterTool.fromArgs(args);
					try {
						String filePath = params.get(Constant.CONFIG_PROP_PARAM_NAME);
						parameter = ParameterTool.fromPropertiesFile(filePath);
					} catch (IOException e) {
						LOG.warn(e.getMessage());
					}

				}
				Env.instance.getExecutionEnv().getConfig().setGlobalJobParameters(parameter);
			}
		}
	}

	/**
	 * gets application properties from global env properties
	 */
	public static ParameterTool getApplicationProperties() {
		return (ParameterTool) Env.instance.getExecutionEnv().getConfig().getGlobalJobParameters();
	}


	/**
	 * Get @StarterJsonDocument list type reference for json deserialization
	 */
	public static TypeReference<List<StarterJsonDocument>> getStarterJsonDocumentTypeReference() {
		return new TypeReference<List<StarterJsonDocument>>() {
		};
	}

	/**
	 * Try deserializing json doc list string to list of @StarterJsonDocument if possible
	 */
	public static List<StarterJsonDocument> acceptJsonStringAsDocument(String jsonDocString) {
		try {
			List<StarterJsonDocument> jsonDocuments = new ObjectMapper().readValue(jsonDocString,
					Util.getStarterJsonDocumentTypeReference());
			return jsonDocuments;
		} catch (Exception ex) {
			LOG.warn(ex.getMessage());
			return null;
		}
	}
}
