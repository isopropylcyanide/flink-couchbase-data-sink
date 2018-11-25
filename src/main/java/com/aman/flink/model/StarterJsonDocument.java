package com.aman.flink.model;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StarterJsonDocument {

	private String id;

	private JsonNode content;

	private Map<String, Object> jsonMap;

	public StarterJsonDocument() {
	}

	/**
	 * Initialize a @Map String, object from the json content
	 */
	private void initializeMap() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			this.setJsonMap(mapper.convertValue(this.content, Map.class));
		} catch (Exception e) {
			this.setJsonMap(new HashMap<>());
		}
	}

	public String getId() {
		return id;
	}

	@JsonProperty(value = "id")
	public void setId(String id) {
		this.id = id;
	}

	public JsonNode getContent() {
		return content;
	}

	@JsonProperty(value = "content")
	public void setContent(JsonNode content) {
		this.content = content;
		this.initializeMap();
	}

	public Map<String, Object> getJsonMap() {
		return jsonMap;
	}

	private void setJsonMap(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
	}

	/**
	 * Get @StarterJsonDocument list type reference for json deserialization
	 */
	public static TypeReference<List<StarterJsonDocument>> getStarterJsonDocumentTypeReference() {
		return new TypeReference<List<StarterJsonDocument>>() {
		};
	}
}
