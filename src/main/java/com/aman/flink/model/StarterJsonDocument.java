package com.prudential.prutopia.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
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
	private void initializeMap(JsonNode content) {
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
		this.initializeMap(content);
	}

	public Map<String, Object> getJsonMap() {
		return jsonMap;
	}

	public void setJsonMap(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
	}
}
