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

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
@JsonIgnoreProperties(ignoreUnknown = true)
public class SinkJsonDocument {

    private String id;
    private JsonNode content;
    private Map<String, Object> jsonMap;

    public SinkJsonDocument() {
    }

    private void initializeMap() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.setJsonMap(mapper.convertValue(this.content, Map.class));

        } catch (Exception e) {
            this.setJsonMap(new HashMap<>());
        }
    }

    String getId() {
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

    Map<String, Object> getJsonMap() {
        return jsonMap;
    }

    private void setJsonMap(Map<String, Object> jsonMap) {
        this.jsonMap = jsonMap;
    }

    static TypeReference<List<SinkJsonDocument>> getStarterJsonDocumentTypeReference() {
        return new TypeReference<List<SinkJsonDocument>>() {
        };
    }
}
