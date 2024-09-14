package io.confluent.developer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.yaml.snakeyaml.Yaml;

public class Configuration {
    private Properties kafkaConfig;
    private List<EndpointConfig> endpoints;

    public static class EndpointConfig {
        private String name;
        private String action;
        private String datastore;
        private String description;
        private String topic;
        private String keySerializer;
        private String valueSerializer;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getAction() { return action; }
        public void setAction(String action) { this.action = action; }
        public String getDatastore() { return datastore; }
        public void setDatastore(String datastore) { this.datastore = datastore; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getKeySerializer() { return keySerializer; }
        public void setKeySerializer(String keySerializer) { this.keySerializer = keySerializer; }
        public String getValueSerializer() { return valueSerializer; }
        public void setValueSerializer(String valueSerializer) { this.valueSerializer = valueSerializer; }

        public String getStoreName() {
            return name + "-" + datastore + "-store";
        }

        public String getEndpointPath() {
            return "/" + name + (action.equals("get") ? "/{id}" : "");
        }
    }

    public static Configuration fromFile(String filename) throws Exception {
        try (InputStream input = new FileInputStream(filename)) {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(input);
            
            Configuration config = new Configuration();
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) data.get("kafka");
            config.kafkaConfig = new Properties();
            for (Map.Entry<String, Object> entry : kafkaConfigMap.entrySet()) {
                config.kafkaConfig.put(entry.getKey(), entry.getValue().toString());
            }
            
            List<Map<String, Object>> endpointsData = (List<Map<String, Object>>) data.get("endpoints");
            config.endpoints = new ArrayList<>();
            for (Map<String, Object> endpointData : endpointsData) {
                EndpointConfig endpoint = new EndpointConfig();
                endpoint.setName((String) endpointData.get("name"));
                endpoint.setAction((String) endpointData.get("action"));
                endpoint.setDatastore((String) endpointData.get("datastore"));
                endpoint.setDescription((String) endpointData.get("description"));
                endpoint.setTopic((String) endpointData.get("topic"));
                endpoint.setKeySerializer((String) endpointData.get("keySerializer"));
                endpoint.setValueSerializer((String) endpointData.get("valueSerializer"));
                config.endpoints.add(endpoint);
            }
            
            return config;
        }
    }

    public Properties getKafkaConfig() { return kafkaConfig; }
    public List<EndpointConfig> getEndpoints() { return endpoints; }
}