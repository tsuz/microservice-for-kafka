package io.confluent.developer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import org.yaml.snakeyaml.Yaml;

public class Configuration {
    private Properties kafkaConfig;
    private Map<String, PathConfig> paths;

    public static class PathConfig {
        private String path;
        private Map<String, MethodConfig> methods;

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        public Map<String, MethodConfig> getMethods() { return methods; }
        public void setMethods(Map<String, MethodConfig> methods) { this.methods = methods; }
    }

    public static class MethodConfig {
        private String description;
        private KafkaConfig kafka;
        private Map<String, ResponseConfig> responses;

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public KafkaConfig getKafka() { return kafka; }
        public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }
        public Map<String, ResponseConfig> getResponses() { return responses; }
        public void setResponses(Map<String, ResponseConfig> responses) { this.responses = responses; }
    }

    public static class KafkaConfig {
        private String topic;
        private QueryConfig query;
        private SerializerConfig serializer;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public QueryConfig getQuery() { return query; }
        public void setQuery(QueryConfig query) { this.query = query; }
        public SerializerConfig getSerializer() { return serializer; }
        public void setSerializer(SerializerConfig serializer) { this.serializer = serializer; }
    }

    public static class QueryConfig {
        private String method;
        private String key;

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
    }

    public static class SerializerConfig {
        private String key;
        private String value;

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    public static class ResponseConfig {
        private String description;
        private Map<String, Object> content;

        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public Map<String, Object> getContent() { return content; }
        public void setContent(Map<String, Object> content) { this.content = content; }
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
            
            Map<String, Object> pathsData = (Map<String, Object>) data.get("paths");
            config.paths = new HashMap<>();
            for (Map.Entry<String, Object> entry : pathsData.entrySet()) {
                PathConfig pathConfig = new PathConfig();
                pathConfig.setPath(entry.getKey());
                Map<String, Object> methodsData = (Map<String, Object>) entry.getValue();
                pathConfig.setMethods(parseMethodConfigs(methodsData));
                config.paths.put(entry.getKey(), pathConfig);
            }
            
            config.validate();
            return config;
        }
    }

    private void validate() throws IllegalArgumentException {
        for (PathConfig pathConfig : paths.values()) {
            for (Map.Entry<String, MethodConfig> methodEntry : pathConfig.getMethods().entrySet()) {
                String method = methodEntry.getKey();
                MethodConfig methodConfig = methodEntry.getValue();

                // Validate only GET methods are supported
                if (!method.equalsIgnoreCase("get")) {
                    throw new IllegalArgumentException("Only GET methods are supported. Found: " + method + " for path: " + pathConfig.getPath());
                }

                // Validate responses
                if (methodConfig.getResponses() == null || methodConfig.getResponses().isEmpty()) {
                    throw new IllegalArgumentException("No responses defined for path: " + pathConfig.getPath());
                }

                for (Map.Entry<String, ResponseConfig> responseEntry : methodConfig.getResponses().entrySet()) {
                    String statusCode = responseEntry.getKey();
                    ResponseConfig responseConfig = responseEntry.getValue();

                    // Validate only 200 status code is supported
                    if (!statusCode.equals("200")) {
                        throw new IllegalArgumentException("Only 200 status code is supported. Found: " + statusCode + " for path: " + pathConfig.getPath());
                    }

                    // Validate content type
                    if (responseConfig.getContent() == null || !responseConfig.getContent().containsKey("application/json")) {
                        throw new IllegalArgumentException("Only application/json content type is supported for path: " + pathConfig.getPath());
                    }

                    // Validate that only application/json is present
                    if (responseConfig.getContent().size() != 1) {
                        throw new IllegalArgumentException("Only application/json content type should be specified for path: " + pathConfig.getPath());
                    }

                    // Validate schema type
                    Object contentObject = responseConfig.getContent().get("application/json");
                    if (!(contentObject instanceof Map)) {
                        throw new IllegalArgumentException("Invalid content structure for application/json in path: " + pathConfig.getPath());
                    }
                    
                    Map<String, Object> contentMap = (Map<String, Object>) contentObject;
                    Object schemaObject = contentMap.get("schema");
                    if (!(schemaObject instanceof Map)) {
                        throw new IllegalArgumentException("Invalid schema structure for application/json in path: " + pathConfig.getPath());
                    }
                    
                    Map<String, Object> schema = (Map<String, Object>) schemaObject;
                    String schemaType = (String) schema.get("type");
                    if (!schemaType.equals("object") && !schemaType.equals("array")) {
                        throw new IllegalArgumentException("Only object or array schema types are supported. Found: " + schemaType + " for path: " + pathConfig.getPath());
                    }
                }
            }
        }
    }

    
    private static Map<String, MethodConfig> parseMethodConfigs(Map<String, Object> methodsData) {
        Map<String, MethodConfig> methodConfigs = new HashMap<>();
        for (Map.Entry<String, Object> entry : methodsData.entrySet()) {
            if (entry.getKey().equals("parameters")) {
                continue; // Skip parameters for now
            }
            MethodConfig methodConfig = new MethodConfig();
            Map<String, Object> methodData = (Map<String, Object>) entry.getValue();
            methodConfig.setDescription((String) methodData.get("description"));
            methodConfig.setKafka(parseKafkaConfig((Map<String, Object>) methodData.get("kafka")));
            methodConfig.setResponses(parseResponseConfigs((Map<String, Object>) methodData.get("responses")));
            methodConfigs.put(entry.getKey(), methodConfig);
        }
        return methodConfigs;
    }


    private static KafkaConfig parseKafkaConfig(Map<String, Object> kafkaData) {
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setTopic((String) kafkaData.get("topic"));
        kafkaConfig.setQuery(parseQueryConfig((Map<String, Object>) kafkaData.get("query")));
        kafkaConfig.setSerializer(parseSerializerConfig((Map<String, Object>) kafkaData.get("serializer")));
        return kafkaConfig;
    }

    private static QueryConfig parseQueryConfig(Map<String, Object> queryData) {
        QueryConfig queryConfig = new QueryConfig();
        queryConfig.setMethod((String) queryData.get("method"));
        queryConfig.setKey((String) queryData.get("key"));
        return queryConfig;
    }

    private static SerializerConfig parseSerializerConfig(Map<String, Object> serializerData) {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setKey((String) serializerData.get("key"));
        serializerConfig.setValue((String) serializerData.get("value"));
        return serializerConfig;
    }

    private static Map<String, ResponseConfig> parseResponseConfigs(Map<String, Object> responsesData) {
        Map<String, ResponseConfig> responseConfigs = new HashMap<>();
        for (Map.Entry<String, Object> entry : responsesData.entrySet()) {
            ResponseConfig responseConfig = new ResponseConfig();
            Map<String, Object> responseData = (Map<String, Object>) entry.getValue();
            responseConfig.setDescription((String) responseData.get("description"));
            responseConfig.setContent((Map<String, Object>) responseData.get("content"));
            responseConfigs.put(entry.getKey(), responseConfig);
        }
        return responseConfigs;
    }

    public Properties getKafkaConfig() { return kafkaConfig; }
    public Map<String, PathConfig> getPaths() { return paths; }
}