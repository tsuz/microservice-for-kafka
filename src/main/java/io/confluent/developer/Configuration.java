package io.confluent.developer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import io.confluent.developer.Configuration.MethodConfig;

public class Configuration {
    private Properties kafkaConfig;
    private Map<String, PathConfig> paths;

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    public static class PathConfig {
        private String path;
        private List<ParameterConfig> parameters;
        private Map<String, MethodConfig> methods;

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        public List<ParameterConfig> getParameters() { return parameters; }
        public void setParameters(List<ParameterConfig> parameters) { this.parameters = parameters; }
        public Map<String, MethodConfig> getMethods() { return methods; }
        public void setMethods(Map<String, MethodConfig> methods) { this.methods = methods; }
    }

    public static class ParameterConfig {
        private String name;
        private String in;
        private String description;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getIn() { return in; }
        public void setIn(String in) { this.in = in; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
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
            if (kafkaConfigMap == null) {
                throw new IllegalArgumentException("`kafka` must be set in the top level");
            }
            config.kafkaConfig = new Properties();

            for (Map.Entry<String, Object> entry : kafkaConfigMap.entrySet()) {
                config.kafkaConfig.put(entry.getKey(), entry.getValue().toString());
            }

            Map<String, Object> pathsData = (Map<String, Object>) data.get("paths");
            if (pathsData == null) {
                throw new IllegalArgumentException("`path` must be set in the top level");
            }
            config.paths = new HashMap<>();
            for (Map.Entry<String, Object> entry : pathsData.entrySet()) {
                String path = entry.getKey();
                Map<String, Object> pathData = (Map<String, Object>) entry.getValue();
                PathConfig pathConfig = parsePathConfig(path, pathData);
                config.paths.put(path, pathConfig);
            }
            
            config.validate();
            return config;
        }
    }

    private void validate() throws IllegalArgumentException {
        // validatePathAndKafka();

        for (Map.Entry<String, PathConfig> entry : paths.entrySet()) {

        // for (PathConfig pathConfig : paths.values()) {
            String path = entry.getKey();
            PathConfig pathConfig = entry.getValue();

            validateParameters(pathConfig);
            for (Map.Entry<String, MethodConfig> methodEntry : pathConfig.getMethods().entrySet()) {
                String method = methodEntry.getKey();
                MethodConfig methodConfig = methodEntry.getValue();

                if (!method.equalsIgnoreCase("get")) {
                    throw new IllegalArgumentException("Only GET methods are supported. Found: " + method + " for path: " + pathConfig.getPath());
                }

                validateKafkaInPath(path, methodConfig);
                validateResponses(pathConfig, methodConfig);
                validateStateStoreQuery(pathConfig, methodConfig);
            }
        }
    }

    private void validateResponses(PathConfig pathConfig, MethodConfig methodConfig) {
        if (methodConfig.getResponses() == null || methodConfig.getResponses().isEmpty()) {
            throw new IllegalArgumentException("`response` must be defined under defined under the method for path: " + pathConfig.getPath());
        }

        for (Map.Entry<String, ResponseConfig> responseEntry : methodConfig.getResponses().entrySet()) {
            String statusCode = responseEntry.getKey();
            ResponseConfig responseConfig = responseEntry.getValue();

            if (!statusCode.equals("200")) {
                throw new IllegalArgumentException("Only 200 status code is supported. Found: " + statusCode + " for path: " + pathConfig.getPath());
            }

            if (responseConfig.getContent() == null || !responseConfig.getContent().containsKey("application/json")) {
                throw new IllegalArgumentException("Only application/json content type is supported for path: " + pathConfig.getPath());
            }

            if (responseConfig.getContent().size() != 1) {
                throw new IllegalArgumentException("Only application/json content type should be specified for path: " + pathConfig.getPath());
            }

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

    private void validateStateStoreQuery(PathConfig pathConfig, MethodConfig methodConfig) {
 
        if (methodConfig.getKafka() == null) {
            throw new IllegalArgumentException("`kafka` configuration is missing for path: " + pathConfig.getPath());
        }
        
        QueryConfig queryConfig = methodConfig.getKafka().getQuery();
        if (queryConfig == null) {
            throw new IllegalArgumentException("`query` is missing in Kafka config for path: " + methodConfig.getKafka());
        }
        String queryMethod = queryConfig.getMethod();
        // If the query method is 'all', we don't need a key
        if ("all".equalsIgnoreCase(queryMethod)) {
            return;
        }
    
        String queryKey = queryConfig.getKey();

        if (queryKey == null) {
            throw new IllegalArgumentException("Query key is missing in Kafka config for path: " + queryConfig);
        }
        
        logger.info("queryKey: " + queryKey);
        if (queryKey.contains("${parameters.")) {
            String paramName = queryKey.substring(queryKey.indexOf("${parameters.") + 13, queryKey.indexOf("}"));
            boolean found = false;

            for (ParameterConfig param : pathConfig.getParameters()) {
                if (param.getName().equals(paramName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Parameter " + paramName + " used in `kafka.query.key` is not defined in path parameters for path: " + pathConfig.getPath());
            }
        }
    }

    private void validateParameters(PathConfig pathConfig) {
        Set<String> pathParameters = new HashSet<>();
        for (String part : pathConfig.getPath().split("/")) {
            if (part.startsWith("{") && part.endsWith("}")) {
                pathParameters.add(part.substring(1, part.length() - 1));
            }
        }

        Set<String> definedParameters = new HashSet<>();

        // not all paths have a varying path parameter
        if (pathConfig.getParameters() == null) {
            return;
        }

        for (ParameterConfig param : pathConfig.getParameters()) {
            if (!"path".equals(param.getIn())) {
                throw new IllegalArgumentException("Only 'path' parameters are supported for `parameters.in`. Found: '" + param.getIn() + "'");
            }
            definedParameters.add(param.getName());
        }

        System.out.println("pathParameters " + pathParameters + ", definedParameters:"+definedParameters);
        if (!pathParameters.equals(definedParameters)) {
            throw new IllegalArgumentException("Mismatch between parameters in the URL path: " + pathParameters + " and configuration defined under `parameters`: " + definedParameters);
        }
    }

    private void validateKafkaInPath(String path, MethodConfig methodConfig) {
        KafkaConfig kafka = methodConfig.getKafka();
        if (kafka == null) {
            throw new IllegalArgumentException("`kafka` configuration is missing for path: " + path);
        }
    
        // Check kafka.topic
        if (kafka.getTopic() == null || kafka.getTopic().isEmpty()) {
            throw new IllegalArgumentException("`kafka.topic` must be set for path: " + path);
        }
    
        // Check kafka.query
        QueryConfig query = kafka.getQuery();
    
        // Check kafka.query.method
        String queryMethod = query.getMethod();
        if (queryMethod == null || (!queryMethod.equals("get") && !queryMethod.equals("all"))) {
            throw new IllegalArgumentException("`kafka.query.method` must be either 'get' or 'all' for path: " + path);
        }
    
        // Check kafka.serializer
        SerializerConfig serializer = kafka.getSerializer();
        // Check kafka.serializer.key
        String keySerializer = serializer.getKey();
        if (keySerializer == null || (!keySerializer.equals("string") && !keySerializer.equals("avro"))) {
            throw new IllegalArgumentException("`kafka.serializer.key` must be set and is one of 'string' or 'avro' for path: " + path);
        }
    
        // Check kafka.serializer.value
        String valueSerializer = serializer.getValue();
        if (valueSerializer == null || (!valueSerializer.equals("string") && 
            !valueSerializer.equals("avro") && 
            !valueSerializer.equals("protobuf")) &&
            !valueSerializer.equals("jsonsr")) {
            throw new IllegalArgumentException(
                "`kafka.serializer.value` must be set and is one of 'string', 'avro', 'protobuf', or 'jsonsr' for path: " + path);
        }
    }

    private static PathConfig parsePathConfig(String path, Map<String, Object> pathData) {
        PathConfig pathConfig = new PathConfig();
        pathConfig.setPath(path);
        pathConfig.setParameters(new ArrayList<>()); // Initialize with an empty list
        
        if (pathData.containsKey("parameters")) {
            List<Map<String, Object>> parametersList = (List<Map<String, Object>>) pathData.get("parameters");
            for (Map<String, Object> paramData : parametersList) {
                ParameterConfig param = new ParameterConfig();
                param.setName((String) paramData.get("name"));
                param.setIn((String) paramData.get("in"));
                param.setDescription((String) paramData.get("description"));
                pathConfig.getParameters().add(param);
            }
        }
        
        pathConfig.setMethods(parseMethodConfigs((Map<String, Object>) pathData));
        return pathConfig;
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
            Map<String, Object> kafka = (Map<String, Object>) methodData.get("kafka");
            if (kafka != null) {
                methodConfig.setKafka(parseKafkaConfig(kafka));
            }
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
        if (queryData != null) {
            queryConfig.setMethod((String) queryData.get("method"));
            queryConfig.setKey((String) queryData.get("key"));
        }
        return queryConfig;
    }

    private static SerializerConfig parseSerializerConfig(Map<String, Object> serializerData) {
        SerializerConfig serializerConfig = new SerializerConfig();
        if (serializerData != null) {
            serializerConfig.setKey((String) serializerData.get("key"));
            serializerConfig.setValue((String) serializerData.get("value"));
        }
        return serializerConfig;
    }

    private static Map<String, ResponseConfig> parseResponseConfigs(Map<String, Object> responsesData) {
        Map<String, ResponseConfig> responseConfigs = new HashMap<>();
        if (responsesData == null) {
            return responseConfigs;
        }
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