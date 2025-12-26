package io.confluent.developer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.confluent.developer.Configuration.AvroConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.developer.Configuration.MethodConfig;

import static io.confluent.developer.AvroJsonConverter.toJsonNode;
import static io.confluent.developer.AvroJsonConverter.toJsonNodeWithTypes;

public class RestApiServer {
    private final KafkaStreams streams;
    private final Configuration config;
    private final int port;
    private HttpServer server;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // Add this as a static final field
    private static final com.google.protobuf.util.JsonFormat.Printer PROTO_PRINTER = com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields();

    private static final Logger logger = LoggerFactory.getLogger(RestApiServer.class);

    // Schema Registry client
    private final SchemaRegistryClient schemaRegistryClient;

    public RestApiServer(KafkaStreams streams, Configuration config, int port) {
        this.streams = streams;
        this.config = config;
        this.port = port;
        
            // Get the Schema Registry config (reuse existing method!)
        Map<String, ?> srConfig = KafkaStreamsApplication.buildSchemaRegistryConfigMap(
            config.getKafkaConfig()
        );
        
        String schemaRegistryUrl = (String) srConfig.get("schema.registry.url");
        
        // Create the client
        this.schemaRegistryClient = new CachedSchemaRegistryClient(
            schemaRegistryUrl,
            1000,  // cache size (number of schemas to cache)
            srConfig
        );
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        
        // Create a single handler for all paths
        server.createContext("/", new AllPathsHandler(config.getPaths()));
        
        server.setExecutor(null);
        server.start();
        logger.info("Server is listening on port {}", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private class AllPathsHandler implements HttpHandler {
        private final List<PathHandler> pathHandlers;
    
        public AllPathsHandler(Map<String, Configuration.PathConfig> paths) {
            this.pathHandlers = new ArrayList<>();
            for (Map.Entry<String, Configuration.PathConfig> entry : paths.entrySet()) {
                pathHandlers.add(new PathHandler(entry.getKey(), entry.getValue()));
            }
            logger.info("Registered {} path handlers", pathHandlers.size());
        }
    
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            logger.info("Incoming path: {}", path);
    
            for (PathHandler handler : pathHandlers) {
                if (handler.matches(path)) {
                    logger.info("Path '{}' matched handler for '{}'", path, handler.getDefinedPath());
                    handler.handle(exchange);
                    return;
                }
            }
    
            // If no handler matched, send 404
            logger.info("No handler matched for path '{}'", path);
            sendResponse(exchange, "Not Found", 404);
        }

        private void sendResponse(HttpExchange exchange, String response, int statusCode) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private class PathHandler {
        private final String definedPath;
        private final Configuration.PathConfig pathConfig;
        private final Pattern pathPattern;
    
        public PathHandler(String definedPath, Configuration.PathConfig pathConfig) {
            this.definedPath = definedPath;
            this.pathConfig = pathConfig;
            this.pathPattern = createPathPattern(definedPath);
            logger.info("Created handler for path: {}, pattern: {}", definedPath, pathPattern);
        }
    
        private Pattern createPathPattern(String path) {
            String regexPath = path.replaceAll("\\{([^/]+)\\}", "(?<$1>[^/]+)");
            return Pattern.compile("^" + regexPath + "$");
        }
    
        public boolean matches(String path) {
            return pathPattern.matcher(path).matches();
        }
    
        public String getDefinedPath() {
            return definedPath;
        }
    
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                Matcher matcher = pathPattern.matcher(path);
                if (matcher.matches()) {
                    Map<String, String> pathParams = extractPathParameters(matcher);
                    Configuration.MethodConfig methodConfig = pathConfig.getMethods().get("get");
                    if (methodConfig == null) {
                        sendResponse(exchange, "Method not supported", 405);
                        return;
                    }
        
                    String queryMethod = methodConfig.getKafka().getQuery().getMethod();
                    String response;
                    int statusCode;
        
                    if ("all".equals(queryMethod)) {
                        response = getAllValues(methodConfig);
                        statusCode = 200;
                    } else if ("get".equals(queryMethod)) {
                        String key = resolveQueryKey(methodConfig.getKafka().getQuery().getKey(), pathParams);
                        response = getValue(key, methodConfig);
                        statusCode = (response != null) ? 200 : 404;
                    } else {
                        response = "Unsupported query method";
                        statusCode = 400;
                    }
        
                    sendResponse(exchange, response, statusCode);
                } else {
                    sendResponse(exchange, "Not Found", 404);
                }
            } catch (IOException e) {
                logger.error("Error serializing value: " + e.getMessage());
            }
        }
    
        private Map<String, String> extractPathParameters(Matcher matcher) {
            Map<String, String> params = new HashMap<>();
            for (int i = 1; i <= matcher.groupCount(); i++) {
                String paramName = pathPattern.pattern().split("\\(\\?<")[i].split(">")[0];
                params.put(paramName, matcher.group(i));
            }
            return params;
        }

        private String resolveQueryKey(String keyTemplate, Map<String, String> pathParams) {
            String resolvedKey = keyTemplate;
            for (Map.Entry<String, String> entry : pathParams.entrySet()) {
                String placeholder = "${parameters." + entry.getKey() + "}";
                resolvedKey = resolvedKey.replace(placeholder, entry.getValue());
            }
            logger.debug("Resolved query key: {}", resolvedKey);
            return resolvedKey;
        }

        private void sendResponse(HttpExchange exchange, String response, int statusCode) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }

        
        private String getAllValues(Configuration.MethodConfig methodConfig) throws IOException {
            String storeName = methodConfig.getKafka().getTopic() + "-store";
            logger.info("Starting getAllValues for store: {}", storeName);

            ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

            ArrayNode jsonArray = objectMapper.createArrayNode();
            
            try (var iterator = keyValueStore.all()) {
                logger.info("Starting iteration over keyValueStore");
                
                while (iterator.hasNext()) {
                    try {
                        var entry = iterator.next();
                        
                        try {
                            JsonNode item = processValue(entry, methodConfig);
                            jsonArray.add(item);
                        } catch (IOException e) {
                            logger.warn("Skipping entry IOException - {}", e.getMessage());
                        }
                        
                    } catch (org.apache.kafka.common.errors.SerializationException e) {
                        // Schema deserialization failed - this record is corrupted/incompatible
                        logger.warn("Skipping record due to deserialization error: {}", e.getMessage());
                        // The iterator has moved past this record, continue to next
                        break;
                    } catch (Exception e) {
                        // Unexpected error - log and try to continue
                        logger.error("Unexpected error during iteration: {}", e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("Fatal exception during iteration: {}", e.getMessage(), e);
            }
            
            String result = objectMapper.writeValueAsString(jsonArray);
            return result;
        }

        private JsonNode processValue(KeyValue<Object, Object> entry, MethodConfig methodConfig) throws IOException {
            boolean mergeKey = methodConfig.getKafka().isMergeKey();
            String valueSerializer = methodConfig.getKafka().getSerializer().getValue();
            JsonNode valueNode = serialize(entry.value, valueSerializer, methodConfig);

            if (!mergeKey) {
                return valueNode;
            }

            String keySerializer = methodConfig.getKafka().getSerializer().getKey();
            JsonNode keyNode = serialize(entry.key, keySerializer, methodConfig);
            JsonNode mergedNode = mergeKeyIntoValue(keyNode, valueNode);

            return mergedNode;
        }

        private JsonNode mergeKeyIntoValue(JsonNode keyNode, JsonNode valueNode) {
            // If value is not an object, return it as-is
            if (!valueNode.isObject()) {
                return valueNode;
            }

            // If key is not an object, return value as-is
            if (!keyNode.isObject()) {
                logger.warn("merge failed");
                return valueNode;
            }

            // Merge key fields into value
            com.fasterxml.jackson.databind.node.ObjectNode result = objectMapper.createObjectNode();
            
            // First add all key fields
            keyNode.properties().forEach(entry -> {
                result.set(entry.getKey(), entry.getValue());
            });
            
            // Then add all value fields (will override key fields if same name exists)
            valueNode.properties().forEach(entry -> {
                result.set(entry.getKey(), entry.getValue());
            });
            
            return result;
        }

        private JsonNode serialize(Object value, String serializerType, Configuration.MethodConfig methodConfig) throws IOException {
            switch (serializerType.toLowerCase()) {
                case "string":
                    return objectMapper.readTree((String) value);
                case "long":
                case "integer":
                case "int":
                case "double":
                case "float":
                    return objectMapper.valueToTree(value);
                case "byte":
                case "bytes":
                    return objectMapper.valueToTree(new String((byte[]) value, StandardCharsets.UTF_8));
                case "avro":
                    return handleAvro((GenericRecord) value, methodConfig.getKafka().getSerializer().getAvro());
                case "protobuf":
                    DynamicMessage protoMessage = (DynamicMessage) value;
                    String jsonString = PROTO_PRINTER.print(protoMessage);
                    return objectMapper.readTree(jsonString);
                case "jsonsr":
                    if (value instanceof JsonNode) {
                        return (JsonNode) value;
                    } else if (value instanceof String) {
                        return objectMapper.readTree((String) value);
                    } else {
                        return objectMapper.valueToTree(value);
                    }
                default:
                    throw new IllegalArgumentException("Unsupported serializer type: " + serializerType);
            }
        }

        private JsonNode handleAvro(Object value, AvroConfig config) throws IOException {
                        GenericRecord avroRecord = (GenericRecord) value;
            // Check if includeType is set
            boolean includeType = false;
            if (config != null) {
                includeType = config.isIncludeType();
            }
            
            if (includeType) {
                return AvroJsonConverter.toJsonNodeWithTypes(avroRecord);
            } else {
                return AvroJsonConverter.toJsonNode(avroRecord);
            }
        }

        private String getValue(String id, Configuration.MethodConfig methodConfig) throws IOException {
            String storeName = methodConfig.getKafka().getTopic() + "-store";
            
            ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

            String keySerializer = methodConfig.getKafka().getSerializer().getKey();

            Object key = id;
            
            // build a key for avro
            if ("avro".equalsIgnoreCase(keySerializer)) {
                String keyField = "productId"; // methodConfig.getKafka().getSerializer().getKeyField();
                String topic = methodConfig.getKafka().getTopic();
                
                try {
                    key = AvroKeyBuilder.buildKey(
                        schemaRegistryClient,
                        topic, 
                        keyField, 
                        id
                    );
                    
                    logger.info("Built Avro key: {}", key);
                } catch (Exception e) {
                    logger.error("Failed to build Avro key", e);
                    throw new IOException("Failed to build Avro key: " + e.getMessage(), e);
                }
            }

            Object value = keyValueStore.get(key);
            if (value != null) {
                // This ensures consistent mergeKey behavior between getAllValues() and getValue()
                KeyValue<Object, Object> entry = new KeyValue<>(key, value);
                JsonNode resultNode = processValue(entry, methodConfig);
                
                return objectMapper.writeValueAsString(resultNode);
            }
            return null;
        }
    }
}