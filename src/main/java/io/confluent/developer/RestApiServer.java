package io.confluent.developer;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpExchange;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

public class RestApiServer {
    private final KafkaStreams streams;
    private final Configuration config;
    private final int port;
    private HttpServer server;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // Add this as a static final field
    private static final com.google.protobuf.util.JsonFormat.Printer PROTO_PRINTER = com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields();

    private static final Logger logger = LoggerFactory.getLogger(RestApiServer.class);

    public RestApiServer(KafkaStreams streams, Configuration config, int port) {
        this.streams = streams;
        this.config = config;
        this.port = port;
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
            ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

            ArrayNode jsonArray = objectMapper.createArrayNode();

            keyValueStore.all().forEachRemaining(entry -> {
                try {
                    logger.debug("Found Key: " +  (String)entry.key);
                    JsonNode jsonNode = serializeValue(entry.value, methodConfig.getKafka().getSerializer().getValue());
                    jsonArray.add(jsonNode);
                } catch (IOException e) {
                    logger.error("Error serializing value: " + entry.value, e);
                }
            });

            return objectMapper.writeValueAsString(jsonArray);
        }

        private JsonNode serializeValue(Object value, String serializerType) throws IOException {
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
                    GenericRecord avroRecord = (GenericRecord) value;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(avroRecord.getSchema(), baos);
                    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(avroRecord.getSchema());
                    writer.write(avroRecord, jsonEncoder);
                    jsonEncoder.flush();
                    return objectMapper.readTree(baos.toString());
                case "protobuf":
                    DynamicMessage protoMessage = (DynamicMessage) value;
                    String jsonString = PROTO_PRINTER.print(protoMessage);
                    return objectMapper.readTree(jsonString);
                default:
                    throw new IllegalArgumentException("Unsupported serializer type: " + serializerType);
            }
        }

        private String getValue(String id, Configuration.MethodConfig methodConfig) throws IOException {
            String storeName = methodConfig.getKafka().getTopic() + "-store";
            ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            Object value = keyValueStore.get(id);
            if (value != null) {
                JsonNode jsonNode = serializeValue(value, methodConfig.getKafka().getSerializer().getValue());
                return objectMapper.writeValueAsString(jsonNode);
            }
            return null;
        }
    }
}