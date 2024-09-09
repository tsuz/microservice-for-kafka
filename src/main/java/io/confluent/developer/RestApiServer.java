package io.confluent.developer;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RestApiServer {
    private final KafkaStreams streams;
    private final Configuration config;
    private final int port;
    private HttpServer server;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(RestApiServer.class);

    public RestApiServer(KafkaStreams streams, Configuration config, int port) {
        this.streams = streams;
        this.config = config;
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        for (Configuration.EndpointConfig endpoint : config.getEndpoints()) {
            server.createContext(endpoint.getEndpointPath(), new DynamicHandler(endpoint));
        }
        server.setExecutor(null); // creates a default executor
        server.start();
        System.out.println("Server is listening on port " + port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private class DynamicHandler implements HttpHandler {
        private final Configuration.EndpointConfig endpoint;
        private final Pattern idPattern;

        public DynamicHandler(Configuration.EndpointConfig endpoint) {
            this.endpoint = endpoint;
            this.idPattern = Pattern.compile(endpoint.getEndpointPath().replace("{id}", "(.+)"));
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            logger.debug("path " + path);

            String response;
            int statusCode;

            if ("listAll".equals(endpoint.getAction())) {
                response = listAllValues();
                statusCode = 200;
            } else if ("get".equals(endpoint.getAction())) {
                Matcher matcher = idPattern.matcher(path);
                if (matcher.matches()) {
                    String id = matcher.group(1);
                    response = getValue(id);
                    statusCode = (response != null) ? 200 : 404;
                } else {
                    response = "Invalid ID";
                    statusCode = 400;
                }
            } else {
                response = "Unsupported action";
                statusCode = 400;
            }

            exchange.sendResponseHeaders(statusCode, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }

        private String listAllValues() throws IOException {
            ReadOnlyKeyValueStore<String, String> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(endpoint.getStoreName(), QueryableStoreTypes.keyValueStore()));
            
            List<JsonNode> jsonNodes = new ArrayList<>();
            keyValueStore.all().forEachRemaining(entry -> {
                try {
                    // Parse each value as a JSON node
                    JsonNode jsonNode = objectMapper.readTree(entry.value);
                    jsonNodes.add(jsonNode);
                } catch (JsonProcessingException e) {
                    // If the value is not valid JSON, you might want to skip it or handle the error
                    logger.error("Error parsing JSON value: " + entry.value, e);
                }
            });
            
            // Convert the list of JsonNodes to a JSON array
            return objectMapper.writeValueAsString(jsonNodes);
        }

        private String getValue(String id) {
            ReadOnlyKeyValueStore<String, String> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(endpoint.getStoreName(), QueryableStoreTypes.keyValueStore()));
            String value = keyValueStore.get(id);
            return (value != null) ? value : "Not found";
        }
    }
}