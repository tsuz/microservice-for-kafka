package io.confluent.developer;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
                response = getAllValues();
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

        private String getAllValues() throws IOException {
            ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(endpoint.getStoreName(), QueryableStoreTypes.keyValueStore()));

            ArrayNode jsonArray = objectMapper.createArrayNode();

            keyValueStore.all().forEachRemaining(entry -> {
                try {
                    JsonNode jsonNode = serializeValue(entry.value, endpoint.getValueSerializer());
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
                default:
                    throw new IllegalArgumentException("Unsupported serializer type: " + serializerType);
            }
        }

        private String getValue(String id) {
            ReadOnlyKeyValueStore<String, String> keyValueStore =
                    streams.store(StoreQueryParameters.fromNameAndType(endpoint.getStoreName(), QueryableStoreTypes.keyValueStore()));
            String value = keyValueStore.get(id);
            return (value != null) ? value : "Not found";
        }
    }
}