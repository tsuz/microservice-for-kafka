package io.confluent.developer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.nio.file.Files;

public class ConfigurationTest {

    @Test
    public void testMissingPath(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("missing-path-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n"
        );
    
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
    
        assertTrue(exception.getMessage().contains("`path` must be set in the top level"));
    }
    
    @Test
    public void testMissingKafka(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("missing-kafka-config.yaml");
        Files.writeString(configPath, 
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      description: Return all flights\n" +
            "      kafka:\n" +
            "        topic: flight-location\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A list of flights\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );
    
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
    
        assertTrue(exception.getMessage().contains("`kafka` must be set in the top level"));
    }

    @Test
    public void testValidConfiguration(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("valid-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "  # Serializer\n" +
            "  key.serializer: org.apache.kafka.common.serialization.StringSerializer\n" +
            "  value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer\n" +
            "  \n" +
            "  # Schema Registry Properties\n" +
            "  schema.registry.url: http://localhost:8081\n" +
            "  basic.auth.credentials.source: USER_INFO\n" +
            "  basic.auth.user.info: username:password\n" +
            "\n" +
            "  metrics.recording.level: DEBUG\n" +
            "\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      description: Return all flights\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A list of flights\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Configuration config = Configuration.fromFile(configPath.toString());
        assertNotNull(config);
        assertEquals(2, config.getPaths().size());

        // Verify /flights path
        Configuration.PathConfig flightsPath = config.getPaths().get("/flights");
        assertNotNull(flightsPath);
        assertEquals("/flights", flightsPath.getPath());
        assertTrue(flightsPath.getParameters().isEmpty());
        assertEquals(1, flightsPath.getMethods().size());
        Configuration.MethodConfig flightsMethod = flightsPath.getMethods().get("get");
        assertNotNull(flightsMethod);
        assertEquals("all", flightsMethod.getKafka().getQuery().getMethod());
        assertNull(flightsMethod.getKafka().getQuery().getKey());

        // Verify /flights/{flightId} path
        Configuration.PathConfig flightPath = config.getPaths().get("/flights/{flightId}");
        assertNotNull(flightPath);
        assertEquals("/flights/{flightId}", flightPath.getPath());
        assertEquals(1, flightPath.getParameters().size());
        assertEquals("flightId", flightPath.getParameters().get(0).getName());
        assertEquals("path", flightPath.getParameters().get(0).getIn());
        assertEquals(1, flightPath.getMethods().size());
        Configuration.MethodConfig flightMethod = flightPath.getMethods().get("get");
        assertNotNull(flightMethod);
        assertEquals("get", flightMethod.getKafka().getQuery().getMethod());
        assertEquals("${parameters.flightId}", flightMethod.getKafka().getQuery().getKey());
    }

    @Test
    public void testInvalidHttpMethod(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-method-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    post:\n" +
            "      description: Create a flight\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: Flight created\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only GET methods are supported"));
    }

    @Test
    public void testInvalidParameterQueryType(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: query\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only 'path' parameters are supported for `parameters.in`. Found: 'query'"));
    }

    @Test
    public void testMismatchedPathParameters(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: wrongId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Mismatch between parameters in the URL path: [flightId] and configuration defined under `parameters`: [wrongId]"));
    }


    @Test
    public void testPathMethodKafkaEmpty(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka` configuration is missing for path: /flights/{flightId}"));
    }

    @Test
    public void testPathKafkaConfigTopicInvalid(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.topic` must be set for path: /flights/{flightId}"));
    }

    @Test
    public void testPathKafkaConfigQueryStateStoreMethodInvalid(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.query.method` must be one of 'get', 'all', 'range', or 'prefix' for path: /flights/{flightId}"));
    }

    @Test
    public void testPathKafkaConfigQueryKeyInvalid(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightIddd}\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("Parameter flightIddd used in `kafka.query.key` is not defined in path parameters for path: /flights/{flightId}"));
    }

    @Test
    public void testPathKafkaConfigSerializerKeyInvalid(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "        topic: my-avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.serializer.key` must be set and is one of 'string' or 'avro' for path: /flights/{flightId}"));
    }

    @Test
    public void testPathKafkaConfigSerializerValueInvalid(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("mismatched-parameter-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /flights/{flightId}:\n" +
            "    parameters:\n" +
            "    - name: flightId\n" +
            "      in: path\n" +
            "      description: the flight identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.flightId}\n" +
            "        topic: my-avro\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A single flight\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.serializer.value` must be set and is one of 'string', 'avro', 'protobuf', or 'jsonsr' for path: /flights/{flightId}"));
    }

    @Test
    public void testNoResponsesDefined(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("no-responses-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("`response` must be defined under defined under the method for path: /flights"));
    }

    @Test
    public void testInvalidStatusCode(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-status-code-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '404':\n" +
            "          description: Not found\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only 200 status code is supported. Found: 404 for path: /flights"));
    }

    @Test
    public void testInvalidContentType(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-content-type-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: OK\n" +
            "          content:\n" +
            "            text/plain:\n" +
            "              schema:\n" +
            "                type: string\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only application/json content type is supported for path: /flights"));
    }

    @Test
    public void testMultipleContentTypes(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("multiple-content-types-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: OK\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n" +
            "            text/plain:\n" +
            "              schema:\n" +
            "                type: string\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only application/json content type should be specified for path: /flights"));
    }

    @Test
    public void testInvalidContentStructure(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-content-structure-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: OK\n" +
            "          content:\n" +
            "            application/json: 'Invalid content'\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Invalid content structure for application/json in path: /flights"));
    }

    @Test
    public void testInvalidSchemaStructure(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-schema-structure-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: OK\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema: 'Invalid schema'\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Invalid schema structure for application/json in path: /flights"));
    }

    @Test
    public void testInvalidSchemaType(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("invalid-schema-type-config.yaml");
        Files.writeString(configPath, 
            "kafka:\n" +
            "  bootstrap.servers:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "paths:\n" +
            "  /flights:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: my-topic\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: avro\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: OK\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: string\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });

        assertTrue(exception.getMessage().contains("Only object or array schema types are supported. Found: string for path: /flights"));
    }

    @Test
    public void testValidPrefixConfiguration(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("valid-prefix-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: prefix\n" +
            "          prefix: \"message:${parameters.conversationId}:\"\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: All messages for a conversation\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Configuration config = Configuration.fromFile(configPath.toString());
        assertNotNull(config);
        Configuration.MethodConfig method =
            config.getPaths().get("/conversations/{conversationId}/messages").getMethods().get("get");
        assertEquals("prefix", method.getKafka().getQuery().getMethod());
        assertEquals("message:${parameters.conversationId}:", method.getKafka().getQuery().getPrefix());
    }

    @Test
    public void testPrefixMissingPrefix(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("prefix-missing.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /conversations:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: prefix\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: All conversations\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.query.prefix` is required when `kafka.query.method` is 'prefix' for path: /conversations"));
    }

    @Test
    public void testPrefixUndefinedParameterReference(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("prefix-bad-param.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: prefix\n" +
            "          prefix: \"message:${parameters.missing}:\"\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: All messages for a conversation\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("Parameter missing used in `kafka.query.prefix` is not defined in path parameters for path: /conversations/{conversationId}/messages"));
    }

    @Test
    public void testPrefixRejectsAvroKey(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("prefix-avro-key.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        keyField: id\n" +
            "        query:\n" +
            "          method: prefix\n" +
            "          prefix: ${parameters.conversationId}\n" +
            "        serializer:\n" +
            "          key: avro\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: All messages for a conversation\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.serializer.key` must be 'string' when `kafka.query.method` is 'prefix' for path: /conversations/{conversationId}/messages"));
    }

    @Test
    public void testValidRangeConfiguration(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("valid-range-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages/{from}/{to}:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation identifier\n" +
            "    - name: from\n" +
            "      in: path\n" +
            "      description: start offset (padded)\n" +
            "    - name: to\n" +
            "      in: path\n" +
            "      description: end offset (padded)\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: range\n" +
            "          from: message:${parameters.conversationId}:${parameters.from}\n" +
            "          to: message:${parameters.conversationId}:${parameters.to}\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Configuration config = Configuration.fromFile(configPath.toString());
        assertNotNull(config);
        Configuration.MethodConfig method =
            config.getPaths().get("/conversations/{conversationId}/messages/{from}/{to}").getMethods().get("get");
        assertEquals("range", method.getKafka().getQuery().getMethod());
        assertEquals("message:${parameters.conversationId}:${parameters.from}", method.getKafka().getQuery().getFrom());
        assertEquals("message:${parameters.conversationId}:${parameters.to}", method.getKafka().getQuery().getTo());
    }

    @Test
    public void testRangeMissingFrom(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("range-missing-from.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /messages/{to}:\n" +
            "    parameters:\n" +
            "    - name: to\n" +
            "      in: path\n" +
            "      description: end offset\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: range\n" +
            "          to: message:${parameters.to}\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.query.from` is required when `kafka.query.method` is 'range' for path: /messages/{to}"));
    }

    @Test
    public void testRangeMissingTo(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("range-missing-to.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /messages/{from}:\n" +
            "    parameters:\n" +
            "    - name: from\n" +
            "      in: path\n" +
            "      description: start offset\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: range\n" +
            "          from: message:${parameters.from}\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.query.to` is required when `kafka.query.method` is 'range' for path: /messages/{from}"));
    }

    @Test
    public void testRangeUndefinedParameterReference(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("range-bad-param.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /messages/{from}/{to}:\n" +
            "    parameters:\n" +
            "    - name: from\n" +
            "      in: path\n" +
            "      description: start offset\n" +
            "    - name: to\n" +
            "      in: path\n" +
            "      description: end offset\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        query:\n" +
            "          method: range\n" +
            "          from: message:${parameters.from}\n" +
            "          to: message:${parameters.missing}\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("Parameter missing used in `kafka.query.to` is not defined in path parameters for path: /messages/{from}/{to}"));
    }

    @Test
    public void testRangeRejectsAvroKey(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("range-avro-key.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /messages/{from}/{to}:\n" +
            "    parameters:\n" +
            "    - name: from\n" +
            "      in: path\n" +
            "      description: start offset\n" +
            "    - name: to\n" +
            "      in: path\n" +
            "      description: end offset\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-messages\n" +
            "        keyField: id\n" +
            "        query:\n" +
            "          method: range\n" +
            "          from: ${parameters.from}\n" +
            "          to: ${parameters.to}\n" +
            "        serializer:\n" +
            "          key: avro\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Configuration.fromFile(configPath.toString());
        });
        assertTrue(exception.getMessage().contains("`kafka.serializer.key` must be 'string' when `kafka.query.method` is 'range' for path: /messages/{from}/{to}"));
    }

    @Test
    public void testIncludeKeyParsing(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("include-key-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "\n" +
            "paths:\n" +
            "  /items:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: items\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "        includeKey: true\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: items\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n" +
            "  /other:\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: items\n" +
            "        query:\n" +
            "          method: all\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: items\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n"
        );

        Configuration config = Configuration.fromFile(configPath.toString());
        assertTrue(config.getPaths().get("/items").getMethods().get("get").getKafka().isIncludeKey());
        // Defaults to false when omitted
        assertFalse(config.getPaths().get("/other").getMethods().get("get").getKafka().isIncludeKey());
    }
}