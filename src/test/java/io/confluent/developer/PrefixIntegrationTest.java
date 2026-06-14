package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test for the `prefix` query method.
 *
 * Builds the REAL topology from a YAML config via {@link KafkaStreamsApplication#buildTopology},
 * drives records through a {@link TopologyTestDriver} (real String serdes + a real materialized
 * GlobalKTable state store), and serves that live store over HTTP through {@link RestApiServer}.
 * Validates the full stack: config &rarr; topology &rarr; state store &rarr; prefix scan &rarr; HTTP/JSON.
 */
public class PrefixIntegrationTest {

    private static final int PORT = 17657;
    private static final String TOPIC = "conversation-messages";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver testDriver;
    private RestApiServer server;

    @BeforeEach
    public void resetStaticStoreRegistry() throws Exception {
        // buildTopology dedupes stores via a static Set that persists across calls; clear it so each
        // test builds its topology from scratch.
        Field f = KafkaStreamsApplication.class.getDeclaredField("createdStores");
        f.setAccessible(true);
        ((Set<?>) f.get(null)).clear();
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
        if (testDriver != null) {
            testDriver.close();
            testDriver = null;
        }
    }

    private Configuration writeConfig(Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("prefix-it-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: prefix-integration-test\n" +
            "  bootstrap.servers: dummy:1234\n" +
            "  schema.registry.url: http://localhost:8081\n" +
            "\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation identifier\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: " + TOPIC + "\n" +
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
        return Configuration.fromFile(configPath.toString());
    }

    private void bootstrap(Path tempDir) throws Exception {
        Configuration config = writeConfig(tempDir);

        Topology topology = KafkaStreamsApplication.buildTopology(config);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "prefix-integration-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDir.resolve("state").toString());

        testDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, String> input =
            testDriver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());

        // Two interleaved conversations; padded offsets keep ordering numeric.
        input.pipeInput("message:conv1:0000000001", "{\"text\":\"hello\"}");
        input.pipeInput("message:conv1:0000000002", "{\"text\":\"how are you?\"}");
        input.pipeInput("message:conv1:0000000010", "{\"text\":\"much later\"}");
        input.pipeInput("message:conv2:0000000001", "{\"text\":\"different conversation\"}");

        ReadOnlyKeyValueStore<Object, Object> store = testDriver.getKeyValueStore(TOPIC + "-store");
        assertNotNull(store, "Expected the topology to materialize a queryable store named " + TOPIC + "-store");

        KafkaStreams streams = Mockito.mock(KafkaStreams.class);
        Mockito.doReturn(store).when(streams).store(Mockito.any());

        server = new RestApiServer(streams, config, PORT);
        server.start();
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + PORT + path))
            .GET()
            .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    @Test
    public void testPrefixScopesToConversationOverRealStore(@TempDir Path tempDir) throws Exception {
        bootstrap(tempDir);

        HttpResponse<String> response = get("/conversations/conv1/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        // All three conv1 messages, ordered (offset 2 before 10 thanks to padding); conv2 excluded.
        assertEquals(3, body.size());
        assertEquals("hello", body.get(0).get("text").asText());
        assertEquals("how are you?", body.get(1).get("text").asText());
        assertEquals("much later", body.get(2).get("text").asText());
    }

    @Test
    public void testPrefixOtherConversationOverRealStore(@TempDir Path tempDir) throws Exception {
        bootstrap(tempDir);

        HttpResponse<String> response = get("/conversations/conv2/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertEquals(1, body.size());
        assertEquals("different conversation", body.get(0).get("text").asText());
    }

    @Test
    public void testPrefixNoMatchOverRealStore(@TempDir Path tempDir) throws Exception {
        bootstrap(tempDir);

        HttpResponse<String> response = get("/conversations/conv9/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        assertEquals(0, body.size());
    }
}
