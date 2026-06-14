package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the `includeKey` option, which adds the raw lookup key to each response object under a
 * "key" field. Because the logic lives in the shared processValue path, it applies consistently to
 * every query method; this test covers the `all` (array) and `get` (single object) shapes.
 */
public class RestApiServerIncludeKeyTest {

    private static final int PORT = 17658;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private RestApiServer server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    private Configuration config(Path tempDir, String fileName, String yaml) throws Exception {
        Path configPath = tempDir.resolve(fileName);
        Files.writeString(configPath, yaml);
        return Configuration.fromFile(configPath.toString());
    }

    private void startServer(Configuration config, ReadOnlyKeyValueStore<Object, Object> store) throws Exception {
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

    private static final String ALL_INCLUDE_KEY =
        "kafka:\n" +
        "  application.id: kafka-streams-101\n" +
        "  bootstrap.servers: localhost:9092\n" +
        "  schema.registry.url: http://localhost:8081\n" +
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
        "                type: array\n";

    @Test
    public void testIncludeKeyAddsKeyToEachItem(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000011", "{\"role\":\"assistant\",\"text\":\"a brand new reply\"}");
        store.put("message:conv1:0000000001", "{\"role\":\"user\",\"text\":\"hello\"}");

        startServer(config(tempDir, "all-includekey.yaml", ALL_INCLUDE_KEY), store);

        HttpResponse<String> response = get("/items");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertEquals(2, body.size());
        // TreeMap order: 0000000001 before 0000000011
        assertEquals("message:conv1:0000000001", body.get(0).get("key").asText());
        assertEquals("hello", body.get(0).get("text").asText());
        assertEquals("message:conv1:0000000011", body.get(1).get("key").asText());
        assertEquals("a brand new reply", body.get(1).get("text").asText());
    }

    @Test
    public void testWithoutIncludeKeyThereIsNoKeyField(@TempDir Path tempDir) throws Exception {
        String yamlNoKey = ALL_INCLUDE_KEY.replace("        includeKey: true\n", "");
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000001", "{\"role\":\"user\",\"text\":\"hello\"}");

        startServer(config(tempDir, "all-nokey.yaml", yamlNoKey), store);

        HttpResponse<String> response = get("/items");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertEquals(1, body.size());
        assertFalse(body.get(0).has("key"), "key field should be absent when includeKey is not set");
    }

    @Test
    public void testIncludeKeyOnGetSingleObject(@TempDir Path tempDir) throws Exception {
        String getYaml =
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "  schema.registry.url: http://localhost:8081\n" +
            "paths:\n" +
            "  /items/{id}:\n" +
            "    parameters:\n" +
            "    - name: id\n" +
            "      in: path\n" +
            "      description: the item id\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: items\n" +
            "        query:\n" +
            "          method: get\n" +
            "          key: ${parameters.id}\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "        includeKey: true\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: one item\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: object\n";

        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("abc", "{\"text\":\"hi\"}");

        startServer(config(tempDir, "get-includekey.yaml", getYaml), store);

        HttpResponse<String> response = get("/items/abc");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isObject());
        assertEquals("abc", body.get("key").asText());
        assertEquals("hi", body.get("text").asText());
    }

    @Test
    public void testIncludeKeyOnRange(@TempDir Path tempDir) throws Exception {
        String rangeYaml =
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "  schema.registry.url: http://localhost:8081\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages/{from}/{to}:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation id\n" +
            "    - name: from\n" +
            "      in: path\n" +
            "      description: start offset\n" +
            "    - name: to\n" +
            "      in: path\n" +
            "      description: end offset\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-store\n" +
            "        query:\n" +
            "          method: range\n" +
            "          from: \"message:${parameters.conversationId}:${parameters.from}\"\n" +
            "          to: \"message:${parameters.conversationId}:${parameters.to}\"\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "        includeKey: true\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: A range of messages\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n";

        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000001", "{\"text\":\"hello\"}");
        store.put("message:conv1:0000000002", "{\"text\":\"how are you?\"}");
        store.put("message:conv1:0000000010", "{\"text\":\"much later\"}");

        startServer(config(tempDir, "range-includekey.yaml", rangeYaml), store);

        HttpResponse<String> response = get("/conversations/conv1/messages/0000000001/0000000002");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertEquals(2, body.size());
        // Each element in the window carries its own key, in order.
        assertEquals("message:conv1:0000000001", body.get(0).get("key").asText());
        assertEquals("hello", body.get(0).get("text").asText());
        assertEquals("message:conv1:0000000002", body.get(1).get("key").asText());
        assertEquals("how are you?", body.get(1).get("text").asText());
    }

    @Test
    public void testIncludeKeyOnPrefix(@TempDir Path tempDir) throws Exception {
        String prefixYaml =
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "  schema.registry.url: http://localhost:8081\n" +
            "paths:\n" +
            "  /conversations/{conversationId}/messages:\n" +
            "    parameters:\n" +
            "    - name: conversationId\n" +
            "      in: path\n" +
            "      description: the conversation id\n" +
            "    get:\n" +
            "      kafka:\n" +
            "        topic: conversation-store\n" +
            "        query:\n" +
            "          method: prefix\n" +
            "          prefix: \"message:${parameters.conversationId}:\"\n" +
            "        serializer:\n" +
            "          key: string\n" +
            "          value: string\n" +
            "        includeKey: true\n" +
            "      responses:\n" +
            "        '200':\n" +
            "          description: All messages for a conversation\n" +
            "          content:\n" +
            "            application/json:\n" +
            "              schema:\n" +
            "                type: array\n";

        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000001", "{\"text\":\"hello\"}");
        store.put("message:conv1:0000000002", "{\"text\":\"how are you?\"}");
        store.put("message:conv2:0000000001", "{\"text\":\"other\"}");

        startServer(config(tempDir, "prefix-includekey.yaml", prefixYaml), store);

        HttpResponse<String> response = get("/conversations/conv1/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        // Only conv1's messages, each carrying its key; conv2 excluded.
        assertEquals(2, body.size());
        assertEquals("message:conv1:0000000001", body.get(0).get("key").asText());
        assertEquals("message:conv1:0000000002", body.get(1).get("key").asText());
    }

    static class FakeKeyValueStore implements ReadOnlyKeyValueStore<Object, Object> {
        private final TreeMap<String, String> data = new TreeMap<>();

        void put(String key, String value) {
            data.put(key, value);
        }

        @Override
        public Object get(Object key) {
            return data.get(key);
        }

        @Override
        public KeyValueIterator<Object, Object> range(Object from, Object to) {
            List<KeyValue<Object, Object>> entries = new ArrayList<>();
            for (var e : data.subMap((String) from, true, (String) to, true).entrySet()) {
                entries.add(new KeyValue<>(e.getKey(), e.getValue()));
            }
            return new ListIterator(entries);
        }

        @Override
        public <PS extends Serializer<P>, P> KeyValueIterator<Object, Object> prefixScan(P prefix, PS prefixKeySerializer) {
            String p = (String) prefix;
            List<KeyValue<Object, Object>> entries = new ArrayList<>();
            for (var e : data.tailMap(p, true).entrySet()) {
                if (!e.getKey().startsWith(p)) {
                    break;
                }
                entries.add(new KeyValue<>(e.getKey(), e.getValue()));
            }
            return new ListIterator(entries);
        }

        @Override
        public KeyValueIterator<Object, Object> all() {
            List<KeyValue<Object, Object>> entries = new ArrayList<>();
            for (var e : data.entrySet()) {
                entries.add(new KeyValue<>(e.getKey(), e.getValue()));
            }
            return new ListIterator(entries);
        }

        @Override
        public long approximateNumEntries() {
            return data.size();
        }
    }

    static class ListIterator implements KeyValueIterator<Object, Object> {
        private final Iterator<KeyValue<Object, Object>> it;
        private final List<KeyValue<Object, Object>> entries;
        private int index = 0;

        ListIterator(List<KeyValue<Object, Object>> entries) {
            this.entries = entries;
            this.it = entries.iterator();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public KeyValue<Object, Object> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            index++;
            return it.next();
        }

        @Override
        public Object peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entries.get(index).key;
        }

        @Override
        public void close() {
        }
    }
}
