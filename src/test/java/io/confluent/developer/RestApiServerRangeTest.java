package io.confluent.developer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Exercises the `range` query method end-to-end: an HTTP request hits the running
 * {@link RestApiServer}, which performs a range scan against a mocked Kafka Streams store.
 * The store is backed by a TreeMap so its String key ordering matches the lexicographic byte
 * ordering that the real RocksDB-backed GlobalKTable store guarantees.
 */
public class RestApiServerRangeTest {

    private static final int PORT = 17654;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private RestApiServer server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    private Configuration rangeConfig(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("range-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
            "  schema.registry.url: http://localhost:8081\n" +
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

    @Test
    public void testRangeReturnsInclusiveOrderedWindow(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        // Two conversations interleaved in the same store; padded offsets keep ordering correct.
        store.put("message:conv1:0000000001", "{\"text\":\"a\"}");
        store.put("message:conv1:0000000002", "{\"text\":\"b\"}");
        store.put("message:conv1:0000000003", "{\"text\":\"c\"}");
        store.put("message:conv1:0000000010", "{\"text\":\"j\"}");
        store.put("message:conv2:0000000001", "{\"text\":\"other\"}");

        startServer(rangeConfig(tempDir), store);

        HttpResponse<String> response = get("/conversations/conv1/messages/0000000002/0000000003");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        // Inclusive on both ends: offsets 2 and 3, in order; offset 10 and conv2 excluded.
        assertEquals(2, body.size());
        assertEquals("b", body.get(0).get("text").asText());
        assertEquals("c", body.get(1).get("text").asText());
    }

    @Test
    public void testRangeOrdersNumericallyWhenPadded(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000002", "{\"text\":\"two\"}");
        store.put("message:conv1:0000000010", "{\"text\":\"ten\"}");

        startServer(rangeConfig(tempDir), store);

        HttpResponse<String> response = get("/conversations/conv1/messages/0000000000/0000000099");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertEquals(2, body.size());
        // Padding ensures 2 sorts before 10 (unlike unpadded "2" vs "10").
        assertEquals("two", body.get(0).get("text").asText());
        assertEquals("ten", body.get(1).get("text").asText());
    }

    @Test
    public void testEmptyRangeReturnsEmptyArray(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000001", "{\"text\":\"a\"}");

        startServer(rangeConfig(tempDir), store);

        HttpResponse<String> response = get("/conversations/conv1/messages/0000000050/0000000060");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        assertEquals(0, body.size());
    }

    /**
     * In-memory {@link ReadOnlyKeyValueStore} backed by a TreeMap. Natural String ordering mirrors
     * the unsigned-byte lexicographic ordering of the real RocksDB store for String keys.
     */
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
            // subMap is inclusive on both bounds, matching ReadOnlyKeyValueStore.range semantics.
            List<KeyValue<Object, Object>> entries = new ArrayList<>();
            for (var e : data.subMap((String) from, true, (String) to, true).entrySet()) {
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
