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
 * Exercises the `prefix` query method end-to-end: an HTTP request hits the running
 * {@link RestApiServer}, which performs a prefix scan against a mocked Kafka Streams store backed by
 * a TreeMap (whose natural ordering matches the UTF-8 byte ordering for the ASCII keys used here).
 */
public class RestApiServerPrefixTest {

    private static final int PORT = 17656;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private RestApiServer server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    private Configuration prefixConfig(@TempDir Path tempDir) throws Exception {
        Path configPath = tempDir.resolve("prefix-config.yaml");
        Files.writeString(configPath,
            "kafka:\n" +
            "  application.id: kafka-streams-101\n" +
            "  bootstrap.servers: localhost:9092\n" +
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
    public void testPrefixReturnsOnlyMatchingConversationInOrder(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv1:0000000001", "{\"text\":\"a\"}");
        store.put("message:conv1:0000000002", "{\"text\":\"b\"}");
        store.put("message:conv1:0000000010", "{\"text\":\"j\"}");
        store.put("message:conv2:0000000001", "{\"text\":\"other\"}");

        startServer(prefixConfig(tempDir), store);

        HttpResponse<String> response = get("/conversations/conv1/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        // Only conv1, ordered; padding keeps offset 2 before offset 10; conv2 excluded.
        assertEquals(3, body.size());
        assertEquals("a", body.get(0).get("text").asText());
        assertEquals("b", body.get(1).get("text").asText());
        assertEquals("j", body.get(2).get("text").asText());
    }

    @Test
    public void testPrefixWithNoMatchesReturnsEmptyArray(@TempDir Path tempDir) throws Exception {
        FakeKeyValueStore store = new FakeKeyValueStore();
        store.put("message:conv2:0000000001", "{\"text\":\"other\"}");

        startServer(prefixConfig(tempDir), store);

        HttpResponse<String> response = get("/conversations/conv1/messages");
        assertEquals(200, response.statusCode());

        JsonNode body = MAPPER.readTree(response.body());
        assertTrue(body.isArray());
        assertEquals(0, body.size());
    }

    /**
     * In-memory {@link ReadOnlyKeyValueStore} backed by a TreeMap. prefixScan is implemented to mirror
     * the real store: every entry whose key starts with the prefix, in sorted (byte-lexicographic) order.
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
        public <PS extends Serializer<P>, P> KeyValueIterator<Object, Object> prefixScan(P prefix, PS prefixKeySerializer) {
            String p = (String) prefix;
            List<KeyValue<Object, Object>> entries = new ArrayList<>();
            for (var e : data.tailMap(p, true).entrySet()) {
                if (!e.getKey().startsWith(p)) {
                    break; // sorted: once we pass the prefix, no further matches
                }
                entries.add(new KeyValue<>(e.getKey(), e.getValue()));
            }
            return new ListIterator(entries);
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
