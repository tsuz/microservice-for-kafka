package io.confluent.developer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.*;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaStreamsApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);
    private static final AtomicReference<RestApiServer> apiServerRef = new AtomicReference<>(null);
    private static Configuration config;

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) -> {
            logger.info("State transition from {} to {}", oldState, newState);
            if (newState == KafkaStreams.State.RUNNING) {
                startRestApiServer(streams, config);
            } else if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                stopRestApiServer();
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            stopRestApiServer();
        }

        logger.info("Streams Closed");
    }

    private static synchronized void startRestApiServer(KafkaStreams streams, Configuration config) {
        if (apiServerRef.get() == null) {
            RestApiServer apiServer = new RestApiServer(streams, config, 7001);
            try {
                apiServer.start();
                apiServerRef.set(apiServer);
                logger.info("REST API server started");
            } catch (Exception e) {
                logger.error("Failed to start REST API server", e);
            }
        }
    }

    private static synchronized void stopRestApiServer() {
        RestApiServer apiServer = apiServerRef.get();
        if (apiServer != null) {
            apiServer.stop();
            apiServerRef.set(null);
            logger.info("REST API server stopped");
        }
    }

    public static Map<String, ?> buildSchemaRegistryConfigMap(final Properties config) {
        final HashMap<String, String> map = new HashMap<>();
        if (config.containsKey(SCHEMA_REGISTRY_URL_CONFIG))
            map.put(SCHEMA_REGISTRY_URL_CONFIG, config.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        if (config.containsKey(BASIC_AUTH_CREDENTIALS_SOURCE))
            map.put(BASIC_AUTH_CREDENTIALS_SOURCE, config.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
        if (config.containsKey(USER_INFO_CONFIG))
            map.put(USER_INFO_CONFIG, config.getProperty(USER_INFO_CONFIG));
            logger.info("got config "  + map.toString());
        return map;
    }

    static Topology buildTopology(Configuration config) {
        StreamsBuilder builder = new StreamsBuilder();

        for (Configuration.EndpointConfig endpoint : config.getEndpoints()) {
            Class<?> keyClass = getClassForType(endpoint.getKeySerializer());
            Class<?> valueClass = getClassForType(endpoint.getValueSerializer());
            processEndpoint(builder, endpoint, keyClass, valueClass);
        }
        return builder.build();
    }

    private static <K, V> void processEndpoint(StreamsBuilder builder, Configuration.EndpointConfig endpoint, Class<K> keyClass, Class<V> valueClass) {
        Serde<K> keySerde = (Serde<K>) getSerdeForType(endpoint.getKeySerializer());
        Serde<V> valueSerde = (Serde<V>) getSerdeForType(endpoint.getValueSerializer());

        KStream<K, V> stream = builder.stream(endpoint.getTopic(), Consumed.with(keySerde, valueSerde));

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(endpoint.getStoreName());

        stream.toTable(Materialized.<K, V>as(storeSupplier)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));
    }

    private static Serde<?> getSerdeForType(String serializerType) {
        switch (serializerType.toLowerCase()) {
            case "string":
                return Serdes.String();
            case "long":
                return Serdes.Long();
            case "integer":
            case "int":
                return Serdes.Integer();
            case "double":
                return Serdes.Double();
            case "float":
                return Serdes.Float();
            case "byte":
            case "bytes":
                return Serdes.ByteArray();
            case "avro":
                Serde<GenericRecord> avroSerde = new GenericAvroSerde();
                avroSerde.configure(buildSchemaRegistryConfigMap(config.getKafkaConfig()), false);
                return avroSerde;
            default:
                throw new IllegalArgumentException("Unsupported serializer type: " + serializerType);
        }
    }

    private static Class<?> getClassForType(String serializerType) {
        switch (serializerType.toLowerCase()) {
            case "string":
                return String.class;
            case "long":
                return Long.class;
            case "integer":
            case "int":
                return Integer.class;
            case "double":
                return Double.class;
            case "float":
                return Float.class;
            case "byte":
            case "bytes":
                return byte[].class;
            case "avro":
                return GenericRecord.class;
            default:
                throw new IllegalArgumentException("Unsupported serializer type: " + serializerType);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        Configuration configuration = Configuration.fromFile(args[0]);
        config = configuration;

        Properties props = new Properties();
        props.putAll(config.getKafkaConfig());

        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(config), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down the application...");
            kafkaStreams.close();
            stopRestApiServer();
        }));

        logger.info("Kafka Streams Application Started");
        runKafkaStreams(kafkaStreams);
    }
}