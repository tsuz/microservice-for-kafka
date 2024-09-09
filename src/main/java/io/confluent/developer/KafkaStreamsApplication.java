package io.confluent.developer;

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
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);

    static void runKafkaStreams(final KafkaStreams streams, Configuration config) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        // Start the REST API server
        RestApiServer apiServer = new RestApiServer(streams, config, 7000);
        try {
            apiServer.start();
        } catch (Exception e) {
            logger.error("Failed to start REST API server", e);
        }

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            apiServer.stop();
        }

        logger.info("Streams Closed");
    }

    static Topology buildTopology(Configuration config) {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        for (Configuration.EndpointConfig endpoint : config.getEndpoints()) {
            KStream<String, String> stream = builder.stream(endpoint.getTopic(), Consumed.with(stringSerde, stringSerde));

            KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(endpoint.getStoreName());
            stream.toTable(Materialized.<String, String>as(storeSupplier)
                    .withKeySerde(stringSerde)
                    .withValueSerde(stringSerde));
        }

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        Configuration config = Configuration.fromFile(args[0]);

        Properties props = new Properties();
        props.putAll(config.getKafkaConfig());

        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(config), props);

        // Start the REST API server
        RestApiServer apiServer = new RestApiServer(kafkaStreams, config, 7001);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down the application...");
            kafkaStreams.close();
            apiServer.stop();
        }));

        try {
            apiServer.start();
        } catch (Exception e) {
            logger.error("Failed to start REST API server", e);
        }

        logger.info("Kafka Streams Application Started");
        runKafkaStreams(kafkaStreams, config);
    }
}