package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SampleGKTable {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SampleGKTable.class);
    private final KafkaStreams streams;
    private final Topics topics;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new SampleGKTable(Collections.emptyMap()).start();
    }

    public SampleGKTable(Map<String, String> config) throws InterruptedException, ExecutionException {
        var properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);

        var topology = buildTopology();
        logger.info(topology.describe().toString());
        logger.info("creating streams with props: {}", properties);
        streams = new KafkaStreams(topology, properties);
        streams.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                logger.info("Global State Store restoration started(topicPartition={}, storeName={}, startingOffset={}, endingOffset={})", topicPartition, storeName, startingOffset, endingOffset);
            }

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                logger.info("Global State Batch restored(topicPartition={}, storeName={}, batchEndOffset={, numRestored={})", topicPartition, storeName, numRestored);
            }

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                logger.info("Global State Batch restoration ended(topicPartition={}, storeName={}, totalRestored={})", topicPartition, storeName, totalRestored);

            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    Topology buildTopology() {

        var builder = new StreamsBuilder();
        var products = builder.globalTable(
                topics.productsTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(
                        "products-global-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        var productEvents = builder.stream(topics.productEventsTopic, Consumed.with(Serdes.String(), Serdes.String()));

        productEvents
                .join(products, (eventKey, productKey) -> eventKey, (eventValue, productValue) -> {
                    System.out.println("HERE");
                    var parts = eventValue.split("-");
                    return String.format("%s-%s", productValue, parts[1]);
                })
                .to(topics.enrichedProductEventsTopic);

        return builder.build();

    }

    public void start() {
        logger.info("Kafka Streams started");
        streams.start();
    }

    public void stop() {
        streams.close(Duration.ofSeconds(3));
        logger.info("Kafka Streams stopped");
    }

    private Map<String, String> defaultProps = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", SampleGKTable.class.getName()),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "1",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
    );

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix, Map<String, String> customProps) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        props.putAll(customProps);
        return props;
    }
}
