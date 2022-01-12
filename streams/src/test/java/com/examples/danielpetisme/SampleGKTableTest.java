package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SampleGKTableTest {

    KafkaProducer<String, String> productProducer;
    KafkaConsumer<String, String> productConsumer;
    KafkaProducer<String, String> productEventProducer;
    KafkaConsumer<String, String> productEventConsumer;
    KafkaConsumer<String, String> enrichedProducerEventConsumer;
    Topics topics;
    SampleGKTable streams;

    public static final String STATE_DIR = "/tmp/SampleGKTableTest/" + System.currentTimeMillis();

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

    @Before
    public void beforeEach() throws ExecutionException, InterruptedException {
        topics = new Topics(AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        )));

        productProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE
                ),
                new StringSerializer(), new StringSerializer()
        );

        productConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "productConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        productConsumer.subscribe(Collections.singletonList(topics.productsTopic));

        productEventProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE
                ),
                new StringSerializer(), new StringSerializer()
        );

        productEventConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "productEventsConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        productEventConsumer.subscribe(Collections.singletonList(topics.productEventsTopic));

        enrichedProducerEventConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "enrichedProducerEventConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );

        enrichedProducerEventConsumer.subscribe(Collections.singletonList(topics.enrichedProductEventsTopic));
    }

    @After
    public void afterEach() {
        streams.stop();
    }

    private void loadProducts(Map<String, String> products) {
        products.forEach((key, value) -> {
            System.out.println("Producing Product key=" + key + ", Value= " + value);
            try {
                productProducer.send(
                        new ProducerRecord<>(topics.productsTopic, key, value),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            }
                            System.out.println("ProductProducer " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        Map<String, String> loadedProducts = new HashMap<>();

        long start = System.currentTimeMillis();
        while (loadedProducts.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = productConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> loadedProducts.put(record.key(), record.value()));
        }
        assertThat(loadedProducts).hasSize(products.size());
        System.out.println("## Products");
        loadedProducts.forEach((k, v) -> {
            System.out.println(k + ", " + v);
        });
    }

    private void loadProductEvents(Map<String, String> productEvents) {
        productEvents.forEach((key, value) -> {
            System.out.println("Producing Product Event key=" + key + ", Value= " + value);
            try {
                productEventProducer.send(
                        new ProducerRecord<>(topics.productEventsTopic, key, value),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            }
                            System.out.println("ProductEventProducer " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        Map<String, String> loadedProductEvents = new HashMap<>();

        long start = System.currentTimeMillis();
        while (loadedProductEvents.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = productEventConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> loadedProductEvents.put(record.key(), record.value()));
        }
        assertThat(loadedProductEvents).hasSize(productEvents.size());
        System.out.println("## Product Events");
        loadedProductEvents.forEach((k, v) -> {
            System.out.println(k + ", " + v);
        });
    }

    @Test
    public void testGKTable() throws Exception {
        streams = new SampleGKTable(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        StreamsConfig.STATE_DIR_CONFIG, STATE_DIR
                )
        );
        streams.start();

        loadProducts(Map.of(
                "1", "101",
                "2", "102",
                "3", "103",
                "4", "104",
                "5", "105",
                "6", "106",
                "7", "107",
                "8", "108",
                "9", "109",
                "10", "110"
        ));

        loadProductEvents(Map.of(
                "1", "1-A",
                "2", "2-B",
                "3", "3-C",
                "4", "4-D"
        ));

        Map<String, String> result = new HashMap<>();

        long start = System.currentTimeMillis();
        while (result.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = enrichedProducerEventConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> result.put(record.key(), record.value()));
        }

        assertThat(result).hasSize(4);
        assertThat(result).contains(
                Map.entry("1", "101-A"),
                Map.entry("2", "102-B"),
                Map.entry("3", "103-C"),
                Map.entry("4", "104-D")
        );

        var stateDirDirectory = Path.of(STATE_DIR);
        assertThat(stateDirDirectory).exists();
        assertThat(stateDirDirectory).isDirectory();

        var globalStore = stateDirDirectory.resolve(SampleGKTable.class.getName()).resolve("global/rocksdb/products-global-store");
        assertThat(globalStore).exists();
        assertThat(globalStore).isDirectory();
    }

}