package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class Topics {

    private final Logger logger = LoggerFactory.getLogger(Topics.class);

    public final String productsTopic;
    public final String productEventsTopic;
    public final String enrichedProductEventsTopic;

    public Topics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        productsTopic = System.getenv().getOrDefault("PRODUCTS_TOPIC", "products");
        productEventsTopic = System.getenv().getOrDefault("PRODUCTS_EVENT_TOPIC", "product-events");
        enrichedProductEventsTopic = System.getenv().getOrDefault("ENRICHED_PRODUCTS_EVENTS_TOPIC", "enriched-product-events");

        final Integer numberOfPartitions = Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS", "1"));
        final Short replicationFactor = Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR", "1"));


        createTopic(adminClient, productsTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, productEventsTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, enrichedProductEventsTopic, numberOfPartitions, replicationFactor);
    }

    public void createTopic(AdminClient adminClient, String topicName, Integer numberOfPartitions, Short replicationFactor) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);
            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }
}
