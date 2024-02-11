package it.unipi.dsmt.Kafka;

import it.unipi.dsmt.utility.Params;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.websocket.EncodeException;
import jakarta.websocket.Session;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

public class KafkaManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaManager.class);

    private static KafkaConsumer<String, String> createKafkaConsumer(String topic, String group) {
        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Params.KAFKA_ENDPOINTS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Params.KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Params.VALUE_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Params.AUTO_OFFSET_RESET);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        return consumer;
    }
    private static AdminClient createKafkaAdmin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Params.KAFKA_ENDPOINTS);

        return AdminClient.create(props);
    }
    public static void startConsumers(int numberOfConsumers,
                                      List<Session> sessions,
                                      ArrayList<Future<?>> consumers,
                                      ExecutorService executor,
                                      String topic,
                                      String group,
                                      Class<?> classType)
    {
        for (var i = 0; i < numberOfConsumers; i++) {
            // Create a new task to consume Kafka messages
            Runnable kafkaTask = () -> {
                consumeKafkaMessages(createKafkaConsumer(topic, group), sessions, classType);
            };

            // Submit the Kafka task to the executor service and store the future
            Future<?> future = executor.submit(kafkaTask);
            consumers.add(future);
        }
    }
    public static void restartConsumers(
            ArrayList<Future<?>> consumers,
            List<Session> sessions,
            ExecutorService executor,
            String topic,
            String group,
            Class<?> classType)
    {
        // Iterate through the consumer futures list
        synchronized (consumers) {
            for (Future<?> future : consumers) {
                // Check if the future is done/completed
                if (future.isDone()) {
                    // Remove the completed future from the list
                    consumers.remove(future);

                    // Create a new task to consume Kafka messages
                    Runnable kafkaTask = () -> {
                        consumeKafkaMessages(createKafkaConsumer(topic, group), sessions, classType);
                    };

                    // Submit the Kafka task to the executor service and store the future
                    Future<?> newFuture = executor.submit(kafkaTask);
                    consumers.add(newFuture);
                }
            }
        }
    }
    private static void consumeKafkaMessages(
            KafkaConsumer<String,String> consumer,
            List<Session> sessions,
            Class<?> classType)
    {
        try (consumer) {
            ObjectMapper mapper = new ObjectMapper();
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Params.POOL_DURATION));
                // Process the received records
                records.forEach(record -> {
                    // DEBUG
                    logger.info("Received Kafka Message - Topic: {}, Partition: {}, Value: {}",
                            record.topic(), record.partition(), record.value());
                    // DEBUG

                    // This block ensures that the creation of the tmp is performed atomically w.r.t. other operations
                    // sessions, that is it creates a consistent snapshot of sessions at that point in time.
                    ArrayList<Session> tmp;
                    synchronized (sessions) {
                        tmp = new ArrayList<>(sessions);
                    }

                    // Parse JSON using ObjectMapper and sending it through WebSocket
                    try {
                        broadcast(mapper.readValue(record.value(), classType), tmp);
                    } catch (JsonProcessingException e) {
                        logger.error("Error deserializing message {}: {}", record.value(), e.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("Error consuming Kafka message: " + e.getMessage());
        }
    }
    public static int getActivePartitions(String topic, int currentActivePartitions) {
        int numPartitions = currentActivePartitions;
        try (AdminClient adminClient = createKafkaAdmin()) {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(
                    Collections.singleton(topic),
                    new DescribeTopicsOptions().timeoutMs(5000)); // Timeout set to 5 seconds

            Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
            KafkaFuture<TopicDescription> topicDescription = values.get(topic);

            numPartitions = topicDescription.get().partitions().size();

            // DEBUG
            logger.info("[KafkaAdmin] Number of active partitions for {}: {}", topic, numPartitions);
            // DEBUG
        } catch (InterruptedException | ExecutionException e) {
            logger.error("[KafkaAdmin] Error while fetching number of active partitions for {}: {}",
                    topic,
                    e.getMessage());
        }

        return numPartitions;
    }
    public static <T> void broadcast(T event, List<Session> sessions) {
        sessions.forEach(session -> {
            send(session, event);
        });
    }
    public static <T> void send(Session session, T event) {
        try {
            session.getBasicRemote().sendObject(event);
        } catch (IOException | EncodeException e) {
            logger.error("Error sending geo-location: " + e.getMessage());
        }
    }
}
