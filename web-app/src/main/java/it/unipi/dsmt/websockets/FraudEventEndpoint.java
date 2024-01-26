package it.unipi.dsmt.websockets;

import it.unipi.dsmt.DTO.FraudEventDTO;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.Kafka.KafkaUtils;
import it.unipi.dsmt.serializers.FraudEventDTOEncoder;

import it.unipi.dsmt.utility.Params;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@ServerEndpoint(value = "/events/frauds", encoders = FraudEventDTOEncoder.class)
public class FraudEventEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(FraudEventEndpoint.class);

    // List to store WebSocket sessions. It is synchronized to handle concurrent access.
    private static final List<Session> sessions = Collections.synchronizedList(new ArrayList<>());

    // Executor service for managing Kafka consumer tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Params.THREADS);

    // List to store futures of Kafka consumer tasks
    private static final List<Future<?>> consumerFutures = new ArrayList<>();

    static {
        for (int i = 0; i < Params.PARTITIONS_PER_TOPIC; i++) {
            // Create a new Kafka consumer
            KafkaConsumer<String, String> consumer =
                    KafkaUtils.createKafkaConsumer(Params.TOPIC_FRAUDS, Params.FRAUDS_GROUP);

            // Create a new task to consume Kafka messages
            Runnable kafkaTask = () -> {
                KafkaUtils.consumeKafkaMessages(consumer, sessions, FraudEventDTO.class);
            };

            // Submit the Kafka task to the executor service and store the future
            Future<?> future = executorService.submit(kafkaTask);
            consumerFutures.add(future);
        }
    }

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // Check and restart completed Kafka consumer tasks
        restartConsumers();

        // DEBUG
        logger.info("WebSocket Session OPENED (ID={})", session.getId());
        // DEBUG
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);

        // Check and restart completed Kafka consumer tasks
        restartConsumers();

        // DEBUG
        logger.info("WebSocket Session CLOSED (ID={})", session.getId());
        // DEBUG
    }
    private void restartConsumers() {
        // Iterate through the consumer futures list
        synchronized (consumerFutures) {
            for (Future<?> future : consumerFutures) {
                // Check if the future is done/completed
                if (future.isDone()) {
                    // Remove the completed future from the list
                    consumerFutures.remove(future);

                    // Create a new Kafka consumer
                    KafkaConsumer<String, String> consumer =
                            KafkaUtils.createKafkaConsumer(Params.TOPIC_FRAUDS, Params.FRAUDS_GROUP);

                    // Create a new task to consume Kafka messages
                    Runnable kafkaTask = () -> {
                        KafkaUtils.consumeKafkaMessages(consumer, sessions, GeoLocalizationDTO.class);
                    };

                    // Submit the Kafka task to the executor service and store the future
                    Future<?> newFuture = executorService.submit(kafkaTask);
                    consumerFutures.add(newFuture);
                }
            }
        }
    }
}
