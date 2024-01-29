package it.unipi.dsmt.websockets;

import it.unipi.dsmt.DTO.FraudEventDTO;
import it.unipi.dsmt.Kafka.KafkaManager;
import it.unipi.dsmt.serializers.FraudEventDTOEncoder;

import it.unipi.dsmt.utility.Params;

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

@ServerEndpoint(value = "/events/frauds", encoders = FraudEventDTOEncoder.class)
public class FraudEventEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(FraudEventEndpoint.class);

    // List to store WebSocket sessions. It is synchronized to handle concurrent access.
    private static final List<Session> sessions = Collections.synchronizedList(new ArrayList<>());

    // Executor service for managing Kafka consumer tasks
    private static final ExecutorService executor = Executors.newFixedThreadPool(Params.THREADS);

    // List to store futures of Kafka consumer tasks
    private static final ArrayList<Future<?>> consumers = new ArrayList<>();

    private static int currentActivePartitions = Params.PARTITIONS_PER_TOPIC;

    static {
        KafkaManager.startConsumers(
                currentActivePartitions,
                sessions,
                consumers,
                executor,
                Params.TOPIC_FRAUDS,
                Params.FRAUDS_GROUP,
                FraudEventDTO.class);

        // Create a new task to dynamically handle KafkaConsumers
        Runnable handleKafkaConsumers = FraudEventEndpoint::handleConsumers;

        // Submit the task to the executor service
        executor.submit(handleKafkaConsumers);
    }

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // Check and restart completed Kafka consumer tasks
        KafkaManager.restartConsumers(
                consumers,
                sessions,
                executor,
                Params.TOPIC_FRAUDS,
                Params.FRAUDS_GROUP,
                FraudEventDTO.class);

        // DEBUG
        logger.info("WebSocket Session OPENED (ID={})", session.getId());
        // DEBUG
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);

        // Check and restart completed Kafka consumer tasks
        KafkaManager.restartConsumers(
                consumers,
                sessions,
                executor,
                Params.TOPIC_FRAUDS,
                Params.FRAUDS_GROUP,
                FraudEventDTO.class);

        // DEBUG
        logger.info("WebSocket Session CLOSED (ID={})", session.getId());
        // DEBUG
    }
    private static void handleConsumers() {
        while(true) {
            int numPartitions = KafkaManager.getActivePartitions(Params.TOPIC_FRAUDS, currentActivePartitions);

            try {
                Thread.sleep(Params.ADMIN_KAFKA_POOL_DURATION);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (numPartitions > Params.THREADS) {
                // DEBUG
                logger.info("Too many partitions ({})! No new KafkaConsumer will be added to topic \"{}\")",
                        numPartitions,
                        Params.TOPIC_FRAUDS);
                // DEBUG
                continue;
            }

            if  (numPartitions > currentActivePartitions) {
                KafkaManager.startConsumers(
                        numPartitions - currentActivePartitions,
                        sessions,
                        consumers,
                        executor,
                        Params.TOPIC_FRAUDS,
                        Params.FRAUDS_GROUP,
                        FraudEventDTO.class);

                // DEBUG
                logger.info("Added {} KafkaConsumer to topic \"{}\"",
                        numPartitions - currentActivePartitions,
                        Params.TOPIC_FRAUDS);
                // DEBUG

                currentActivePartitions = numPartitions;

            } else if (numPartitions < currentActivePartitions) {
                int difference = currentActivePartitions - numPartitions;

                for (var i = 0; i < difference; i++) {
                    Future<?> toStop = consumers.removeFirst();
                    toStop.cancel(true);
                }

                currentActivePartitions = numPartitions;
            } else {
                // DEBUG
                logger.info("Nothing has changed for topic \"{}\"", Params.TOPIC_FRAUDS);
                // DEBUG
            }
        }
    }
}
