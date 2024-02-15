package it.unipi.dsmt.websockets;

import it.unipi.dsmt.models.Violation;
import it.unipi.dsmt.serializers.ViolationEncoder;
import it.unipi.dsmt.Kafka.KafkaManager;
import it.unipi.dsmt.utility.Params;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;

@ServerEndpoint(value = "/events/violations", encoders = ViolationEncoder.class)
public class ViolationEventEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(ViolationEventEndpoint.class);
    private static final List<Session> sessions = new CopyOnWriteArrayList<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(Params.THREADS);
    private static final ArrayList<Future<?>> consumers = new ArrayList<>();
    private static int currentActivePartitions = Params.PARTITIONS_PER_TOPIC;

    static {
        KafkaManager.startConsumers(
                currentActivePartitions,
                sessions,
                consumers,
                executor,
                Params.TOPIC_VIOLATIONS,
                Params.VIOLATIONS_GROUP,
                Violation.class);

        // Create a new task to dynamically handle KafkaConsumers
        Runnable handleKafkaConsumers = ViolationEventEndpoint::handleConsumers;

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
                Params.TOPIC_VIOLATIONS,
                Params.VIOLATIONS_GROUP,
                Violation.class);

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
                Params.TOPIC_VIOLATIONS,
                Params.VIOLATIONS_GROUP,
                Violation.class);

        // DEBUG
        logger.info("WebSocket Session CLOSED (ID={})", session.getId());
        // DEBUG
    }

    private static void handleConsumers() {
        while(true) {
            int numPartitions = KafkaManager.getActivePartitions(Params.TOPIC_VIOLATIONS, currentActivePartitions);

            try {
                Thread.sleep(Params.ADMIN_KAFKA_POOL_DURATION);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (numPartitions > Params.THREADS) {
                // DEBUG
                logger.info("Too many partitions ({})! No new KafkaConsumer will be added to topic \"{}\")",
                        numPartitions,
                        Params.TOPIC_VIOLATIONS);
                // DEBUG
                continue;
            }

            if  (numPartitions > currentActivePartitions) {
                KafkaManager.startConsumers(
                        numPartitions - currentActivePartitions,
                        sessions,
                        consumers,
                        executor,
                        Params.TOPIC_VIOLATIONS,
                        Params.VIOLATIONS_GROUP,
                        Violation.class);

                // DEBUG
                logger.info("Added {} KafkaConsumer to topic \"{}\"",
                        numPartitions - currentActivePartitions,
                        Params.TOPIC_VIOLATIONS);
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
                logger.info("Nothing has changed for topic \"{}\"", Params.TOPIC_VIOLATIONS);
                // DEBUG
            }
        }
    }
}
