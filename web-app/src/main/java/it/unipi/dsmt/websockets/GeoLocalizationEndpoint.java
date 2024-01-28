package it.unipi.dsmt.websockets;

import it.unipi.dsmt.Kafka.KafkaUtils;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;
import it.unipi.dsmt.utility.Params;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@ServerEndpoint(value = "/events/geo-localization", encoders = GeoLocalizationDTOEncoder.class)
public class GeoLocalizationEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationEndpoint.class);

    // List to store WebSocket sessions. It is synchronized to handle concurrent access.
    private static final List<Session> sessions = Collections.synchronizedList(new ArrayList<>());

    // Executor service for managing Kafka consumer tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Params.THREADS);

    // List to store futures of Kafka consumer tasks
    private static final List<Future<?>> consumerFutures = new ArrayList<>();

    private static int currentActivePartitions = Params.PARTITIONS_PER_TOPIC;

    static {
        startConsumers(currentActivePartitions);

        // Create a new task to dynamically handle KafkaConsumers
        Runnable handleKafkaConsumers = GeoLocalizationEndpoint::handleConsumers;

        // Submit the Kafka task to the executor service and store the future
        Future<?> future = executorService.submit(handleKafkaConsumers);
    }
    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // Sending the geo-localization of the headquarters to the newly opened session
        sendHeadquartersGeoLocalization(session);

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
    private static void sendHeadquartersGeoLocalization(Session session) {
        GeoLocalizationDTO headquarters = new GeoLocalizationDTO(
                new Car("HEADQUARTERS", "/", "/", "/", "/"),
                45.442998228,
                9.273665572,
                GeoLocalizationDTO.Type.HEADQUARTER);
        KafkaUtils.send(session, headquarters);
    }
    private static void startConsumers(int numberOfConsumers) {
        for (var i = 0; i < numberOfConsumers; i++) {
            // Create a new Kafka consumer
            KafkaConsumer<String, String> consumer =
                    KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);

            // Create a new task to consume Kafka messages
            Runnable kafkaTask = () -> {
                KafkaUtils.consumeKafkaMessages(consumer, sessions, GeoLocalizationDTO.class);
            };

            // Submit the Kafka task to the executor service and store the future
            Future<?> future = executorService.submit(kafkaTask);
            consumerFutures.add(future);
        }
    }
    private static void restartConsumers() {
        // Iterate through the consumer futures list
        synchronized (consumerFutures) {
            for (Future<?> future : consumerFutures) {
                // Check if the future is done/completed
                if (future.isDone()) {
                    // Remove the completed future from the list
                    consumerFutures.remove(future);

                    // Create a new Kafka consumer
                    KafkaConsumer<String, String> consumer = KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);

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
    private static void handleConsumers() {
        while(true) {
            int numPartitions = KafkaUtils.getActivePartitions(Params.TOPIC_CARS, currentActivePartitions);

            try {
                Thread.sleep(Params.ADMIN_KAFKA_POOL_DURATION);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (numPartitions > Params.THREADS) {
                // DEBUG
                logger.info("Too many partitions ({})! No new KafkaConsumer will be added to topic \"{}\")",
                        numPartitions,
                        Params.TOPIC_CARS);
                // DEBUG
                continue;
            }

            if  (numPartitions > currentActivePartitions) {
                startConsumers(numPartitions - currentActivePartitions);

                // DEBUG
                logger.info("Added {} KafkaConsumer to topic \"{}\"",
                        numPartitions - currentActivePartitions,
                        Params.TOPIC_CARS);
                // DEBUG

                currentActivePartitions = numPartitions;

            } else if (numPartitions < currentActivePartitions) {
                int difference = currentActivePartitions - numPartitions;

                for (var i = 0; i < difference; i++) {
                    Future<?> toStop = consumerFutures.removeFirst();
                    toStop.cancel(true);
                }

                currentActivePartitions = numPartitions;
            } else {
                // DEBUG
                logger.info("Nothing has changed for topic \"{}\"", Params.TOPIC_CARS);
                // DEBUG
            }
        }
    }
}
