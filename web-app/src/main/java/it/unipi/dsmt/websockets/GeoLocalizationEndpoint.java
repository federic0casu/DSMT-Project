package it.unipi.dsmt.websockets;

import it.unipi.dsmt.Kafka.KafkaManager;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;
import it.unipi.dsmt.utility.Params;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

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
                Params.TOPIC_CARS,
                Params.CARS_GROUP,
                GeoLocalizationDTO.class);

        // Create a new task to dynamically handle KafkaConsumers
        Runnable handleKafkaConsumers = GeoLocalizationEndpoint::handleConsumers;

        // Submit the task to the executor service
        executor.submit(handleKafkaConsumers);
    }
    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // Sending the geo-localization of the headquarters to the newly opened session
        sendHeadquartersGeoLocalization(session);

        // Check and restart completed Kafka consumer tasks
        KafkaManager.restartConsumers(
                consumers,
                sessions,
                executor,
                Params.TOPIC_CARS,
                Params.CARS_GROUP,
                GeoLocalizationDTO.class);

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
                Params.TOPIC_CARS,
                Params.CARS_GROUP,
                GeoLocalizationDTO.class);

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
        KafkaManager.send(session, headquarters);
    }
    private static void handleConsumers() {
        while(true) {
            int numPartitions = KafkaManager.getActivePartitions(Params.TOPIC_CARS, currentActivePartitions);

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
                KafkaManager.startConsumers(
                        numPartitions - currentActivePartitions,
                        sessions,
                        consumers,
                        executor,
                        Params.TOPIC_CARS,
                        Params.CARS_GROUP,
                        GeoLocalizationDTO.class);

                // DEBUG
                logger.info("Added {} KafkaConsumer to topic \"{}\"",
                        numPartitions - currentActivePartitions,
                        Params.TOPIC_CARS);
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
                logger.info("Nothing has changed for topic \"{}\"", Params.TOPIC_CARS);
                // DEBUG
            }
        }
    }
}
