package it.unipi.dsmt.websockets;

import it.unipi.dsmt.Kafka.KafkaUtils;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;

import it.unipi.dsmt.utility.Params;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static it.unipi.dsmt.Kafka.KafkaUtils.send;

@ServerEndpoint(value = "/events/geo-localization", encoders = GeoLocalizationDTOEncoder.class)
public class GeoLocalizationEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationEndpoint.class);

    // List to store WebSocket sessions. It is synchronized to handle concurrent access.
    private static final List<Session> sessions = Collections.synchronizedList(new ArrayList<>());

    // List to store AtomicBoolean representing Kafka consumers. Used for managing Kafka consumer tasks.
    private static final ArrayList<AtomicBoolean> consumers = new ArrayList<>();
    private static int activeKafkaConsumers = 0; // Counter to keep track of the number of active Kafka consumers
    private static final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(Params.THREADS);

    @OnOpen
    public void onOpen(Session session) {
        // DEBUG
        var activeSessions = 0;
        // DEBUG

        // Add the new session to the list
        synchronized (sessions) {
            sessions.add(session);
            activeSessions = sessions.size();
        }

        // Sending the geo-localization of the headquarters to the newly opened session
        GeoLocalizationDTO headquarters = new GeoLocalizationDTO(
                new Car("HEADQUARTERS", "/", "/", "/", "/"),
                45.442998228,
                9.273665572,
                GeoLocalizationDTO.Type.HEADQUARTER);
        send(session, headquarters);

        // If the maximum number of Kafka consumers has been reached, return without starting a new consumer
        if (activeKafkaConsumers >= Params.THREADS) {
            // DEBUG
            logger.info("WebSocket Session OPENED (ID={}) | Current active sessions: {} | Current active KafkaConsumers: {}",
                    session.getId(),
                    activeSessions,
                    activeKafkaConsumers);
            // DEBUG
            return;
        }

        // Instantiating a new KafkaConsumer
        KafkaConsumer<String, String> consumer = KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);
        activeKafkaConsumers += 1;
        AtomicBoolean flag = new AtomicBoolean(false);
        consumers.add(flag);

        // Wrap a new task in a Runnable to consume Kafka messages
        Runnable kafkaTask = () -> {
            KafkaUtils.consumeKafkaMessages(consumer, flag, sessions, GeoLocalizationDTO.class);
        };

        // Submit the Kafka task to the ExecutorService and store the Future in the consumers list
        Future<?> future = threadPoolExecutor.submit(kafkaTask);

        // DEBUG
        logger.info("WebSocket Session OPENED (ID={}) | Current active sessions: {} | Current active KafkaConsumers: {}",
                session.getId(),
                activeSessions,
                activeKafkaConsumers);
        // DEBUG
    }
    @OnClose
    public void onClose(Session session) {
        int activeSessions;

        synchronized (sessions) {
            // Remove the closed session from the list
            sessions.remove(session);

            activeSessions = sessions.size();
        }

        // If the number of sessions is less than the maximum threads,
        // cancel the corresponding Kafka consumer task (the associated KafkaConsumer
        // will be deallocated).
        if (activeSessions < Params.THREADS) {
            activeKafkaConsumers -= 1;
            var toInterrupt = consumers.getFirst();
            toInterrupt.set(true);
            consumers.remove(toInterrupt);
        }

        // DEBUG
        logger.info("WebSocket Session CLOSED (ID={}) | Current active sessions: {} | Current active KafkaConsumers: {}",
                session.getId(),
                activeSessions,
                activeKafkaConsumers);
        // DEBUG
    }
}
