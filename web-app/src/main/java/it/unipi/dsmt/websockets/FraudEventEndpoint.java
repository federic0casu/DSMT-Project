package it.unipi.dsmt.websockets;

import it.unipi.dsmt.DTO.FraudEventDTO;
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

    // List to store Futures representing Kafka consumers. Used for managing Kafka consumer tasks.
    private static final ArrayList<AtomicBoolean> consumers = new ArrayList<>();
    private static int activeKafkaConsumers = 0; // Counter to keep track of the number of active Kafka consumers
    private static final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(Params.THREADS);

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // If the maximum number of Kafka consumers has been reached, return without starting a new consumer
        if (activeKafkaConsumers >= Params.THREADS) {
            return;
        }

        // Instantiating a new KafkaConsumer
        KafkaConsumer<String, String> consumer = KafkaUtils.createKafkaConsumer(Params.TOPIC_FRAUDS, Params.FRAUDS_GROUP);
        activeKafkaConsumers += 1;
        AtomicBoolean flag = new AtomicBoolean(false);
        consumers.add(flag);

        // Wrap the KafkaUtils.consumeKafkaMessages in a Runnable
        Runnable kafkaTask = () -> KafkaUtils.consumeKafkaMessages(consumer, flag, sessions, FraudEventDTO.class);

        // Submit the task to the ExecutorService
        Future<?> future = threadPoolExecutor.submit(kafkaTask);
    }
    @OnClose
    public void onClose(Session session) {
        var activeSessions = 0;

        synchronized (sessions) {
            // Remove the closed session from the list
            sessions.remove(session);

            activeSessions = sessions.size();
        }

        if (activeSessions < Params.THREADS) {
            activeKafkaConsumers -= 1;
            var toInterrupt = consumers.getFirst();
            toInterrupt.set(true);
            consumers.remove(toInterrupt);
        }
    }
}
