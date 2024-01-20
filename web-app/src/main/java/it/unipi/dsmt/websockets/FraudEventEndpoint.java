package it.unipi.dsmt.websockets;

import it.unipi.dsmt.DTO.FraudEventDTO;
import it.unipi.dsmt.Kafka.KafkaUtils;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.FraudEventDTOEncoder;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;

import it.unipi.dsmt.utility.Constants;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.util.concurrent.*;

import static it.unipi.dsmt.Kafka.KafkaUtils.send;

@ServerEndpoint(value = "/events/frauds", encoders = FraudEventDTOEncoder.class)
public class FraudEventEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(FraudEventEndpoint.class);
    private static final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
    // refactor createkafkaconsumer
    private static final KafkaConsumer<String, String> consumer =
            KafkaUtils.createKafkaConsumer(Constants.TOPIC_FRAUDS);
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);
        logger.info("WebSocket Session OPENED: {}", session.getId());

        // Start a new thread for handling sessions
        // Wrap the KafkaUtils.consumeKafkaMessages in a Runnable
        Class<FraudEventDTO> valueType = FraudEventDTO.class;
        Runnable kafkaTask = () -> KafkaUtils.consumeKafkaMessages(consumer,sessions,valueType);
        // Submit the task to the ExecutorService
        Future<?> future = executorService.submit(kafkaTask);
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);
        logger.info("WebSocket Session CLOSED: {}", session.getId());
    }
}
