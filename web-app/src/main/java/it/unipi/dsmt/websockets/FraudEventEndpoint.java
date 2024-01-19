package it.unipi.dsmt.websockets;


import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.Kafka.KafkaUtils;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@ServerEndpoint(value = "/events/fraud")
public class FraudEventEndpoint implements EventEndpoint {
    private static final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
    private static final KafkaConsumer<String, String> consumer = KafkaUtils.createKafkaConsumer();
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationEndpoint.class);;
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static Future<?> future = null;

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        logger.info("WebSocket Session OPENED: {}", session.getId());
        sessions.add(session);

        // TODO: Usare executorService?
        if (!future.equals(null) || future.isCancelled()) {
            // Wrap the KafkaUtils.consumeKafkaMessages in a Runnable
            Class<GeoLocalizationDTO> valueType = GeoLocalizationDTO.class;
            Runnable kafkaTask = () -> KafkaUtils.consumeKafkaMessages(consumer,
                    sessions,valueType);
            // Submit the task to the ExecutorServic
            future = executorService.submit(kafkaTask);
        }
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);
        logger.info("WebSocket Session CLOSED: {}", session.getId());

        if (sessions.isEmpty()) {
            future.cancel(true);
        }
    }
    public void sendFraudEventUpdate(String timestamp, String severity, String event, String description) {
        String message = String.format("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>", timestamp, severity, event, description);
        for (Session session : sessions) {
            try {
                session.getBasicRemote().sendText(message);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}

