package it.unipi.dsmt.websockets;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.utility.Constants;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.EncodeException;
import jakarta.websocket.server.ServerEndpoint;

import java.io.IOException;

import java.time.Duration;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

@ServerEndpoint(value = "/events/geo-localization", encoders = GeoLocalizationDTOEncoder.class)
public class GeoLocalizationEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationEndpoint.class);
    private static final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
    private static final KafkaConsumer<String, String> consumer = createKafkaConsumer();
    private static Thread consumerThread = null;

    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);
        logger.info("WebSocket Session OPENED: {}", session.getId());

        // Sending the geo-localization of the headquarters
        GeoLocalizationDTO headquarters = new GeoLocalizationDTO(
                new Car("HEADQUARTERS", "/", "/", "/", "/"),
                45.442998228,
                9.273665572,
                GeoLocalizationDTO.Type.HEADQUARTER);

        send(session, headquarters);

        // Start a new thread for handling sessions
        if (consumerThread == null || consumerThread.isInterrupted()) {
            consumerThread = new Thread(this::consumeKafkaMessages);
            consumerThread.start();
        }
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);
        logger.info("WebSocket Session CLOSED: {}", session.getId());

        if (sessions.isEmpty()) {
            consumerThread.interrupt();
        }
    }
    // Initialize Kafka consumer
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_DEFAULT);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_ENDPOINT);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(Constants.TOPIC));

        return consumer;
    }
    private void consumeKafkaMessages() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Constants.POLL_DURATION));
                // Process the received records
                records.forEach(record -> {
                    // Logging (DEBUG)
                    logger.info("Received Kafka Message - Topic: {}, Partition: {}, Offset: {}, Value: {}",
                            record.topic(), record.partition(), record.offset(), record.value());

                    // Parse JSON using ObjectMapper and sending it through WebSocket
                    try {
                        broadcast(mapper.readValue(record.value(), GeoLocalizationDTO.class));
                    } catch (JsonProcessingException e) {
                        logger.error("Error deserializing message {}: {}", record.value(), e.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("Error consuming Kafka message: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
    private static void broadcast(GeoLocalizationDTO geoLocalizationEvent) {
        sessions.forEach(session -> {
            synchronized (session) {
                send(session, geoLocalizationEvent);
            }
        });
    }
    private static void send(Session session, GeoLocalizationDTO geoLocalizationEvent) {
        try {
            session.getBasicRemote()
                    .sendObject(geoLocalizationEvent);
        } catch (IOException | EncodeException e) {
            logger.error("Error sending geo-location: " + e.getMessage());
        }
    }
}
