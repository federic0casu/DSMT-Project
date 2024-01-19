package it.unipi.dsmt.Kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.utility.Constants;
import jakarta.websocket.EncodeException;
import jakarta.websocket.Session;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaUtils {
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
    public static KafkaConsumer<String, String> createKafkaConsumer() {
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

    public static <T> void consumeKafkaMessages(KafkaConsumer<String,String> consumer,
                                             CopyOnWriteArrayList<Session> sessions
    ,Class<?> valueType) {

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
                        broadcast(mapper.readValue(record.value(), valueType), sessions);
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

    public static <T> void broadcast(T event, CopyOnWriteArrayList<Session> sessions) {
        sessions.forEach(session -> {
            synchronized (session) {
                send(session, event);
            }
        });
    }
    public static <T> void send(Session session, T event) {
        try {
            session.getBasicRemote().sendObject(event);
        } catch (IOException | EncodeException e) {
            logger.error("Error sending geo-location: " + e.getMessage());
        }
    }
}
