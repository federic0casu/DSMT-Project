package it.unipi.dsmt.Serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;

public class JsonSerializationSchema<T> implements KafkaSerializationSchema<T> {

    private final String topic;
    private final ObjectMapper objectMapper;

    public JsonSerializationSchema(String topic) {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, Long timestamp) {
        try {
            // Convert the object to a JSON string
            String jsonString = objectMapper.writeValueAsString(element);

            // Serialize the JSON string to bytes
            byte[] key = null;  // You can provide a key if needed
            byte[] value = jsonString.getBytes(StandardCharsets.UTF_8);

            // Create a Kafka ProducerRecord
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            // Handle serialization exception
            e.printStackTrace();
            return null;  // or throw an exception, depending on your error handling strategy
        }
    }

}