package org.oncode;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.oncode.models.Customer;
import org.oncode.models.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@AllArgsConstructor
class OrderTask implements Runnable {
    public Customer customer;
    private static final String TOPIC = "orders";

    private static final int ORDERS_NUMBER = 100;
    private static final Logger logger = LoggerFactory.getLogger(OrderTask.class);

    @Override
    public void run() {
        var producer = createKafkaProducer();
        try {
            var faker = new Faker();
            for (var i = 0; i < ORDERS_NUMBER; i++) {
                var order = Order.builder()
                        .orderId(faker.internet().uuid())
                        .amount(Double.parseDouble(faker.commerce().price())) // ALWAYS BETWEEN 1 AND 100
                        .customer(customer)
                        .productName(faker.commerce().productName())
                        .build();

                SendToKafka(producer, order);
                Thread.sleep(1000); // 1 order each second
            }
            producer.close();
        } catch (
                Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    private void SendToKafka(KafkaProducer<String, String> producer, Order order) {
        try {
            var objectMapper = new ObjectMapper();
            String orderJson = objectMapper.writeValueAsString(order);
            logger.info("Sending: {}", orderJson);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, orderJson);
            producer.send(record);
            producer.flush();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            logger.error(e.toString());
        }
    }
}
