package it.unipi.dsmt.utility;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaProducerFactory {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerFactory.class);
    public KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Params.SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Params.KEY_SERIALIZER_CLASS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Params.VALUE_SERIALIZER_CLASS);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // DEBUG
        logPartitionsInfo(producer);
        // DEBUG

        return producer;
    }

    private void logPartitionsInfo(KafkaProducer<String, String> producer) {
        try {
            List<PartitionInfo> partitions = producer.partitionsFor(Params.TOPIC);

            if (partitions != null && !partitions.isEmpty()) {
                StringBuilder partitionInfo = new StringBuilder("Producer subscribed to partitions: ");
                for (PartitionInfo partition : partitions) {
                    partitionInfo.append(partition.partition()).append(" ");
                }
                logger.info(partitionInfo.toString());
            } else {
                logger.warn("No partitions found for topic: {}", Params.TOPIC);
            }

        } catch (Exception e) {
            logger.error("Error while getting partition information: {}", e.getMessage());
        }
    }
}
