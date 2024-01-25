package it.unipi.dsmt.utility;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class RoundRobinPartitioner implements Partitioner {
    private static int currentPartition = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        // Use round-robin logic to determine the next partition
        int partition = currentPartition;
        currentPartition = (currentPartition + 1) % numPartitions;

        return partition;
    }
    @Override
    public void close() {}
    @Override
    public void configure(Map<String, ?> configs) {}
}

