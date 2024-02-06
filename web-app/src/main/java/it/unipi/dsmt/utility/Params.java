package it.unipi.dsmt.utility;

public class Params {
    public static final String TOPIC_CARS = "cars-data";
    public static final String CARS_GROUP = "0";

    public static final String TOPIC_FRAUDS = "frauds";
    public static final String FRAUDS_GROUP = "1";

    public static final String TOPIC_TRIPS = "trip-reports";
    public static final String TRIPS_GROUP = "2";

    public static final String TOPIC_VIOLATIONS = "violations";
    public static final String VIOLATIONS_GROUP = "3";

    public static final String KAFKA_ENDPOINTS = "kafka-broker-1:9093,kafka-broker-2:9095";
    public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String AUTO_OFFSET_RESET = "latest";
    public static final int THREADS = 32;
    public static final int PARTITIONS_PER_TOPIC = 2;
    public static final int POOL_DURATION = 100;
    public static final long ADMIN_KAFKA_POOL_DURATION = 60000;
    public static final String KAFKA_REMOTE_ENDPOINTS = "10.2.1.118:9092,10.2.1.119:9092";
}


