package it.unipi.dsmt.utility;

public class Params {
    public static final String TOPIC = "cars-data";
    public static final String SERVERS = "localhost:9092,localhost:9094";
    public static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String CUSTOM_PARTITIONER = "it.unipi.dsmt.utility.RoundRobinPartitioner";
    public static final double AVG_SPEED = 50.0;
    public static final double EARTH_RADIUS = 6356.752;
}
