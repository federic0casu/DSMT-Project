package it.unipi.dsmt;
import it.unipi.dsmt.Functions.DynamicUpdates.DynamicKeyFunction;
import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


public class Main {
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

			KafkaSource<Order> source = KafkaSource.<Order>builder()
					.setBootstrapServers("kafka:9093")
					.setTopics("orders")
					.setStartingOffsets(OffsetsInitializer.earliest())
					.setValueOnlyDeserializer(new JsonDeserializationSchema<>(Order.class))
					.build();

			DataStream<Order> stream = env.fromSource(source,
					WatermarkStrategy
							.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
							.withTimestampAssigner((order, timestamp) -> order.timestamp = timestamp)
					, "Kafka Source");

			// sink code
			KafkaSink<Fraud> sink = KafkaSink.<Fraud>builder()
					.setBootstrapServers("kafka:9093")
					.setRecordSerializer(
							KafkaRecordSerializationSchema.builder()
									.setTopic("frauds")
									.setValueSerializationSchema(new JsonSerializationSchema<Fraud>())
									.build())
					.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
					.build();

			DataStream<Fraud> fraudStream = stream
					.process(new DynamicKeyFunction());


			env.setParallelism(1);
			env.execute("Flink Streaming Java API Skeleton");
		}
	}
}

