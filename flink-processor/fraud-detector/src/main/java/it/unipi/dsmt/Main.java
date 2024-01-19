package it.unipi.dsmt;
import it.unipi.dsmt.Functions.FraudEnrichment;
import it.unipi.dsmt.Functions.MultipleLocationFunction;
import it.unipi.dsmt.Functions.RateWindowFunction;
import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

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

			// large orders stream
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

			/*
				the large transaction stream is the simplest because it does not
			 	require grouping by a key or keeping state information
			*/
			DataStream<Fraud> largeOrdersStream = stream
					.filter(order -> order.getAmount() > 10)
					.map(new FraudEnrichment());

			/*
				Detecting multiple transactions in a short period of time
				keeping the state and grouping orders by the customerId
			 */
			DataStream<Fraud> highRateOrdersStream =
					stream.keyBy(order -> order.getCustomer().getId())
							.window(TumblingEventTimeWindows.of(Time.seconds(20)))
							.trigger(CountTrigger.of(10))
							.process(new RateWindowFunction());

			highRateOrdersStream.sinkTo(sink);
			largeOrdersStream.sinkTo(sink);
			/*
				Detecting Multiple transaction in the same window from different Locations
			*/

			DataStream<Fraud> multipleLocationsOrderStream = stream
					.keyBy(order -> order.getCustomer().getId())
					.process(new MultipleLocationFunction());

			env.setParallelism(1);
			env.execute("Flink Streaming Java API Skeleton");
		}
	}
}

