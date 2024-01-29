package it.unipi.dsmt;
import it.unipi.dsmt.Functions.FraudEnrichment;
import it.unipi.dsmt.Functions.MultipleLocationFunction;
import it.unipi.dsmt.Functions.RateWindowFunction;
import it.unipi.dsmt.Functions.TripReportProcessFunction;
import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.GeoLocalization;
import it.unipi.dsmt.Models.Order;
import it.unipi.dsmt.Models.TripReport;
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


		final String remoteKakfaServers = "10.2.1.118:9092,10.2.1.119:9092";
		final String dockerKakfkaServers = 	"kafka-broker-1:9093,kafka-broker-2:9095";

		// set up the streaming execution environment
		try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

			/*
			// KAFKA SOURCES
			KafkaSource<Order> orderSource = KafkaSource.<Order>builder()
					.setBootstrapServers("kafka-broker-1:9093,kafka-broker-2:9095")
					.setTopics("orders")
					.setStartingOffsets(OffsetsInitializer.earliest())
					.setValueOnlyDeserializer(new JsonDeserializationSchema<>(Order.class))
					.build();


			// large orders stream
			DataStream<Order> stream = env.fromSource(orderSource,
					WatermarkStrategy
							.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(20))
							.withTimestampAssigner((order, timestamp) -> order.timestamp = timestamp)
					, "Kafka Source");

*/

			KafkaSource<GeoLocalization> tripSource = KafkaSource.<GeoLocalization>builder()
					.setBootstrapServers(remoteKakfaServers)
					.setTopics("cars-data")
					.setStartingOffsets(OffsetsInitializer.earliest())
					.setValueOnlyDeserializer(new JsonDeserializationSchema<>(GeoLocalization.class))
					.build();


			DataStream<GeoLocalization> geolocStream = env.fromSource(tripSource,
					WatermarkStrategy
							.<GeoLocalization>forBoundedOutOfOrderness(Duration.ofSeconds(20))
							.withTimestampAssigner((geoloc, timestamp) -> geoloc.timestamp =
									timestamp)
					, "Kafka Source");

			DataStream<TripReport> reportStream = geolocStream
					.keyBy(geoloc -> geoloc.car.getVin())
					.process(new TripReportProcessFunction());

/*
			// sink code
			KafkaSink<Fraud> sink = KafkaSink.<Fraud>builder()
					.setBootstrapServers("kafka-broker-1:9093,kafka-broker-2:9095")
					.setRecordSerializer(
							KafkaRecordSerializationSchema.builder()
									.setTopic("frauds")
									.setValueSerializationSchema(new JsonSerializationSchema<Fraud>())
									.build())
					.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
					.build();

 */

			KafkaSink<TripReport> tripsink = KafkaSink.<TripReport>builder()
					.setBootstrapServers(remoteKakfaServers)
					.setRecordSerializer(
							KafkaRecordSerializationSchema.builder()
									.setTopic("trip-reports")
									.setValueSerializationSchema(new JsonSerializationSchema<TripReport>())
									.build())
					.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
					.build();

/*
			DataStream<Fraud> largeOrdersStream = stream
					.filter(order -> order.getAmount() > 10)
					.map(new FraudEnrichment());

			DataStream<Fraud> highRateOrdersStream =
					stream.keyBy(order -> order.getCustomer().getId())
							.window(TumblingEventTimeWindows.of(Time.seconds(20)))
							.trigger(CountTrigger.of(10))
							.process(new RateWindowFunction());

			DataStream<Fraud> multipleLocationsOrderStream = stream
					.keyBy(order -> order.getCustomer().getId())
					.process(new MultipleLocationFunction());



			highRateOrdersStream.sinkTo(sink);
			largeOrdersStream.sinkTo(sink);
			multipleLocationsOrderStream.sinkTo(sink);


 */
			reportStream.sinkTo(tripsink);


			env.execute("Flink Streaming Java API Skeleton");
		}
	}
}

