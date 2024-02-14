package it.unipi.dsmt;

import it.unipi.dsmt.utility.Params;
import it.unipi.dsmt.functions.InactivityViolationFunction;
import it.unipi.dsmt.functions.SpeedingViolationFunction;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import it.unipi.dsmt.models.Violation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

// TEORIA
//Keyed Context: Poiché stai utilizzando una KeyedProcessFunction, il contesto è "chiaviato". Significa che tutte le
// operazioni eseguite all'interno della processElement o onTimer sono specifiche per la chiave corrente
// (il vin dell'auto in questo caso).


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // set up the streaming execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            // sorgente Kafka (KafkaSource) per leggere i dati dal topic "cars-data". I dati vengono
            // deserializzati in oggetti GeoLocalizationEvent utilizzando JsonDeserializationSchema
            KafkaSource<GeoLocalizationEvent> source = KafkaSource.<GeoLocalizationEvent>builder()
                    .setBootstrapServers(Params.LOCAL_KAFKA_BROKER)
                    .setTopics("cars-data")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new JsonDeserializationSchema<>(GeoLocalizationEvent.class))
                    .build();

            // gli  eventi sono poi inseriti in un flusso di dati (DataStream) in Flink, dove vengono
            // applicate diverse trasformazioni e processi.
            DataStream<GeoLocalizationEvent> stream = env.fromSource(source,
                    WatermarkStrategy
                            .<GeoLocalizationEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                            // non modifica o aggiorna il campo timestamp della tua classe GeoLocalizationEvent. La sua
                            // unica funzione è dire a Flink quale valore di timestamp utilizzare per la gestione del
                            // tempo dell'evento all'interno del sistema Flink, come per le operazioni di windowing o
                            // per determinare quando generare watermark.
                    , "Kafka Source");

            // sink Kafka (KafkaSink) per inviare i dati al topic "violations" su un server Kafka
            KafkaSink<Violation> sink = KafkaSink.<Violation>builder()
                    .setBootstrapServers(Params.LOCAL_KAFKA_BROKER)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic("violations")
                                    .setValueSerializationSchema(new JsonSerializationSchema<Violation>())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            /*
                Detecting speed violations
            */
            DataStream<Violation> speedingViolationStream = stream
                    .keyBy(event -> event.getCar().getVin())
                    .process(new SpeedingViolationFunction());

            /*
                Detecting inactivity violations
            */
            DataStream<Violation> inactivityViolationStream = stream
                    .keyBy(event -> event.getCar().getVin())
                    .process(new InactivityViolationFunction());

            // publish the results to Kafka
            // Queste righe di codice collegano i flussi trasformati speedViolationStreamAsViolations e
            // inactivityViolationStreamAsViolations al sink Kafka (sink).
            // (Il sink Kafka è configurato per inviare dati al topic "violations" su un server Kafka).
            speedingViolationStream.sinkTo(sink);
            inactivityViolationStream.sinkTo(sink);
            

            env.setParallelism(2);
            env.execute("\"Fleet Monitoring - Speeding and Inactivity Violation Detector");
        }
    }
}
