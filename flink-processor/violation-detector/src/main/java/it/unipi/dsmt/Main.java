package it.unipi.dsmt;

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

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

// TEORIA
//Keyed Context: Poiché stai utilizzando una KeyedProcessFunction, il contesto è "chiaviato". Significa che tutte le
// operazioni eseguite all'interno della processElement o onTimer sono specifiche per la chiave corrente
// (il vin dell'auto in questo caso).


public class Main {
    public static void main(String[] args) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();

        // set up the streaming execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            // sorgente Kafka (KafkaSource) per leggere i dati dal topic "cars-data". I dati vengono
            // deserializzati in oggetti GeoLocalizationEvent utilizzando JsonDeserializationSchema
            KafkaSource<GeoLocalizationEvent> source = KafkaSource.<GeoLocalizationEvent>builder()
                    .setBootstrapServers("10.2.1.118:9092,10.2.1.119:9092")
                    .setTopics("cars-data")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new JsonDeserializationSchema<>(GeoLocalizationEvent.class))
                    .build();

            // gli  eventi sono poi inseriti in un flusso di dati (DataStream) in Flink, dove vengono
            // applicate diverse trasformazioni e processi.
            DataStream<GeoLocalizationEvent> stream = env.fromSource(source,
                    WatermarkStrategy
                            .<GeoLocalizationEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                            .withTimestampAssigner((event, timestamp) -> event.timestamp = Instant.now().toEpochMilli())
                    , "Kafka Source");

//            // Configurazione dell'ObjectMapper con i serializzatori personalizzati
//            ObjectMapper mapper = new ObjectMapper();
//            SimpleModule module = new SimpleModule();
//            module.addSerializer(SpeedingViolation.class, new SpeedingViolationSerializer());
//            module.addSerializer(InactivityViolation.class, new InactivityViolationSerializer());
//            mapper.registerModule(module);
//
//            // Creazione del KafkaRecordSerializationSchema personalizzato
//            KafkaRecordSerializationSchema<Violation> serializationSchema = new KafkaRecordSerializationSchema<Violation>() {
//                @Override
//                public ProducerRecord<byte[], byte[]> serialize(Violation element, KafkaSinkContext context, Long timestamp) {
//                    try {
//                        String json = mapper.writeValueAsString(element);
//                        return new ProducerRecord<>("violations", json.getBytes(StandardCharsets.UTF_8));
//                    } catch (JsonProcessingException e) {
//                        throw new RuntimeException("Failed to serialize Violation", e);
//                    }
//                }
//            };
//
//            // Configurazione del KafkaSink
//            KafkaSink<Violation> sink = KafkaSink.<Violation>builder()
//                    .setBootstrapServers("kafka:9093")
//                    .setRecordSerializer(serializationSchema)
//                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                    .build();


            KafkaSink<Violation> sink = KafkaSink.<Violation>builder()
                    .setBootstrapServers("10.2.1.118:9092,10.2.1.119:9092")
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

            // cast the streams to Violation
            // stai trasformando (map) ciascun elemento dei flussi di dati speedingViolationStream e
            // inactivityViolationStream da SpeedingViolation e InactivityViolation (rispettivamente) a Violation
            //
            // Quando scrivi .map(x -> (Violation) x), stai indicando che ogni elemento x del DataStream (dove x
            // è un SpeedingViolation o un InactivityViolation) dovrebbe essere trattato come un Violation.
            // Questo "casting" è in realtà solo un cambio di riferimento; non modifichi o alteri l'oggetto x.
            // Stai solo dicendo a Flink di trattare x come un Violation.
            DataStream<Violation> speedViolationStreamAsViolations = speedingViolationStream.map(x -> (Violation) x);
            DataStream<Violation> inactivityViolationStreamAsViolations = inactivityViolationStream.map(x -> (Violation) x);

            // publish the results to Kafka
            // Queste righe di codice collegano i flussi trasformati speedViolationStreamAsViolations e
            // inactivityViolationStreamAsViolations al sink Kafka (sink).
            // (Il sink Kafka è configurato per inviare dati al topic "violations" su un server Kafka).
            speedViolationStreamAsViolations.sinkTo(sink);
            inactivityViolationStreamAsViolations.sinkTo(sink);
            

            env.setParallelism(2);
            env.execute("\"Fleet Monitoring - Speeding and Inactivity Violation Detector");
        }
    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        // Configurazioni di default sono generalmente sufficienti.
        return properties;
    }
}
