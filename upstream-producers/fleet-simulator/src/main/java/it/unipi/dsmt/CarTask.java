package it.unipi.dsmt;

import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.models.GeoLocalizationEvent;

import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.TrackSegment;

import lombok.AllArgsConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;

@AllArgsConstructor
class CarTask implements Runnable {
    public Car car;
    public Path gpxPath;
    private static final String TOPIC = "cars-data";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @Override
    public void run() {
        Random random = new Random();
        // Create Kafka producer
        try (var producer = createKafkaProducer()) {
            GPX gpxFile = GPX.read(gpxPath);

            if (gpxFile != null) {
                logger.info("Started trip {} for car {}", gpxPath.getFileName(), car.getVin());

                var points = gpxFile
                        .tracks()
                        .flatMap(Track::segments)
                        .flatMap(TrackSegment::points)
                        .toList();

                // START EVENT
                var firstPoint = points.get(0);
                var startEvent = new GeoLocalizationEvent(this.car, firstPoint.getLatitude(),
                        firstPoint.getLongitude(), GeoLocalizationEvent.Type.START);
                SendToKafka(producer, startEvent);

                // MOVING EVENTS
                points.forEach(p -> {
                    var carLog = new GeoLocalizationEvent(
                            this.car,
                            p.getLatitude(),
                            p.getLongitude(),
                            GeoLocalizationEvent.Type.MOVE);

                    try {
                        Thread.sleep(random.nextInt(1000));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    SendToKafka(producer, carLog);
                });

                // END EVENT
                var lastPoint = points.get(points.size() - 1);
                var endEvent = new GeoLocalizationEvent(
                        this.car,
                        lastPoint.getLatitude(),
                        lastPoint.getLongitude(),
                        GeoLocalizationEvent.Type.END);
                SendToKafka(producer, endEvent);

                logger.info("Ended trip {} for car {}", gpxPath.getFileName(), car.getVin());
            } else {
                logger.error("No file found for {}", gpxPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    private void SendToKafka(KafkaProducer<String, String> producer, GeoLocalizationEvent geoLocalizationEvent) {
        try {
            var objectMapper = new ObjectMapper();
            String carJson = objectMapper.writeValueAsString(geoLocalizationEvent);
            logger.info("Sending: {}", carJson);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, carJson);
            producer.send(record);
            producer.flush();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            logger.error(e.toString());
        }
    }
}
