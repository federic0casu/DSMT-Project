package it.unipi.dsmt;

import io.jenetics.jpx.*;

import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.utility.Params;
import it.unipi.dsmt.models.GeoLocalizationEvent;
import it.unipi.dsmt.utility.KafkaProducerFactory;

import lombok.AllArgsConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

@AllArgsConstructor
class CarTask implements Runnable {
    public Car car;
    public Path gpxPath;
    private static final Logger logger = LoggerFactory.getLogger(CarTask.class);
    private static final KafkaProducerFactory factory = new KafkaProducerFactory();

    @Override
    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        try {
            var waitingTime = random.nextInt(60) ;
            logger.info("Car {}: starting in {} s...", car.getVin(), waitingTime);
            Thread.sleep(waitingTime * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Create Kafka producer
        try (var producer = factory.createKafkaProducer()) {
            GPX gpxFile = GPX.read(gpxPath);

            if (gpxFile != null) {
                // DEBUG
                logger.info("Started trip {} for car {}", gpxPath.getFileName(), car.getVin());
                // DEBUG

                var points = gpxFile
                        .tracks()
                        .flatMap(Track::segments)
                        .flatMap(TrackSegment::points)
                        .toList();

                // START EVENT
                var firstPoint = points.get(0);
                sendEvent(producer,
                        this.car,
                        firstPoint.getLatitude(),
                        firstPoint.getLongitude(),
                        GeoLocalizationEvent.Type.START,
                        System.currentTimeMillis());

                // MOVING EVENTS
                boolean first = true;
                var previous = points.get(0);
                for (var p : points) {
                    if (!first) {
                        double distance =
                                haversineDistance(previous.getLatitude().doubleValue(),
                                        previous.getLongitude().doubleValue(),
                                        p.getLatitude().doubleValue(),
                                        p.getLongitude().doubleValue());
                        double timeMillis = (distance / Params.AVG_SPEED) * 60 * 60 * 1000; // time in milliseconds
                        double v = timeMillis > 1 ? (timeMillis * 0.125) : (timeMillis * 0.5);
                        double min = timeMillis - v;
                        double max = timeMillis + v;
                        //long waitingTime = random.nextInt((int) (max - min + 1)) + (int) min;
                        long waitingTime = (long) random.nextInt((int) (max - min + 1)) + (int) min;

                        // Calculate current speed, avoiding division by zero
                        double currentSpeed = distance / (waitingTime / (60.0 * 60.0 * 1000.0));

                        // DEBUG
                        logger.info("Car {} >> current speed = {} [km/h], simulated time = {} [s]",
                                car.getVin(),
                                currentSpeed,
                                (float)waitingTime / 1000 // simulated time to cover distance in seconds
                        );

                        // DEBUG
                        try {
                            Thread.sleep(waitingTime);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        sendEvent(producer,
                                this.car,
                                p.getLatitude(),
                                p.getLongitude(),
                                GeoLocalizationEvent.Type.MOVE,
                                System.currentTimeMillis()
                                );
                        previous = p;
                    } else
                        first = false;
                }

                // END EVENT
                var lastPoint = points.get(points.size() - 1);
                sendEvent(producer,
                        this.car,
                        lastPoint.getLatitude(),
                        lastPoint.getLongitude(),
                        GeoLocalizationEvent.Type.END,
                        System.currentTimeMillis()
                );

                // DEBUG
                logger.info("Ended trip {} for car {}", gpxPath.getFileName(), car.getVin());
                // DEBUG
            } else {
                logger.error("No file found for {}", gpxPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void sendEvent(KafkaProducer<String, String> producer,
                           Car car, Latitude lat, Longitude lon, GeoLocalizationEvent.Type type, long ts) {
        GeoLocalizationEvent event = new GeoLocalizationEvent(car, lat, lon, type, ts);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Params.TOPIC,
                    new ObjectMapper().writeValueAsString(event));

            System.out.println("sent: " + new ObjectMapper().writeValueAsString(event) + '\n');


            producer.send(record, (metadata, exception) -> {
//                if (exception == null)
//                    logger.info("Car {} >> Sent event: {} - timestamp: {}",
//                            car.getVin(),
//                            type,
//                            ts);
            });

            producer.flush();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            logger.error(e.toString());
        }
    }
    private static double haversineDistance(double latA, double lonA, double latB, double lonB) {
        // Convert latitude and longitude from degrees to radians
        latA = Math.toRadians(latA);
        lonA = Math.toRadians(lonA);
        latB = Math.toRadians(latB);
        lonB = Math.toRadians(lonB);

        // Calculate differences
        double dLat = latB - latA;
        double dLon = lonB - lonA;

        // Haversine formula
        double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(latA) * Math.cos(latB) * Math.pow(Math.sin(dLon / 2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        // Distance in kilometers
        return Params.EARTH_RADIUS * c;
    }
}
