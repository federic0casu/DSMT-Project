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
import java.util.Random;

@AllArgsConstructor
class CarTask implements Runnable {
    public Car car;
    public Path gpxPath;
    private static final Logger logger = LoggerFactory.getLogger(CarTask.class);
    private static final KafkaProducerFactory factory = new KafkaProducerFactory();

    @Override
    public void run() {
        Random random = new Random();

        try {
            Thread.sleep(random.nextInt(10) * 1000);
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
                        GeoLocalizationEvent.Type.START);

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
                        double min = timeMillis - (timeMillis * 0.25);
                        double max = timeMillis + (timeMillis * 0.25);
                        long waitingTime = (long) (Math.abs(random.nextDouble() * (max - min) + min));

                        // Calculate current speed, avoiding division by zero
                        double currentSpeed = (waitingTime != 0) ?
                                distance / (waitingTime / (60.0 * 60.0 * 1000.0)) : Double.POSITIVE_INFINITY;

                        // DEBUG
                        logger.info("Car {} >> current speed = {} [km/h], simulated time = {} [s]",
                                car.getVin(),
                                currentSpeed,
                                waitingTime / 1000); // simulated time to cover distance in seconds
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
                                GeoLocalizationEvent.Type.MOVE);
                    } else
                        first = false;
                }

                // END EVENT
                var lastPoint = points.get(points.size() - 1);
                sendEvent(producer,
                        this.car,
                        lastPoint.getLatitude(),
                        lastPoint.getLongitude(),
                        GeoLocalizationEvent.Type.END);

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
                           Car car, Latitude lat, Longitude lon, GeoLocalizationEvent.Type type) {
        GeoLocalizationEvent event = new GeoLocalizationEvent(car, lat, lon, type);
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Params.TOPIC,
                    new ObjectMapper().writeValueAsString(event));
            producer.send(record);
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
