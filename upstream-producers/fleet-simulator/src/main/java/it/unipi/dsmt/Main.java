package it.unipi.dsmt;

import it.unipi.dsmt.models.Car;

import net.datafaker.Faker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    final static int CARS_NUMBER = 10;

    public static void main(String[] args) {
        var carsMap = generateCars();
        var gpxQueue = loadGpxFiles();

        try {
            ExecutorService executor = Executors.newFixedThreadPool(CARS_NUMBER);

            for (Car car : carsMap.values()) {
                if(gpxQueue.isEmpty()){
                    // if the queue is empty we already assigned every gpx to a car > restart
                    gpxQueue = loadGpxFiles();
                }

                Runnable carTask = new CarTask(car, gpxQueue.poll());
                executor.execute(carTask);
            }

            executor.shutdown();
        } catch (Exception e) {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private static Queue<Path> loadGpxFiles() {
        Queue<Path> gpxQueue = new PriorityQueue<>();
        logger.info("========== Loading GPX files .... ==========");
        try {
            ClassLoader classLoader = Main.class.getClassLoader();
            Path folderPath = Paths.get(Objects.requireNonNull(classLoader.getResource("")).toURI());

            if (Files.isDirectory(folderPath)) {

                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(folderPath, "*.gpx")) {
                    for (Path filePath : directoryStream) {
                        gpxQueue.add(filePath);
                    }
                }

                gpxQueue.forEach((v) -> logger.info("Loaded file: {}", v));
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }

        logger.info("========== Loaded GPX files ==========");
        return gpxQueue;
    }

    private static Map<String, Car> generateCars() {
        logger.info("========== Generating {} cars .... ==========%n", CARS_NUMBER);

        var faker = new Faker();

        Stream<Car> fakeCars =
                faker.stream(
                                () -> Car.builder()
                                        .vin(faker.vehicle().vin())
                                        .model(faker.vehicle().model())
                                        .color(faker.vehicle().color())
                                        .manufacturer(faker.vehicle().manufacturer())
                                        .fuelType(faker.vehicle().fuelType())
                                        .build())
                        .maxLen(CARS_NUMBER)
                        .generate();

        Map<String, Car> carsMap = fakeCars.collect(Collectors.toMap(Car::getVin, car -> car));


        carsMap.forEach((vin, car) -> logger.info("Vin: {} - Car: {}", vin, car));

        logger.info("========== Cars generated ==========");

        return carsMap;
    }
}