package it.unipi.dsmt.websockets;

import it.unipi.dsmt.Kafka.KafkaUtils;
import it.unipi.dsmt.models.Car;
import it.unipi.dsmt.DTO.GeoLocalizationDTO;
import it.unipi.dsmt.serializers.GeoLocalizationDTOEncoder;
import it.unipi.dsmt.utility.Params;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@ServerEndpoint(value = "/events/geo-localization", encoders = GeoLocalizationDTOEncoder.class)
public class GeoLocalizationEndpoint implements EventEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocalizationEndpoint.class);

    // List to store WebSocket sessions. It is synchronized to handle concurrent access.
    private static final List<Session> sessions = Collections.synchronizedList(new ArrayList<>());

    // Executor service for managing Kafka consumer tasks
    private static final ExecutorService executorService = Executors.newFixedThreadPool(Params.THREADS + 1);

    // List to store futures of Kafka consumer tasks
    private static final List<Future<?>> consumerFutures = new ArrayList<>();

    private static int currentActivePartitions = Params.PARTITIONS_PER_TOPIC;

    static {
        for (int i = 0; i < currentActivePartitions; i++) {
            // Create a new Kafka consumer
            KafkaConsumer<String, String> consumer =
                    KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);

            // Create a new task to consume Kafka messages
            Runnable kafkaTask = () -> {
                KafkaUtils.consumeKafkaMessages(consumer, sessions, GeoLocalizationDTO.class);
            };

            // Submit the Kafka task to the executor service and store the future
            Future<?> future = executorService.submit(kafkaTask);
            consumerFutures.add(future);
        }

        // Create a new task to dynamically handle KafkaConsumers
        Runnable handleKafkaConsumer = () -> {
            while(true) {
                int numPartitions = 0;

                try (AdminClient adminClient = KafkaUtils.createKafkaAdmin()) {
                    DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(
                            Collections.singleton(Params.TOPIC_CARS),
                            new DescribeTopicsOptions().timeoutMs(5000)); // Timeout set to 5 seconds

                    Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
                    KafkaFuture<TopicDescription> topicDescription = values.get(Params.TOPIC_CARS);

                    numPartitions = topicDescription.get().partitions().size();

                    // DEBUG
                    logger.info("KafkaAdmin >> number of active partitions for {}: {}", Params.TOPIC_CARS, numPartitions);
                    // DEBUG
                } catch (InterruptedException | ExecutionException e) {
                    continue;
                }

                if  (numPartitions > currentActivePartitions) {
                    int difference = numPartitions - currentActivePartitions;
                    currentActivePartitions = numPartitions;

                    for (var i = 0; i < difference; i++) {
                        // Create a new Kafka consumer
                        KafkaConsumer<String, String> consumer =
                                KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);

                        // Create a new task to consume Kafka messages
                        Runnable kafkaTask = () -> {
                            KafkaUtils.consumeKafkaMessages(consumer, sessions, GeoLocalizationDTO.class);
                        };

                        // Submit the Kafka task to the executor service and store the future
                        Future<?> future = executorService.submit(kafkaTask);
                        consumerFutures.add(future);
                    }
                } else {
                    int difference = currentActivePartitions - numPartitions;
                    currentActivePartitions = numPartitions;

                    for (var i = 0; i < difference; i++) {
                        Future<?> toStop = consumerFutures.removeFirst();
                        toStop.cancel(true);
                    }
                }
            }
        };

        // Submit the Kafka task to the executor service and store the future
        Future<?> future = executorService.submit(handleKafkaConsumer);
    }
    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);

        // Sending the geo-localization of the headquarters to the newly opened session
        sendHeadquartersGeoLocalization(session);

        // Check and restart completed Kafka consumer tasks
        restartConsumers();

        // DEBUG
        logger.info("WebSocket Session OPENED (ID={})", session.getId());
        // DEBUG
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);

        // Check and restart completed Kafka consumer tasks
        restartConsumers();

        // DEBUG
        logger.info("WebSocket Session CLOSED (ID={})", session.getId());
        // DEBUG
    }
    private static void sendHeadquartersGeoLocalization(Session session) {
        GeoLocalizationDTO headquarters = new GeoLocalizationDTO(
                new Car("HEADQUARTERS", "/", "/", "/", "/"),
                45.442998228,
                9.273665572,
                GeoLocalizationDTO.Type.HEADQUARTER);
        KafkaUtils.send(session, headquarters);
    }
    private void restartConsumers() {
        // Iterate through the consumer futures list
        synchronized (consumerFutures) {
            for (Future<?> future : consumerFutures) {
                // Check if the future is done/completed
                if (future.isDone()) {
                    // Remove the completed future from the list
                    consumerFutures.remove(future);

                    // Create a new Kafka consumer
                    KafkaConsumer<String, String> consumer = KafkaUtils.createKafkaConsumer(Params.TOPIC_CARS, Params.CARS_DATA_GROUP);

                    // Create a new task to consume Kafka messages
                    Runnable kafkaTask = () -> {
                        KafkaUtils.consumeKafkaMessages(consumer, sessions, GeoLocalizationDTO.class);
                    };

                    // Submit the Kafka task to the executor service and store the future
                    Future<?> newFuture = executorService.submit(kafkaTask);
                    consumerFutures.add(newFuture);
                }
            }
        }
    }
}
