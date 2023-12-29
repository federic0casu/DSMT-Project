package org.oncode;

import net.datafaker.Faker;
import org.oncode.models.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    final static int CUSTOMER_NUMBER = 10;

    public static void main(String[] args) {
        var customersMap = generateCustomers();


        try {
            ExecutorService executor = Executors.newFixedThreadPool(CUSTOMER_NUMBER);

            for (Customer c : customersMap.values()) {
                Runnable orderTask = new OrderTask(c);
                executor.execute(orderTask);
            }

            executor.shutdown();
        } catch (Exception e) {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Customer> generateCustomers() {
        logger.info("========== Generating {} customers .... ==========%n", CUSTOMER_NUMBER);

        var faker = new Faker();

        Stream<Customer> fakeCustomers = faker.stream(() ->
                Customer.builder()
                        .id(faker.internet().uuid())
                        .email(faker.internet().emailAddress())
                        .name(faker.name().fullName())
                        .country(faker.address().country())
                        .build())
                .maxLen(CUSTOMER_NUMBER)
                .generate();

        Map<String, Customer> customersMap = fakeCustomers.collect(Collectors.toMap(Customer::getId, customer -> customer));
        customersMap.forEach((id, customer) -> logger.info("Id: {} - Customer: {}", id, customer));

        logger.info("========== Customers generated ==========");

        return customersMap;
    }
}