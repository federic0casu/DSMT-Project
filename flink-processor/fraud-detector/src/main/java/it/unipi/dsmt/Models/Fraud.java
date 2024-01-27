package it.unipi.dsmt.Models;




public class Fraud {
    private Long timestamp;
    private String customerId;
    private Customer customer;
    private FraudType fraudType;

    public Fraud() {}

    public Fraud (Long timestamp, String customerId, Customer customer,FraudType type) {
        this.timestamp = timestamp;
        this.customerId = customerId;
        this.customer = new Customer(customer.getId(),customer.getName(),
                customer.getEmail(),customer.getCountry());
        this.fraudType = type;
    }

    // Getter for customerId
    public String getCustomerId() {
        return customerId;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    // Setter for customerId
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    // Getter for fraudType
    public FraudType getFraudType() {
        return fraudType;
    }

    // Setter for fraudType
    public void setFraudType(FraudType fraudType) {
        this.fraudType = fraudType;
    }

    // Enum for fraudType
    public enum FraudType {
        LARGE_TRANSACTION,
        SHORT_PERIOD,
        MULTIPLE_LOCATION
    }
}