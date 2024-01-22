package it.unipi.dsmt.Models;

public class Fraud {
    private String customerId;
    private FraudType fraudType;



    public Fraud (String customerId, FraudType type) {
        this.customerId = customerId;
        this.fraudType = type;
    }

    // Getter for customerId
    public String getCustomerId() {
        return customerId;
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