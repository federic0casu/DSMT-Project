package it.unipi.dsmt.DTO;


import it.unipi.dsmt.models.Customer;
import lombok.Data;

@Data
public class FraudEventDTO {
    public Long timestamp;
    public String customerId;
    public Customer customer;
    public FraudType fraudType;


    public FraudEventDTO() {}
    public FraudEventDTO (Long timestamp, String customerId, Customer customer,
                          FraudType type) {
        this.timestamp = timestamp;
        this.customerId = customerId;
        this.customer = new Customer(customer.getId(),customer.getName(),
                customer.getEmail(),customer.getCountry());
        this.fraudType = type;
    }


    // Enum for fraudType
    public enum FraudType {
        LARGE_TRANSACTION,
        SHORT_PERIOD,
        MULTIPLE_LOCATION
    }
}