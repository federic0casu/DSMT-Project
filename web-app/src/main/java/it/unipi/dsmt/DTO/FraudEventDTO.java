package it.unipi.dsmt.DTO;


import it.unipi.dsmt.models.Customer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class FraudEventDTO {
    private String customerId;
    private Customer customer;
    private FraudType fraudType;


    // Enum for fraudType
    public enum FraudType {
        LARGE_TRANSACTION,
        SHORT_PERIOD,
        MULTIPLE_LOCATION
    }

}