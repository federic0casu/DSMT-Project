package it.unipi.dsmt.DTO;


import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FraudEventDTO {
    private String customerId;
    private FraudType fraudType;


    // Enum for fraudType
    public enum FraudType {
        LARGE_TRANSACTION,
        SHORT_PERIOD,
        MULTIPLE_LOCATION
    }

    public FraudEventDTO (String customerId, FraudType type) {
        this.customerId = customerId;
        this.fraudType = type;
    }
}