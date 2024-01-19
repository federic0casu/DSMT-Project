public class FraudEventDTO {

    String customerId;
    FraudType fraudType;
    public enum FraudType {
        LARGE_TRANSACTION,
        SHORT_PERIOD,
        MULTIPLE_LOCATION
    }
}