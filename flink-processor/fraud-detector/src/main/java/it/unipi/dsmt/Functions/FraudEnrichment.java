package it.unipi.dsmt.Functions;

import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Order;
import org.apache.flink.api.common.functions.MapFunction;

public class FraudEnrichment implements MapFunction<Order, Fraud> {
    @Override
    public Fraud map(Order order) throws Exception {
        return new Fraud(order.getCustomer().getId(), Fraud.FraudType.LARGE_TRANSACTION);
    }
}
