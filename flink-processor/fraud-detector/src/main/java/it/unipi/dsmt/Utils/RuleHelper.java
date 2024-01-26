package it.unipi.dsmt.Utils;

import java.math.BigDecimal;

import it.unipi.dsmt.Utils.Accumulators.BigDecimalCounter;
import it.unipi.dsmt.Models.Rule;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/* Collection of helper methods for Rules. */
public class RuleHelper {
    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregationType()) {
            case "SUM":
                return new BigDecimalCounter();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregationType());
        }
    }
}