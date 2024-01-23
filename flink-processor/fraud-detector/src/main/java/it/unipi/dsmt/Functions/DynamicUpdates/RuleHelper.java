
package it.unipi.dsmt.Functions.DynamicUpdates;


import java.math.BigDecimal;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/* Collection of helper methods for Rules. */
public class RuleHelper {

    /* Picks and returns a new accumulator, based on the Rule's aggregator function type. */
    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregatorFunctionType()) {
            case SUM:
                return new BigDecimalCounter();
            case AVG:
                return new AverageAccumulator();
            case MAX:
                return new BigDecimalMaximum();
            case MIN:
                return new BigDecimalMinimum();
            default:
                throw new RuntimeException(
                        "Unsupported aggregation function type: " + rule.getAggregatorFunctionType());
        }
    }
}