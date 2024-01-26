package it.unipi.dsmt.Functions.DynamicUpdates;

import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Keyed;
import it.unipi.dsmt.Models.Order;
import it.unipi.dsmt.Models.Rule;
import it.unipi.dsmt.Utils.FieldsExtractor;
import it.unipi.dsmt.Utils.RuleHelper;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Set;

import static it.unipi.dsmt.Functions.DynamicUpdates.DynamicKeyFunction.WIDEST_RULE_KEY;
import static it.unipi.dsmt.Utils.StateUtils.addToStateValuesSet;

public class DynamicFraudProcessFunction extends KeyedBroadcastProcessFunction<String,
        Keyed<Order,String,Integer>, Rule, Fraud> {

    private static final String COUNT = "COUNT";

    // window state contains the order collected in the processing window
    private MapState<Long, Set<Order>> windowState;


    @Override
    public void processElement(Keyed<Order, String, Integer> value, ReadOnlyContext ctx, Collector<Fraud> out) throws Exception {
        long currentEventTime = value.getWrapped().getTimestamp();

        // TODO: Qua si aggiungono gli ordini che mi arrivano nello stato della finestra
        addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

        Rule rule =
                ctx.getBroadcastState(Rule.Descriptors.rulesDescriptor).get(value.getId());

        Long windowStartTimestampForEvent = rule.getWindowStartTimestampFor(currentEventTime);

        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
        for (Long stateEventTime : windowState.keys()) {
            if (isStateValueInWindow(stateEventTime, windowStartTimestampForEvent,
                    currentEventTime)) {
                aggregateValuesInState(stateEventTime, aggregator, rule);
            }
        }
        BigDecimal aggregateValue = aggregator.getLocalValue();
        boolean ruleResult = rule.apply(aggregateValue);

    }

    private void aggregateValuesInState(
            Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
        Set<Order> inWindow = windowState.get(stateEventTime);
        if (COUNT.equals(rule.getAggregationType())) {
            for (Order event : inWindow) {
                aggregator.add(BigDecimal.ONE);
            }
        } else {
            for (Order event : inWindow) {
                BigDecimal aggregatedValue =
                        FieldsExtractor.getBigDecimalByName(rule.getAggregationAttribute(), event);
                aggregator.add(aggregatedValue);
            }
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Fraud> out) throws Exception {
        BroadcastState<Integer, Rule> broadcastState =
                ctx.getBroadcastState(Rule.Descriptors.rulesDescriptor);

        // update the rule descriptor
        broadcastState.put(rule.getRuleId(),rule);
        updateWidestWindowRule(rule,broadcastState);
    }


    // Utility functions
    private boolean isStateValueInWindow(Long stateEventTime,
                                         Long windowStartTimestampForEvent,
                                         Long currentEventTime) {

        if (stateEventTime >= windowStartTimestampForEvent && stateEventTime < currentEventTime)
            return true;
        return false;
    }

    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState)
            throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

        if (widestWindowRule == null) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
            return;
        }

        if (widestWindowRule.getWindowLenght() < rule.getWindowLenght()) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
        }
    }
}
