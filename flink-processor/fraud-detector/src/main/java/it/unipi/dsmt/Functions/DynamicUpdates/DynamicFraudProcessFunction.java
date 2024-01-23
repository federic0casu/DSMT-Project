package it.unipi.dsmt.Functions.DynamicUpdates;

import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Keyed;
import it.unipi.dsmt.Models.Order;
import it.unipi.dsmt.Models.Rule;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskexecutor.ShuffleDescriptorsCache;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Set;

import static it.unipi.dsmt.Utils.StateUtils.addToStateValuesSet;

public class DynamicFraudProcessFunction extends KeyedBroadcastProcessFunction<String,
        Keyed<Order,String,Integer>, Rule, Fraud> {

    // window state contains the order collected in the processing window
    private MapState<Long, Set<Order>> windowState;


    @Override
    public void processElement(Keyed<Order, String, Integer> value, ReadOnlyContext ctx, Collector<Fraud> out) throws Exception {
        long currentEventTime = value.getWrapped().getTimestamp();
        // TODO: Qua si aggiungono gli ordini che mi arrivano nello stato della finestra
        addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

        Rule rule = ctx.getBroadcastState(new MapStateDescriptor<>("rules",
                Integer.class,Rule.class)).get(value.getId());

        Long windowStartTimestampForEvent = rule.getWindowStartTimestampFor(currentEventTime);// <--- (3)

        SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);            // <--- (4)
        for (Long stateEventTime : windowState.keys()) {
            if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
                aggregateValuesInState(stateEventTime, aggregator, rule);
            }
        }
    }

    @Override
    public void processBroadcastElement(Rule value, Context ctx, Collector<Fraud> out) throws Exception {

    }
}
