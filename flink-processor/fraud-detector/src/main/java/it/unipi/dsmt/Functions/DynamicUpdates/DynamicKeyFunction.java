package it.unipi.dsmt.Functions.DynamicUpdates;

import it.unipi.dsmt.Models.Keyed;
import it.unipi.dsmt.Models.Order;
import it.unipi.dsmt.Models.Rule;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class DynamicKeyFunction extends BroadcastProcessFunction
        <Order, Rule, Keyed<Order, String, Integer>> {

    // data structure that holds rules
    public static MapStateDescriptor<Integer, Rule> RULES_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("rules", Integer.class, Rule.class);

    // used for remembering which is the wides window to use
    public static Integer WIDEST_RULE_KEY = 1337;


    @Override
    public void processElement(Order order, ReadOnlyContext ctx, Collector<Keyed<Order,
            String, Integer>> out) throws Exception {

        // ottengo le regole
        ReadOnlyBroadcastState<Integer, Rule> rulesState =
                ctx.getBroadcastState(RULES_STATE_DESCRIPTOR);

        // itero sulle regole e mando in output un evento per ogni regola che ho stabilito
        for (Map.Entry<Integer,Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue();
            out.collect(new Keyed<>(order,rule.getGroupingAttribute(),rule.getRuleId()));
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Keyed<Order, String, Integer>> out) throws Exception {
        BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(RULES_STATE_DESCRIPTOR);
        // quando mi sta arrivando una nuova regola la aggiungo al broadcast state
        broadcastState.put(rule.getRuleId(), rule);
        updateWidestWindowRule(rule, broadcastState);
    }

    private void updateWidestWindowRule(Rule rule, BroadcastState<Integer, Rule> broadcastState) throws Exception {
        Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);

        if (widestWindowRule == null) {
            // set the rule as new widest window rule
            broadcastState.put(WIDEST_RULE_KEY,rule);
            return;
        }

        // check if the new rule has bigger window lenght
        if (widestWindowRule.getWindowLenght() < rule.getWindowLenght()) {
            broadcastState.put(WIDEST_RULE_KEY, rule);
        }
    }
}
