package it.unipi.dsmt.Functions;

import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.LocationState;
import it.unipi.dsmt.Models.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.time.Duration;

public class MultipleLocationFunction
        extends KeyedProcessFunction<String, Order, Fraud> {

    // state
    private ValueState<LocationState> state;
    private static int TIME_THRESHOLD= 20;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "myLocationState", LocationState.class));
    }

    @Override
    public void processElement(Order value,
                               KeyedProcessFunction<String, Order, Fraud>.Context ctx,
                               Collector<Fraud> out) throws Exception {

        // if it is the first time the process function is called we do not have to
        // perform checks, we only update state values for the next time
        LocationState current = state.value();
        if (current == null) {
            current = new LocationState();
            current.lastOrderLocation = value.getCustomer().getCountry();
            current.lastOrderTimestamp = ctx.timestamp();
            return;
        }

        if (!current.lastOrderLocation.equals(value.getCustomer().getCountry()) &&
            current.lastOrderTimestamp + Duration.ofSeconds(TIME_THRESHOLD).toMillis() > ctx.timestamp()) {
                // output a new Fraud
                out.collect(new Fraud(ctx.getCurrentKey(),value.getCustomer(),
                        Fraud.FraudType.MULTIPLE_LOCATION));
        }

        current.lastOrderLocation = value.getCustomer().getCountry();
        current.lastOrderTimestamp = ctx.timestamp();

        state.update(current);
    }
}
