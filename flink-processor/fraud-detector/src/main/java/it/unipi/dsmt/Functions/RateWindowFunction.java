package it.unipi.dsmt.Functions;

import it.unipi.dsmt.Models.Fraud;
import it.unipi.dsmt.Models.Order;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RateWindowFunction extends ProcessWindowFunction<Order, Fraud,
        String,TimeWindow> {
    @Override
    public void process(String key, Context ctx, Iterable<Order> elements, Collector<Fraud> out) throws Exception {
        out.collect(new Fraud(key, Fraud.FraudType.SHORT_PERIOD));
    }
}