package it.unipi.dsmt;

import org.apache.flink.api.common.functions.MapFunction;


public class UppercaseMapper implements MapFunction<String, String> {
    @Override
    public String map(String value) {
        return value.toUpperCase();
    }
}
