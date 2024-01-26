package it.unipi.dsmt.Models;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.Collection;

@Data
@AllArgsConstructor
public class Rule {
    Integer ruleId;
    String ruleState;
    String groupingAttribute;
    String aggregationAttribute;
    String aggregationType;
    Integer windowLenght;

    public Long getWindowStartTimestampFor(long currentEventTime) {
        return currentEventTime - windowLenght;
    }

    public boolean apply(BigDecimal value) {
        switch (limitOperatorType) {
            case "EQUAL":
                return comparisonValue.compareTo(limit) == 0;
            case"NOT_EQUAL":
                return comparisonValue.compareTo(limit) != 0;
            case "GREATER":
                return comparisonValue.compareTo(limit) > 0;
            case "LESS":
                return comparisonValue.compareTo(limit) < 0;
            case "LESS_EQUAL":
                return comparisonValue.compareTo(limit) <= 0;
            case "GREATER_EQUAL":
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }

    public static class Descriptors {
        public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));
    }
}
