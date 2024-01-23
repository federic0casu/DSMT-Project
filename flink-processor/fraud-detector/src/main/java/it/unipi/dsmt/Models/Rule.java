package it.unipi.dsmt.Models;

import lombok.AllArgsConstructor;
import lombok.Data;

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
}
