package it.unipi.dsmt.Utils;

import org.apache.flink.api.common.state.MapState;

import java.util.HashSet;
import java.util.Set;

public class StateUtils {
    public static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
            throws Exception {
        Set<V> valuesSet = mapState.get(key);
        if (valuesSet != null) {
            valuesSet.add(value);
        } else {
            valuesSet = new HashSet<>();
            valuesSet.add(value);
        }
        mapState.put(key, valuesSet);
        return valuesSet;
    }
}
