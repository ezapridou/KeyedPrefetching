package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;

public interface EventTypeHelper<T> {
    ListStateDescriptor<Tuple2<T, Long>> getStateDescriptor(String stateName);
    boolean elemOfTypeA(T element);
    boolean elemOfTypeB(T element);
}
