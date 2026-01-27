package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;

import java.util.List;

public interface EventTypeHelper<K, T> {
    ValueStateDescriptor<List<T>> getStateDescriptor(String stateName);
    // returns true if elem1 should be ranked higher than elem2
    boolean compareElements(T elem1, T elem2);

    void fillStaticTable(KeyGroupRange keyGroupRange, int maxParallelism,
                                          KeyAccessibleState<K, List<T>> state, int bidsPerAuction);
}
