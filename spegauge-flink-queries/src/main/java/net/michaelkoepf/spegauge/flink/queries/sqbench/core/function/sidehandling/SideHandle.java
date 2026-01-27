package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;

import java.util.List;

/**
 * Wiring details for one operator side (cache + keyed state).
 */
public final class SideHandle<K, T> {
    public final KeyAccessibleState<K, List<T>> state;
    public SideHandle(KeyAccessibleState<K, List<T>> state) {
        this.state = state;
    }

    public static long mergedKeyStateId(long key, StreamSide side) {
        return (key << 1) | (side == StreamSide.FIRST ? 1L : 0L);
    }
}
