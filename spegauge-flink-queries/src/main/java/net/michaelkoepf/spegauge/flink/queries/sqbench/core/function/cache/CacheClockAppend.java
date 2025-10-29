package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import org.apache.flink.api.java.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class CacheClockAppend<K, T extends Tuple> extends CacheClock<K, T> implements CacheAppend<K, T> {
    private final int positionOfTs;

    public CacheClockAppend(int cacheSize, boolean singleInput, int positionOfTs) {
        super(cacheSize, singleInput);
        this.positionOfTs = positionOfTs;
    }

    public void removeExpired(K key, long minTimestamp) {
        ClockCacheEntry entry = entries.get(key);
        if (entry == null) return;

        int oldSize = entry.value.size();
        List<T> filtered = new ArrayList<>();

        for (T tuple : entry.value) {
            if ((long)tuple.getField(positionOfTs) >= minTimestamp) {
                filtered.add(tuple);
            }
        }

        if (filtered.isEmpty()) {
            entries.remove(key);
            clockHand.remove(key);
            if (handIndex >= clockHand.size()) {
                handIndex = 0;
            }
        } else {
            entry.value = filtered;
        }

        cacheSize = cacheSize - oldSize + filtered.size();
    }
}
