package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import org.apache.flink.api.java.tuple.Tuple;

import java.util.Iterator;
import java.util.List;

public class CacheLRUAppend<K, T extends Tuple> extends CacheLRU<K, T> implements CacheAppend<K, T> {
    private final int positionOfTs;

    public CacheLRUAppend(int cacheSize, boolean singleInput, int positionOfTs) {
        super(cacheSize, singleInput);
        this.positionOfTs = positionOfTs;
    }

    public void removeExpired(K key, long minTimestamp) {
        LRUCacheEntry entry = entries.peek(key);

        List<T> val = entry.value;
        if (val == null) {
            entries.remove(key);
            return;
        }

        Iterator<T> iter = val.iterator();
        int removed = 0;
        while (iter.hasNext()) {
            T tuple = iter.next();
            if ((long)tuple.getField(positionOfTs) < minTimestamp) {
                iter.remove();
                removed++;
            }
        }
        if (entry.value.isEmpty()) {
            entries.remove(key);
            //order.remove(key);
        }
        cacheSize -= removed;
    }
}
