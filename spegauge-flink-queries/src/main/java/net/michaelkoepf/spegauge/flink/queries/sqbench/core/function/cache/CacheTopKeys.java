package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

public class CacheTopKeys{}

/*import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheTopKeys implements Cache{
    private int maxCacheSize; // in number of tuples
    private int cacheSize; // in number of tuples

    private long maxKey = 100000019;

    private long minEvictedKey = maxKey + 1;

    HashMap<Long, List<Tuple2<EntityRecordFull, Long>>> entries;

    public CacheTopKeys(int cacheSize) {
        this.maxCacheSize = cacheSize;
        this.cacheSize = 0;
        //this.maxKey = Long.MIN_VALUE;
        entries = new HashMap<>();
        //minEvictedKey = Long.MAX_VALUE;
    }

    @Override
    public List<Tuple2<Long, List<Tuple2<EntityRecordFull, Long>>>> addAndEvictIfFull(
            long key, Tuple2<EntityRecordFull, Long> value, long currentTime) {
        entries.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        //if (key > maxKey) {
            //maxKey = key;
        //}
        cacheSize++;

        return null;//evictIfFull();
    }

    @Override
    public List<Tuple2<Long, List<Tuple2<EntityRecordFull, Long>>>> insertEntry(
            long key, List<Tuple2<EntityRecordFull, Long>> values, long currentTime, boolean dirty) {
        throw new UnsupportedOperationException("This method is not implemented in CacheTopKeys.");
    }

    private List<Tuple2<Long, List<Tuple2<EntityRecordFull, Long>>>> evictIfFull() {
        if (cacheSize <= maxCacheSize) {
            return null;
        }
        List<Tuple2<Long, List<Tuple2<EntityRecordFull, Long>>>> evictedEntries = new ArrayList<>();
        while (cacheSize >= maxCacheSize) {
            long newMaxKey = Long.MIN_VALUE;
            Map.Entry<Long, List<Tuple2<EntityRecordFull, Long>>> removeEntry = null;
            for (Map.Entry<Long, List<Tuple2<EntityRecordFull, Long>>> entry : entries.entrySet()) {
                long k = entry.getKey();
                if (k > newMaxKey) {
                    if (k == maxKey) {
                        removeEntry = entry;
                    } else {
                        newMaxKey = k;
                    }
                }
            }
            entries.remove(maxKey);
            cacheSize -= removeEntry.getValue().size();
            if (maxKey < minEvictedKey) {
                minEvictedKey = maxKey;
            }
            maxKey = newMaxKey;
            if (removeEntry.getValue().size() > 0) {
                evictedEntries.add(Tuple2.of(removeEntry.getKey(), removeEntry.getValue()));
            }
        }
        return evictedEntries;
    }

    public List<Tuple2<EntityRecordFull, Long>> computeIfAbsent(long key, long timestamp) {
        List<Tuple2<EntityRecordFull, Long>> state = entries.get(key);
        if (state == null) {
            state = new ArrayList<>();
            entries.put(key, state);
        }
        return state;
    }

    @Override
    public void removeExpired(long key, long minTimestamp) {
        List<Tuple2<EntityRecordFull, Long>> allElements = new ArrayList<>();
        List<Tuple2<EntityRecordFull, Long>> stateOfKey = entries.get(key);
        for (Tuple2<EntityRecordFull, Long> entry : stateOfKey) {
            if (entry.f1 >= minTimestamp) {
                allElements.add(entry);
            }
        }

        // keys remain in the cache even if there are no active tuples.
        entries.put(key, allElements);

        cacheSize = cacheSize - stateOfKey.size() + allElements.size();
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    @Override
    public boolean hasSpace() {
        return cacheSize < maxCacheSize;
    }

    @Override
    public boolean keyShouldBeInCache(long key) {
        return key <= maxKey;
    }

    private boolean keyShouldBeInBackend(long key) {
        return key >= minEvictedKey;
    }

    public long getMaxKey() {
        return maxKey;
    }

    public long getMinEvictedKey() {
        return minEvictedKey;
    }

    @Override
    public boolean tupleShouldBeInsertedInCache(long key) {
        if (keyShouldBeInCache(key)) {
            return true;
        }
        else if (keyShouldBeInBackend(key)){
            return false;
        }
        else if (hasSpace()){
            return true;
        }
        else {
            return false;
        }
    }
}*/
