package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.Cache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.CompositeKeyInterpreter;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.KeyInterpreter;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SingleSideKeyInterpreter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class CacheClock<K, T> implements Cache<K, T> {

    protected class ClockCacheEntry {
        List<T> value;
        boolean referenceBit;
        boolean dirtyBit;

        ClockCacheEntry() {
            this.value = new ArrayList<>();
            this.referenceBit = true;
            this.dirtyBit = false;
        }

        ClockCacheEntry(List<T> value, boolean dirtyBit) {
            this.value = value;
            this.referenceBit = true;
            this.dirtyBit = dirtyBit;
        }

        void add(T newValue) {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(newValue);
            referenceBit = true;
            dirtyBit = true; // mark as dirty when adding a new value
        }

        void add(T newValue, int index) {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(index, newValue);
            referenceBit = true;
            dirtyBit = true; // mark as dirty when adding a new value
        }

        void remove(int index) {
            if (value != null) {
                value.remove(index);
            }
        }
    }

    protected int maxCacheSize; // in number of tuples
    protected int cacheSize; // in number of tuples

    protected Map<K, ClockCacheEntry> entries;
    protected List<K> clockHand;
    protected int handIndex;

    private final KeyInterpreter keyCodec;

    public CacheClock(int maxCacheSize, boolean singleInput) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = 0;
        this.entries = new HashMap<>(1000);
        this.clockHand = new ArrayList<>(1000);
        this.handIndex = 0;
        if (singleInput) {
            this.keyCodec = new SingleSideKeyInterpreter();
        } else {
            this.keyCodec = new CompositeKeyInterpreter();
        }
    }

    @Override
    public List<Tuple2<K, List<T>>> addAndReturnEvicted(K key, T value, long currentTime) {
        addValue(key, value);
        cacheSize++;

        return evictIfFull();
    }

    @Override
    public List<Tuple2<K, List<T>>> addAndReturnEvictedTopN(K key, T value, int index, long currentTime, int n) {
        addValueTopN(key, value, index, n);
        cacheSize++;

        return evictIfFull();
    }

    private void addValue(K key, T value) {
        if (entries.containsKey(key)) {
            entries.get(key).add(value);
        }
        else {
            entries.put(key, new ClockCacheEntry()).add(value);
        }
    }

    private void addValueTopN(K key, T value, int index, int n) {
        if (entries.containsKey(key)) {
            entries.get(key).add(value, index);
        }
        else {
            entries.put(key, new ClockCacheEntry()).add(value);
        }
        if (entries.get(key).value.size() > n) {
            entries.get(key).remove(entries.get(key).value.size() - 1);
            cacheSize--;
        }
    }

    @Override
    public List<Tuple2<K, List<T>>> insertEntry(K key, List<T> values, long currentTime, boolean dirtyBit) {
        assert !entries.containsKey(key) : "Key " + key + " already exists in cache.";
        entries.put(key, new ClockCacheEntry(values, dirtyBit));
        clockHand.add(key);
        cacheSize += values.size();

        return evictIfFull();
    }

    private List<Tuple2<K, List<T>>> evictIfFull() {
        long overflow = cacheSize - maxCacheSize;
        if (overflow <= 0) {
            return Collections.emptyList();
        }

        List<Tuple2<K, List<T>>> evictedEntries = new ArrayList<>();
        while (overflow > 0) {
            K currentKey = clockHand.get(handIndex);
            ClockCacheEntry entry = entries.get(currentKey);

            if (entry.referenceBit) {
                entry.referenceBit = false;
                handIndex = (handIndex + 1) % clockHand.size();
            } else {
                List<T> evicted = entry.value;
                boolean dirtyBit = entry.dirtyBit;
                entries.remove(currentKey);
                clockHand.remove(handIndex);
                cacheSize -= evicted.size();
                overflow -= evicted.size();

                if (handIndex >= clockHand.size()) {
                    handIndex = 0;
                }

                if (evicted.size() > 0 && dirtyBit) {
                    evictedEntries.add(Tuple2.of(currentKey, evicted));
                }
            }
        }
        return evictedEntries;
    }

    @Override
    public List<T> computeIfAbsent(K key, long timestamp) {
        ClockCacheEntry entry = entries.get(key);
        if (entry == null) {
            entry = new ClockCacheEntry(null, false);
            entries.put(key, entry);
            clockHand.add(key);
        }
        touch(key, timestamp);
        return entry.value;
    }

    @Override
    public List<T> get(K key, long timestamp) {
        ClockCacheEntry entry = entries.get(key);
        if (entry == null) {
            return null;
        }
        touch(key, timestamp);
        return entry.value;
    }

    @Override
    public void remove(K key) {
        ClockCacheEntry entry = entries.remove(key);
        if (entry != null) {
            cacheSize -= entry.value.size();
            clockHand.remove(key);
            if (handIndex >= clockHand.size()) {
                handIndex = 0;
            }
        }
    }

    @Override
    public void touch(K compositeKey, long ts) {
        entries.get(compositeKey).referenceBit = true;
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
    public boolean keyIsInCache(K key) {
        return entries.containsKey(key);
    }

    @Override
    public boolean tupleShouldBeInsertedInCache(K key) {
        return true;
    }

    @Override
    public KeyInterpreter getKeyInterpreter() {
        return keyCodec;
    }
}

