package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.CompositeKeyInterpreter;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.KeyInterpreter;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SingleSideKeyInterpreter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class CacheLRU<K, T> implements Cache<K, T> {

    protected class LRUCacheEntry {
        List<T> value; // entity and timestamp from entity's arrival
        long lastAccessTime;
        boolean dirtyBit ;

        LRUCacheEntry() {
            this.value = new ArrayList<>();
            this.dirtyBit = false;
        }

        LRUCacheEntry(List<T> value, boolean dirty) {
            if (value == null) {
                this.value = new ArrayList<>();
            } else {
                this.value = value;
            }
            dirtyBit = dirty;
        }

        void add(T newValue, long timestamp) {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(newValue);
            if (timestamp > lastAccessTime) {
                lastAccessTime = timestamp;
            }
            dirtyBit = true; // mark as dirty when adding a new value
        }

        void add(T newValue, int index, long timestamp) {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(index, newValue);
            if (timestamp > lastAccessTime) {
                lastAccessTime = timestamp;
            }
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

    // --- Two-structure design ---
    protected final PeekableLinkedHashMap<K, LRUCacheEntry> entries;

    private final KeyInterpreter keyCodec;

    public long totalElements = 0;

    public CacheLRU(int maxCacheSize, boolean singleInput) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = 0;
        this.entries = new PeekableLinkedHashMap<>(1000, 0.75f, true);
        if (singleInput) {
            this.keyCodec = new SingleSideKeyInterpreter();
        } else {
            this.keyCodec = new CompositeKeyInterpreter();
        }
    }

    @Override
    public List<Tuple2<K, List<T>>> addAndReturnEvicted(K key, T value, long timestamp) {
        addValue(key, value, timestamp);
        cacheSize++;

        return evictIfFull();
    }

    @Override
    public List<Tuple2<K, List<T>>> addAndReturnEvictedTopN(K key, T value, int index, long timestamp, int n) {
        addValueTopN(key, value, index, timestamp, n);
        cacheSize++;

        return evictIfFull();
    }

    private void addValue(K key, T value, long timestamp) {
        LRUCacheEntry entry = entries.get(key);
        if (entry == null) {
            entry = new LRUCacheEntry();
            entries.put(key, entry);
        }
        entry.add(value, timestamp);
    }

    private void addValueTopN(K key, T value, int index, long timestamp, int n) {
        LRUCacheEntry entry = entries.get(key);
        if (entry == null) {
            entry = new LRUCacheEntry();
            entries.put(key, entry);
        }
        entry.add(value, index, timestamp);

        if (entry.value.size() > n) {
            entry.value.remove(entry.value.size() - 1);
            cacheSize--;
        } else {
            totalElements++;
        }
    }

    @Override
    public List<Tuple2<K, List<T>>> insertEntry(K key, List<T> values, long timestamp, boolean dirty) {
        assert !entries.containsKey(key) : "Key " + key + " already exists in cache.";
        LRUCacheEntry newEntry = new LRUCacheEntry(values, dirty);
        entries.put(key, newEntry);
        newEntry.lastAccessTime = timestamp;
        cacheSize += newEntry.value.size();

        return evictIfFull();
    }

    public List<Tuple2<K, List<T>>> insertEntry(
            K key, List<T> values, long timestamp, long lastProcessedTimestamp, boolean dirty) {
        assert !entries.containsKey(key) : "Key " + key + " already exists in cache.";
        LRUCacheEntry newEntry = new LRUCacheEntry(values, dirty);
        entries.put(key, newEntry);
        newEntry.lastAccessTime = timestamp;
        cacheSize += newEntry.value.size();

        return evictIfFull(lastProcessedTimestamp);
    }

    private List<Tuple2<K, List<T>>> evictIfFull() {
        long overflow = cacheSize - maxCacheSize;
        if (overflow <= 0) {
            return Collections.emptyList();
        }

        List<Tuple2<K, List<T>>> evicted = new ArrayList<>();
        for (Iterator<Map.Entry<K, LRUCacheEntry>> it = entries.entryIteratorLRU();
             overflow > 0 && it.hasNext();) {

            Map.Entry<K, LRUCacheEntry> e = it.next();
            LRUCacheEntry val = e.getValue();

            // remove from map in O(1)
            it.remove();

            // adjust size by number of contained records
            int removedCount = (val.value != null) ? val.value.size() : 0;
            cacheSize -= removedCount;
            overflow -= removedCount;

            if (removedCount > 0 && val.dirtyBit) {
                evicted.add(Tuple2.of(e.getKey(), val.value));
            }
        }

        return evicted;
    }

    private List<Tuple2<K, List<T>>> evictIfFull(long lastProcessedTimestamp) {
        long overflow = cacheSize - maxCacheSize;
        if (overflow <= 0) {
            return Collections.emptyList();
        }

        List<Tuple2<K, List<T>>> evicted = new ArrayList<>();
        for (Iterator<Map.Entry<K, LRUCacheEntry>> it = entries.entryIteratorLRU();
             overflow > 0 && it.hasNext();) {

            Map.Entry<K, LRUCacheEntry> e = it.next();
            LRUCacheEntry val = e.getValue();

            // remove from map in O(1)
            it.remove();

            // adjust size by number of contained records
            int removedCount = (val.value != null) ? val.value.size() : 0;
            cacheSize -= removedCount;
            overflow -= removedCount;

            if (removedCount > 0 && val.dirtyBit) {
                evicted.add(Tuple2.of(e.getKey(), val.value));
            }
            long LRUTime = e.getValue().lastAccessTime;
            K LRUKey = e.getKey();
            if (LRUTime > lastProcessedTimestamp) {
                System.out.println("PROBLEM: Prefetching causes eviction of useful elements " + LRUKey
                        + " with timestamp " + LRUTime + " while last processed timestamp is " + lastProcessedTimestamp);
                throw new RuntimeException("PROBLEM: Prefetching causes eviction of useful elements " + LRUKey
                        + " with timestamp " + LRUTime + " while last processed timestamp is " + lastProcessedTimestamp);
            }
        }

        return evicted;
    }

    @Override
    public List<T> computeIfAbsent(K key, long timestamp) {
        LRUCacheEntry state = entries.get(key);
        if (state == null) {
            state = new LRUCacheEntry(null, false); // create empty entry
            entries.put(key, state);
        }
        touch(key, timestamp); // update last access time to current timestamp
        //order.put(key, DUMMY); // update access order
        return state.value;
    }

    @Override
    public List<T> get(K key, long timestamp) {
        LRUCacheEntry state = entries.get(key);
        if (state == null) {
            return null;
        }
        touch(key, timestamp); // update last access time to current timestamp
        return state.value;
    }

    @Override
    public void remove(K key) {
        LRUCacheEntry entry = entries.remove(key);
        if (entry != null) {
            cacheSize -= entry.value.size();
        }
    }

    @Override
    public void touch(K compositeKey, long ts) {
        updateTimestamp(compositeKey, ts);
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
    public KeyInterpreter getKeyInterpreter() {
        return keyCodec;
    }

    // for LRU always insert in cache first
    @Override
    public boolean tupleShouldBeInsertedInCache(K key) {
        return true;
    }

    private void updateTimestamp(K key, long timestamp) {
        LRUCacheEntry entry = entries.get(key);
        if (entry.lastAccessTime < timestamp) {
            entry.lastAccessTime = timestamp; // important when using prefetching
        }
    }
}
