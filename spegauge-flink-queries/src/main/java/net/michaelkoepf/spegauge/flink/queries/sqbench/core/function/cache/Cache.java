package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.KeyInterpreter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public interface Cache<K, T> {
    List<Tuple2<K, List<T>>> addAndReturnEvicted(
            K key, T value, long currentTime);

    List<Tuple2<K, List<T>>> addAndReturnEvictedTopN(
            K key, T value, int index, long currentTime, int n);

    List<Tuple2<K, List<T>>> insertEntry(
            K key, List<T> values, long currentTime, boolean dirty);

    List<T> computeIfAbsent(K key, long timestamp);

    List<T> get(K key, long timestamp);

    int getCacheSize();

    int getMaxCacheSize();

    boolean hasSpace();

    boolean keyIsInCache(K key);

    boolean tupleShouldBeInsertedInCache(K key);

    void remove(K key);

    KeyInterpreter<K> getKeyInterpreter();

    void touch(K compositeKey, long ts);

}
