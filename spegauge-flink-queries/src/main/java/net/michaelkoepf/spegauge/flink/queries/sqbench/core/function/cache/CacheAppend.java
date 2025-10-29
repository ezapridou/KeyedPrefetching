package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import org.apache.flink.api.java.tuple.Tuple;

public interface CacheAppend<K, T extends Tuple> extends Cache<K, T> {

    public abstract void removeExpired(K key, long minTimestamp) ;
}
