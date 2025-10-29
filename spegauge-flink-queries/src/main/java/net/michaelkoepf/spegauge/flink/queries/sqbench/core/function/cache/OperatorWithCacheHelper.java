package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.UserFacingListState;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction.Context;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.logging.Logger;

public abstract class OperatorWithCacheHelper {
    public static <T> void joinTuple(T tuple, long currentTime, List<Tuple2<T, Long>> otherStreamElems,
                                 Collector<Tuple2<T, T>> out, long windowSize) {
        for (Tuple2<T, Long> otherTuple : otherStreamElems) {
            if (currentTime - otherTuple.f1 <= windowSize) {
                out.collect(Tuple2.of(tuple, otherTuple.f0));
            }
        }
    }

    public static void registerTimerForEvent(long eventTime, long windowSize, long windowSlide, Context ctx) {
        long startOfSlide = eventTime/windowSlide * windowSlide;

        long tupleExpirationTime = startOfSlide + windowSize;
        ctx.timerService().registerEventTimeTimer(tupleExpirationTime);
    }

    public static void registerTimerForEvent(long eventTime, long windowSize, long windowSlide,
                                             KeyedProcessFunction.Context ctx) {
        long startOfSlide = eventTime/windowSlide * windowSlide;

        long tupleExpirationTime = startOfSlide + windowSize;
        ctx.timerService().registerEventTimeTimer(tupleExpirationTime);
    }

    public static <T> void removeExpiredElements(ListState<Tuple2<T, Long>> state, long currentTime,
                                       long windowSize, long windowSlide)
            throws Exception {
        List<Tuple2<T, Long>> allElements = new ArrayList<>();
        long minTimestamp = currentTime - windowSize + windowSlide;
        for (Tuple2<T, Long> entry : state.get()) {
            if (entry.f1 >= minTimestamp) {
                allElements.add(entry);
            }
        }
        state.update(allElements);
    }

    public static <K> void removeExpiredElementsCache(CacheAppend<K, ?> cache, long currentTime, K compositeKey,
                                                      long windowSize, long windowSlide) {
        long minTimestamp = Math.min(0, currentTime - windowSize + windowSlide);
        cache.removeExpired(compositeKey, minTimestamp);
    }

    public static <K, T> List<T> getStateCache(
            K compositeKey, K key, KeyAccessibleState<K, List<T>> state, Cache<K, T> cache, long timestamp) {
        if (cache.keyIsInCache(compositeKey)) {
            return cache.get(compositeKey, timestamp);
        }
        else {
            try {
                List<T> stateIt = state.get(key);
                if (stateIt == null ) {
                    stateIt = new ArrayList<>();
                }
                insertPairToCache(compositeKey, stateIt, timestamp, cache, state, false);
                return stateIt;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> KeyAccessibleState<Long, List<Tuple2<T, Long>>> getKeyAccessibleListState(
            ListState state, String stateName) {
        if (state instanceof UserFacingListState) {
            ListState originalState = ((UserFacingListState) state).getOriginalState();
            if (originalState instanceof KeyAccessibleState) {
                KeyAccessibleState<Long, List<Tuple2<T, Long>>> kvState =
                        (KeyAccessibleState<Long, List<Tuple2<T, Long>>>) originalState;
                return kvState;
            } else {
                throw new RuntimeException("State is not a KeyAccessibleState but a " +
                        originalState.getClass().getName() + " for state " + stateName);
            }
        }
        else {
            throw new RuntimeException("State is not a UserFacingListState but a " +
                    state.getClass().getName() + " for state " + stateName);
        }
    }

    public static <K, T> KeyAccessibleState<K, List<T>> getKeyAccessibleValueStateList(
            ValueState<List<T>> state, String stateName) {
        if (state instanceof KeyAccessibleState) {
            KeyAccessibleState<K, List<T>> kvState =
                    (KeyAccessibleState<K, List<T>>) state;
            return kvState;
        } else {
            throw new RuntimeException("State is not a KeyAccessibleState but a " +
                    state.getClass().getName() + " for state " + stateName);
        }
    }

    public static <K, T> KeyAccessibleState<K, T> getKeyAccessibleValueState(
            ValueState state, String stateName) {
        if (state instanceof KeyAccessibleState) {
            KeyAccessibleState<K, T> kvState = (KeyAccessibleState<K, T>) state;
            return kvState;
        } else {
            throw new RuntimeException("State is not a KeyAccessibleState but a " +
                    state.getClass().getName() + " for state " + stateName);
        }
    }

    public static <K, T> void insertTupleToCache(K compositeKey, T value, long currentTime,
                                                 CacheAppend<K, Tuple2<T, Long>> cache,
                                                 KeyAccessibleState<K, List<Tuple2<T, Long>>> state) {
        insertTupleToCache(compositeKey, Tuple2.of(value, currentTime), currentTime, cache, state);
    }

    public static <K, T> void insertTupleToCache(K compositeKey, Tuple2<T, Long> tuple, long currentTime,
                                                       CacheAppend<K, Tuple2<T, Long>> cache,
                                                 KeyAccessibleState<K, List<Tuple2<T, Long>>> state) {
        List<Tuple2<K, List<Tuple2<T, Long>>>> possibleEvictedEntries =
                cache.addAndReturnEvicted(compositeKey, tuple, currentTime);
        updateEvictedElements(possibleEvictedEntries, state, cache);
    }

    public static <K, T> void insertTupleToCacheTopN(K compositeKey, T tuple, long currentTime,
                                                 Cache<K, T> cache,
                                                 KeyAccessibleState<K, List<T>> state, int index, int n) {
        List<Tuple2<K, List<T>>> possibleEvictedEntries =
                cache.addAndReturnEvictedTopN(compositeKey, tuple, index, currentTime, n);
        updateEvictedElements(possibleEvictedEntries, state, cache);
    }

    public static <K, T> void insertPairToCache(K compositeKey, List<T> value,
                                                long timestamp, Cache<K, T> cache,
                                                KeyAccessibleState<K, List<T>> state, boolean dirty) {
        List<Tuple2<K, List<T>>> possibleEvictedEntries =
                cache.insertEntry(compositeKey, value, timestamp, dirty);
        updateEvictedElements(possibleEvictedEntries, state, cache);
    }

    public static <K, T> void updateEvictedElements(List<Tuple2<K, List<T>>> possibleEvictedEntries,
                                                    KeyAccessibleState<K, List<T>> state, Cache<K, T> cache) {
        if (possibleEvictedEntries != null) {
            try {
                for (Tuple2<K, List<T>> possibleEvictedEntry : possibleEvictedEntries) {
                    if (possibleEvictedEntry.f1 == null) {
                        continue;
                    }
                    K compositeKey = possibleEvictedEntry.f0;
                    K joinKey = cache.getKeyInterpreter().joinKey(compositeKey);
                    state.update(joinKey, possibleEvictedEntry.f1);

                    possibleEvictedEntry.f1.clear(); // help GC
                    possibleEvictedEntry.f1 = null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

