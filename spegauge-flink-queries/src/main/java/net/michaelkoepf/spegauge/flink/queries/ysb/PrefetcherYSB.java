package net.michaelkoepf.spegauge.flink.queries.ysb;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PrefetcherYSB extends Prefetcher<Long, Tuple3<Long, String, String>> {
    JedisPooled jedisPooled;


    public PrefetcherYSB(CacheLRU<Long, Tuple3<Long, String, String>> cache, boolean windowing) {
        super(cache, windowing);
        ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxTotal(2);
        poolConfig.setMaxIdle(2);

        // Connect to Redis instance
        this.jedisPooled = new JedisPooled(poolConfig, "10.90.46.32", 6379, 10000);
    }

    public boolean fetchKeyAsync(StreamSide side, Long compositeKey, Long key, long ts, int priority, boolean pendingTuple,
                                          long windowSize, long windowSlide, String addId) {
        if (isHotOrCanBecomeWithoutBackendAccess(compositeKey, cache, side, ts)) {
            cache.touch(compositeKey, ts);
            return true;
        }
        Future<FetchResult<Long, Tuple3<Long, String, String>>> fetchReq  = requestPrefetch(compositeKey, key, side, ts, pendingTuple, priority, addId);
        // if the fetch already finished and was enqueued to the non-pending queue,
        // promote its result to the pending queue so TopN can consume it.
        if (pendingTuple && fetchReq.isDone()) {
            try {
                FetchResult<Long, Tuple3<Long, String, String>> res = fetchReq.get();
                // TODO I can store in inFlight which queue it is in to avoid this search
                if (!fetchResultQueue.remove(res))
                    fetchResultQueuePending.remove(res);
                processFetchResult(res, windowSize, windowSlide);
                //fetchResultQueuePending.add(fut.get()); // safe: non-blocking because isDone() is true
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
                throw new RuntimeException(ee);
            }
            return true;
        }

        return false;
    }


    /** Async state request */
    protected Future<FetchResult<Long, Tuple3<Long, String, String>>> requestPrefetch(Long compositeKey, Long key, StreamSide side, long timestamp,
                                                      boolean pendingTuple, int priority, String addId) {
        // Fast deduplication: if already in-flight, update timestamp and return same future.
        if (inFlightRequests.containsKey(compositeKey)) {
            InFlightInfo<Long, Tuple3<Long, String, String>> info = inFlightRequests.get(compositeKey);
            if (info.latestNeededTs < timestamp) {
                info.latestNeededTs = timestamp;
            }
            if (pendingTuple) {
                prefetchingHits++;
                info.hasPendingWaiter = true;
            }
            return info.future;
        }
        if (pendingTuple){
            prefetchingMisses++;
        }

        InFlightInfo<Long, Tuple3<Long, String, String>> info = new InFlightInfo(timestamp, pendingTuple, null);
        inFlightRequests.put(compositeKey, info);

        // async path via pool
        info.future = fetchPool.submit(priority, () -> {
            try {
                String strRedisKey = Long.toString(key);
                String campaignID = jedisPooled.get(strRedisKey);
                List<Tuple3<Long, String, String>> stateList = new ArrayList<>(1);
                stateList.add(new Tuple3<Long, String, String>(key, addId, campaignID));
                FetchResult<Long, Tuple3<Long, String, String>> fetchResult = new FetchResult<Long, Tuple3<Long, String, String>>(key, stateList, side);
                InFlightInfo infoOfKey = inFlightRequests.get(compositeKey);
                if ((infoOfKey != null && infoOfKey.hasPendingWaiter) || pendingTuple){
                    fetchResultQueuePending.add(fetchResult);
                } else {
                    fetchResultQueue.add(fetchResult);
                }
                return fetchResult;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        return info.future;
    }
}
