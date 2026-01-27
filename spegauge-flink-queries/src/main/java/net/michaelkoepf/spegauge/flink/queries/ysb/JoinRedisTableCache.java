package net.michaelkoepf.spegauge.flink.queries.ysb;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.Cache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheClock;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class JoinRedisTableCache extends KeyedProcessFunction<Long, Tuple3<Long, String, Long>, Tuple3<Long, String, String>> {
    protected transient LatencyTracker latencyTracker;
    private transient Jedis jedis;

    protected KeyGroupRange keyGroupRange;

    private transient Cache<Long, Tuple3<Long, String, String>> cache;
    private final int cacheSize= 2_500_000; 
    private final int cacheType = 1; // 0 = TopKeys, 1 = LRU, 2 = Clock

    public JoinRedisTableCache() {
        super();
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);

        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = QueryConfig.PARALLELISM;
        int maxParallelism = QueryConfig.MAX_PARALLELISM;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        keyGroupRange = new KeyGroupRange(start, end);

        super.open(parameters);

        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");

        if (cacheType == 0) {
            // TopKeys cache
            throw new RuntimeException("TopKeys cache not supported now.");
        }
        else if (cacheType == 1) {
            // LRU cache
            cache = new CacheLRU<Long, Tuple3<Long, String, String>>(cacheSize, true);
        }
        else if (cacheType == 2) {
            // Clock cache
            cache = new CacheClock<Long, Tuple3<Long, String, String>>(cacheSize, true);
        }

        // Connect to Redis instance
        this.jedis = new Jedis(QueryConfig.REDIS_HOST, QueryConfig.REDIS_PORT);
    }

    @Override
    public void processElement(Tuple3<Long, String, Long> value, Context ctx, Collector<Tuple3<Long, String, String>> out) throws Exception {
        long startTime = value.f2;

        long addIDlong = value.f0;
        String addID = value.f1;
        long currentTime = ctx.timestamp();

        // if element in cache read from there, otherwise bring it to cache
        Tuple3<Long, String, String> enrichedTuple;
        if (cache.keyIsInCache(addIDlong)) {
            List<Tuple3<Long, String, String>> campaignIDList = cache.get(addIDlong, currentTime);
            enrichedTuple = campaignIDList.get(0);
        } else{
            String campaignID = jedis.get(Long.toString(addIDlong));
            enrichedTuple = new Tuple3<>(addIDlong, addID, campaignID);
            if (enrichedTuple == null) {
                throw new RuntimeException("Problem. Unassigned value");
            }
            List<Tuple3<Long, String, String>> stateIt = new ArrayList<>(1);
            stateIt.add(enrichedTuple);
            cache.insertEntry(addIDlong, stateIt, currentTime, false);
        }

        out.collect(enrichedTuple);

        latencyTracker.updateLatency(1, startTime, ctx.timestamp(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), cache.getCacheSize(), cache.getMaxCacheSize());
    }
}
