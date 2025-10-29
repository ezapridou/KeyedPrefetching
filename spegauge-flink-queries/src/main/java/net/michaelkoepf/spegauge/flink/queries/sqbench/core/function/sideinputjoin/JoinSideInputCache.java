package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin;

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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// for Q13
public class JoinSideInputCache extends JoinSideInput {
    private transient Cache<Long, String> cache;
    private final int cacheSize= 93750; // (20gb/24 task slots/8000 value size)*0.9
    private final int cacheType = 1; // 0 = TopKeys, 1 = LRU, 2 = Clock

    public JoinSideInputCache() {
        super();
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);

        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");

        if (cacheType == 0) {
            // TopKeys cache
            throw new RuntimeException("TopKeys cache not supported now.");
        }
        else if (cacheType == 1) {
            // LRU cache
            cache = new CacheLRU<Long, String>(cacheSize, true);
        }
        else if (cacheType == 2) {
            // Clock cache
            cache = new CacheClock<Long, String>(cacheSize, true);
        }
    }

    @Override
    public void processElement(NexmarkEvent value, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;

        long key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        // if element in cache read from there, otherwise bring it to cache
        List<String> stateForThisKeyList = OperatorWithCacheHelper.getStateCache(key, key, keyAccessibleState, cache, currentTime);
        if (stateForThisKeyList == null) {
            throw new RuntimeException("Problem. Unassigned value");
        }
        if (stateForThisKeyList.size() == 0) {
            throw new RuntimeException("Problem. Empty value list " + key);
        }
        String stateForThisKey = stateForThisKeyList.get(0);

        out.collect(new Tuple2<>(value, stateForThisKey));

        latencyTracker.updateLatency(1, startTime, ctx.timestamp(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), cache.getCacheSize(), cache.getMaxCacheSize());
    }
}
