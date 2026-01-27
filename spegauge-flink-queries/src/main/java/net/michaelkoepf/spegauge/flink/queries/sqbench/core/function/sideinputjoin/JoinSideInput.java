package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
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
public class JoinSideInput extends KeyedProcessFunction<Long, NexmarkEvent, Tuple2<NexmarkEvent, String>> {

    protected transient ValueState<List<String>> staticTable;
    protected transient KeyAccessibleState<Long, List<String>> keyAccessibleState;

    protected transient LatencyTracker latencyTracker;

    protected KeyGroupRange keyGroupRange;

    public JoinSideInput() {
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);
        String stateName = "staticTable";
        staticTable = getRuntimeContext().getState(new ValueStateDescriptor<List<String>>(stateName, Types.LIST(Types.STRING)));
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999");
        keyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueState(staticTable, stateName);

        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = QueryConfig.PARALLELISM;
        int maxParallelism = QueryConfig.MAX_PARALLELISM;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        keyGroupRange = new KeyGroupRange(start, end);

        fillStaticTable(keyGroupRange, maxParallelism, keyAccessibleState, operatorIndex, parallelism);
    }

    @Override
    public void processElement(NexmarkEvent value, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;

        List<String> stateForThisKeyList = staticTable.value();
        if (stateForThisKeyList == null) {
            throw new RuntimeException("Problem. Unassigned value");
        }

        String stateForThisKey = stateForThisKeyList.get(0);

        out.collect(new Tuple2<>(value, stateForThisKey));

        latencyTracker.updateLatency(1, startTime, ctx.timestamp(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    protected static void fillStaticTable(KeyGroupRange keyGroupRange, int maxParallelism,
                                        KeyAccessibleState<Long, List<String>> state, int opIndex, int parallelism){
        // generate a random string of 8000 bytes for auction ids 0 to 500000. 4gb total
        // Alphabet: all ASCII so each char = 1 byte in UTF-8
        final byte[] alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                .getBytes(StandardCharsets.US_ASCII);
        final int valueLen = 8_000;//2_500;

        // Reusable buffers to avoid constant allocations
        final byte[] buffer = new byte[valueLen];
        final Random rng = new Random(45);

        int numAuctions = parallelism * 500_000; //30_000_000; // for 250 gb //48*500_000; for ~200gb

        for (long auctionId = 0; auctionId <= numAuctions; auctionId++) {
            if (!keyGroupRange.contains(getKeyGroupIndex(auctionId, maxParallelism))){
                continue;
            }
            // Fill buffer with random alphabet bytes
            for (int i = 0; i < valueLen; i++) {
                buffer[i] = alphabet[rng.nextInt(alphabet.length)];
            }

            String payload = new String(buffer, StandardCharsets.US_ASCII);
            List<String> list = new ArrayList<>(1);
            list.add(payload);

            try {
                state.update(auctionId, list);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static int getKeyGroupIndex(long key, int maxParallelism) {
        return MathUtils.murmurHash(((Long)key).hashCode()) % maxParallelism;
    }
}
