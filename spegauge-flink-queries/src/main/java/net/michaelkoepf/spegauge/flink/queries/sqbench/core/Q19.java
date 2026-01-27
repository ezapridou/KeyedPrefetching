package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.JSONToPOJONexmark;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.QueryUtils;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN_Async;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN_Cache;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy (auction) --> Top10 bid per auction ranked based on price
 */
public class Q19 extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query19";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(false, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO");

        DataStream<List<NexmarkEvent>> result;

        result = mappedStream
                .keyBy(e -> e.bid.auction)
                .process(new TopN_Cache<Long, NexmarkEvent>(10, false))
                .setParallelism(getParallelism())
                .name("TopN");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
