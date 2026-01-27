package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
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
 * Source --> Filter to keep only bids --> keyBy (bidder,auction) --> Top1 bid per (bidder,auction) ranked based on dateTime
 */
public class Q18 extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query18";
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
                .keyBy(new KeySelector<NexmarkEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(NexmarkEvent value) {
                        return Tuple2.of(value.bid.bidder, value.bid.auction);
                    }
                })
                .process(new TopN_Async<Tuple2<Long, Long>, NexmarkEvent>(1, true))
                .setParallelism(getParallelism())
                .name("TopN");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
