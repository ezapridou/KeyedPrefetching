package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinCache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin.JoinSideInput;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin.JoinSideInputAsync;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin.JoinSideInputCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy auction --> enrich each bid with value from static table (based on its auction)
 */
public class Q13 extends QueryGroupWithConfig {
    @Override
    public String getName() {
        return "Query13";
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

        DataStream<Tuple2<NexmarkEvent, String>> result;

        result = mappedStream
                .keyBy(e -> e.bid.auction)
                .process(new JoinSideInput())
                .setParallelism(getParallelism())
                .name("JoinStaticTable");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
