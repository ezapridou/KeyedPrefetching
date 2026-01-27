package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSONQ19;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN_Prefetching;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy (bidder,auction) --> Top1 bid per (bidder,auction) ranked based on dateTime
 */
public class Q19Prefetching extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query19_Prefetching";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism());

        SingleOutputStreamOperator<Tuple2<Long, Long>> hintedStream = source
                .process(new HintExtractorNexmarkJSONQ19(false, false, 0))
                .setParallelism(16) // same parallelism as the source for chaining
                .name("HintExtractorBID")
                .setBufferTimeout(0);

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(false, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO");

        DataStream<List<NexmarkEvent>> result;

        result = hintedStream.keyBy(value -> value.f0)
                .connect(mappedStream
                        .keyBy(e -> e.bid.auction))
                .process(new TopN_Prefetching(10, false))
                .returns(new TypeHint<List<NexmarkEvent>>() {})
                .setParallelism(getParallelism())
                .name("TopN");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
