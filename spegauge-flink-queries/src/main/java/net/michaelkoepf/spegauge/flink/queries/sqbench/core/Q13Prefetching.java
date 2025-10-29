package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSONQ19;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin.JoinSideInputPrefetching;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy auction --> enrich each bid with value from static table (based on its auction)
 */
public class Q13Prefetching extends QueryGroup<Object> {

    public DataAnalyticsQuery queryConfig;

    public void setQueryConfig(DataAnalyticsQuery thisQuery) {
        this.queryConfig = thisQuery;
    }

    @Override
    public String getName() {
        return "Query13_Prefetching";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism());

        SingleOutputStreamOperator<Tuple2<Long, Long>> hintedStream = source
                // same as Q19
                .process(new HintExtractorNexmarkJSONQ19("auction", false))
                .setParallelism(16) // same parallelism as the source for chaining
                .name("HintExtractorBID")
                .setBufferTimeout(0);

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(false, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO");

        DataStream<Tuple2<NexmarkEvent, String>> result;

        result = hintedStream.keyBy(h -> h.f0)
                .connect(mappedStream
                    .keyBy(e -> e.bid.auction))
                .process(new JoinSideInputPrefetching())
                .setParallelism(getParallelism())
                .name("TopN");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
