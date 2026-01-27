package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSONQ18;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSONQ20;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN_Prefetching;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy (bidder,auction) --> Top1 bid per (bidder,auction) ranked based on dateTime
 */
public class Q18Prefetching extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query18_Prefetching";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism());
                        //.rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        SingleOutputStreamOperator<Tuple2<Tuple2<Long, Long>, Long>> hintedStream = source
                .process(new HintExtractorNexmarkJSONQ18(true, false, false))
                .setParallelism(16) // same parallelism as the source for chaining
                .name("HintExtractorBID")
                .setBufferTimeout(0);

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(false, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO");

        DataStream<List<NexmarkEvent>> result;

        result = hintedStream
                .keyBy(new KeySelector<Tuple2<Tuple2<Long,Long>, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(Tuple2<Tuple2<Long,Long>, Long> value) {
                        return value.f0;
                    }
                })
                .connect(mappedStream
                    .keyBy(new KeySelector<NexmarkEvent, Tuple2<Long, Long>>() {
                        @Override
                        public Tuple2<Long, Long> getKey(NexmarkEvent value) {
                            return new Tuple2<Long, Long>(value.bid.bidder, value.bid.auction);
                        }
                    }))
                .process(new TopN_Prefetching<Tuple2<Long, Long>, NexmarkEvent>(1, true))
                .returns(new TypeHint<List<NexmarkEvent>>() {})
                .setParallelism(getParallelism())
                .name("TopN");

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
