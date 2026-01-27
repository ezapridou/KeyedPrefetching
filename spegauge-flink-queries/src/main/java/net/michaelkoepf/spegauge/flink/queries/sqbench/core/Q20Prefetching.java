package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinPrefetching;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSONQ20;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkQ20;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Auction and Bid join with filter for Auction (category = 10)
 */
public class Q20Prefetching extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query20_Prefetching";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism());

        // this hint extraction is only for bids
        SingleOutputStreamOperator<Tuple3<Long, Long, Boolean>> hintedBidStream = source
                .process(new HintExtractorNexmarkJSONQ20(false, false))
                .setParallelism(16) // same parallelism as the source for chaining
                .name("HintExtractorBID")
                .setBufferTimeout(0);

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(true, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO")
                .filter(e -> e.type == NexmarkEvent.Type.BID || (e.type == NexmarkEvent.Type.AUCTION && e.newAuction.category == 0))
                .setParallelism(getParallelism())
                .name("FilterCategory0");

        // hint extraction for auction
        SingleOutputStreamOperator<Tuple3<Long, Long, Boolean>> hintedAuctionStream = mappedStream
                .process(new HintExtractorNexmarkQ20(true))
                .setParallelism(getParallelism()) // same parallelism as the source for chaining
                .name("HintExtractorAuction")
                .setBufferTimeout(0);

        DataStream<Tuple3<Long, Long, Boolean>> metadataStream = hintedBidStream
                .union(hintedAuctionStream);

        DataStream<Tuple2<NexmarkEvent, NexmarkEvent>> joinResult;

        joinResult = metadataStream.keyBy(e -> e.f0)
                .connect(mappedStream
                        .keyBy(e -> e.type == NexmarkEvent.Type.AUCTION ? e.newAuction.id : e.bid.auction))
                .process(new SingleInputJoinPrefetching<NexmarkEvent>(
                        queryConfig.joinOperators.windowSizeMs, queryConfig.joinOperators.windowSlideMs, true))
                .returns(new TypeHint<Tuple2<NexmarkEvent, NexmarkEvent>>() {})
                .setParallelism(getParallelism())
                .name("Join_Prefetching");

        List<DataStream> result = new LinkedList<>();
        result.add(joinResult);

        return result;
    }
}
