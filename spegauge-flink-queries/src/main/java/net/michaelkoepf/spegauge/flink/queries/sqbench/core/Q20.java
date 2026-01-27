package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoin;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinAsync;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinCache;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Auction and Bid join with filter for Auction (category = 10)
 */
public class Q20 extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "Query20";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<NexmarkJSONEvent> source =
                QueryUtils.newSourceStreamNexmarkJSON(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance();

        SingleOutputStreamOperator<NexmarkEvent> mappedStream = source
                .process(new JSONToPOJONexmark(true, true, false))
                .setParallelism(getParallelism())
                .name("JSONToPOJO")
                .filter(e -> e.type == NexmarkEvent.Type.BID || (e.type == NexmarkEvent.Type.AUCTION && e.newAuction.category == 0))
                .setParallelism(getParallelism())
                .name("FilterCategory0");

        DataStream<Tuple2<NexmarkEvent, NexmarkEvent>> joinResult;

        joinResult = mappedStream
                .keyBy(e -> e.type == NexmarkEvent.Type.AUCTION ? e.newAuction.id : e.bid.auction)
                .process(new SingleInputJoinCache<NexmarkEvent>(
                        queryConfig.joinOperators.windowSizeMs, queryConfig.joinOperators.windowSlideMs, true))
                .returns(new TypeHint<Tuple2<NexmarkEvent, NexmarkEvent>>() {})
                .setParallelism(getParallelism())
                .name("Join")
                .disableChaining();

        List<DataStream> result = new LinkedList<>();
        result.add(joinResult);

        return result;
    }
}
