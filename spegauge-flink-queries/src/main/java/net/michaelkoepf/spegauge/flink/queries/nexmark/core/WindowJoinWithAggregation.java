package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.nexmark.core.function.AvgForOneId;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** Implementation of Nexmark query 1 */
public class WindowJoinWithAggregation extends QueryGroup<Map<Long, Double>> {
    @Override
    public List<DataStream> register(StreamExecutionEnvironment env)
            throws IOException {
        DataStream<Event> source =
                QueryUtils.newSourceStream(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance();

        DataStream<Event> bids = QueryUtils.filterBids(source, getParallelism());
        DataStream<Event> auctions = QueryUtils.filterAuctions(source, getParallelism());

        DataStream<Map<Long, Double>> result =
                auctions.join(bids).where((KeySelector<Event, Long>) value -> value.newAuction.id)
                        .equalTo((KeySelector<Event, Long>) value -> value.bid.auction)
                        .window(SlidingEventTimeWindows.of(
                                Duration.ofMillis(getWindowSizeMS()), Duration.ofMillis(getWindowSlideMS())))
                        .apply(new RichJoinFunction<Event, Event, Tuple2<Long, Long>>() {
                            @Override
                            public Tuple2<Long, Long> join(Event first, Event second) throws Exception {
                                return new Tuple2<>(first.newAuction.category, second.bid.price);
                            }
                        }).windowAll(
                                SlidingEventTimeWindows.of(
                                        Duration.ofMillis(getWindowSizeMS()), Duration.ofMillis(getWindowSlideMS())))
                        .aggregate(new AvgForOneId());

        return new LinkedList<DataStream>() {
            {
                add(result);
            }
        };

    }

    @Override
    public String getName() {
        return "MAP_STRING_MICRO_BENCHMARK";
    }
}
