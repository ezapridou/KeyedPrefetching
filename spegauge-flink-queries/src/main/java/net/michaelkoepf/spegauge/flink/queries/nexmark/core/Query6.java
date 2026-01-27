package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.nexmark.core.function.AvgPriceTop10AuctionPerSeller;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** Implementation of Nexmark query 3 */
public class Query6 extends QueryGroup<Map<Long, Double>> {

  // TODO 4: fix this

  @Override
  public List<DataStream> register(StreamExecutionEnvironment env)
      throws IOException {
    DataStream<Event> source =
        QueryUtils.newSourceStream(
            env, getHostname(), getPort(), getSourceParallelism());

    DataStream<Event> auctions = QueryUtils.filterAuctions(source, getParallelism());

    DataStream<Event> bids = QueryUtils.filterBids(source, getParallelism());

    DataStream<Tuple4<Long, Long, Long, Long>> winningBids =
        QueryUtils.getWinningBids(auctions, bids, getParallelism());

    DataStream<Map<Long, Double>> result =
        winningBids
            .keyBy(x -> x.f3) // keyBy seller
            .window(SlidingEventTimeWindows.of(
                    Duration.ofMillis(getWindowSizeMS()), Duration.ofMillis(getWindowSlideMS())))
            .aggregate(new AvgPriceTop10AuctionPerSeller())
            .returns(TypeInformation.of(new TypeHint<Map<Long, Double>>() {}));

    return new LinkedList<DataStream>() {
      {
        add(result);
      }
    };
  }

  @Override
  public String getName() {
    return "Nexmark Query6: AVERAGE_SELLING_PRICE_BY_SELLER";
  }
}
