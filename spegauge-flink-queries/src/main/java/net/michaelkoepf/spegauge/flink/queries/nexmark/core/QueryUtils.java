package net.michaelkoepf.spegauge.flink.queries.nexmark.core;

import net.michaelkoepf.spegauge.flink.queries.nexmark.core.function.TopBid;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import net.michaelkoepf.spegauge.flink.sdk.source.SocketReader;
import net.michaelkoepf.spegauge.flink.sdk.source.TCPSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public final class QueryUtils {

  private static final AtomicInteger sourceNumber = new AtomicInteger();

  private QueryUtils() {}

  /**
   * Returns a new source that is in its own slot sharing group.
   *
   * @param env
   * @param hostname
   * @param port
   * @param parallelism
   * @return
   */
  public static DataStream<Event> newSourceStream(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism) {
    TCPSource<Event> source = new TCPSource<>(hostname, port, false, true);
    WatermarkStrategy<Event> wmStrategy = WatermarkStrategy
            .<Event>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimestamp);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
            .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("NEXMarkParallelTCPSource").assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()).name("watermark");
  }

  public static SingleOutputStreamOperator<Event> filterBids(
      DataStream<Event> source, int parallelism, String slotSharingGroup) {
    return filterBids(source, parallelism).slotSharingGroup(slotSharingGroup);
  }

  public static SingleOutputStreamOperator<Event> filterBids(
      DataStream<Event> source, int parallelism) {
    return source
        .filter((FilterFunction<Event>) event -> event.type == Event.Type.BID)
        .name("filterBids")
        .setParallelism(parallelism);
  }

  public static SingleOutputStreamOperator<Event> filterAuctions(
      DataStream<Event> source, int parallelism, String slotSharingGroup) {
    return filterAuctions(source, parallelism).slotSharingGroup(slotSharingGroup);
  }

  public static SingleOutputStreamOperator<Event> filterAuctions(
      DataStream<Event> source, int parallelism) {
    return source
        .filter((FilterFunction<Event>) event -> event.type == Event.Type.AUCTION)
        .name("filterAuctions")
        .setParallelism(parallelism);
  }

  public static SingleOutputStreamOperator<Event> filterPeople(
      DataStream<Event> source, int parallelism, String slotSharingGroup) {
    return filterPeople(source, parallelism).slotSharingGroup(slotSharingGroup);
  }

  public static SingleOutputStreamOperator<Event> filterPeople(
      DataStream<Event> source, int parallelism) {
    return source
        .filter((FilterFunction<Event>) event -> event.type == Event.Type.PERSON)
        .name("filterPersons")
        .setParallelism(parallelism);
  }

  /**
   * @returns Tuple4<Long, Long, Long, Long> --> Tuple4<auctionId, maxprice, category, sellerId>
   */
  public static DataStream<Tuple4<Long, Long, Long, Long>> getWinningBids(
      DataStream<Event> auctions, DataStream<Event> bids, int parallelism) throws IOException {

    // DataStream containing 1 tuple per auction of type Tuple4<auctionId, maxprice, category,
    // sellerId>
    DataStream<Tuple4<Long, Long, Long, Long>> result =
        auctions
            .connect(bids)
            .keyBy(
                (KeySelector<Event, Long>) e -> e.newAuction.id,
                (KeySelector<Event, Long>) e -> e.bid.auction)
            .process(new TopBid())
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    return result;
  }
}
