package net.michaelkoepf.spegauge.flink.queries.nexmark.core.function;

import net.michaelkoepf.spegauge.api.common.model.nexmark.Auction;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Bid;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Random;

/**
 * Receives input from two streams (auction and bid) and outputs for every auction the winning
 * (highest price) bid after the auction has expired
 *
 * @returns Tuple4<Long, Long, Long, Long> --> Tuple4<auctionId, price of top bid, category,
 *     sellerId>
 */
public class TopBid
    extends KeyedCoProcessFunction<Long, Event, Event, Tuple4<Long, Long, Long, Long>> {

  private static final Random random = new Random();
  private ValueState<Tuple5<Long, Instant, Long, Long, Boolean>>
      auctionInformation; // category, expires, seller, reserved threshold, sell below reserve
  private ValueState<Tuple3<Long, Instant, Long>> topBid; //  bidder, datetime, price

  private ListState<Bid> pendingBids; // bids that arrived before the corresponding auction

  @Override
  public void open(OpenContext config) {
    ValueStateDescriptor<Tuple5<Long, Instant, Long, Long, Boolean>> auctionInformationDescription =
        new ValueStateDescriptor<>(
            "auctionInformation",
            TypeInformation.of(new TypeHint<Tuple5<Long, Instant, Long, Long, Boolean>>() {}));

    auctionInformation = getRuntimeContext().getState(auctionInformationDescription);

    ValueStateDescriptor<Tuple3<Long, Instant, Long>> topBidDescription =
        new ValueStateDescriptor<>(
            "topBid", TypeInformation.of(new TypeHint<Tuple3<Long, Instant, Long>>() {}));

    topBid = getRuntimeContext().getState(topBidDescription);

    // TODO: consider storing bids in a heap
    ListStateDescriptor<Bid> pendingBidsDescription =
        new ListStateDescriptor<>("pendingBids", TypeInformation.of(new TypeHint<Bid>() {}));

    pendingBids = getRuntimeContext().getListState(pendingBidsDescription);
  }

  private void setCurrentTopBid(Bid bid) throws Exception {
    if (topBid.value().f2 <= bid.price
        && bid.dateTime.isBefore(topBid.value().f1)
        && bid.dateTime.isBefore(auctionInformation.value().f1)) {
      topBid.value().setField(bid.bidder, 0);
      topBid.value().setField(bid.dateTime, 1);
      topBid.value().setField(bid.price, 2);
    }
  }

  /**
   * Receives an auction event
   *
   * @param e the auction event
   * @param ctx context
   * @param out output collector
   * @throws Exception
   */
  @Override
  public void processElement1(Event e, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out)
      throws Exception {
    Auction auction = e.newAuction;

    topBid.update(new Tuple3<>(null, Instant.MAX, Long.MIN_VALUE));
    auctionInformation.update(
        new Tuple5<>(
            auction.category,
            auction.expires,
            auction.seller,
            auction.reserve,
            auction.sellBelowReserve));

    // check if there are bids that arrived before the auction
    // and get the highest one of them (can only be done after the auction has been received,
    // since we need to validate if the bid was placed before the auction expires
    for (Bid bid : pendingBids.get()) {
      setCurrentTopBid(bid);
    }

    // since the auction arrived, there cannot be more pending bids
    pendingBids.clear();

    // emit top bid when auction expires
    ctx.timerService().registerEventTimeTimer(auction.expires.toEpochMilli());
  }

  /**
   * Receives a Bid event
   *
   * @param e the bid event
   * @param ctx context
   * @param out output collector
   * @throws Exception
   */
  @Override
  public void processElement2(Event e, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out)
      throws Exception {
    Bid bid = e.bid;

    if (auctionInformation.value() != null) {
      // corresponding auction event has already arrived
      setCurrentTopBid(bid);
    } else {
      // still waiting for corresponding auction event (we need it expiration date)
      pendingBids.add(bid);
    }
  }

  @Override
  public void onTimer(
      long timestamp,
      KeyedCoProcessFunction<Long, Event, Event, Tuple4<Long, Long, Long, Long>>.OnTimerContext ctx,
      Collector<Tuple4<Long, Long, Long, Long>> out)
      throws Exception {

    // Value of topBid will never be null
    Tuple3<Long, Instant, Long> bid = topBid.value();
    Tuple5<Long, Instant, Long, Long, Boolean> auction = auctionInformation.value();

    if (bid.f0 != null && (bid.f2 >= auction.f3 || auction.f4)) {
      // there is a valid top bid for this auction and it is above the reserve threshold
      // or the item can be sold below the reserve threshold
      out.collect(new Tuple4<>(ctx.getCurrentKey(), bid.f2, auction.f0, auction.f2));
    }

    // IMPORTANT: clear COMPLETE state so nothing remains (otherwise, Flink will run out of
    // resources at some point)
    auctionInformation.clear();
    topBid.clear();
    pendingBids.clear();
  }
}
