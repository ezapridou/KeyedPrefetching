package net.michaelkoepf.spegauge.flink.queries.nexmark.core.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Calculates the average price of the top 10 auctions per seller Input Tuple4<auctionId, maxprice,
 * category, sellerId> Accumulator Map<SellerId, top10 auctions> Output Map<SellerId, AvgOfTop10>
 */
public class AvgPriceTop10AuctionPerSeller
    implements AggregateFunction<
        Tuple4<Long, Long, Long, Long>, Map<Long, List<Long>>, Map<Long, Double>> {
  @Override
  public Map<Long, List<Long>> createAccumulator() {
    return new HashMap<>();
  }

  @Override
  public Map<Long, List<Long>> add(
      Tuple4<Long, Long, Long, Long> input, Map<Long, List<Long>> accumulator) {
    long price = input.f1;
    long seller = input.f3;
    if (!accumulator.containsKey(seller)) {
      accumulator.put(seller, new ArrayList<>(11)); // initial capacity must be 11.
      // 10 for the top 10 elements and 1 extra for the cases where I add an element and then remove
      // the last one
    }
    List<Long> top10prices = accumulator.get(seller);
    if (top10prices.isEmpty()) {
      top10prices.add(price);
      return accumulator;
    }
    int i = top10prices.size() - 1;
    for (; i >= 0; i--) {
      if (price < top10prices.get(i)) {
        break;
      }
    }
    if (i == 9) {
      return accumulator;
    }
    top10prices.add(i + 1, price);
    if (top10prices.size() > 10) {
      top10prices.remove(10);
    }
    return accumulator;
  }

  @Override
  public Map<Long, Double> getResult(Map<Long, List<Long>> accumulator) {
    Map<Long, Double> result = new HashMap<>(accumulator.size());
    for (Map.Entry<Long, List<Long>> entry : accumulator.entrySet()) {
      List<Long> top10prices = entry.getValue();
      long sum = 0;
      long count = top10prices.size();
      for (long p : top10prices) {
        sum += p;
      }
      double avg = sum / ((double) count);
      result.put(entry.getKey(), avg);
    }

    accumulator.clear();

    return result;
  }

  @Override
  public Map<Long, List<Long>> merge(Map<Long, List<Long>> a, Map<Long, List<Long>> b) {
    throw new UnsupportedOperationException("Merge not supported for custom accumulator!");
  }
}
