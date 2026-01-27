package net.michaelkoepf.spegauge.flink.queries.nexmark.core.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.HashMap;
import java.util.Map;

/**
 * Calculates the average price for each auction category Input Tuple4<auctionId, maxprice,
 * category, sellerId> Accumulator Map<CategoryId, SumCount> (SumCount is a struct, see bellow)
 * Output Map<CategoryId, AvgPrice></CategoryId,>
 */
public class AvgPricePerCategory
    implements AggregateFunction<
        Tuple4<Long, Long, Long, Long>,
        Map<Long, AvgPricePerCategory.SumCount>,
        Map<Long, Double>> {

  class SumCount {
    long sum = 0;
    long count = 0;
  }

  @Override
  public Map<Long, SumCount> createAccumulator() {
    return new HashMap<>();
  }

  @Override
  public Map<Long, SumCount> add(
      Tuple4<Long, Long, Long, Long> input, Map<Long, SumCount> accumulator) {
    long price = input.f1;
    long categoryId = input.f2;
    if (!accumulator.containsKey(categoryId)) {
      accumulator.put(categoryId, new SumCount());
    }
    accumulator.get(categoryId).count++;
    accumulator.get(categoryId).sum += price;
    return accumulator;
  }

  @Override
  public Map<Long, Double> getResult(Map<Long, SumCount> accumulator) {
    Map<Long, Double> result = new HashMap<>(accumulator.size());
    for (Map.Entry<Long, SumCount> entry : accumulator.entrySet()) {
      result.put(entry.getKey(), entry.getValue().sum / (1.0 * entry.getValue().count));
    }
    accumulator.clear();
    return result;
  }

  @Override
  public Map<Long, SumCount> merge(Map<Long, SumCount> a, Map<Long, SumCount> b) {
    throw new UnsupportedOperationException("Merge not supported for custom accumulator!");
  }
}
