package net.michaelkoepf.spegauge.flink.queries.nexmark.core.function;

import com.codahale.metrics.UniformReservoir;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;

import java.util.HashMap;
import java.util.Map;

/**
 * Calculates the average price for each auction category Input Tuple4<auctionId, maxprice,
 * category, sellerId> Accumulator Map<CategoryId, SumCount> (SumCount is a struct, see bellow)
 * Output Map<CategoryId, AvgPrice></CategoryId,>
 */
public class AvgForOneId
        extends RichAggregateFunction<
        Tuple2<Long, Long>,
                Map<Long, AvgForOneId.SumCount>,
                Map<Long, Double>> {

    private transient Histogram histogram;

    class SumCount {
        long sum = 0;
        long count = 0;

        long startTime = 0;
    }

    @Override
    public Map<Long, SumCount> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<Long, SumCount> add(
            Tuple2<Long, Long> input, Map<Long, SumCount> accumulator) {
        var startTime = System.nanoTime();
        long id = input.f0;
        if (!accumulator.containsKey(id)) {
            var sumcount = new SumCount();
            accumulator.put(id, new SumCount());
        }
        accumulator.get(id).count++;
        accumulator.get(id).sum += input.f1;
        accumulator.get(id).startTime += System.nanoTime() - startTime;
        return accumulator;
    }

    @Override
    public Map<Long, Double> getResult(Map<Long, SumCount> accumulator) {
        Map<Long, Double> result = new HashMap<>(accumulator.size());
        for (Map.Entry<Long, SumCount> entry : accumulator.entrySet()) {
            var startTime = System.nanoTime();
            result.put(entry.getKey(), entry.getValue().sum / (1.0 * entry.getValue().count));
            this.histogram.update(entry.getValue().startTime + (System.nanoTime()-startTime));
        }
        accumulator.clear();
        return result;
    }

    @Override
    public Map<Long, SumCount> merge(Map<Long, SumCount> a, Map<Long, SumCount> b) {
        throw new UnsupportedOperationException("Merge not supported for custom accumulator!");
    }

    @Override
    public void open(OpenContext config) {
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(new UniformReservoir());
        this.histogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("windowAggregation", new DropwizardHistogramWrapper(dropwizardHistogram));
    }
}
