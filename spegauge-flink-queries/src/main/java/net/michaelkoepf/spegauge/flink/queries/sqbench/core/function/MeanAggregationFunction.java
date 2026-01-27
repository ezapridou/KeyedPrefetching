package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.List;

public class MeanAggregationFunction implements AggregateFunction<Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {


    @Override
    public Tuple4<Long, Long, Long, Long> createAccumulator() {
        return Tuple4.of(-1L, -1L, -1L, -1L);
    }

    @Override
    public Tuple4<Long, Long, Long, Long> add(Tuple4<Long, Long, Long, Long> value, Tuple4<Long, Long, Long, Long> accumulator) {
        return Tuple4.of(
                -1L,
                value.f1,
                accumulator.f2 + value.f2,
                -1L);
    }


    @Override
    public Tuple4<Long, Long, Long, Long> getResult(Tuple4<Long, Long, Long, Long> accumulator) {
        return Tuple4.of(
                accumulator.f1,
                accumulator.f2,
                accumulator.f2 / accumulator.f3,
                -1L);
    }

    @Override
    public Tuple4<Long, Long, Long, Long> merge(Tuple4<Long, Long, Long, Long> a, Tuple4<Long, Long, Long, Long> b) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
