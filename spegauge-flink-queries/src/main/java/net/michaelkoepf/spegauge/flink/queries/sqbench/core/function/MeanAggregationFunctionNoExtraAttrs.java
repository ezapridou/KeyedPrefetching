package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.sut.BitSet;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.List;

public class MeanAggregationFunctionNoExtraAttrs implements AggregateFunction<Tuple3<Integer, Integer, BitSet>, Tuple2<Long, Long>, Double> {


    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return Tuple2.of(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Tuple3<Integer, Integer, BitSet> value, Tuple2<Long, Long> accumulator) {
        return Tuple2.of(
                accumulator.f0 + 1L,
                accumulator.f1 + value.f0);
    }


    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return accumulator.f1 / (double)accumulator.f0;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
