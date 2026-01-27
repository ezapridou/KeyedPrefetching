package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import net.michaelkoepf.spegauge.api.sut.BitSet;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MedianAggregateFunctionJoinAggr implements AggregateFunction<Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>>,
        List<Tuple2<Integer, BitSet>>, Tuple2<Integer, BitSet>> {
    @Override
    public List<Tuple2<Integer, BitSet>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple2<Integer, BitSet>> add(Tuple2<Tuple2<Integer, BitSet>, Tuple2<Integer, BitSet>> value,
                                             List<Tuple2<Integer, BitSet>> accumulator) {
        accumulator.add(value.f1);
        return accumulator;
    }

    @Override
    public Tuple2<Integer, BitSet> getResult(List<Tuple2<Integer, BitSet>> accumulator) {
        Collections.sort(accumulator, Comparator.comparingLong(e -> e.f0));
        return accumulator.get(accumulator.size() / 2);
    }

    @Override
    public List<Tuple2<Integer, BitSet>> merge(List<Tuple2<Integer, BitSet>> a, List<Tuple2<Integer, BitSet>> b) {
        return null;
    }
}
