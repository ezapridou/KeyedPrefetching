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

public class MedianAggregateFunctionNoExtraAttrs implements AggregateFunction<Tuple3<Integer, Integer, BitSet>, List<Tuple3<Integer, Integer, BitSet>>, Tuple3<Integer, Integer, BitSet>> {
    @Override
    public List<Tuple3<Integer, Integer, BitSet>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple3<Integer, Integer, BitSet>> add(Tuple3<Integer, Integer, BitSet> value, List<Tuple3<Integer, Integer, BitSet>> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Tuple3<Integer, Integer, BitSet> getResult(List<Tuple3<Integer, Integer, BitSet>> accumulator) {
        Collections.sort(accumulator, Comparator.comparingLong(e -> e.f0));
        return accumulator.get(accumulator.size() / 2);
    }

    @Override
    public List<Tuple3<Integer, Integer, BitSet>> merge(List<Tuple3<Integer, Integer, BitSet>> a, List<Tuple3<Integer, Integer, BitSet>> b) {
        return null;
    }
}
