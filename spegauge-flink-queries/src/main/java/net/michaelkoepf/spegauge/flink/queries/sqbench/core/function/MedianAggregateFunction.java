package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MedianAggregateFunction implements AggregateFunction<Tuple2<EntityRecordFull, EntityRecordFull>, List<Tuple2<EntityRecordFull, EntityRecordFull>>, Tuple2<EntityRecordFull, EntityRecordFull>> {
    @Override
    public List<Tuple2<EntityRecordFull, EntityRecordFull>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple2<EntityRecordFull, EntityRecordFull>> add(Tuple2<EntityRecordFull, EntityRecordFull> value, List<Tuple2<EntityRecordFull, EntityRecordFull>> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Tuple2<EntityRecordFull, EntityRecordFull> getResult(List<Tuple2<EntityRecordFull, EntityRecordFull>> accumulator) {
        Collections.sort(accumulator, Comparator.comparingLong(e -> e.f1.longAttribute2));
        return accumulator.get(accumulator.size() / 2);
    }

    @Override
    public List<Tuple2<EntityRecordFull, EntityRecordFull>> merge(List<Tuple2<EntityRecordFull, EntityRecordFull>> a, List<Tuple2<EntityRecordFull, EntityRecordFull>> b) {
        return null;
    }
}
