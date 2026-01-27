package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class CountAggregateFunction implements AggregateFunction<Tuple2<EntityRecordFull, EntityRecordFull>, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<EntityRecordFull, EntityRecordFull> value, Long accumulator) {
        return accumulator + 1;
    }


    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
