package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import org.apache.flink.api.common.functions.AggregateFunction;


public class CountAggregateFunctionLong implements AggregateFunction<Long, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Long value, Long accumulator) {
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
