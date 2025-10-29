package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.AggregateFunction;

public class SumAggregationFunction implements AggregateFunction<EntityRecordFull, EntityRecordFull, Object> {

  @Override
  public EntityRecordFull createAccumulator() {
    return new EntityRecordFull();
  }

  @Override
  public EntityRecordFull add(EntityRecordFull value, EntityRecordFull accumulator) {
    accumulator.PK = value.PK;
    accumulator.longAttribute1 += value.longAttribute1;
    return accumulator;
  }

  @Override
  public Object getResult(EntityRecordFull accumulator) {
    return accumulator;
  }

  @Override
  public EntityRecordFull merge(EntityRecordFull a, EntityRecordFull b) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
