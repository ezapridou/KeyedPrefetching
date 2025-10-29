package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MaxKeyedProcessFunction extends KeyedProcessFunction<Long, Tuple2<EntityRecordFull, EntityRecordFull>, Tuple2<EntityRecordFull, EntityRecordFull>> {

  private ValueState<Long> max;

  @Override
  public void open(OpenContext parameters) throws Exception {
    super.open(parameters);
    max = getRuntimeContext().getState(new ValueStateDescriptor<>("max", Long.class));
  }

  @Override
  public void processElement(Tuple2<EntityRecordFull, EntityRecordFull> value, Context ctx, Collector<Tuple2<EntityRecordFull, EntityRecordFull>> out) throws Exception {
    if (max.value() == null || value.f0.longAttribute2 > max.value()) {
      max.update(value.f0.longAttribute2);
      out.collect(new Tuple2<>(value.f0, value.f1));
    }
  }
}
