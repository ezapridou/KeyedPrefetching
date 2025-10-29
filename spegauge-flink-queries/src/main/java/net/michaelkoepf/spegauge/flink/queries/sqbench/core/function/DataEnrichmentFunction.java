package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.MapFunction;

public class DataEnrichmentFunction implements MapFunction<EntityRecordFull, Object> {

      @Override
      public Object map(EntityRecordFull value) throws Exception {
//          long start = System.nanoTime();
          long duration = value.longAttribute2 * 1_000L;
          Thread.sleep(duration/1_000L);
          return value;
      }
}
