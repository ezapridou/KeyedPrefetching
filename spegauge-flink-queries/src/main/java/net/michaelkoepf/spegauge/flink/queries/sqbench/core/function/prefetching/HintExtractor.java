package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

// Custom hint extractor for a test query
public class HintExtractor extends ProcessFunction<EntityRecordFull, EntityRecordFull>{

    private final OutputTag<Tuple3<Long, Long, Boolean>> metadataOutputTag;

    public HintExtractor(OutputTag<Tuple3<Long, Long, Boolean>> metadataOutputTag) {
        this.metadataOutputTag = metadataOutputTag;
    }

    @Override
    public void processElement(EntityRecordFull value, Context ctx, Collector<EntityRecordFull> out) throws Exception {

            boolean state1 = (value.type == EntityRecordFull.Type.JSONA);
            long key = state1 ? value.PK : value.FK;
            // non-hot elements filter
            // key % hot_seller_ratio != first_person_id
            if (key % 4000 != 1000) {
                ctx.output(metadataOutputTag, new Tuple3<>(key, value.eventTimeStampMilliSecondsSinceEpoch, state1));
            }
        out.collect(value);
    }
}
