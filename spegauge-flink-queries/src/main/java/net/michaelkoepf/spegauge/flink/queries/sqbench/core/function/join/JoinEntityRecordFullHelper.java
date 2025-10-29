package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinEntityRecordFullHelper implements EventTypeHelper<EntityRecordFull> {
    @Override
    public ListStateDescriptor<Tuple2<EntityRecordFull, Long>> getStateDescriptor(String stateName) {
        ListStateDescriptor<Tuple2<EntityRecordFull, Long>> descriptor =
                new ListStateDescriptor<>(stateName,
                        Types.TUPLE(Types.POJO(EntityRecordFull.class), Types.LONG));
        return descriptor;
    }

    @Override
    public boolean elemOfTypeA(EntityRecordFull value) {
        return value.type == EntityRecordFull.Type.A;
    }

    @Override
    public boolean elemOfTypeB(EntityRecordFull value) {
        return value.type == EntityRecordFull.Type.B;
    }

}
