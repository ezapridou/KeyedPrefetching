package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinNexmarkEventHelper implements EventTypeHelper<NexmarkEvent> {
    @Override
    public ListStateDescriptor<Tuple2<NexmarkEvent, Long>> getStateDescriptor(String stateName) {
        ListStateDescriptor<Tuple2<NexmarkEvent, Long>> descriptor =
                new ListStateDescriptor<>(stateName,
                        Types.TUPLE(Types.POJO(NexmarkEvent.class), Types.LONG));
        return descriptor;
    }

    @Override
    public boolean elemOfTypeA(NexmarkEvent value) {
        return value.type == NexmarkEvent.Type.AUCTION;
    }

    @Override
    public boolean elemOfTypeB(NexmarkEvent value) {
        return value.type == NexmarkEvent.Type.BID;
    }

}
