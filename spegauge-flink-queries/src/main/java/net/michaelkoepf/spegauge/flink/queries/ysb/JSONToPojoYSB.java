package net.michaelkoepf.spegauge.flink.queries.ysb;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JSONToPojoYSB extends ProcessFunction<YSBJSONEvent, YSB_EventTs> {

    private final ObjectMapper mapper = new ObjectMapper();

    public JSONToPojoYSB() {}

    @Override
    public void processElement(YSBJSONEvent value, Context ctx, Collector<YSB_EventTs> out) throws Exception {
        YSB_Event event = mapper.readValue(value.jsonString, YSB_Event.class);
        out.collect(new YSB_EventTs(event, value.eventTimeStampMilliSecondsSinceEpoch, value.processingTimeMilliSecondsStart));
    }
}