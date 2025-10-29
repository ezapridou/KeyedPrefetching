package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;

public class JSONToPOJO extends ProcessFunction<EntityRecordFull, EntityRecordFull> {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void processElement(EntityRecordFull value, Context ctx, Collector<EntityRecordFull> out) throws Exception {
    out.collect(deserialize(value));
  }

  protected EntityRecordFull deserialize(EntityRecordFull value) throws Exception {
    if (value.type == EntityRecordFull.Type.JSONA || value.type == EntityRecordFull.Type.JSONB) {

      EntityRecordFull result =  mapper.readValue(value.jsonString, EntityRecordFull.class);
      result.processingTimeMilliSecondsStart = value.processingTimeMilliSecondsStart;


      assert result != null;
      assert result.type != EntityRecordFull.Type.JSONA && result.type != EntityRecordFull.Type.JSONB :
              "Deserialized type should not be JSONA or JSONB, but was: " + result.type;

      return result;
    } else {
      throw new RuntimeException("Invalid entity type " + value.type);
    }
  }
}