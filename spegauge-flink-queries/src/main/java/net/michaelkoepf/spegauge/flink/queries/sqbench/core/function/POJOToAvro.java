package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.ByteArrayOutputStream;

public class POJOToAvro implements MapFunction<Object, Object> {
  private final String schemaString = "{\n" +
          "  \"type\": \"record\",\n" +
          "  \"name\": \"Aggregate\",\n" +
          "  \"fields\": [\n" +
          "    {\"name\": \"key\", \"type\": \"long\"},\n" +
          "    {\"name\": \"sum\", \"type\": \"long\"}\n" +
          "  ]\n" +
          "}";

  @Override
  public Object map(Object value) throws Exception {
    Schema schema = new Schema.Parser().parse(schemaString);
    DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);

    var record = (EntityRecordFull) value;

    GenericRecord aggregate = new GenericData.Record(schema);
    aggregate.put("key", record.PK);
    aggregate.put("sum", record.longAttribute1);

    byte[] serializedBytes;
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      datumWriter.write(aggregate, encoder);
      encoder.flush();
      serializedBytes = outputStream.toByteArray();
    }

    return serializedBytes;
  }
}

