package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class StreamingETLQuery implements Shareable, ConfigurableQuery {

  @JsonProperty
  @FieldSerializer.NotNull
  public ExtractOperator extractOperation;

  @JsonProperty
  public DownstreamOperator downstreamOperator;

  @JsonProperty
  public FilterSpec filter;

  @Override
  public boolean isShareable(Shareable o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamingETLQuery otherQuery = (StreamingETLQuery) o;
    return extractOperation.equals(otherQuery.extractOperation);
  }

}
