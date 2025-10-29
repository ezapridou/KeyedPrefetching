package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class WindowAggregation extends WindowOperator implements DownstreamOperator {
  public enum AggregationType {
    MEAN,
    MEDIAN,
    COUNT
  }

  @JsonProperty
  @FieldSerializer.NotNull
  public AggregationType aggregationType;

  @JsonProperty
  @FieldSerializer.NotNull
  public String groupByField;

  public boolean isStateful() {
    return true;
  }

  public Object groupByField() {
    return this.groupByField;
  }
}
