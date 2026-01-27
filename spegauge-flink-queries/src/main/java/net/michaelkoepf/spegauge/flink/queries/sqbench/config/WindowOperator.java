package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public abstract class WindowOperator extends Operator {

  @JsonProperty
  @FieldSerializer.NotNull
  public int windowSizeMs;

  @JsonProperty
  @FieldSerializer.NotNull
  public int windowSlideMs;

}
