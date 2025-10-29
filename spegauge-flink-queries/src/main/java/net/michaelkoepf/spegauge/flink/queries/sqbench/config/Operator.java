package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public abstract class Operator {
  @JsonProperty
  @Builder.Default
  public int parallelismFactor = 1;
}
