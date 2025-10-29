package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

@Builder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class DataEnrichtmentFunction extends Operator implements DownstreamOperator {
  public enum EnrichmentType {
    SIMULATED_ENRICHMENT
  }

  @JsonProperty
  @FieldSerializer.NotNull
  public EnrichmentType enrichmentType;

  @JsonProperty
  @FieldSerializer.NotNull
  public EntityRecordFull.Type filterField;

  public boolean isStateful() {
    return false;
  }

  public Object filterField() {
    return this.filterField;
  }
}
