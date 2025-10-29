package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Objects;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class ExtractOperator extends Operator {
  public enum OperationType {
    EXTRACT_JSON
  }

  @JsonProperty
  @FieldSerializer.NotNull
  public OperationType operationType;

  @Override
  public int hashCode() {
    return Objects.hashCode(operationType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExtractOperator that = (ExtractOperator) o;
    return operationType == that.operationType;
  }
}
