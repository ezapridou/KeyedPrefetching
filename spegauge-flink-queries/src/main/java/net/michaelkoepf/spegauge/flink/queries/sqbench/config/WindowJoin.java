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
public class WindowJoin extends WindowOperator {
  public enum JoinType {
    TWO_WAY_EQUI_JOIN,
    THREE_WAY_EQUI_JOIN
  }

  @JsonProperty
  @FieldSerializer.NotNull
  public JoinType joinType;


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WindowJoin that = (WindowJoin) o;

    return joinType == that.joinType && windowSizeMs == that.windowSizeMs && windowSlideMs == that.windowSlideMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(joinType, windowSizeMs, windowSlideMs);
  }

}
