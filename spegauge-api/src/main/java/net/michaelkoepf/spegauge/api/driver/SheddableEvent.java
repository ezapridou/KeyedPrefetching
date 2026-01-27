package net.michaelkoepf.spegauge.api.driver;

import lombok.Getter;

public interface SheddableEvent extends Event {

  SheddingInformation getSheddingInformation();

  @Getter
  class SheddingInformation {

    private final long desiredThroughputPerSecond;

    public SheddingInformation(long desiredThroughputPerSecond) {
      this.desiredThroughputPerSecond = desiredThroughputPerSecond;
    }
  }

}
