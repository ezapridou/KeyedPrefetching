package net.michaelkoepf.spegauge.flink.sdk.common;

import lombok.Getter;

@Getter
public class EventTimeLatencyMeterObject<T> {

  private final T event;
  private final long eventEmissionTimeStampMilliSeconds;

  public EventTimeLatencyMeterObject(T event, long timestamp) {
    this.event = event;
    this.eventEmissionTimeStampMilliSeconds = timestamp;
  }

}
