package net.michaelkoepf.spegauge.api.driver;

/** An event is the basic unit of transmission between the benchmarking driver and the system under test. */
// TODO: move to common (incl. implementations)
public interface Event {

  /**
   * Serializes an event to a string.
   *
   * @return The event serialized as a string.
   */
  String serializeToString();

  /**
   * Serializes an event to a byte array.
   *
   * @return The event serialized as a byte array.
   */
  default byte[] serializeToBytes() {
    throw new UnsupportedOperationException("serializeToBytes");
  }

  long getEventId();

}
