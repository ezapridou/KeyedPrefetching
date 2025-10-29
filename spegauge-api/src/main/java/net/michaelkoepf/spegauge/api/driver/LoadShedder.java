package net.michaelkoepf.spegauge.api.driver;

/**
 * Interface for load shedders. Load shedders are used in readers. Each reader has an instance of a load shedder
 * that is independent of other load shedder instances.
 */
public interface LoadShedder extends Copyable<LoadShedder> {

  /**
   * Determines if the event should be shed. Called exactly once for each event.
   * If this method returns true (i.e., the event should be shed), the event will be discarded and not
   * sent to the system under test.
   *
   * @param event the event
   * @return true if the event should be shed, false otherwise
   */
  boolean shed(SheddableEvent event);

  /**
   * Perform a step in this load shedder (called directly AFTER each call to shed).
   * This method can be used to update internal state of the load shedder (default: no-op).
   */
  default void step() {}

  /**
   * Create a copy of this instance.
   *
   * @return the copy
   */
  LoadShedder copy();

}
