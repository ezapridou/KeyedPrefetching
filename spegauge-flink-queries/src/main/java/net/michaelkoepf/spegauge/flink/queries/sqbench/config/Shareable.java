package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

public interface Shareable {

  /**
   * Check if this Shareable can be shared with another Shareable.
   *
   * @param o The other Shareable to check for.
   * @return True if this Shareable can be shared with the other Shareable, false otherwise.
   */
  boolean isShareable(Shareable o);
}
