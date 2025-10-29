package net.michaelkoepf.spegauge.api.driver;

/**
 * A copyable object.
 *
 * @param <T> The type of the object.
 */
public interface Copyable<T> {

  /**
   * A copy of this object with the same state.
   *
   * @return the copy
   */
  T copy();

}
