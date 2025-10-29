package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;

/**
 * Classes implementing this interface can be suspended and resumed with the purpose of suspending inactive operators
 * to save resources (i.e., they will not be scheduled by the scheduler OS and hence will not consume CPU time).
 */
public interface Suspendable {

  /**
   * Serializable (dummy) lock object (java.lang.Object is not serializable).
   */
  class SuspendableLockObject implements Serializable {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Possible state a suspendable object can be in.
   */
  enum State {
    ACTIVE,
    INACTIVE,
    RESUME_REQ
  }

  /**
   * Resumes the object (i.e., wakes it up from the INACTIVE state).
   *
   * @throws UnsupportedOperationException if the object does not support resuming
   * @throws SuspendableException if the object cannot be resumed (e.g., if the object is not in the INACTIVE state)
   */
  void resume();

  /**
   * Returns the current state of the object.
   *
   * @return the current state
   */
  State getState();

  /**
   * Suspends the object (i.e., puts it in the INACTIVE state). This method is executed in a best-effort manner, i.e.,
   * the object may not be suspended immediately after this method returns.
   * <p>
   * The object may be resumed by calling {@link #resume()}.
   * <p>
   * Some objects may not support pausing via direct calls (e.g. they are suspended via markers/control messages
   * in the data flow). In this case, this method will throw an exception.
   *
   * @throws UnsupportedOperationException if the object does not support pausing via direct calls
   */
  default void suspend() {
    throw new UnsupportedOperationException("This operator does not support pausing via direct calls.");
  }

}
