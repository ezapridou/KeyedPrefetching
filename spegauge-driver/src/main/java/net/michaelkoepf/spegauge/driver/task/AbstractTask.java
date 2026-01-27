package net.michaelkoepf.spegauge.driver.task;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import lombok.Getter;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.collection.UncheckedInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for tasks.
 * A task is a unit of work that either produces or consumes events.
 * The entire lifecycle of a task is managed by {@link net.michaelkoepf.spegauge.driver.core.Controller}.
 */
public abstract class AbstractTask implements Callable<Long> {

  private final Logger LOGGER = LoggerFactory.getLogger(AbstractTask.class);
  private final long initialDelay;

  public enum Status {
    WAITING_TO_START,
    RUNNING,
    SUSPENDED,
    TERMINATED_BY_CONTROLLER,
    TERMINATED_UNEXPECTEDLY,
    EXPECTED_TERMINATION_BY_SUT,
    FINISHED
  }

  private final CountDownLatch allTasksReadyToStartLatch;

  @Getter
  private final EventContainer<Event> queue;

  // TODO: instead of using the id of the thread, we could use the subtaskIndex.
  // TODO: this also needs modification in the controller (enhancement)
  @Getter
  private long id;

  @Getter
  private final int subtaskIndex;

  @Getter
  private CountDownLatch suspensionLatch;

  private Status status = Status.WAITING_TO_START;

  public AbstractTask(CountDownLatch allTasksReadyToStartLatch, EventContainer<Event> queue, int subtaskIndex, long initialDelay) {
    this.allTasksReadyToStartLatch = allTasksReadyToStartLatch;
    this.queue = queue;
    this.subtaskIndex = subtaskIndex;
    this.initialDelay = initialDelay;
  }

  public final Long call() {
    waitUntilReady();

    try {
      if (this.initialDelay > 0) {
        LOGGER.info("Producer " + subtaskIndex + " started... waiting for " + this.initialDelay + " ms.");
        Thread.sleep(this.initialDelay);
      }
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e);
    }

    status = Status.RUNNING;

    executeTask(queue);

    return id;
  }

  private void waitUntilReady() {
    id = Thread.currentThread().getId();
    allTasksReadyToStartLatch.countDown();

    try {
      allTasksReadyToStartLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Thread interrupted while waiting for other threads", e);
    }
  }

  /**
   * Hook that contains actual task logic (called by {@link #call()}).
   *
   * @param queue the queue to which the task should write events
   */
  public abstract void executeTask(EventContainer<Event> queue);

  /**
   * Returns the current status of the task.
   *
   * @return the status
   */
  public synchronized Status getStatus() {
      return status;
  }

  /**
   * Sets the status of the task.
   *
   * @param status the new status
   */
  public synchronized void setStatus(Status status) {
    this.status = status;
  }

  public synchronized void suspend(CountDownLatch modificationLatch) {
      this.status = Status.SUSPENDED;
      this.suspensionLatch = modificationLatch;
      this.notifyAll();
  }

  public synchronized void resume() {
      if (this.status != Status.SUSPENDED) {
        throw new IllegalStateException("Cannot resume task that is not suspended");
      }
      this.status = Status.RUNNING;
      this.suspensionLatch = null;
      this.notifyAll();
  }

  public synchronized void cancel() {
    this.status = Status.TERMINATED_BY_CONTROLLER;
    this.notifyAll();
  }

}
