package net.michaelkoepf.spegauge.driver.core;

import net.michaelkoepf.spegauge.driver.Constants;
import net.michaelkoepf.spegauge.driver.task.AbstractTask;
import net.michaelkoepf.spegauge.driver.webapi.DataGenerationJob;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface AbstractController extends Callable<Long> {

  /**
   * Cancels the execution of this controller. Implementing methods may be synchronized or use locks for thread safety.
   */
  void cancelExecution();

  /**
   * Suspends all tasks of this controller.  Implementing methods may be synchronized or use locks for thread safety.
   *
   * @return true if a state change occurred, false otherwise
   */
  boolean suspendTasks();

  /**
   * Resumes all tasks in this controller. Implementing methods may be synchronized or use locks for thread safety.
   *
   * @return true if a state change occurred, false otherwise
   */
  boolean resumeTasks();

  /**
   * Set function that allows controller to update the job status of its associated data generation job.
   */
  void setUpdateJobStatus(Consumer<DataGenerationJob.Status> updateJobStatusFunction);

  /**
   * Get the port this controller is assigned to.
   */
  int getPort();

  /**
   * Get state of this controller.
   */
  default Map<String, Object> getState() {
    throw new UnsupportedOperationException("Not supported for this controller");
  }
}
