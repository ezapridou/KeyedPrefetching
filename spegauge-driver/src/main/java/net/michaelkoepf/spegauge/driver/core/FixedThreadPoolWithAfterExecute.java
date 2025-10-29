package net.michaelkoepf.spegauge.driver.core;

import lombok.Setter;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.task.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedThreadPoolWithAfterExecute extends ThreadPoolExecutor {

    private static final Logger LOGGER  = LoggerFactory.getLogger(FixedThreadPoolWithAfterExecute.class);
    private final AtomicInteger nThreadsAvailable;
  private final int nThreads;
  private final List<AbstractTask> producer;

  @Setter
  private List<EventContainer<Event>> eventContainers;

    public FixedThreadPoolWithAfterExecute(int nThreads, AtomicInteger nThreadsAvailable, List<AbstractTask> producer, ThreadFactory threadFactory) {
      super(nThreads, nThreads,
              0L, TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(),
              threadFactory);
      this.nThreadsAvailable = nThreadsAvailable;
      this.nThreads = nThreads;
      this.producer = producer;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      LOGGER.info("Thread finished", t);

      if (t != null) {
        LOGGER.error("Thread in thread pool threw an exception", t);
        if (eventContainers != null) {
          LOGGER.info("Setting poison pill in event containers to stop consumers");
          for (EventContainer eventContainer : eventContainers) {
            eventContainer.activatePoisonPill();
          }
        }
      }

      // exception may be wrapped in future
      if (r instanceof Future) {
        try {
          ((Future<?>) r).get();
        } catch (CancellationException | ExecutionException | InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error("Thread in thread pool threw an exception", e);
          if (eventContainers != null) {
            LOGGER.info("Setting poison pill in event containers to stop consumers");
            for (EventContainer eventContainer : eventContainers) {
              eventContainer.activatePoisonPill();
            }
          }

          // cancel all other producers if one throws an exception
          for (AbstractTask task : producer) {
            task.cancel();
          }

        }
      }

      if (nThreadsAvailable.get() == (nThreads-1)) {
        LOGGER.info("All threads finished. Clearing producer list.");
        producer.clear();
      }

      int n = nThreadsAvailable.incrementAndGet();
      LOGGER.info("Number of finished threads: " + n);

    }
}
