package net.michaelkoepf.spegauge.driver.task;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.api.driver.SheddableEvent;
import net.michaelkoepf.spegauge.driver.Utils;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.collection.UncheckedInterruptedException;
import net.michaelkoepf.spegauge.driver.core.TelemetryMetricType;
import net.michaelkoepf.spegauge.driver.exception.ConnectionGracefullyTerminateByClientException;
import net.michaelkoepf.spegauge.driver.exception.StaleEventsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPConsumerTask extends AbstractTask implements MonitorableTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(TCPConsumerTask.class);

  private final Socket clientConnection;

  private final AtomicLong eventCountMonitoring = new AtomicLong(0);
  private final AtomicLong nonSheddedEventsCountMonitoring = new AtomicLong(0);
  private final boolean autoFlush;
  private final CountDownLatch sutTerminationRequestLatch;
  private final LoadShedder loadShedder;

  private long lastThroughputTime = System.nanoTime();
  private long lastDataConsumptionRateTime = System.nanoTime();

  private Long maybeDesiredLastEventId = null;

  private boolean monitorableIsReady = false;

  @Getter
  private AtomicInteger currIdx = new AtomicInteger(0);

  private int startIdx = 0;

  public TCPConsumerTask(
          CountDownLatch readyCounter, EventContainer<Event> queue, Socket clientConnection, boolean autoFlush, int subtaskIndex, CountDownLatch sutTerminationRequestLatch, LoadShedder loadShedder, int currIdx) {
    super(readyCounter, queue, subtaskIndex, 0);
    this.clientConnection = clientConnection;
    this.autoFlush = autoFlush;
    this.sutTerminationRequestLatch = sutTerminationRequestLatch;
    this.loadShedder = loadShedder;
    this.currIdx.set(currIdx);
    this.startIdx = currIdx;
  }

  public TCPConsumerTask(
          CountDownLatch readyCounter, EventContainer<Event> queue, Socket clientConnection, boolean autoFlush, int subtaskIndex, CountDownLatch sutTerminationRequestLatch, LoadShedder loadShedder) {
    super(readyCounter, queue, subtaskIndex, 0);
    this.clientConnection = clientConnection;
    this.autoFlush = autoFlush;
    this.sutTerminationRequestLatch = sutTerminationRequestLatch;
    this.loadShedder = loadShedder;
  }


  @Override
  public void executeTask(EventContainer<Event> queue) {
    try (
            Writer writer = new BufferedWriter(new OutputStreamWriter(clientConnection.getOutputStream()), 8192);
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientConnection.getInputStream()), 4096);
    ) {
      String sourceCommand;
      lastThroughputTime = System.nanoTime();
      lastDataConsumptionRateTime = lastThroughputTime;
      monitorableIsReady = true;
      Event event = null;
        while (true) {
          try {
            if (queue.totalEvents() == currIdx.get()) {
              LOGGER.info("All events have been sent to the client.");
              break;
            }

            if (getStatus() == Status.SUSPENDED) {
              handleSuspension(writer, event);
            }

            // process next event
            Event temp = null;
            if (queue.accessibleByIndex()) {
              while (temp == null) {
                temp = queue.get(currIdx.get());
              }
              event = temp;
            } else {
              event = queue.get();
            }
            currIdx.incrementAndGet();

            boolean shed = this.loadShedder.shed((SheddableEvent) event);
            if (!shed) {
              String eventString = event.serializeToString();
              writeln(writer, eventString, autoFlush);
              nonSheddedEventsCountMonitoring.incrementAndGet();
            }
            eventCountMonitoring.incrementAndGet(); // TODO: what to do if we shedded?
            this.loadShedder.step();

            // check if source has sent a command (non-blocking)
            if (reader.ready() && (sourceCommand = reader.readLine()) != null) {
              handleCommandFromClient(sourceCommand, writer);
            }

            // should we stop sending events? >= because event may have been already sent
            if (maybeDesiredLastEventId != null && event.getEventId() >= maybeDesiredLastEventId) {
              if (event.getEventId() > maybeDesiredLastEventId) {
                LOGGER.warn("Actual last event id was greater than the event id of the desired last event: " + event.getEventId() + " > " + maybeDesiredLastEventId + ". Duplicates have been sent to the client.");
              }

              LOGGER.info("Send last event ack");
              writer.flush();
              clientConnection.setTcpNoDelay(true);
              writer.write("LAST EVENT ACK " + getSubtaskIndex() + " " + maybeDesiredLastEventId + "\n");
              writer.flush();
              clientConnection.setTcpNoDelay(false);
              String msg;

              if ((msg = reader.readLine()) == null) { // block until client closes connection
                LOGGER.info("Client closed connection after last event ack");
                setStatus(Status.EXPECTED_TERMINATION_BY_SUT);
                break;
              } else {
                LOGGER.error("Client did not close connection after last event ack. Received: " + msg);
                setStatus(Status.TERMINATED_UNEXPECTEDLY);
                throw new RuntimeException("Client did not close connection after last event ack. Received: " + msg);
              }
            }
            // inner catch block; if you want the producer to terminate, either break or throw an exception!
            // if you want  to continue, don't forget to clear the interruption flag
            event = null;
          } catch (UncheckedInterruptedException e) {
            if (getStatus() == Status.TERMINATED_BY_CONTROLLER) {
              if (!queue.isEmpty()) {
                LOGGER.warn("Termination was requested but not all data was transmitted to the client. Remaining events in queue: " + queue.size());
                throw new StaleEventsException("Termination was requested but not all data was transmitted to the client. Remaining events in queue: " + queue.size());
              } else {
                LOGGER.info("Thread was interrupted, termination was requested and there are no more events in the queue... return gracefully :)");
                break;
              }
            } else if (getStatus() == Status.SUSPENDED) {
              Thread.interrupted();
              LOGGER.info("Consumer suspended at event with ID " + (event != null ? event.getEventId() : null));
              // continues at the beginning of the loop and will realize that it is suspended
            } else {
              // if an interruption occurs in any other status than the above,
              // we make the defensive decision to stop the producer
              LOGGER.error("Unexpected interruption. Current status " + getStatus() + ". Stopping producer.");
              setStatus(Status.TERMINATED_UNEXPECTEDLY);
              throw new RuntimeException(e);
            }
          } catch (IOException e) {
            if (getStatus() == Status.TERMINATED_BY_CONTROLLER) {
              if (!queue.isEmpty()) {
                LOGGER.warn("Termination was requested but not all data was transmitted to the client. Remaining events in queue: " + queue.size());
                throw new StaleEventsException("Termination was requested but not all data was transmitted to the client. Remaining events in queue: " + queue.size());
              }
            } else {
              LOGGER.error("Client connection error");
              setStatus(Status.TERMINATED_UNEXPECTEDLY);
              throw new UncheckedIOException(e);
            }
          } finally { // inner finally block
            if (getStatus() == Status.EXPECTED_TERMINATION_BY_SUT) {
              sutTerminationRequestLatch.countDown();
              try {
                // wait until all other threads have also finished, otherwise controller will terminate all connections
                // as soon as the first thread finished its execution
                sutTerminationRequestLatch.await();
              } catch (InterruptedException ex) {
                LOGGER.error("Unexpected interruption while waiting for other threads to finish. Bug in Driver source code?");
                setStatus(Status.TERMINATED_UNEXPECTEDLY);
                throw new RuntimeException(ex);
              }

              // TODO: not the best way to solve a graceful return with an exception... but suffices for now
              throw new ConnectionGracefullyTerminateByClientException("Last event was acked and client closed connection... exiting");
            }
          }
        } // end of while loop
    } catch (IOException e) { // outer catch block for try-with-resources
      throw new UncheckedIOException("Connection closed", e);
    } finally { // outer finally block
      cleanUp();
    }
  }

  private void handleCommandFromClient(String sourceCommand, Writer writer) throws IOException {
    LOGGER.info("Received command from source: " + sourceCommand);

    if (sourceCommand.startsWith("LAST EVENT REQ ")) {
      String[] parts = sourceCommand.split(" ");
      long subTaskIdx = Integer.parseInt(parts[3]);

      if (subTaskIdx != getSubtaskIndex()) {
        LOGGER.error("Received LAST EVENT REQ for wrong subtask: " + subTaskIdx);
        writer.flush();
        clientConnection.setTcpNoDelay(true);
        writer.write("LAST EVENT DENY " + getSubtaskIndex() + " " + maybeDesiredLastEventId + "\n");
        writer.flush();
        throw new IllegalStateException("Received LAST EVENT REQ for wrong subtask: " + subTaskIdx);
      }

      maybeDesiredLastEventId = Long.parseLong(parts[4]);

      LOGGER.info("Received valid LAST EVENT REQ from source: " + maybeDesiredLastEventId);
    } else {
      LOGGER.error("Received unknown command from source: " + sourceCommand);
      throw new IllegalStateException("Received unknown command from source: " + sourceCommand);
    }
  }

  private void handleSuspension(Writer writer, Event event) throws IOException {
    LOGGER.info("Discovered suspension at index " + currIdx);

    assert getSuspensionLatch() != null: "Suspension latch is null. Bug in Driver source code?";

    // flush buffers and send last event id to source + wait for ack
    writer.flush();
    clientConnection.setTcpNoDelay(true);
    writer.write("LAST EVENT BEFORE COPY REQ " + getSubtaskIndex() + " " + (event != null ? event.getEventId() : null) + "\n");
    writer.flush();
    clientConnection.setTcpNoDelay(false);

    // wait until all tasks are suspended
    getSuspensionLatch().countDown();

    try {
      LOGGER.info("Waiting for all tasks to be suspended.");
      getSuspensionLatch().await();
    } catch (InterruptedException e) {
      LOGGER.error("Unexpected interruption. Current status" + getStatus() + ". Stopping producer. Bug in Driver source code?");
      setStatus(Status.TERMINATED_UNEXPECTEDLY);
      throw new RuntimeException(e);
    }

    // suspend the consumer until it is resumed
    while (getStatus() == Status.SUSPENDED) { // guard against spurious wakeup
      synchronized (this) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          LOGGER.info("Woke up from suspension. Current status: " + getStatus());
        }
      }
    }

    LOGGER.info("Exited suspension");

    if (getStatus() == Status.RUNNING) {
      LOGGER.info("Resuming consumer.");
    } else {
      LOGGER.warn("Consumer resumed, but status is not RUNNING. Interrupting...");
      throw new IllegalStateException("Consumer resumed, but status is not RUNNING. Interrupting...");
    }
  }

  private static void writeln(Writer writer, String eventString, boolean autoFlush) throws IOException {
    writer.write(eventString + "\n");

    if (autoFlush) {
      writer.flush();
    }
  }

  private void cleanUp() {
    Utils.closeConnection(clientConnection);
  }

  @Override
  public long getMetric(TelemetryMetricType metricType) {
    if (!monitorableIsReady) {
      return 0;
    }

    switch ((TelemetryMetricType.RawMetric) metricType) {
      case CURRENT_THROUGHPUT:
        return measureThroughput();
      case CURRENT_BACKLOG:
        return backlog();
      case CONSUMED_SINCE_START:
        return currIdx.get();
      case CONSUMED_SINCE_START_SINCE_COPY:
        return currIdx.get() - startIdx;
      case CURRENT_RATE_OF_DATA_CONSUMPTION:
        return measureDataConsumptionRate();
      default:
        LOGGER.warn("Illegal metric type for consumer: " + metricType);
        return 0;
    }

  }

  private long measureThroughput() {
    if (!monitorableIsReady) {
      return 0;
    }

    long currentTime = System.nanoTime();
    long throughput = (long) (nonSheddedEventsCountMonitoring.getAndSet(0) / ((currentTime - lastThroughputTime) * 1e-9));
    lastThroughputTime = currentTime;
    return throughput;
  }

  private long measureDataConsumptionRate() {
    if (!monitorableIsReady) {
      return 0;
    }

    long currentTime = System.nanoTime();
    long dataConsRate = (long) (eventCountMonitoring.getAndSet(0) / ((currentTime - lastDataConsumptionRateTime) * 1e-9));
    lastDataConsumptionRateTime = currentTime;
    return dataConsRate;
  }

  private long backlog() {
    if (!monitorableIsReady) {
      return 0;
    }

    if (this.getQueue().isAppendOnly()) {
      return this.getQueue().size() - currIdx.get();
    } else {
      return this.getQueue().size();
    }
  }

}
