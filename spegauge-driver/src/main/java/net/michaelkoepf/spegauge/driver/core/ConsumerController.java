package net.michaelkoepf.spegauge.driver.core;

import lombok.Getter;
import lombok.Setter;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.driver.Constants;
import net.michaelkoepf.spegauge.driver.Utils;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.exception.ConnectionGracefullyTerminateByClientException;
import net.michaelkoepf.spegauge.driver.exception.ControllerExecutionException;
import net.michaelkoepf.spegauge.driver.exception.StaleEventsException;
import net.michaelkoepf.spegauge.driver.task.*;
import net.michaelkoepf.spegauge.driver.webapi.DataGenerationJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * A controller is responsible for managing the lifecycle of tasks in a data generation job.
 * Each task runs in its own thread.
 */
public class ConsumerController implements AbstractController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerController.class);
    private final List<Socket> clientConnections = new ArrayList<>();
    private final List<EventContainer<Event>> queues;
    private final int numDataGenerators;
    @Getter
    private final int port;
    private final Set<Long> consumerThreadIds = new HashSet<>();
    private final List<AbstractTask> consumerTasks = new LinkedList<>();
    private final boolean tcpNoDelay;
    private final boolean autoFlush;
    private final UUID jobId;
    private final Queue<Integer> availablePorts;
    private final CountDownLatch controllerReadyLatch;
    private final TelemetryObject telemetryObject;
    private final boolean activateMonitoring;
    private final List<LoadShedder> loadShedders;
    private final CountDownLatch allTasksReadyLatch;
    private final Map<String, Object> state;
    private ExecutorService taskThreadPool;
    private ServerSocket serverSocket;
    private volatile boolean cancelledViaCancelExecution = false;

    @Setter
    private Consumer<DataGenerationJob.Status> updateJobStatus;

    private volatile boolean suspended = false;

    public ConsumerController(int numDataGenerators, int port, Queue<Integer> availablePorts, List<EventContainer<Event>> queues, boolean tcpNoDelay, boolean autoFlush, TelemetryObject telemetryObject, UUID jobId, CountDownLatch controllerReadyLatch, List<LoadShedder> loadShedders, CountDownLatch allTasksReadyLatch, ExecutorService threadPool, Map<String, Object> state) {
        this.numDataGenerators = numDataGenerators;

        this.availablePorts = availablePorts;
        this.port = port;

        this.queues = queues;
        this.tcpNoDelay = tcpNoDelay;
        this.autoFlush = autoFlush;
        this.telemetryObject = telemetryObject;

        this.jobId = jobId;

        this.controllerReadyLatch = controllerReadyLatch;

        if (telemetryObject != null) {
            this.activateMonitoring = true;
        } else {
            this.activateMonitoring = false;
        }

        this.loadShedders = loadShedders;
        this.allTasksReadyLatch = allTasksReadyLatch;
        this.taskThreadPool = threadPool;
        this.state = state;
    }

    @Override
    public Long call() {
        updateJobStatus.accept(DataGenerationJob.Status.WAITING_FOR_CLIENTS);

        acceptClientConnections();

        if (cancelledViaCancelExecution) {
            return Thread.currentThread().getId();
        }

        updateJobStatus.accept(DataGenerationJob.Status.CLIENTS_CONNECTED);

        CompletionService<Long> completionService = new ExecutorCompletionService<>(taskThreadPool);

        // coordinates consumers when SUT merges sources
        CountDownLatch sutTerminationRequestLatch = new CountDownLatch(numDataGenerators);

        assert loadShedders.size() == numDataGenerators : "Wrong number of load shedders. Expected: " + numDataGenerators + ". Got: " + loadShedders.size();

        // start producers and consumers
        for (int i = 0; i < numDataGenerators ; i++) {
            // a queue is always shared between one producer and one consumer to avoid that events
            // arrive out of order
            EventContainer<Event> queue = queues.get(i);

            // consumer
            AbstractTask consumer;
            if (state != null) {
                int currIdx = (int) state.get(i + "");
                LOGGER.info("Consumer " + i + " will start at index " + currIdx);
                consumer = new TCPConsumerTask(allTasksReadyLatch, queue, clientConnections.get(i), autoFlush, i, sutTerminationRequestLatch, loadShedders.get(i), currIdx);
            } else {
                consumer = new TCPConsumerTask(allTasksReadyLatch, queue, clientConnections.get(i), autoFlush, i, sutTerminationRequestLatch, loadShedders.get(i));
            }
            completionService.submit(consumer);
            consumerTasks.add(consumer);
        }

        // wait until all threads are ready
        allTasksReadyLatch.countDown();

        try {
            allTasksReadyLatch.await();
        } catch (InterruptedException e) {
            cleanUpAndThrow(new ControllerExecutionException("Controller interrupted while waiting for other threads", e));
        }

        updateJobStatus.accept(DataGenerationJob.Status.RUNNING);

        for (int i = 0; i < consumerTasks.size(); i++) {
            long consumerThreadId = consumerTasks.get(i).getId();
            consumerThreadIds.add(consumerThreadId);
        }

        assert consumerThreadIds.size() == numDataGenerators : "Wrong number of consumer threads. Expected: " + numDataGenerators + ". Got: " + consumerThreadIds.size();

        if (activateMonitoring) {
            for (int i = 0; i < consumerTasks.size(); i++) {
                if (consumerTasks.get(i) instanceof MonitorableTask) {
                    telemetryObject.registerTask((MonitorableTask) consumerTasks.get(i), i);
                } else {
                    LOGGER.warn("Monitoring activated but consumer " + i + " does not implement MonitorableTask interface");
                }
            }

            telemetryObject.activateMonitoring();
        }

        if (cancelledViaCancelExecution) {
            return Thread.currentThread().getId();
        }

        LOGGER.info("Waiting for consumers to finish");

        try {
            // TODO: bug -- for some reason all consumers stop when the first one stops
            while(!consumerThreadIds.isEmpty()) {
                Future<Long> future;
                future = completionService.take();

                Long threadId = future.get();

                if (consumerThreadIds.remove(threadId)) {
                    LOGGER.info("Consumer with ID " + threadId + " finished");
                } else {
                    throw new IllegalStateException("Consumer with ID " + threadId + " finished but was not in the list of active consumer threads");
                }
            }

        } catch (InterruptedException e) {
            updateJobStatus.accept(DataGenerationJob.Status.ILLEGAL_STATE);
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StaleEventsException) {
                updateJobStatus.accept(DataGenerationJob.Status.STALE_EVENTS);
                throw new ControllerExecutionException(e);
            } else if (e.getCause() instanceof ConnectionGracefullyTerminateByClientException) {
                LOGGER.info("Connection was gracefully terminated by client");
            } else {
                cleanUpAndThrow(new ControllerExecutionException("Consumer thread terminated with error", e));
            }
        } finally {
            cleanUp();
        }

        updateJobStatus.accept(DataGenerationJob.Status.FINISHED);

        return Thread.currentThread().getId();
    }

    private void acceptClientConnections() {
        int numTimeouts = 0;

        try {
            this.serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(Constants.SERVER_SOCKET_TIMEOUT_MS);
            LOGGER.info(
                    "Waiting for " + numDataGenerators + " client(s) to connect on TCP port " + port);

            while (clientConnections.size() < numDataGenerators) {
                try {
                    this.controllerReadyLatch.countDown();

                    try {
                        this.controllerReadyLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    Socket socket = serverSocket.accept();

                    // TODO: also client hello necessary?
                    LOGGER.info("Client connected... Sending DRIVER HELLO to client");
                    socket.setTcpNoDelay(true);
                    socket.getOutputStream().write(("DRIVER HELLO " + jobId.toString() + " " + clientConnections.size() + "\n").getBytes());
                    socket.getOutputStream().flush();

                    socket.setTcpNoDelay(tcpNoDelay);
                    clientConnections.add(socket);
                    LOGGER.info(clientConnections.size() + "/" + numDataGenerators + " client(s) connected");
                } catch (SocketTimeoutException e) {
                    numTimeouts++;

                    LOGGER.warn(
                            numTimeouts
                                    + "/"
                                    + Constants.SERVER_SOCKET_MAX_TIMEOUTS
                                    + " of server socket timeouts reached");

                    if (numTimeouts == Constants.SERVER_SOCKET_MAX_TIMEOUTS) {
                        LOGGER.warn("Maximum number of server socket timeouts reached");
                        updateJobStatus.accept(DataGenerationJob.Status.TIMEOUT);
                        cleanUp();
                        throw new ControllerExecutionException("Clients did not connect within specified time limit");
                    }
                }
            }
        } catch (IOException e) {
            if (cancelledViaCancelExecution) {
                LOGGER.info("Execution cancelled by another thread while waiting for clients to connect");
                return;
            }
            throw new UncheckedIOException(e);
        } finally {
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    LOGGER.error("IOException while closing server socket", e);
                    throw new UncheckedIOException(e);
                }
            }
        }

        LOGGER.info("All clients have connected successfully");
    }

    private void cleanUp() {
        LOGGER.info("Deactivate monitoring");
        if (activateMonitoring) {
            if (telemetryObject.deactivateMonitoring()) {
                LOGGER.info("Monitoring deactivated");
            } else {
                LOGGER.warn("Monitoring could not be deactivated");
            }
        }

        try {
            if (this.serverSocket != null && !this.serverSocket.isClosed()) {
                LOGGER.info("Server socket open... closing it");
                this.serverSocket.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOGGER.info("Cancel all consumer tasks");
        for (AbstractTask consumerTask : consumerTasks) {
            consumerTask.cancel();
        }

        LOGGER.info("Close connections to clients");
        Utils.closeConnections(clientConnections);

        LOGGER.info("Adding port " + port + " back to available ports queue");
        synchronized (availablePorts) {
            if (!availablePorts.contains(port)) {
                availablePorts.add(port);
            }
        }
    }

    private void cleanUpAndThrow(RuntimeException e) {
        LOGGER.error("Execution aborted... cleaning up");

        if (e == null) {
            throw new IllegalArgumentException("Argument e might not be null");
        }

        LOGGER.info("Activate poison pill on all queues");
        for (EventContainer<Event> queue : queues) {
            queue.activatePoisonPill();
        }

        cleanUp();

        updateJobStatus.accept(DataGenerationJob.Status.UNEXPECTED_ABORTION_BY_SUT);

        throw e;
    }

    /**
     * Cancels the execution of the controller.
     */
    public synchronized void cancelExecution() {
        LOGGER.info("Execution cancelled by another thread");

        cancelledViaCancelExecution = true;


        cleanUp();

        updateJobStatus.accept(DataGenerationJob.Status.CANCELLED);
    }



    // TODO
    /**
     * Suspends all tasks. This method is synchronized to avoid race conditions in case of multiple calls.
     *
     * @return true if a state change occurred, false otherwise
     */
    public synchronized boolean suspendTasks() {
        if (suspended) {
            LOGGER.warn("Tasks are already suspended");
            return false;
        }

        CountDownLatch latch = new CountDownLatch(numDataGenerators + 1);

        LOGGER.info("Latch count: " + latch.getCount());

        LOGGER.info("Suspending " + consumerTasks.size() + " consumer tasks");
        for (AbstractTask consumerTask : consumerTasks) {
            consumerTask.suspend(latch);
        }

        latch.countDown();

        try {
            if (!latch.await(Constants.CONTROLLER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                return false;
            }
        } catch (InterruptedException e) {
            LOGGER.error("Controller unexpectedly interrupted while waiting for tasks to suspend");
            throw new RuntimeException(e);
        }

        LOGGER.info("All tasks suspended");

        suspended = true;

        return true;
    }

    @Override
    public Map<String, Object> getState() {
        if (!suspended) {
            throw new IllegalStateException("Controller is not suspended. This should not happen");
        }

        if (consumerTasks.isEmpty()) {
            throw new IllegalStateException("No consumer tasks found. This should not happen");
        }

        Map<String, Object> state = new HashMap<>();

        for (AbstractTask consumerTask : consumerTasks) {
            state.put(consumerTask.getSubtaskIndex() + "", ((TCPConsumerTask) consumerTask).getCurrIdx().get());
        }

        return state;
    }

    /**
     * Resumes all tasks. This method is synchronized to avoid race conditions in case of multiple calls.
     *
     * @return true if a state change occurred, false otherwise
     */
    public synchronized boolean resumeTasks() {
        if (!suspended) {
            LOGGER.warn("Tasks are not suspended");
            return false;
        }

        LOGGER.info("Resuming " + consumerTasks.size() + " consumer tasks");
        for (AbstractTask consumerTask : consumerTasks) {
            consumerTask.resume();
        }

        LOGGER.info("Called resume on all tasks");

        suspended = false;

        return true;
    }
}
