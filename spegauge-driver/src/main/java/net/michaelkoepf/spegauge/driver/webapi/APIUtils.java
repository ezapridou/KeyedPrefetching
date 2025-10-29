package net.michaelkoepf.spegauge.driver.webapi;

import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.driver.Constants;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.core.*;
import net.michaelkoepf.spegauge.driver.exception.GenericAPIException;
import net.michaelkoepf.spegauge.driver.exception.NotEnoughResourcesException;
import net.michaelkoepf.spegauge.driver.exception.NotFoundException;
import net.michaelkoepf.spegauge.driver.exception.UnprocessableEntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Utility class for the web API.
 */
public final class APIUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(APIUtils.class);

    private APIUtils() {
        // not meant to be instantiated
    }

    /**
     * Tries to claim the given desired number of resources. This method is *NOT* thread-safe.
     * You need to synchronize access to the availablePorts queue if you want to use this method in a multi-threaded environment.
     *
     * @param numResources the desired number of resources
     * @param availablePorts the queue of available ports
     * @return the claimed resources
     * @throws NotEnoughResourcesException if not enough resources are available
     */
    public static List<Integer> claimResources(int numResources, Queue<Integer> availablePorts, List<Runnable> resources) {
        List<Integer> ports = new LinkedList<>();

        for (int i = 0; i < numResources; i++) {
            try {
                int port = availablePorts.remove();
                ports.add(port);
            } catch (NoSuchElementException e) {
                resources.forEach(Runnable::run);
                throw new NotEnoughResourcesException("Not enough resources available");
            }
        }
        return ports;
    }

    public static Map<String, Object> createAndSubmitJobAPI(Map<UUID, DataGenerationJob> jobs, ExecutorService controllerThreadPool, int numParallelGeneratorInstances, int port, Queue<Integer> availablePorts, List<EventContainer<Event>> queues, boolean tcpNoDelay, boolean autoFlush, TelemetryObject.TelemetryData telemetryData, List<LoadShedder> loadShedders, CountDownLatch allTasksReadyLatch, List<Runnable> resources, ExecutorService taskThreadPool, Map<String, Object> state) {
        UUID jobId = UUID.randomUUID();
        LOGGER.info("Creating new job with ID " + jobId);

        TelemetryObject telemetryObject = null;

        if (telemetryData != null) {
            telemetryObject = TelemetryObject
                    .builder(telemetryData, jobId)
                    .withMonitoredTask(
                            numParallelGeneratorInstances,
                            new TelemetryMetricType[]{
                                    TelemetryMetricType.RawMetric.CONSUMED_SINCE_START,
                                    TelemetryMetricType.RawMetric.CURRENT_THROUGHPUT,
                                    TelemetryMetricType.RawMetric.CURRENT_BACKLOG,
                                    TelemetryMetricType.RawMetric.CURRENT_RATE_OF_DATA_CONSUMPTION,
                                    TelemetryMetricType.SmoothedMetric.CURRENT_THROUGHPUT
                            }
                    )
                    .withLogging(new TelemetryMetricType[]{
                            TelemetryMetricType.RawMetric.CONSUMED_SINCE_START,
                            TelemetryMetricType.RawMetric.CURRENT_THROUGHPUT,
                            TelemetryMetricType.RawMetric.CURRENT_BACKLOG,
                            TelemetryMetricType.RawMetric.CURRENT_RATE_OF_DATA_CONSUMPTION,
                            TelemetryMetricType.SmoothedMetric.CURRENT_THROUGHPUT
                    })
                    .build();

            try {
                ObjectName objectName = new ObjectName("net.michaelkoepf.spegauge.driver:name=telemetry,id=" + jobId);
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                StandardMBean telemetryMBean = new StandardMBean(telemetryObject, TelemetryMBean.class);
                server.registerMBean(telemetryMBean, objectName);
            } catch (MalformedObjectNameException | InstanceAlreadyExistsException |
                     MBeanRegistrationException | NotCompliantMBeanException e) {
                throw new GenericAPIException("Could not register telemetry object" + e.getMessage(), e);
            }
        }

        AbstractController controller;
        CountDownLatch controllerReadyLatch = new CountDownLatch(1);
        controller = new ConsumerController(numParallelGeneratorInstances, port, availablePorts, queues, tcpNoDelay, autoFlush, telemetryObject, jobId, controllerReadyLatch, loadShedders, allTasksReadyLatch, taskThreadPool, state);

        for (LoadShedder loadShedder : loadShedders) {
            if (loadShedder instanceof FrequencyLoadShedder) {
                ((FrequencyLoadShedder) loadShedder).setTelemetryObject(telemetryObject);
            }
        }

        DataGenerationJob job = new DataGenerationJob(controller, queues, telemetryObject, null, loadShedders);
        controller.setUpdateJobStatus(job::setStatus); // can be set after creation, because controller is not running yet
        jobs.put(jobId, job);
        Future<Long> future = controllerThreadPool.submit(controller);
        resources.add(controller::cancelExecution);
        job.setControllerFuture(future);
        job.setPort(controller.getPort());

        // wait until controller is at sync point to make sure that server socket is ready
        boolean error = false;
        try {
            if (!controllerReadyLatch.await(Constants.WEB_API_CALLS_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                error = true;
                throw new GenericAPIException("Controller did not reach sync point within specified time limit of " + Constants.WEB_API_CALLS_TIMEOUT_MS + "ms");
            }
        } catch (InterruptedException e) {
            error = true;
            throw new GenericAPIException("Interrupted while waiting for controller", e);
        } finally {
            if (error) {
                resources.forEach(Runnable::run);
            }
        }

        Map<String, Object> response = new HashMap<>(){{put("jobId", jobId); put("port", job.getPort());}};

        try {
            response.put("hostname", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            // ignored
            response.put("hostname", null);
        }

        return response;
    }

    public static JobCopyRequest validateJobCopyRequest(Map<String, List<String>> queryParams) {
        if (queryParams.size() != 2 || !queryParams.containsKey("toBeCopied") || !queryParams.containsKey("numCopies") || queryParams.get("toBeCopied").size() != 1 || queryParams.get("numCopies").size() != 1) {
            throw new UnprocessableEntityException("Query parameters must contain toBeCopied and numCopies. Got: " + queryParams);
        }

        return new JobCopyRequest(queryParams.get("toBeCopied").get(0), Integer.parseInt(queryParams.get("numCopies").get(0)));
    }

    /**
     * Cancels the job with the given ID.
     */
    public static void cancelJob(DataGenerationJob job) {
        job.getController().cancelExecution();

        try {
            LOGGER.info("Wait for " + Constants.CANCEL_TIME_WAIT_MS + " milliseconds...");
            Thread.sleep(Constants.CANCEL_TIME_WAIT_MS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Check if controller was successfully cancelled");

        if (job.getControllerFuture().isDone() || job.getStatus().equals(DataGenerationJob.Status.WAITING_FOR_CLIENTS)) {
            LOGGER.info("Controller successfully cancelled!");
        } else {
            job.setStatus(DataGenerationJob.Status.ILLEGAL_STATE);
            throw new IllegalStateException("Expected controller to be done");
        }

        for (EventContainer<Event> queue: job.getQueues()) {
            queue.clear();
        }

        job.getQueues().clear();
    }

    /**
     * Returns the job with the given ID if it exists, otherwise throws a {@link NotFoundException}.
     *
     * @param jobId the job ID
     * @param jobs the map of jobs
     * @return the job
     * @throws NotFoundException if the job does not exist
     */
    public static DataGenerationJob returnJobIfExistsElseThrow(String jobId, Map<UUID, DataGenerationJob> jobs) {
        DataGenerationJob job = jobs.get(UUID.fromString(jobId));
        if (job == null) {
            throw new NotFoundException("Job with id " + jobId + " does not exist");
        }
        return job;
    }
}
