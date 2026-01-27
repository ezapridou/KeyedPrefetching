package net.michaelkoepf.spegauge.driver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.util.ConcurrencyUtil;
import net.michaelkoepf.spegauge.api.driver.Config;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.EventGeneratorFactory;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.driver.collection.EventArrayBlockingQueue;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import net.michaelkoepf.spegauge.driver.core.AbstractController;
import net.michaelkoepf.spegauge.driver.core.FixedThreadPoolWithAfterExecute;
import net.michaelkoepf.spegauge.driver.core.TelemetryMetricType;
import net.michaelkoepf.spegauge.driver.core.TelemetryObject;
import net.michaelkoepf.spegauge.driver.exception.*;
import net.michaelkoepf.spegauge.driver.task.AbstractTask;
import net.michaelkoepf.spegauge.driver.task.BroadcastProducerTask;
import net.michaelkoepf.spegauge.driver.collection.InMemoryAppendOnlyLog;
import net.michaelkoepf.spegauge.driver.webapi.APIUtils;
import net.michaelkoepf.spegauge.driver.webapi.DataGenerationJob;
import net.michaelkoepf.spegauge.driver.webapi.JobCopyRequest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Entry point for the driver application and web API.
 */
@CommandLine.Command(
    name = "spegauge-driver",
    mixinStandardHelpOptions = true,
    description = "Driver application for SPEGauge")
public class Main implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ScheduledExecutorService telemetryThreadPool = Executors.newScheduledThreadPool(Constants.TELEMETRY_THREAD_POOL_SIZE, new ThreadFactory() {
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    private final AtomicLong threadNumber = new AtomicLong(1);

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = defaultFactory.newThread(r);
      thread.setPriority(Thread.MIN_PRIORITY);
      thread.setName("telemetry-thread-pool-thread-" + threadNumber.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  });

  @CommandLine.Option(
      names = {"-g", "--num-parallel-generator-instances"},
      description = "Number of parallel data generators")
  private int numParallelGeneratorInstances = 1;

  @CommandLine.Option(
      names = {"-p", "--port"},
      split = ",",
      description = "Data generator ports. The number of ports determines the number of parallel data generators. Default: 8100")
  private List<Integer> ports = List.of(8110);

  @CommandLine.Option(
          names = {"--tcp-no-delay"},
          arity = "0",
          description = "Disable Nagle's algorithm in TCP stack when transmitting events (i.e., send data immediately). Normally combined with --auto-flush.")
  private boolean tcpNoDelay;

  @CommandLine.Option(
          names = {"--auto-flush"},
          arity = "0",
          description = "Automatically flush the output stream after each write operation (i.e., after each event).")
  private boolean autoFlush;

  @CommandLine.Option(
          names = {"--deactivate-monitoring"},
          arity = "0",
          description = "Disable monitoring of throughput and queue sizes. Default: false")
  private boolean deactivateMonitoring;

  @CommandLine.Option(
          names = {"--web-api-port"},
          description = "Web API port")
  private int webApiPort = 8100;

  @CommandLine.Parameters(
      index = "0",
      description = "Path to JAR file that contains the implementation of EventGeneratorFactory that should be used")
  private Path jarFilePath;

  public static void main(String[] args) {
    LOGGER.info("Starting driver");
    // TODO: validate arguments
    new CommandLine(new Main()).execute(args);
  }

  @Override
  public void run() {
    // === Setup ===
    final ExecutorService controllerThreadPool = Executors.newFixedThreadPool(ports.size(), new ThreadFactory() {
      private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
      private final AtomicLong threadNumber = new AtomicLong(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultFactory.newThread(r);
        thread.setPriority(Thread.MIN_PRIORITY);
        thread.setName("controller-thread-pool-thread-" + threadNumber.getAndIncrement());
        return thread;
      }
    });

    final AtomicInteger numGeneratorsAvailable = new AtomicInteger(numParallelGeneratorInstances);

    final List<AbstractTask> producers = new LinkedList<>();

    final FixedThreadPoolWithAfterExecute broadcastProducerThreadPool = new FixedThreadPoolWithAfterExecute(numParallelGeneratorInstances, numGeneratorsAvailable, producers, new ThreadFactory() {
      private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
      private final AtomicLong threadNumber = new AtomicLong(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultFactory.newThread(r);
        thread.setName("broadcast-producer-thread-pool-thread-" + threadNumber.getAndIncrement());
        return thread;
      }
    });

    final ExecutorService consumerThreadPool = Executors.newFixedThreadPool(numParallelGeneratorInstances * ports.size(), new ThreadFactory() {
      private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
      private final AtomicLong threadNumber = new AtomicLong(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = defaultFactory.newThread(r);
        thread.setName("consumer-thread-pool-thread-" + threadNumber.getAndIncrement());
        return thread;
      }
    });

    final Map<UUID, DataGenerationJob> jobs = new ConcurrentHashMap<>();
    final Config configClass = Utils.getConfigClass(jarFilePath);
    final Queue<Integer> availablePorts = new LinkedList<>(ports);
    final TypeReference<HashMap<String,Object>> typeRef
            = new TypeReference<>() {};
    final TelemetryObject.TelemetryData telemetryData = TelemetryObject.TelemetryData.getTelemetryData(telemetryThreadPool, Constants.REPORTING_INTERVAL_MS);

    // setup jetty server and javalin
    ConcurrencyUtil.INSTANCE.setUseLoom(false);
    QueuedThreadPool threadPool = new QueuedThreadPool(Constants.JETTY_THREAD_POOL_MAX_THREADS, Constants.JETTY_THREAD_POOL_MIN_THREADS, Constants.JETTY_THREAD_IDLE_TIMEOUT_MS/1000);
    Server server = new Server(threadPool);
    Javalin app = Javalin.create(config -> {
      config.jetty.server(() -> server);
      config.showJavalinBanner = false;
    });

    // === API Endpoints ===
    // TODO: refactor web api into separate class (enhancement)
    app.get("/", ctx -> ctx.result("Server is up and running"));

    // UUIDs of all jobs
    app.get("/jobs", new Handler() {
      // TODO: add filter for status (enhancement)
      @Override
      public void handle(@NotNull Context ctx) {
        ctx.json(new HashMap<String, Set<UUID>>(){{put("jobIds", jobs.keySet());}});
      }
    });

    // information about a specific job
    app.get("/jobs/{jobId}", new Handler() {
      @Override
      public void handle(@NotNull Context ctx) {
        UUID jobId = UUID.fromString(ctx.pathParam("jobId"));
        var job = jobs.get(jobId);
        if (job == null) {
          throw new NotFoundException("Job with ID " + jobId + " not found.");
        }

        // TODO: adapt for new telemetry objects
        ctx.json(
                new HashMap<String, Object>(){
                  {put("jobId", jobId.toString());
                    put("status", job.getStatus().name());
                    put("port", job.getPort());
                    put("currentThroughput", job.getTelemetryObject().getMetricAggregated(TelemetryMetricType.RawMetric.CURRENT_THROUGHPUT));
//                    put("currentBacklog", job.getTelemetryObject().getMetrics(TelemetryMetricType.RawMetric.CURRENT_BACKLOG));
                  }
                }
                );
      }
    });


    // API with single parallel producer and multiple parallel consumers and an in-memory append only log
    // Submit a new job or copy an existing job (only consumers are copied)
    // example request for COPYING: curl -X POST "http://<host>:<port>/jobs?toBeCopied=<jobId>&numCopies=<n greater or equal to 1>"
    app.post("/jobs", new Handler() {
      @Override
      public void handle(@NotNull Context ctx) {
        List<Runnable> resources = new LinkedList<>();
        if (ctx.queryParamMap().isEmpty()) { // new job
          if (!numGeneratorsAvailable.compareAndSet(numParallelGeneratorInstances, 0)) {
            throw new OperationInCurrentStateNotAllowedException("Data generator is already running. You can only copy an existing job.");
          }

          resources.add(() -> numGeneratorsAvailable.set(numParallelGeneratorInstances));

          // TODO: check for load shedder
          LOGGER.info("Requested new job submission");
          // covert submitted config (json) to hashmap
          var json = ctx.bodyInputStream();

          Map<String, Object> jsonMap;
          try {
            jsonMap = OBJECT_MAPPER.readValue(json, typeRef);
          } catch (DatabindException e) {
            resources.forEach(Runnable::run);
            throw new UnprocessableEntityException(e);
          } catch (IOException e) {
            resources.forEach(Runnable::run);
            throw new UncheckedIOException(e);
          }

          // *remove* parameters from the configuration since the remaining map expects a benchmark-specific schema that is parsed with jackson
          String maybeBenchmark = (String) jsonMap.remove("benchmark");
          String loadShedder = (String) jsonMap.remove("loadShedder");

          if (maybeBenchmark == null) {
            resources.forEach(Runnable::run);
            throw new IllegalArgumentException("No benchmark specified");
          }

          if (loadShedder == null) {
            loadShedder = "DummyLoadShedder";
          }

          if (!configClass.getEventGeneratorFactoriesFullyQualifiedNames().contains(maybeBenchmark)) {
            resources.forEach(Runnable::run);
            throw new IllegalArgumentException("Benchmark " + maybeBenchmark + " not supported Expected one of " + configClass.getEventGeneratorFactoriesFullyQualifiedNames());
          }

          int numJobs;
          if (jsonMap.containsKey("numOfQueries")) {
            try {
              numJobs = (int) jsonMap.get("numOfQueries");
            } catch (ClassCastException e) {
              resources.forEach(Runnable::run);
              throw new IllegalArgumentException("Number of queries must be an integer");
            }
          } else {
            resources.forEach(Runnable::run);
            throw new IllegalArgumentException("Number of queries not specified");
          }

          // claim necessary resources
          List<Integer> claimedPorts;
          synchronized (availablePorts) {
            claimedPorts = APIUtils.claimResources(numJobs, availablePorts, resources);
          }

          resources.add(() -> {
            synchronized (availablePorts) {
              availablePorts.addAll(claimedPorts);
            }
          });

          EventGeneratorFactory eventGeneratorFactory;
          try {
            // instantiate and initialize event generator factory
            LOGGER.info("Loading, instantiating and initializing EventGeneratorFactor " + maybeBenchmark + " from JAR file " + jarFilePath);
            eventGeneratorFactory =
                    Utils.createEventGeneratorFactory(jarFilePath, maybeBenchmark);
            eventGeneratorFactory.initialize(numParallelGeneratorInstances);

            if (eventGeneratorFactory.hasNext()) {
              LOGGER.warn("Event generator factory has more events than needed to fill all queues");
            }

            eventGeneratorFactory.setDataGenerationConfiguration(jsonMap);
          } catch (Exception e) {
            resources.forEach(Runnable::run);
            throw e;
          }

          long numEventsTotal;
          try {
            numEventsTotal = (long) jsonMap.get("numEventsTotal");
          } catch (ClassCastException e) {
            resources.forEach(Runnable::run);
            throw new IllegalArgumentException("Number of total events must be a long");
          }

          LOGGER.info("Setting up blocking queues");//("Setting up in-memory append only logs");
          List<EventContainer<Event>> queues = new LinkedList<>();
          int rem, perGeneratorNumEvents;
          try{
            rem = (int)numEventsTotal % numParallelGeneratorInstances;
            perGeneratorNumEvents = (int)numEventsTotal / numParallelGeneratorInstances;
          } catch (ClassCastException e) {
            resources.forEach(Runnable::run);
            throw new IllegalArgumentException("Number of total events divided by number of generators must be an integer");
          }
          for (int i = 0; i < numParallelGeneratorInstances; i++) {
            //InMemoryAppendOnlyLog<Event> queue = new InMemoryAppendOnlyLog<>(Event.class, numEventsTotal/numParallelGeneratorInstances + (i == (numParallelGeneratorInstances-1) ? rem : 0));
            EventArrayBlockingQueue<Event> queue = new EventArrayBlockingQueue<>(40000000,perGeneratorNumEvents + (i == (numParallelGeneratorInstances-1) ? rem : 0));
            queues.add(queue);
          }

          resources.add(() -> queues.forEach(EventContainer::clear));

          // (n parallel consumers + controller) * m jobs + n parallel producers
          CountDownLatch allTasksReadyLatch = new CountDownLatch(((numParallelGeneratorInstances + 1) * numJobs) + numParallelGeneratorInstances);

          for (int i = 0; i < numParallelGeneratorInstances; i++) {
            if (eventGeneratorFactory.hasNext()) {
              AbstractTask producer = new BroadcastProducerTask(allTasksReadyLatch, queues.get(i), eventGeneratorFactory.next(), i, Constants.DATA_GENERATION_INITIAL_DELAY_MS);

              broadcastProducerThreadPool.submit(producer);
              producers.add(producer);
              resources.add(producer::cancel);
            } else {
              resources.forEach(Runnable::run);
              throw new IllegalArgumentException("Event generator factory does not have enough events to fill all queues");
            }
          }

          broadcastProducerThreadPool.setEventContainers(queues);

          List<Map<String, Object>> responses = new LinkedList<>();

          for (int i = 0; i < numJobs; i++) {
            LOGGER.info("Setting up consumer " + (i + 1) + " of " + numJobs);
            List<LoadShedder> loadShedders = new LinkedList<>();
            for (int j = 0; j < numParallelGeneratorInstances; j++) {
              jsonMap.put("subtaskIndex", j);
              LoadShedder shedder = Utils.getLoadShedder(loadShedder, jsonMap);
              loadShedders.add(shedder);
            }

            // create and submit job
            responses.add(APIUtils.createAndSubmitJobAPI(jobs, controllerThreadPool, numParallelGeneratorInstances, claimedPorts.get(i), availablePorts, queues, tcpNoDelay, autoFlush, deactivateMonitoring ? null : telemetryData, loadShedders, allTasksReadyLatch, resources, consumerThreadPool, null));
          }

          resources.clear();

          LOGGER.info("Request successful");

          ctx.json(responses);
        } else { // copy job
          LOGGER.info("Requested job copy");
          if (numGeneratorsAvailable.get() != 0) {
            LOGGER.error("Copying of job failed. Not all data generators are running.");
            throw new OperationInCurrentStateNotAllowedException("Not all data generators are running.");
          }

          JobCopyRequest jobCopyRequest = APIUtils.validateJobCopyRequest(ctx.queryParamMap());

          UUID jobId = jobCopyRequest.getJobId();
          int numCopies = jobCopyRequest.getNumCopies();

          if (numCopies < 1) {
            throw new UnprocessableEntityException("numCopies must be greater than 0. Got " + numCopies);
          }

          DataGenerationJob jobToBeCopied = APIUtils.returnJobIfExistsElseThrow(jobId.toString(), jobs);

          if (jobToBeCopied.getStatus() != DataGenerationJob.Status.RUNNING) {
            throw new OperationInCurrentStateNotAllowedException("Job with ID " + jobId + " is not running. Cannot copy a job that is not running.");
          }

          resources = new LinkedList<>();
          List<Integer> claimedPorts;
          synchronized (availablePorts) {
            claimedPorts = APIUtils.claimResources(numCopies, availablePorts, resources);
          }

          resources.add(() -> {
            synchronized (availablePorts) {
              availablePorts.addAll(claimedPorts);
            }
          });

          assert claimedPorts.size() == numCopies: "Number of claimed ports does not match number of requested copies";

          // at this point, we can be sure that we have enough resources and hence we can directly create and submit jobs
          AbstractController controller = jobToBeCopied.getController();

          // suspend tasks and get state of consumers
          if (!controller.suspendTasks()) {
            LOGGER.error("Could not suspend tasks. Aborting request.");
            resources.forEach(Runnable::run);
            throw new OperationInCurrentStateNotAllowedException("Could not suspend tasks. Aborting request.");
          }

          Map<String, Object> state = controller.getState();

          // submit new jobs
          LOGGER.info("Creating and submitting " + numCopies + " new jobs");
          List<Map<String, Object>> response = new LinkedList<>();
          for (int i = 0; i < numCopies; i++) {
            List<LoadShedder> newLoadShedder = new LinkedList<>();

            // copy queues
            for (int j = 0; j < numParallelGeneratorInstances; j++) {
              LoadShedder shedder = jobToBeCopied.getLoadShedders().get(j).copy();
              newLoadShedder.add(shedder);
            }

            int newPort = ports.get(i);

            Map<String, Object> singleResponse = APIUtils.createAndSubmitJobAPI(jobs, controllerThreadPool, numParallelGeneratorInstances, newPort, availablePorts, jobToBeCopied.getQueues(), tcpNoDelay, autoFlush, deactivateMonitoring ? null : telemetryData, newLoadShedder, new CountDownLatch(numParallelGeneratorInstances), resources, consumerThreadPool, state);

            LOGGER.info("Created and submitted new job (" + (i+1) + "/" + numCopies + ") with ID " + singleResponse.get("jobId"));

            response.add(singleResponse);
          }

          // resume tasks
          if (!controller.resumeTasks()) { // sanity check
            LOGGER.error("Could not resume tasks. Aborting request.");
            throw new IllegalStateException("Could not resume tasks. This should not happen. Implementation error?");
          }

          LOGGER.info("Copying request successful");

          ctx.json(response);
        }
      }
    });

    // exception handlers
    app.exception(IllegalArgumentException.class, (e, ctx) -> {
      int code = 400;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(NotFoundException.class, (e, ctx) -> {
      int code = 404;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(OperationInCurrentStateNotAllowedException.class, (e, ctx) -> {
      int code = 409;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(NotEnoughResourcesException.class, (e, ctx) -> {
      int code = 409;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(UnprocessableEntityException.class, (e, ctx) -> {
      int code = 422;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(ControllerExecutionException.class, (e, ctx) -> {
      int code = 500;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    app.exception(Exception.class, (e, ctx) -> {
      int code = 500;
      ctx.status(code);
      ctx.json(new HashMap<String, String>(){{put("code", String.valueOf(code));put("message", e.getMessage());}});
    });

    // shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOGGER.info("Shutting down driver");

        LOGGER.info("Shutting down web server");
        app.stop();

        LOGGER.info("Shutting down controller and telemetry thread pool");
        controllerThreadPool.shutdownNow();
        consumerThreadPool.shutdownNow();
        broadcastProducerThreadPool.shutdownNow();
        telemetryThreadPool.shutdownNow();
      }});

    // run application
    app.start(webApiPort);
  }
}
