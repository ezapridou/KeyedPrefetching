package net.michaelkoepf.spegauge.driver.core;

import lombok.Getter;
import net.michaelkoepf.spegauge.driver.exception.UnknownMetricException;
import net.michaelkoepf.spegauge.driver.task.MonitorableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Responsible for monitoring the throughput and queue sizes of a set of tasks.
 */
public class TelemetryObject implements TelemetryMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryObject.class);

  private long numRegisteredTasks;

  public static Builder builder(TelemetryData telemetryData, UUID jobId) {
    return new Builder(telemetryData, jobId);
  }

  public static class Builder {
    private static final int DEFAULT_SMOOTHING_TIMESPAN_MS = 60000;
    private final TelemetryData telemetryData;
    private final UUID jobId;
    private Integer numTasks = null;
    private TelemetryMetricType[] metricTypes = null;
    private Set<TelemetryMetricType> toBeLogged = new HashSet<>();

    private int smoothingTimespan = DEFAULT_SMOOTHING_TIMESPAN_MS;

    public Builder(TelemetryData telemetryData, UUID jobId) {
      this.telemetryData = telemetryData;
      this.jobId = jobId;
    }

    public Builder withMonitoredTask(int numTasks, TelemetryMetricType[] metricTypes) {
      this.numTasks = numTasks;
      this.metricTypes = metricTypes;
      return this;
    }

    public Builder withLogging(TelemetryMetricType[] metricTypes) {
      this.toBeLogged.addAll(Arrays.asList(metricTypes));
      return this;
    }

    public Builder withSmoothingTimespan(int smoothingTimespan) {
      this.smoothingTimespan = smoothingTimespan;
      return this;
    }

    public TelemetryObject build() {
      if (numTasks == null) {
        throw new IllegalStateException("No tasks to monitor specified");
      }

      return new TelemetryObject(this.telemetryData, numTasks, this.jobId, this.metricTypes, this.toBeLogged, this.smoothingTimespan);
    }
  }

  private final TelemetryData telemetryData;
  private final UUID jobId;
  private ScheduledFuture<?> future;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final MonitorableTask[] registeredTasks;
  private final Map<TelemetryMetricType, long[]> rawMetrics = new LinkedHashMap<>(); // LinkedHashMap to preserve order when writing to a file

  private final Map<TelemetryMetricType, Map<Integer, long[]>> smoothedMetrics = new HashMap<>();
  private final Set<TelemetryMetricType> toBeLogged;

  private final int historySize;
  private final int timespanInSeconds;

  private long throughputMeasurementID = 0;

  private final boolean WRITE_TO_FILE = true;

  private FileWriter fileWriter;

  private final String filePath = "~/GSexperiments/";

  private double[] currentSheddingRate;

  private int numExecutions = 0;

  private TelemetryObject(TelemetryData telemetryData, int numTasks, UUID jobId, TelemetryMetricType metricTypes[], Set<TelemetryMetricType> toBeLogged, int smoothingTimespanMS) {
    this.telemetryData = telemetryData;
    this.registeredTasks = new MonitorableTask[numTasks];
    this.jobId = jobId;

    // timespan and history size calculation based on src/main/java/org/apache/flink/metrics/MeterView.java (Flink 1.13.6)
    this.timespanInSeconds =
            (int) Math.max(
                    smoothingTimespanMS - (smoothingTimespanMS % telemetryData.getMonitoringIntervalMS()),
                    telemetryData.getMonitoringIntervalMS()) / 1000;

    this.historySize = this.timespanInSeconds / ((int) telemetryData.getMonitoringIntervalMS() / 1000) + 1;

    if (this.historySize < 2) {
      LOGGER.warn("The history for smoothed values size is less than 2 which leads to incorrect results. " +
              "Adjust the monitoring interval or the smoothing timespan. Current monitoring interval: " +
              telemetryData.getMonitoringIntervalMS() + "ms, " +
              "current smoothing timespan: " + smoothingTimespanMS + "ms");
    }

    for (TelemetryMetricType metricType : metricTypes) {
      if (metricType instanceof TelemetryMetricType.RawMetric) {
        rawMetrics.put(metricType, new long[numTasks]);
      } else if (metricType instanceof TelemetryMetricType.SmoothedMetric) {
        Map<Integer, long[]> smoothedMetricValues = new HashMap<>();

        // same logic as in flink-metrics/flink-metrics-core/src/main/java/org/apache/flink/metrics/MeterView.java
        for (int i = 0; i < numTasks; i++) {
          smoothedMetricValues.put(i, new long[this.historySize]);
        }

        smoothedMetrics.put(metricType, smoothedMetricValues);
      } else {
        throw new IllegalArgumentException("Unknown metric type: " + metricType);
      }
    }

    this.toBeLogged = toBeLogged;

    if (WRITE_TO_FILE) {
      createFile();
    }

    currentSheddingRate = new double[numTasks];
  }

  public void registerTask(MonitorableTask task, int taskIndex) {
    if (taskIndex < 0 || taskIndex >= registeredTasks.length || registeredTasks[taskIndex] != null) {
      throw new IllegalArgumentException("Task index out of bounds");
    }

    registeredTasks[taskIndex] = task;
    numRegisteredTasks++;
  }

  public void activateMonitoring() {
    try {
      lock.writeLock().lock();

      if (numRegisteredTasks != registeredTasks.length) {
        throw new IllegalStateException("Not all tasks have been registered. Expected " + registeredTasks.length + ", got " + numRegisteredTasks + " tasks.");
      }

      this.future = telemetryData.getThreadPool().scheduleAtFixedRate(this::observeTask, 0, telemetryData.getMonitoringIntervalMS(), TimeUnit.MILLISECONDS);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void observeTask() {
    try {
      lock.writeLock().lock();

      // update metrics
      numExecutions++;

      for (int i = 0; i < registeredTasks.length; i++) {
        for (var metricType : rawMetrics.entrySet()) {
          metricType.getValue()[i] = registeredTasks[i].getMetric(metricType.getKey());
        }

        for (var metricType : smoothedMetrics.entrySet()) {
          var currHistoryPoint = numExecutions % this.historySize;

          var mappedKey = TelemetryMetricType.MAP_TO_RAW_METRIC.get(metricType.getKey());

          if (rawMetrics.containsKey(mappedKey)) { // consumed since start is monitored
            metricType.getValue().get(i)[currHistoryPoint] = rawMetrics.get(mappedKey)[i];
          } else { // consumed since start is NOT monitored -- get the current value
            metricType.getValue().get(i)[currHistoryPoint] = registeredTasks[i].getMetric(mappedKey);
          }
        }
      }

      // TODO: incremented is done twice within this method. bug?
      // TODO: this is the number of executions, right?
      throughputMeasurementID++;

      // log to the console if desired
      if (!toBeLogged.isEmpty()) {
        try {
          for (var metricType : rawMetrics.entrySet()) {
            if (toBeLogged.contains(metricType.getKey())) {
              Long val = aggregateRawMetric(metricType.getValue());
              LOGGER.info(metricType.getKey() + " (jobID " + jobId + "): " + val);
              if (WRITE_TO_FILE) {
                fileWriter.append(val + ",");
              }
            }
          }

          if (WRITE_TO_FILE) {
            fileWriter.append((Arrays.stream(currentSheddingRate).sum()) / getNumOfDataGenerators() + ",");
          }

          for (var metricType : smoothedMetrics.entrySet()) {
            if (toBeLogged.contains(metricType.getKey())) {
              long sum = aggregateSmoothedMetric(metricType.getValue());
              LOGGER.info(metricType.getKey() + " (jobID " + jobId + "): " + sum);
              if (WRITE_TO_FILE) {
                fileWriter.append(sum + "\n");
              }
            }
          }
        } catch (IOException e) {
          LOGGER.error("Error when writing to file. " + e.getMessage());
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean deactivateMonitoring() {
    try {
      lock.writeLock().lock();
      if (this.future == null) {
        return false;
      }
      this.future.cancel(true);
      for (var metricType : rawMetrics.entrySet()) {
        for (int i = 0; i < registeredTasks.length; i++) {
          metricType.getValue()[i] = 0;
        }
      }
      return true;
    } finally {
      lock.writeLock().unlock();
      if (WRITE_TO_FILE) {
        try {
          fileWriter.close();
        } catch (IOException e) {
          LOGGER.error("Error when writing to file. " + e.getMessage());
        }
      }
    }
  }

  public long getMetricBySubtaskIndex(TelemetryMetricType metricType, int subtaskIndex) {
    try {
      lock.readLock().lock();
      return rawMetrics.get(metricType)[subtaskIndex];
    } finally {
      lock.readLock().unlock();
    }
  }

  public long getMetricAggregated(TelemetryMetricType metricType) {
    try {
      lock.readLock().lock();
      return Arrays.stream(rawMetrics.get(metricType)).sum();
    } finally {
      lock.readLock().unlock();
    }
  }

  public Set<TelemetryMetricType> getAvailableMetrics() {
    Set<TelemetryMetricType> availableMetrics = new HashSet<>();
    availableMetrics.addAll(rawMetrics.keySet());
    availableMetrics.addAll(smoothedMetrics.keySet());

    return availableMetrics;
  }

  @Override
  public Set<String> getAvailableMetricsMBean() {
    Set<String> availableMetrics = new HashSet<>();
    for (var metricType : getAvailableMetrics()) {
      availableMetrics.add(metricType.toString());
    }
    return availableMetrics;
  }

  @Override
  public long getMetricBySubtaskIndexMBean(String metricType, int subtaskIndex) {
    if (metricType == null) {
      throw new IllegalArgumentException("Argument metricType must not be null");
    }

    TelemetryMetricType mappedMetricType = TelemetryMetricType.metricMapper(metricType);

    if (TelemetryMetricType.isRawMetric(metricType)) {
      return getMetricBySubtaskIndex(mappedMetricType, subtaskIndex);
    } else if (TelemetryMetricType.isSmoothedMetric(metricType)) {
      throw new UnsupportedOperationException("Smoothed metrics are not supported");
    } else {
      LOGGER.error("Unknown metric type: " + metricType);
      throw new IllegalArgumentException("Unknown metric type: " + metricType);
    }
  }

  @Override
  public long getMetricAggregatedMBean(String metricType) {
    if (metricType == null) {
      throw new IllegalArgumentException("Argument metricType must not be null");
    }

    TelemetryMetricType mappedMetricType = TelemetryMetricType.metricMapper(metricType);

    if (TelemetryMetricType.isRawMetric(metricType)) {
      return getMetricAggregated(mappedMetricType);
    } else if (TelemetryMetricType.isSmoothedMetric(metricType)) {
      var val = smoothedMetrics.get(mappedMetricType);
      if (val == null) {
        throw new IllegalStateException("Unknown metric type: " + metricType);
      }
      return aggregateSmoothedMetric(val);
    } else {
      LOGGER.error("Unknown metric type: " + metricType);
      throw new IllegalArgumentException("Unknown metric type: " + metricType);
    }
  }

  public ThroughputMeasurementWithID getCurrentThroughputWithId() {
    try {
      lock.readLock().lock();
      return new ThroughputMeasurementWithID(getMetricAggregated(
              TelemetryMetricType.RawMetric.CURRENT_THROUGHPUT), throughputMeasurementID);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getNumOfDataGenerators() {
    return registeredTasks.length;
  }

  public void setSheddingRate(double sheddingRate, int subtaskIndex) {
    currentSheddingRate[subtaskIndex] = sheddingRate;
  }

  private void createFile() {
    try {
      DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd-HH_mm");
      Date date = new Date();
      String dateStr = dateFormat.format(date);
      Files.createDirectories(Paths.get(filePath + dateStr));
      fileWriter = new FileWriter(filePath + dateStr + "/two-way-join-" + jobId.toString() + ".csv");
      for (var metricType : rawMetrics.keySet()) {
        fileWriter.append(metricType + ",");
      }
      fileWriter.append("SheddingRate,");
      for (var metricType : smoothedMetrics.keySet()) {
        fileWriter.append(metricType + ",");
      }
      fileWriter.append("\n");
    } catch (IOException e) {
      LOGGER.error("Error when creating the file. " + e.getMessage());
    }
  }

  public class ThroughputMeasurementWithID {
    public long throughput;
    public long id;

    public ThroughputMeasurementWithID(long throughput, long id) {
      this.throughput = throughput;
      this.id = id;
    }
  }

  @Getter
  public static class TelemetryData {

    private static TelemetryData instance;

    private final ScheduledExecutorService threadPool;

    private final long monitoringIntervalMS;

    private TelemetryData(ScheduledExecutorService threadPool, long monitoringIntervalMS) {
      this.threadPool = threadPool;
      this.monitoringIntervalMS = monitoringIntervalMS;
    }

    public static synchronized TelemetryData getTelemetryData(ScheduledExecutorService threadPool, long monitoringIntervalMS) {
      if (instance == null) {
        instance = new TelemetryData(threadPool, monitoringIntervalMS);
      }
      return instance;
    }
  }

  private static long aggregateRawMetric(long[] metricType) {
    return Arrays.stream(metricType).sum();
  }

  private long aggregateSmoothedMetric(Map<Integer, long[]> historiesPerSubtask) {
    // logic based on src/main/java/org/apache/flink/metrics/MeterView.java (Flink 1.13.6)
    long sum = 0;

    var currHistoryPoint = numExecutions % this.historySize;
    var nextHistoryPoint = (numExecutions + 1) % this.historySize;

    for (int i = 0; i < registeredTasks.length; i++) {
      sum += (long) ((double) (historiesPerSubtask.get(i)[currHistoryPoint] - historiesPerSubtask.get(i)[nextHistoryPoint]) / this.timespanInSeconds);
    }

    return sum;
  }
}
