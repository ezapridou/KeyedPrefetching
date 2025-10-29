package net.michaelkoepf.spegauge.driver.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Marker interface for different types of metrics.
 */
public interface TelemetryMetricType {

  enum RawMetric implements TelemetryMetricType {
    CURRENT_THROUGHPUT("RAW_METRIC.CURRENT_THROUGHPUT"),
    CURRENT_BACKLOG("RAW_METRIC.CURRENT_BACKLOG"),
    CONSUMED_SINCE_START("RAW_METRIC.CONSUMED_SINCE_START"), // for copied subtasks: includes the number of elements that were consumed before copying
    CONSUMED_SINCE_START_SINCE_COPY("RAW_METRIC.CONSUMED_SINCE_START_SINCE_COPY"), // for non-copied subtasks: same as CONSUMED_SINCE_START
    CURRENT_RATE_OF_DATA_CONSUMPTION("RAW_METRIC.CURRENT_RATE_OF_DATA_CONSUMPTION"); // includes shedded and not shedded elements

    private final String name;

    RawMetric(String s) {
      name = s;
    }

    public String toString() {
      return this.name;
    }

  }

  enum SmoothedMetric implements net.michaelkoepf.spegauge.driver.core.TelemetryMetricType {
    CURRENT_THROUGHPUT("SMOOTHED_METRIC.CURRENT_THROUGHPUT");

    private final String name;

    SmoothedMetric(String s) {
      name = s;
    }

    public String toString() {
      return this.name;
    }

  }

  static TelemetryMetricType metricMapper(String metricType) {
    switch (metricType) {
      case "RAW_METRIC.CURRENT_THROUGHPUT":
        return RawMetric.CURRENT_THROUGHPUT;
      case "RAW_METRIC.CURRENT_BACKLOG":
        return RawMetric.CURRENT_BACKLOG;
      case "RAW_METRIC.CONSUMED_SINCE_START":
        return RawMetric.CONSUMED_SINCE_START;
      case "RAW_METRIC.CONSUMED_SINCE_START_SINCE_COPY":
        return RawMetric.CONSUMED_SINCE_START_SINCE_COPY;
      case "RAW_METRIC.CURRENT_RATE_OF_DATA_CONSUMPTION":
        return RawMetric.CURRENT_RATE_OF_DATA_CONSUMPTION;
      case "SMOOTHED_METRIC.CURRENT_THROUGHPUT":
        return SmoothedMetric.CURRENT_THROUGHPUT;
      default:
        throw new IllegalArgumentException("Unknown metric type: " + metricType);
    }
  }

  static boolean isRawMetric(String metricType) {
    return metricType.equals("RAW_METRIC.CURRENT_THROUGHPUT") || metricType.equals("RAW_METRIC.CURRENT_BACKLOG") || metricType.equals("RAW_METRIC.CONSUMED_SINCE_START") || metricType.equals("RAW_METRIC.CONSUMED_SINCE_START_SINCE_COPY") || metricType.equals("RAW_METRIC.CURRENT_RATE_OF_DATA_CONSUMPTION");
  }

  static boolean isSmoothedMetric(String metricType) {
    return metricType.equals("SMOOTHED_METRIC.CURRENT_THROUGHPUT");
  }

  Map<SmoothedMetric, RawMetric> MAP_TO_RAW_METRIC = new HashMap<>() {{
    put(SmoothedMetric.CURRENT_THROUGHPUT, RawMetric.CONSUMED_SINCE_START_SINCE_COPY);
  }};

}
