package net.michaelkoepf.spegauge.driver.core;

import java.util.Set;

/**
 * Exposes telemetry metrics via JMX.
 */
public interface TelemetryMBean {
  Set<String> getAvailableMetricsMBean();
  long getMetricBySubtaskIndexMBean(String metricType, int subtaskIndex);
  long getMetricAggregatedMBean(String metricType);
}
