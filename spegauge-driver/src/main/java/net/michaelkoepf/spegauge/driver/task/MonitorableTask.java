package net.michaelkoepf.spegauge.driver.task;

import net.michaelkoepf.spegauge.driver.core.TelemetryMetricType;

public interface MonitorableTask {

  long getMetric(TelemetryMetricType metricType);

}
