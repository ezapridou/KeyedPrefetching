package net.michaelkoepf.spegauge.driver;

// TODO: move to config file (enhancement)
public final class Constants {
  public static final int JETTY_THREAD_POOL_MIN_THREADS = 4;
  public static final int JETTY_THREAD_POOL_MAX_THREADS = 8;
  public static final int JETTY_THREAD_IDLE_TIMEOUT_MS = 120_000;
  public static final int TELEMETRY_THREAD_POOL_SIZE = 2;
  public static final int MAX_QUEUED_EVENTS = 300_000_000;
  public static final int SERVER_SOCKET_TIMEOUT_MS = 30_000;
  public static final int CANCEL_TIME_WAIT_MS = 5_000;
  public static final int SERVER_SOCKET_MAX_TIMEOUTS = 5;
  public static final int THREAD_POOL_SHUTDOWN_TIMEOUT_MS = 30_000;
  public static final int CONSUMER_TERMINATION_TIMEOUT_MS = 10_000;
  public static final int REPORTING_INTERVAL_MS = 5_000;
  public static final int WEB_API_CALLS_TIMEOUT_MS = 60_000;
  public static final int CONTROLLER_TIMEOUT_MS = 10_000;
  public static final int DATA_GENERATION_INITIAL_DELAY_MS =  10; //Q19 1_080_000; // Q13: 480_000;
}
