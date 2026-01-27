package net.michaelkoepf.spegauge.driver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import net.michaelkoepf.spegauge.api.driver.Config;
import net.michaelkoepf.spegauge.api.driver.EventGeneratorFactory;
import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.driver.core.DummyLoadShedder;
import net.michaelkoepf.spegauge.driver.core.FrequencyLoadShedder;
import net.michaelkoepf.spegauge.driver.exception.UnprocessableEntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utility class for the driver.
 */
public final class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final String CONFIG_INTERFACE = "net.michaelkoepf.spegauge.api.driver.Config";

  private Utils() {
    // not meant to be instantiated
  }

  // TODO: maybe move to APIUtils (enhancement)
  /**
   * Returns the class implementing the Config interface from the given jar file.
   *
   * @param jarFilePath the path to the jar file
   * @return instance of the class implementing the Config interface
   */
  public static Config getConfigClass(Path jarFilePath) {
    Config configClass = null;

    try (ScanResult scanResult = new ClassGraph().overrideClasspath(jarFilePath.toString()).enableAllInfo().scan()) {
      ClassInfoList controlClasses = scanResult.getClassesImplementing(CONFIG_INTERFACE);

      if (controlClasses.size() != 1) {
        throw new RuntimeException("Expected exactly one class implementing " + CONFIG_INTERFACE);
      }

      configClass = (Config) controlClasses.get(0).loadClass().getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }

    return configClass;
  }

  public static LoadShedder getLoadShedder(String loadShedderName, Map<String, Object> params) {
    if (loadShedderName == null) {
      loadShedderName = "DummyLoadShedder";
    }

    switch (loadShedderName) {
      case "DummyLoadShedder":
        return new DummyLoadShedder();
      case "FrequencyLoadShedder":
        return new FrequencyLoadShedder(params);
      default:
        throw new UnprocessableEntityException("Unknown load shedder: " + loadShedderName);
    }
  }

  // TODO: maybe move to APIUtils (enhancement)
  /**
   * Creates an instance of the EventGeneratorFactory with the given name from the given jar file.
   *
   * @param jarFilePath the path to the jar file
   * @param eventGeneratorQualifiedName the qualified name of the event generator class (has to be a subtype of EventGeneratorFactory)
   * @return the created instance
   */
  public static EventGeneratorFactory createEventGeneratorFactory(Path jarFilePath, String eventGeneratorQualifiedName) {
    EventGeneratorFactory eventGeneratorFactory = null;

    try (ScanResult scanResult = new ClassGraph().overrideClasspath(jarFilePath.toString()).enableAllInfo().scan()) {
      ClassInfoList controlClasses = scanResult.getAllClasses().filter(c -> c.getName().equals(eventGeneratorQualifiedName));

      if (controlClasses.size() != 1) {
        throw new RuntimeException("Expected exactly one class with name " + eventGeneratorQualifiedName);
      }

      eventGeneratorFactory = (EventGeneratorFactory) controlClasses.get(0).loadClass().getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }

    return eventGeneratorFactory;
  }

  /**
   * Gracefully shuts down the given thread pool.
   *
   * @param threadPool the thread pool to be shut down
   */
  public static void gracefullyShutdownThreadPool(ExecutorService threadPool) {
    threadPool.shutdown();
    try {
      // Wait a while for existing tasks to terminate
      if (!threadPool.awaitTermination(
          Constants.THREAD_POOL_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        threadPool.shutdownNow();

        if (!threadPool.awaitTermination(
            Constants.THREAD_POOL_SHUTDOWN_TIMEOUT_MS, TimeUnit.SECONDS)) {
          LOGGER.error(
              "Thread pool could not be shutdown gracefully within the specified time limit (maybe the thread is in a blocking I/O call?)");
          throw new RuntimeException("Thread pool could not be shutdown");
        }
      }
    } catch (InterruptedException ie) {
      threadPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public static void closeConnections(List<Socket> connections) {
    LOGGER.info("Trying to close " + connections.size() + " client connections.");
    closeConnections(connections, false);
  }

  public static void closeConnections(List<Socket> connections, boolean throwExceptionOnError) {
    LOGGER.info("Trying to close " + connections.size() + " connections.");

    for (Socket socket : connections) {
      closeConnection(socket, throwExceptionOnError);
    }

    LOGGER.info("Closed all connections.");
  }

  public static void closeConnection(Socket socket) {
    closeConnection(socket, false);
  }

  public static void closeConnection(Socket socket, boolean throwExceptionOnError) {
    if (!socket.isClosed()) {
      try {
        socket.close();
      } catch (IOException e) {
        LOGGER.error(
                "Could not close client connection of client "
                        + socket.getRemoteSocketAddress()
                        + " (local port "
                        + socket.getPort()
                        + ")");

        if (throwExceptionOnError) {
          throw new UncheckedIOException(e);
        }
      }
    } else {
      LOGGER.debug("Connection already closed");
    }
  }
}
