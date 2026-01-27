package net.michaelkoepf.spegauge.driver.exception;

public class StaleEventsException extends RuntimeException {
    public StaleEventsException() {
    }

    public StaleEventsException(String message) {
        super(message);
    }

    public StaleEventsException(String message, Throwable cause) {
        super(message, cause);
    }

    public StaleEventsException(Throwable cause) {
        super(cause);
    }
}
