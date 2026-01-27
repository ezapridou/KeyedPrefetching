package net.michaelkoepf.spegauge.driver.exception;

public class UnknownMetricException extends RuntimeException {
    public UnknownMetricException() {
    }

    public UnknownMetricException(String message) {
        super(message);
    }

    public UnknownMetricException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownMetricException(Throwable cause) {
        super(cause);
    }
}
