package net.michaelkoepf.spegauge.driver.exception;

public class ControllerExecutionException extends RuntimeException {
    public ControllerExecutionException() {
    }

    public ControllerExecutionException(String message) {
        super(message);
    }

    public ControllerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ControllerExecutionException(Throwable cause) {
        super(cause);
    }
}
