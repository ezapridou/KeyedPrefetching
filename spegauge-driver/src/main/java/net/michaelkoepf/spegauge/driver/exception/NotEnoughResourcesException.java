package net.michaelkoepf.spegauge.driver.exception;

public class NotEnoughResourcesException extends RuntimeException {
    public NotEnoughResourcesException() {
    }

    public NotEnoughResourcesException(String message) {
        super(message);
    }

    public NotEnoughResourcesException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotEnoughResourcesException(Throwable cause) {
        super(cause);
    }
}
