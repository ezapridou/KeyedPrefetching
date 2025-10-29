package net.michaelkoepf.spegauge.driver.exception;

public class GenericAPIException extends RuntimeException {
    public GenericAPIException() {
    }

    public GenericAPIException(String message) {
        super(message);
    }

    public GenericAPIException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenericAPIException(Throwable cause) {
        super(cause);
    }
}
