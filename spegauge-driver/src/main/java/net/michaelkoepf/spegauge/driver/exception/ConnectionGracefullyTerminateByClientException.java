package net.michaelkoepf.spegauge.driver.exception;

public class ConnectionGracefullyTerminateByClientException extends RuntimeException {
    public ConnectionGracefullyTerminateByClientException() {
    }

    public ConnectionGracefullyTerminateByClientException(String message) {
        super(message);
    }

    public ConnectionGracefullyTerminateByClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionGracefullyTerminateByClientException(Throwable cause) {
        super(cause);
    }
}
