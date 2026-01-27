package net.michaelkoepf.spegauge.driver.exception;

public class OperationInCurrentStateNotAllowedException extends RuntimeException {
    public OperationInCurrentStateNotAllowedException() {
    }

    public OperationInCurrentStateNotAllowedException(String message) {
        super(message);
    }

    public OperationInCurrentStateNotAllowedException(String message, Throwable cause) {
        super(message, cause);
    }

    public OperationInCurrentStateNotAllowedException(Throwable cause) {
        super(cause);
    }
}
