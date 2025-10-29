package net.michaelkoepf.spegauge.api.sut;

public class SuspendableException extends RuntimeException {
    public SuspendableException() {
        super();
    }

    public SuspendableException(String message) {
        super(message);
    }

    public SuspendableException(String message, Throwable cause) {
        super(message, cause);
    }

    public SuspendableException(Throwable cause) {
        super(cause);
    }

    protected SuspendableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
