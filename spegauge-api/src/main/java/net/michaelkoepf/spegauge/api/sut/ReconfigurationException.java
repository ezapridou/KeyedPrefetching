package net.michaelkoepf.spegauge.api.sut;

public class ReconfigurationException extends RuntimeException {
    public ReconfigurationException() {
        super();
    }

    public ReconfigurationException(String message) {
        super(message);
    }

    public ReconfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReconfigurationException(Throwable cause) {
        super(cause);
    }

    protected ReconfigurationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
