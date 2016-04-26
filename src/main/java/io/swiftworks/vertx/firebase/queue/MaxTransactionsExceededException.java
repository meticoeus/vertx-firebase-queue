package io.swiftworks.vertx.firebase.queue;


public class MaxTransactionsExceededException  extends Exception {

    public static final String DEFAULT_MESSAGE = "Transaction errored too many times. No longer retrying!";

    public MaxTransactionsExceededException() {
        super(DEFAULT_MESSAGE);
    }

    public MaxTransactionsExceededException(String message) {
        super(message);
    }

    public MaxTransactionsExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    public MaxTransactionsExceededException(Throwable cause) {
        super(cause);
    }

    public MaxTransactionsExceededException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
