package io.swiftworks.vertx.firebase.queue;


public class MaxTransactionsExceededException  extends Exception {

    public static final String DEFAULT_MESSAGE = "Transaction errored too many times. No longer retrying!";

    MaxTransactionsExceededException() {
        super(DEFAULT_MESSAGE);
    }

    MaxTransactionsExceededException(String message) {
        super(message);
    }

    MaxTransactionsExceededException(String message, Throwable cause) {
        super(message, cause);
    }

    MaxTransactionsExceededException(Throwable cause) {
        super(cause);
    }

    MaxTransactionsExceededException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
