package com.cajunsystems.catalyst.engine;

import java.time.Duration;

/**
 * A model or tool call ran past its {@link CallPolicy#timeout(Duration)}. The call was interrupted,
 * but the provider or system it reached may already have acted on it. Retryable like any failure, by
 * the call's own policy or the task's {@link RetryPolicy}.
 */
public class CallTimeoutException extends RuntimeException {

    private final String call;
    private final Duration timeout;

    public CallTimeoutException(String call, Duration timeout) {
        super(call + " did not finish within " + timeout);
        this.call = call;
        this.timeout = timeout;
    }

    /** Which call: {@code model} or {@code tool <name>}. */
    public String call() {
        return call;
    }

    public Duration timeout() {
        return timeout;
    }
}
