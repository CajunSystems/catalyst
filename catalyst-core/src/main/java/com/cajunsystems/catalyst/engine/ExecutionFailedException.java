package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;

/**
 * Surfaces the recorded failure of a terminal execution when a caller attaches to it (e.g. by
 * re-submitting its idempotency key). The original exception type isn't stored — only its message is
 * in the {@code ExecutionFailed} event — so this carries the recorded error text rather than
 * re-running the task to reproduce a (possibly different) exception.
 */
public class ExecutionFailedException extends RuntimeException {

    private final ExecutionId executionId;

    public ExecutionFailedException(ExecutionId executionId, String recordedError) {
        super("Execution " + executionId + " failed: " + recordedError);
        this.executionId = executionId;
    }

    public ExecutionId executionId() {
        return executionId;
    }
}
