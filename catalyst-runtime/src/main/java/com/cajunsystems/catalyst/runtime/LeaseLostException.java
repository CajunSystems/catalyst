package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;

/**
 * Completes the handle of an attempt that stopped because its node lost the execution's lease: a
 * renewal found the row changed, so another node has taken the execution over. The attempt unwound
 * at its next boundary and appended nothing, just as it would on shutdown. The new holder decides
 * what happens next.
 */
public class LeaseLostException extends RuntimeException {

    private final ExecutionId executionId;

    public LeaseLostException(ExecutionId executionId) {
        super("Lost the lease on " + executionId.value() + " to another node; this attempt stopped and recorded nothing");
        this.executionId = executionId;
    }

    public ExecutionId executionId() {
        return executionId;
    }
}
