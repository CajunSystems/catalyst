package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;

/**
 * Thrown by {@code ctx.child(task)} when the child execution ended without completing: it failed or
 * was cancelled. The outcome is recorded in the parent's stream, so a replay or resume throws the
 * same exception, and a parent that catches it to recover carries on identically.
 */
public class ChildExecutionFailedException extends RuntimeException {

    private final ExecutionId childId;
    private final String status;
    private final String error;

    public ChildExecutionFailedException(ExecutionId childId, String status, String error) {
        super("Child execution " + childId.value() + " ended " + status + ": " + error);
        this.childId = childId;
        this.status = status;
        this.error = error;
    }

    public ExecutionId childId() {
        return childId;
    }

    /** {@code FAILED} or {@code CANCELLED}. */
    public String status() {
        return status;
    }

    public String error() {
        return error;
    }
}
