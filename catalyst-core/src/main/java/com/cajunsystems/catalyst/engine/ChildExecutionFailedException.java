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
    private final java.time.Instant at;
    private final int restarts;

    public ChildExecutionFailedException(ExecutionId childId, String status, String error) {
        this(childId, status, error, null, 0);
    }

    /**
     * @param at       when the outcome was recorded in the parent (what supervision windows measure)
     * @param restarts how many times supervision restarted the child before this failure escalated
     */
    public ChildExecutionFailedException(ExecutionId childId, String status, String error, java.time.Instant at, int restarts) {
        super("Child execution " + childId.value() + " ended " + status + ": " + error
                + (restarts == 0 ? "" : " (after " + restarts + (restarts == 1 ? " restart)" : " restarts)")));
        this.childId = childId;
        this.status = status;
        this.error = error;
        this.at = at;
        this.restarts = restarts;
    }

    /** The same failure, escalated after {@code restarts} restarts. */
    ChildExecutionFailedException afterRestarts(int restarts) {
        return restarts == this.restarts ? this : new ChildExecutionFailedException(childId, status, error, at, restarts);
    }

    /** When the outcome was recorded in the parent's stream; null if not known. */
    public java.time.Instant at() {
        return at;
    }

    /** How many times supervision restarted the child before giving up; 0 without supervision. */
    public int restarts() {
        return restarts;
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
