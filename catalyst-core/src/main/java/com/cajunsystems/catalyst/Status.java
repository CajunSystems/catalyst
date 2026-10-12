package com.cajunsystems.catalyst;

/**
 * The lifecycle state of an execution (spec §5):
 *
 * <pre>
 * NEW → STARTING → RUNNING → COMPLETED
 *                    │  ▲
 *                    ▼  │
 *        WAITING / PAUSED → RESUMING          FAILED / CANCELLED
 * </pre>
 *
 * {@code WAITING} is an execution parked on {@code ctx.awaitSignal(name, type)}, holding no thread
 * until {@code runtime.signal(id, name, payload)} delivers it, which folds it to {@code RESUMING}.
 */
public enum Status {
    NEW,
    STARTING,
    RUNNING,
    WAITING,
    PAUSED,
    RESUMING,
    COMPLETED,
    FAILED,
    CANCELLED;

    /** Whether this is a terminal state — no further transitions occur. */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
