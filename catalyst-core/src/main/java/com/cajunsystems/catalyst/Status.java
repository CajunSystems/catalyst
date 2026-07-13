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
 * {@code WAITING} is reserved for v1 durable signals / human-in-the-loop; it is in the state machine
 * now so v1 adds await APIs without a breaking schema change, but v0 drives no transitions into it.
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
