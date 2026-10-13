package com.cajunsystems.catalyst.engine;

/**
 * Thrown to unwind task code when it waits for a signal that has not arrived ({@code ctx.awaitSignal}).
 * By the time it is thrown the context has recorded {@code SignalAwaited}; the runtime catches it,
 * records nothing more, and the execution stays {@code WAITING} — holding no thread — until
 * {@code runtime.signal(id, name, payload)} delivers the signal and resumes it.
 *
 * <p>An execution handle completes exceptionally with this signal: the work is parked, not failed.
 */
public final class ExecutionWaitingSignal extends RuntimeException {

    private final String signalName;
    private final java.time.Instant wakeAt;
    private final com.cajunsystems.catalyst.ExecutionId child;

    public ExecutionWaitingSignal(String signalName, String message) {
        this(signalName, null, message);
    }

    /**
     * A wait for a signal ({@code wakeAt} null) or for a durable timer ({@code signalName} null,
     * {@code wakeAt} when it comes due; {@code ctx.sleep}).
     */
    public ExecutionWaitingSignal(String signalName, java.time.Instant wakeAt, String message) {
        this(signalName, wakeAt, null, message);
    }

    /** A wait for a child execution ({@code ctx.child}) when {@code child} is set. */
    public ExecutionWaitingSignal(String signalName, java.time.Instant wakeAt, com.cajunsystems.catalyst.ExecutionId child, String message) {
        super(message);
        this.signalName = signalName;
        this.wakeAt = wakeAt;
        this.child = child;
    }

    /** The child execution this one waits on; null unless it is a child wait. */
    public com.cajunsystems.catalyst.ExecutionId child() {
        return child;
    }

    /** The signal the execution is waiting for; null for a timer wait. */
    public String signalName() {
        return signalName;
    }

    /** When the timer this execution sleeps on comes due; null for a signal wait. */
    public java.time.Instant wakeAt() {
        return wakeAt;
    }
}
