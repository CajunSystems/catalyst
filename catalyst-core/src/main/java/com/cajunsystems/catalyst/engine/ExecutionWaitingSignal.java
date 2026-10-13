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

    public ExecutionWaitingSignal(String signalName, String message) {
        this(signalName, null, message);
    }

    /**
     * A wait for a signal ({@code wakeAt} null) or for a durable timer ({@code signalName} null,
     * {@code wakeAt} when it comes due; {@code ctx.sleep}).
     */
    public ExecutionWaitingSignal(String signalName, java.time.Instant wakeAt, String message) {
        super(message);
        this.signalName = signalName;
        this.wakeAt = wakeAt;
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
