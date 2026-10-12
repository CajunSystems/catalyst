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

    public ExecutionWaitingSignal(String signalName, String message) {
        super(message);
        this.signalName = signalName;
    }

    /** The signal the execution is waiting for. */
    public String signalName() {
        return signalName;
    }
}
