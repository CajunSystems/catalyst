package com.cajunsystems.catalyst.engine;

/**
 * Thrown to unwind task code when an execution must pause (e.g. an in-doubt tool call under
 * {@link InDoubtPolicy#ASK}). The runtime catches it and leaves the execution in {@code PAUSED}
 * rather than {@code FAILED}. Used by the in-doubt {@code ASK} path. Waiting for a signal is a
 * different state ({@code WAITING}) with its own unwind, {@link ExecutionWaitingSignal}.
 */
public class ExecutionPausedSignal extends RuntimeException {
    public ExecutionPausedSignal(String reason) {
        super(reason);
    }
}
