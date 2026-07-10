package com.cajunsystems.catalyst.engine;

/**
 * Thrown to unwind task code when an execution must pause (e.g. an in-doubt tool call under
 * {@link InDoubtPolicy#ASK}). The runtime catches it and leaves the execution in {@code PAUSED}
 * rather than {@code FAILED}. The {@code WAITING}/pause machinery is reserved for v1 signal APIs;
 * M0 uses it only for the in-doubt {@code ASK} path.
 */
public class ExecutionPausedSignal extends RuntimeException {
    public ExecutionPausedSignal(String reason) {
        super(reason);
    }
}
