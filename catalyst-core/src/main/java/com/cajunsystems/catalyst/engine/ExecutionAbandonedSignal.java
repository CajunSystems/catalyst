package com.cajunsystems.catalyst.engine;

/**
 * Thrown to unwind task code when the runtime is shutting down. The context raises it at the next
 * live boundary once the attempt's {@link CancellationToken} reports abandoned, and the runtime
 * records <strong>nothing</strong> in response — no {@code ExecutionFailed}, no
 * {@code ExecutionCancelled} — so the execution stays {@code RUNNING} in the log and the next start's
 * recovery scan resumes it, substituting everything recorded so far.
 *
 * <p>An attempt's handle completes exceptionally with this signal: the work is not lost, it is
 * handed to the next process.
 */
public final class ExecutionAbandonedSignal extends RuntimeException {
    public ExecutionAbandonedSignal(String message) {
        super(message);
    }
}
