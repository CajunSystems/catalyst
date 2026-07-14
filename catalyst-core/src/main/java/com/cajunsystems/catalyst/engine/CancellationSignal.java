package com.cajunsystems.catalyst.engine;

/**
 * Thrown to unwind task code when an execution has been cancelled cooperatively. The context raises it
 * at the next live boundary once a {@link CancellationToken} is tripped; the runtime catches it and
 * records {@link com.cajunsystems.catalyst.events.CatalystEvent.ExecutionCancelled} so the execution
 * folds to {@code CANCELLED} rather than {@code FAILED}.
 *
 * <p>Cancellation is cooperative: a task that never reaches another recorded boundary after being
 * cancelled (e.g. a tight compute loop making no {@code ctx} calls) cannot be unwound this way. The
 * runtime also interrupts the worker thread so blocking boundaries can bail out.
 */
public final class CancellationSignal extends RuntimeException {
    public CancellationSignal(String reason) {
        super(reason);
    }
}
