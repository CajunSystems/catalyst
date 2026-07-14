package com.cajunsystems.catalyst.engine;

/**
 * A one-way, thread-safe cancellation flag shared between the runtime (which trips it out-of-band on
 * {@code cancel(id)}) and the {@link ReplayingContext} driving a running attempt (which observes it at
 * the next live boundary and raises a {@link CancellationSignal}). Once cancelled it stays cancelled.
 */
public final class CancellationToken {

    private volatile boolean cancelled;
    private volatile String reason;

    /** Trips the token. Idempotent: the first reason wins so a later cancel does not overwrite it. */
    public void cancel(String reason) {
        if (!cancelled) {
            this.reason = reason;
            this.cancelled = true;
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }

    /** The reason supplied to {@link #cancel}, or a default if none was given. */
    public String reason() {
        return reason == null ? "cancelled by request" : reason;
    }
}
