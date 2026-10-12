package com.cajunsystems.catalyst.engine;

import java.util.function.BooleanSupplier;

/**
 * A thread-safe stop request shared between the runtime and the {@link ReplayingContext} driving a
 * running attempt, which observes it at the next live boundary. Two kinds of stop, with opposite
 * effects on the log:
 *
 * <ul>
 *   <li><strong>Cancel</strong> ({@link #cancel}) — tripped out-of-band by {@code cancel(id)}. The
 *       context raises {@link CancellationSignal} and the runtime records {@code ExecutionCancelled}:
 *       terminal.</li>
 *   <li><strong>Abandon</strong> ({@link #isAbandoned}) — the runtime is shutting down. The context
 *       raises {@link ExecutionAbandonedSignal} and the runtime records <em>nothing</em>, so the
 *       execution stays non-terminal and the next start resumes it. Read from a supplier rather than
 *       tripped per token, so one runtime-wide flag abandons every attempt at once — including one
 *       scheduled concurrently with the shutdown, which a per-token trip could miss.</li>
 * </ul>
 *
 * Once cancelled a token stays cancelled. Cancel takes precedence: a cancel requested before a
 * shutdown is still honoured.
 */
public final class CancellationToken {

    private static final BooleanSupplier NEVER = () -> false;

    private final BooleanSupplier abandoned;
    private volatile boolean cancelled;
    private volatile String reason;

    /** A token that can be cancelled but is never abandoned. */
    public CancellationToken() {
        this(NEVER);
    }

    /** A token that is abandoned whenever {@code abandoned} reports true. */
    public CancellationToken(BooleanSupplier abandoned) {
        this.abandoned = abandoned;
    }

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

    /** Whether the attempt should unwind without recording anything — the runtime is shutting down. */
    public boolean isAbandoned() {
        return abandoned.getAsBoolean();
    }

    /** The reason supplied to {@link #cancel}, or a default if none was given. */
    public String reason() {
        return reason == null ? "cancelled by request" : reason;
    }
}
