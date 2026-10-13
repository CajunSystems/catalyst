package com.cajunsystems.catalyst.engine;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * How a parent supervises a child execution ({@code ctx.child(task, supervision)}): whether a failed
 * child is restarted, how often, and after what delay. One-for-one: each {@code ctx.child} call is
 * supervised on its own.
 *
 * <p>A restart starts a <strong>fresh</strong> child execution, with its own id and stream, from the
 * top. (Retrying a child in place, with its successful prefix substituted, is the runtime's
 * {@link RetryPolicy}, which applies to a child like any execution and runs before the child is
 * recorded as failed.) Only a {@code FAILED} child is restarted. A cancelled child was stopped on
 * purpose, so its {@link ChildExecutionFailedException} reaches the parent at once. When restarts run
 * out, the last failure reaches the parent, carrying how many restarts were made.
 *
 * <p><strong>Every decision is deterministic.</strong> The parent decides from outcomes it has
 * recorded: each child's status, and the time its {@code ChildCompleted} was recorded. The restart
 * window is measured on those recorded times, never a live clock, and the backoff is a durable
 * {@code ctx.sleep}. A replay or resume therefore makes the same decisions and substitutes them.
 */
public final class Supervision {

    private static final Supervision ESCALATE = new Supervision(0, null, Duration.ZERO);

    private final int maxRestarts;
    private final Duration window;
    private final Duration backoff;

    private Supervision(int maxRestarts, Duration window, Duration backoff) {
        this.maxRestarts = maxRestarts;
        this.window = window;
        this.backoff = backoff;
    }

    /** No restarts: a failed child's failure reaches the parent at once. The default for {@code ctx.child(task)}. */
    public static Supervision escalate() {
        return ESCALATE;
    }

    /** Restart a failed child up to {@code maxRestarts} times in all (or within {@link #within}), then escalate. */
    public static Supervision restart(int maxRestarts) {
        if (maxRestarts < 0) throw new IllegalArgumentException("maxRestarts must be zero or positive");
        return new Supervision(maxRestarts, null, Duration.ZERO);
    }

    /**
     * Counts only failures within {@code window} of the latest one: more than {@code maxRestarts}
     * there escalates, while failures spread further apart keep being restarted.
     */
    public Supervision within(Duration window) {
        Objects.requireNonNull(window, "window");
        if (window.isNegative() || window.isZero()) throw new IllegalArgumentException("window must be positive");
        return new Supervision(maxRestarts, window, backoff);
    }

    /** Waits {@code delay} (a durable {@code ctx.sleep}) before each restart. */
    public Supervision backoff(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) throw new IllegalArgumentException("delay must be zero or positive");
        return new Supervision(maxRestarts, window, delay);
    }

    public int maxRestarts() {
        return maxRestarts;
    }

    public Optional<Duration> window() {
        return Optional.ofNullable(window);
    }

    public Duration backoff() {
        return backoff;
    }

    /**
     * Whether to restart after the latest failure.
     *
     * @param status   the latest child's status ({@code FAILED} or {@code CANCELLED})
     * @param failures the recorded times of every failure of this child so far, oldest first, the
     *                 latest included
     */
    public boolean restarts(String status, List<Instant> failures) {
        if (!"FAILED".equals(status) || failures.isEmpty()) return false;
        long counted = failures.size();
        if (window != null) {
            Instant latest = failures.get(failures.size() - 1);
            Instant from = latest.minus(window);
            counted = failures.stream().filter(at -> at.isAfter(from)).count();
        }
        // The first failure is restart 1: at most maxRestarts failures in scope may be restarted.
        return counted <= maxRestarts;
    }

    @Override
    public String toString() {
        return maxRestarts == 0 ? "Supervision.escalate()"
                : "Supervision.restart(" + maxRestarts + ")" + (window == null ? "" : ".within(" + window + ")")
                + (backoff.isZero() ? "" : ".backoff(" + backoff + ")");
    }
}
