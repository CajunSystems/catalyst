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

    /** Which children a failure restarts, in a group run with {@code ctx.supervise}. */
    public enum Strategy {
        /** Only the failed child. */
        ONE_FOR_ONE,
        /** Every child in the group: the others are cancelled (or their results discarded) and all start again. */
        ONE_FOR_ALL,
        /** The failed child and every child listed after it; the ones before it keep their results. */
        REST_FOR_ONE
    }

    private static final Supervision ESCALATE = new Supervision(0, null, Duration.ZERO, Strategy.ONE_FOR_ONE);

    private final int maxRestarts;
    private final Duration window;
    private final Duration backoff;
    private final Strategy strategy;

    private Supervision(int maxRestarts, Duration window, Duration backoff, Strategy strategy) {
        this.maxRestarts = maxRestarts;
        this.window = window;
        this.backoff = backoff;
        this.strategy = strategy;
    }

    /** No restarts: a failed child's failure reaches the parent at once. The default for {@code ctx.child(task)}. */
    public static Supervision escalate() {
        return ESCALATE;
    }

    /** Restart a failed child up to {@code maxRestarts} times in all (or within {@link #within}), then escalate. */
    public static Supervision restart(int maxRestarts) {
        if (maxRestarts < 0) throw new IllegalArgumentException("maxRestarts must be zero or positive");
        return new Supervision(maxRestarts, null, Duration.ZERO, Strategy.ONE_FOR_ONE);
    }

    /**
     * Counts only failures within {@code window} of the latest one: more than {@code maxRestarts}
     * there escalates, while failures spread further apart keep being restarted.
     */
    public Supervision within(Duration window) {
        Objects.requireNonNull(window, "window");
        if (window.isNegative() || window.isZero()) throw new IllegalArgumentException("window must be positive");
        return new Supervision(maxRestarts, window, backoff, strategy);
    }

    /**
     * In a group ({@code ctx.supervise}), a failure restarts every child. For children that only make
     * sense together: the others are cancelled, any results they already returned are discarded, and
     * the whole group starts again. Has no effect on a single {@code ctx.child}.
     */
    public Supervision oneForAll() {
        return new Supervision(maxRestarts, window, backoff, Strategy.ONE_FOR_ALL);
    }

    /**
     * In a group ({@code ctx.supervise}), a failure restarts the failed child and every child listed
     * after it, which may depend on it. The children before it keep their results. Has no effect on a
     * single {@code ctx.child}.
     */
    public Supervision restForOne() {
        return new Supervision(maxRestarts, window, backoff, Strategy.REST_FOR_ONE);
    }

    /** Waits {@code delay} (a durable {@code ctx.sleep}) before each restart. */
    public Supervision backoff(Duration delay) {
        Objects.requireNonNull(delay, "delay");
        if (delay.isNegative()) throw new IllegalArgumentException("delay must be zero or positive");
        return new Supervision(maxRestarts, window, delay, strategy);
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

    public Strategy strategy() {
        return strategy;
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
                + (backoff.isZero() ? "" : ".backoff(" + backoff + ")")
                + (strategy == Strategy.ONE_FOR_ALL ? ".oneForAll()" : strategy == Strategy.REST_FOR_ONE ? ".restForOne()" : "");
    }
}
