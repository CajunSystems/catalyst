package com.cajunsystems.catalyst.engine;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Retries and a timeout for <em>one</em> model or tool call, inside the attempt that makes it. This
 * is unlike {@link RetryPolicy}, which re-runs the whole task with its successful steps substituted.
 *
 * <p>Only the call's outcome is recorded: the request, then its final result or its last failure.
 * The attempts in between leave an informational {@code CallRetried} each. A replay substitutes the
 * outcome without retrying or waiting, so a call that needed three tries replays like one that
 * needed one.
 *
 * <ul>
 *   <li><b>Retries</b> run after a failure the predicate accepts, with an exponential backoff
 *       (doubling, capped at {@link #maxBackoff()}). They stop early, surfacing the last failure as
 *       if retries had run out, when the execution is cancelled or the runtime is shutting down, so
 *       the usual stop rule applies.</li>
 *   <li><b>A timeout</b> runs the call on its own virtual thread and gives up after the limit with
 *       {@link CallTimeoutException}, which can then be retried. The abandoned call is interrupted,
 *       but a provider may already have accepted (and billed) it.</li>
 *   <li><b>Streamed completions</b> are retried only while no chunk has been delivered, since
 *       re-delivering text would change what the task assembled. Timeouts do not apply to them, so
 *       the sink keeps running on the task's thread.</li>
 * </ul>
 */
public final class CallPolicy {

    private static final CallPolicy NONE = new CallPolicy(1, Duration.ZERO, Duration.ofSeconds(30), null, e -> true);

    private final int maxAttempts;
    private final Duration backoff;
    private final Duration maxBackoff;
    private final Duration timeout;
    private final Predicate<Throwable> retryOn;

    private CallPolicy(int maxAttempts, Duration backoff, Duration maxBackoff, Duration timeout, Predicate<Throwable> retryOn) {
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
        this.maxBackoff = maxBackoff;
        this.timeout = timeout;
        this.retryOn = retryOn;
    }

    /** One attempt, no timeout: the default. */
    public static CallPolicy none() {
        return NONE;
    }

    /**
     * Up to {@code maxRetries} more attempts after a failure, waiting {@code backoff} before the first
     * retry and doubling each time (capped at 30s; see {@link #maxBackoff(Duration)}).
     */
    public static CallPolicy retries(int maxRetries, Duration backoff) {
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be zero or positive");
        requireNonNegative(backoff, "backoff");
        return new CallPolicy(maxRetries + 1, backoff, NONE.maxBackoff, null, NONE.retryOn);
    }

    /** A timeout, with no retries. */
    public static CallPolicy timeoutAfter(Duration timeout) {
        return NONE.timeout(timeout);
    }

    /** Gives up on an attempt after {@code timeout} with {@link CallTimeoutException}. */
    public CallPolicy timeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
        return new CallPolicy(maxAttempts, backoff, maxBackoff, timeout, retryOn);
    }

    /** The longest wait between attempts. */
    public CallPolicy maxBackoff(Duration maxBackoff) {
        requireNonNegative(maxBackoff, "maxBackoff");
        return new CallPolicy(maxAttempts, backoff, maxBackoff, timeout, retryOn);
    }

    /** Retries only failures {@code retryOn} accepts; every other failure is surfaced at once. */
    public CallPolicy retryOn(Predicate<Throwable> retryOn) {
        Objects.requireNonNull(retryOn, "retryOn");
        return new CallPolicy(maxAttempts, backoff, maxBackoff, timeout, retryOn);
    }

    /** Total attempts, the first included. */
    public int maxAttempts() {
        return maxAttempts;
    }

    public Optional<Duration> timeout() {
        return Optional.ofNullable(timeout);
    }

    public Duration maxBackoff() {
        return maxBackoff;
    }

    /** Whether to try again after attempt {@code attempt} (1-based) failed with {@code failure}. */
    public boolean retries(int attempt, Throwable failure) {
        return attempt < maxAttempts && retryOn.test(failure);
    }

    /** The wait before attempt {@code attempt + 1}. */
    public Duration backoffAfter(int attempt) {
        long millis = backoff.toMillis();
        for (int i = 1; i < attempt && millis < maxBackoff.toMillis(); i++) millis *= 2;
        return Duration.ofMillis(Math.min(millis, maxBackoff.toMillis()));
    }

    private static void requireNonNegative(Duration d, String what) {
        Objects.requireNonNull(d, what);
        if (d.isNegative()) throw new IllegalArgumentException(what + " must be zero or positive");
    }

    @Override
    public String toString() {
        return "CallPolicy[attempts=" + maxAttempts + ", backoff=" + backoff + ", maxBackoff=" + maxBackoff
                + (timeout == null ? "" : ", timeout=" + timeout) + "]";
    }
}
