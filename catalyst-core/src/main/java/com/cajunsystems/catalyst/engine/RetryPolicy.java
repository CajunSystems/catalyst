package com.cajunsystems.catalyst.engine;

import java.time.Duration;
import java.util.Optional;

/**
 * Decides whether — and after how long — a failed attempt is retried as a new attempt on the same
 * stream (retry-as-attempt; spec §13.3). Consulted by the runtime after a task throws a retryable
 * failure: an empty result stops and records {@code ExecutionFailed}; a present {@link Duration} is a
 * backoff after which the execution resumes as attempt N+1 with the recorded prefix substituted.
 *
 * <p>The default is {@link #none()} — failures are terminal on the first throw, matching M0/M1/M2.
 * Retry is opt-in per runtime ({@code Catalyst.builder().retryPolicy(...)}) or per execution
 * ({@code ExecutionOptions.retryPolicy(...)}).
 *
 * <p><strong>Retryability is an engine gate, not a policy choice.</strong> The runtime never consults
 * a policy for failures where a retry cannot help — a non-deterministic replay divergence, an in-doubt
 * boundary (governed by {@code InDoubtPolicy}), an interrupt, or an {@code Error}. A policy therefore
 * only ever sees failures that are at least plausibly transient.
 *
 * <p><strong>Retry is whole-task, not per-tool.</strong> A retry re-enters the task from the top with
 * every recorded boundary substituted, re-running only the boundary that failed (and anything after
 * it). Failures in pure task code, which record no boundary, replay deterministically and fail
 * identically — the policy still bounds them, but they cannot succeed on retry.
 *
 * @see ExecutionState#retries()
 */
@FunctionalInterface
public interface RetryPolicy {

    /**
     * The backoff before retry number {@code retriesSoFar + 1}, or {@link Optional#empty()} to stop and
     * let the failure become terminal.
     *
     * @param retriesSoFar failure retries already taken on this execution — never crash resumes
     *                     (see {@link ExecutionState#retries()}); {@code 0} on the first failure
     * @param failure      the throwable the task raised (already gated to a retryable kind)
     */
    Optional<Duration> nextBackoff(int retriesSoFar, Throwable failure);

    /** Never retry: a failure is terminal on the first throw. The default. */
    static RetryPolicy none() {
        return (retriesSoFar, failure) -> Optional.empty();
    }

    /** Up to {@code maxRetries} retries, each after a fixed {@code backoff}. */
    static RetryPolicy maxRetries(int maxRetries, Duration backoff) {
        return (retriesSoFar, failure) ->
                retriesSoFar < maxRetries ? Optional.of(backoff) : Optional.empty();
    }

    /**
     * Up to {@code maxRetries} retries with an exponentially growing backoff:
     * {@code initial * multiplier^retriesSoFar}, capped at {@code max}.
     */
    static RetryPolicy exponential(int maxRetries, Duration initial, double multiplier, Duration max) {
        return (retriesSoFar, failure) -> {
            if (retriesSoFar >= maxRetries) return Optional.empty();
            double millis = initial.toMillis() * Math.pow(multiplier, retriesSoFar);
            return Optional.of(Duration.ofMillis((long) Math.min(millis, max.toMillis())));
        };
    }
}
