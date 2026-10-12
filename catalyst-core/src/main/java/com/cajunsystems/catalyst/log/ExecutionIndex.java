package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.events.CatalystEvent;

/**
 * Which events move an execution into or out of the <em>open set</em> that
 * {@link EventLog#openExecutions()} reports — the one place that classification lives, so every log
 * that keeps an index agrees on it.
 *
 * <p>An execution opens at {@link CatalystEvent.ExecutionCreated} and closes at its terminal event
 * ({@link CatalystEvent.ExecutionCompleted}, {@link CatalystEvent.ExecutionFailed},
 * {@link CatalystEvent.ExecutionCancelled}). Everything in between — retries, pauses, resumes — leaves
 * it open: a paused execution is still work that exists, and deciding whether to <em>run</em> it is
 * the recovery scan's call, made from the execution's own fold, not the index's.
 *
 * <p>Branch children are deliberately not indexed. They begin with
 * {@link CatalystEvent.ExecutionBranched} rather than {@code ExecutionCreated}, run synchronously in
 * the caller, and depend on overrides (a swapped model, counterfactual tool results) that are not
 * recorded — so there is nothing a restart could correctly resume them with.
 */
public final class ExecutionIndex {

    /** A log-resident queue of executions. Only the default exists today. */
    public static final String DEFAULT_QUEUE = "default";

    /** What an event does to its execution's membership in the open set. */
    public enum Change { OPENS, CLOSES, NONE }

    private ExecutionIndex() {}

    public static Change changeFor(CatalystEvent event) {
        return switch (event) {
            case CatalystEvent.ExecutionCreated c -> Change.OPENS;
            case CatalystEvent.ExecutionCompleted c -> Change.CLOSES;
            case CatalystEvent.ExecutionFailed f -> Change.CLOSES;
            case CatalystEvent.ExecutionCancelled c -> Change.CLOSES;
            default -> Change.NONE;
        };
    }

    /** Whether {@code event} must be written to the index as well as to its execution's stream. */
    public static boolean isIndexed(CatalystEvent event) {
        return changeFor(event) != Change.NONE;
    }
}
