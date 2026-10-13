package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * The immutable, folded state of an execution: exactly what you get by reducing its events (spec
 * §5). Returned by {@code runtime.inspect(id)} and used internally to decide resume vs. done.
 *
 * <p>{@code attempt} counts every (re)start of the execution — including crash resumes; {@code retries}
 * counts only failure retries elected by a retry policy. They differ deliberately: a crash resume must
 * not consume the retry budget. Both are folded from the log, so the budget is crash-safe.
 */
public record ExecutionState(
        ExecutionId id,
        Status status,
        String taskType,
        String idempotencyKey,
        int attempt,
        int retries,
        Instant startedAt,
        Instant endedAt,
        Cost cost,
        long totalLatencyMillis,
        JsonNode result,
        String error,
        long lastSeq,
        List<TimelineStep> timeline) {

    public ExecutionState {
        timeline = timeline == null ? List.of() : List.copyOf(timeline);
    }

    public boolean isTerminal() {
        return status != null && status.isTerminal();
    }

    /** The typed timeline of steps, tool calls, tokens and cost. */
    public List<TimelineStep> trajectory() {
        return timeline;
    }

    /**
     * The signal a {@code WAITING} execution is parked on, or empty if it is not waiting. Derived from
     * the timeline (the latest {@code WAITING} step) rather than stored, so the folded state and its
     * snapshots keep their shape.
     */
    public java.util.Optional<String> awaitingSignal() {
        if (status != Status.WAITING) return java.util.Optional.empty();
        for (int i = timeline.size() - 1; i >= 0; i--) {
            if (timeline.get(i).kind() == TimelineStep.Kind.SLEEPING) return java.util.Optional.empty(); // waiting on a timer
            if (timeline.get(i).kind() == TimelineStep.Kind.WAITING) return java.util.Optional.ofNullable(timeline.get(i).label());
        }
        return java.util.Optional.empty();
    }

    /**
     * When a {@code WAITING} execution's timer comes due ({@code ctx.sleep}), or empty if it is not
     * waiting on a timer (a signal wait, or not waiting at all). Derived from the timeline, like
     * {@link #awaitingSignal()}.
     */
    public java.util.Optional<java.time.Instant> awaitingTimer() {
        if (status != Status.WAITING) return java.util.Optional.empty();
        for (int i = timeline.size() - 1; i >= 0; i--) {
            TimelineStep step = timeline.get(i);
            if (step.kind() == TimelineStep.Kind.WAITING) return java.util.Optional.empty();
            if (step.kind() == TimelineStep.Kind.SLEEPING && step.detail() != null) {
                return java.util.Optional.of(java.time.Instant.parse(step.detail().asText()));
            }
        }
        return java.util.Optional.empty();
    }

    /** The payload type a {@code WAITING} execution's signal must have (a class name), or empty. */
    public java.util.Optional<String> awaitingSignalType() {
        if (status != Status.WAITING) return java.util.Optional.empty();
        for (int i = timeline.size() - 1; i >= 0; i--) {
            TimelineStep step = timeline.get(i);
            if (step.kind() == TimelineStep.Kind.SLEEPING) return java.util.Optional.empty();
            if (step.kind() == TimelineStep.Kind.WAITING) {
                return step.detail() == null || !step.detail().isTextual()
                        ? java.util.Optional.empty() : java.util.Optional.of(step.detail().asText());
            }
        }
        return java.util.Optional.empty();
    }

    /** The aggregated timeline view: model/tool counts, token usage, cost and latency roll-ups. */
    public Timeline timelineView() {
        return Timeline.from(this);
    }
}
