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
     * The step a {@code WAITING} execution is parked at: the latest wait in its timeline (a signal,
     * a timer, a child or a set of spawned children). Only waits fold to {@code WAITING}, so the latest
     * one is the current one. Every {@code awaiting...} accessor reads it, so none can mistake an
     * earlier, answered wait of another kind for the current one.
     */
    private java.util.Optional<TimelineStep> currentWait() {
        if (status != Status.WAITING) return java.util.Optional.empty();
        for (int i = timeline.size() - 1; i >= 0; i--) {
            TimelineStep step = timeline.get(i);
            switch (step.kind()) {
                case WAITING, SLEEPING, CHILD_STARTED, CHILD_AWAITED -> {
                    return java.util.Optional.of(step);
                }
                default -> { }
            }
        }
        return java.util.Optional.empty();
    }

    /**
     * The signal a {@code WAITING} execution is parked on, or empty if it is not waiting on one.
     * Derived from the timeline rather than stored, so the folded state and its snapshots keep their
     * shape.
     */
    public java.util.Optional<String> awaitingSignal() {
        return currentWait().filter(step -> step.kind() == TimelineStep.Kind.WAITING).map(TimelineStep::label);
    }

    /**
     * When a {@code WAITING} execution's timer comes due ({@code ctx.sleep}), or empty if it is not
     * waiting on a timer.
     */
    public java.util.Optional<java.time.Instant> awaitingTimer() {
        return currentWait().filter(step -> step.kind() == TimelineStep.Kind.SLEEPING && step.detail() != null)
                .map(step -> java.time.Instant.parse(step.detail().asText()));
    }

    /** The payload type a {@code WAITING} execution's signal must have (a class name), or empty. */
    public java.util.Optional<String> awaitingSignalType() {
        return currentWait().filter(step -> step.kind() == TimelineStep.Kind.WAITING
                        && step.detail() != null && step.detail().isTextual())
                .map(step -> step.detail().asText());
    }

    /**
     * The child execution a {@code WAITING} parent waits on, or empty if it is not waiting on a child.
     * For a wait on several spawned children ({@code ctx.awaitAny}), the first of them; see
     * {@link #awaitingChildren()}.
     */
    public java.util.Optional<com.cajunsystems.catalyst.ExecutionId> awaitingChild() {
        List<com.cajunsystems.catalyst.ExecutionId> children = awaitingChildren();
        return children.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(children.get(0));
    }

    /**
     * The child executions a {@code WAITING} parent waits on, any one of which ending wakes it:
     * one for {@code ctx.child} and {@code ctx.await}, several for {@code ctx.awaitAny}. Empty if it
     * is not waiting on children.
     */
    public List<com.cajunsystems.catalyst.ExecutionId> awaitingChildren() {
        return currentWait().map(step -> {
            if (step.kind() == TimelineStep.Kind.CHILD_STARTED && step.detail() != null) {
                return List.of(com.cajunsystems.catalyst.ExecutionId.of(step.detail().asText()));
            }
            if (step.kind() == TimelineStep.Kind.CHILD_AWAITED && step.detail() != null) {
                List<com.cajunsystems.catalyst.ExecutionId> ids = new java.util.ArrayList<>();
                step.detail().forEach(id -> ids.add(com.cajunsystems.catalyst.ExecutionId.of(id.asText())));
                return List.copyOf(ids);
            }
            return List.<com.cajunsystems.catalyst.ExecutionId>of();
        }).orElse(List.of());
    }

    /** The aggregated timeline view: model/tool counts, token usage, cost and latency roll-ups. */
    public Timeline timelineView() {
        return Timeline.from(this);
    }
}
