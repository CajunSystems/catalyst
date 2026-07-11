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
 */
public record ExecutionState(
        ExecutionId id,
        Status status,
        String taskType,
        String idempotencyKey,
        int attempt,
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

    /** The aggregated timeline view: model/tool counts, token usage, cost and latency roll-ups. */
    public Timeline timelineView() {
        return Timeline.from(this);
    }
}
