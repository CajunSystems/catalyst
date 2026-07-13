package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * The reducer's accumulator: the complete, resumable fold state through {@link #lastSeq()} (spec
 * §5, §8). {@link Reducer#fold} produces one by folding from {@link #initial()}; a snapshot is just
 * this value serialized, and {@link Reducer#foldFrom} continues a fold from a restored one.
 *
 * <p>It carries one field the public {@link ExecutionState} view omits — {@link #lastToolStepIndex()},
 * the index of an open {@code ToolRequested} step still awaiting its {@code ToolCompleted}. Keeping it
 * on the accumulator is what makes a snapshot taken <em>anywhere</em>, even between a tool request and
 * its completion, fold forward to exactly the same result as a full fold.
 */
public record ReducerState(
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
        int lastToolStepIndex,
        List<TimelineStep> timeline) {

    public ReducerState {
        timeline = timeline == null ? List.of() : List.copyOf(timeline);
        if (cost == null) cost = Cost.ZERO;
        if (status == null) status = Status.NEW;
        // A snapshot's JSON round-trip turns a null result into NullNode; canonicalize back to null so
        // a restored accumulator folds forward to the same state as a full fold.
        if (result != null && result.isNull()) result = null;
    }

    /** The empty accumulator: an execution with no events folded yet. */
    public static ReducerState initial() {
        return new ReducerState(Status.NEW, null, null, 0, null, null, Cost.ZERO, 0,
                null, null, -1, -1, List.of());
    }

    /** Projects the accumulator to the public folded view for {@code id} (drops private fold state). */
    public ExecutionState toExecutionState(ExecutionId id) {
        return new ExecutionState(id, status, taskType, idempotencyKey, attempt, startedAt, endedAt,
                cost, totalLatencyMillis, result, error, lastSeq, timeline);
    }
}
