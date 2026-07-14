package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The fold: {@code Events → ExecutionState}. Exactly event sourcing (spec §5). A pure function of
 * the event list — the log is the single source of truth, the state is a derived view, and the typed
 * timeline, token usage and cost are all folds over the same events.
 */
public final class Reducer {

    private Reducer() {}

    /** Folds an execution's events into its current {@link ExecutionState}. */
    public static ExecutionState fold(ExecutionId id, List<SequencedEvent> events) {
        return foldFrom(ReducerState.initial(), events).toExecutionState(id);
    }

    /**
     * Continues a fold from a restored {@code base} accumulator (spec §8): applies {@code events} —
     * which must be exactly the events with seq &gt; {@code base.lastSeq()} — and returns the new
     * accumulator. {@code foldFrom(initial(), all)} equals a full {@link #fold}; restoring a snapshot
     * and folding only the tail yields the identical result.
     */
    public static ReducerState foldFrom(ReducerState base, List<SequencedEvent> events) {
        Status status = base.status();
        String taskType = base.taskType();
        String idempotencyKey = base.idempotencyKey();
        int attempt = base.attempt();
        Instant startedAt = base.startedAt();
        Instant endedAt = base.endedAt();
        Cost cost = base.cost();
        long totalLatencyMillis = base.totalLatencyMillis();
        com.fasterxml.jackson.databind.JsonNode result = base.result();
        String error = base.error();
        long lastSeq = base.lastSeq();
        int lastToolStepIndex = base.lastToolStepIndex();
        List<TimelineStep> timeline = new ArrayList<>(base.timeline());

        for (SequencedEvent se : events) {
            lastSeq = se.seq();
            CatalystEvent e = se.event();
            switch (e) {
                case ExecutionCreated c -> {
                    status = Status.STARTING;
                    taskType = c.taskType();
                    idempotencyKey = c.idempotencyKey();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.CREATED, c.taskType(), c.at(), 0, null));
                }
                case ExecutionStarted s -> {
                    status = Status.RUNNING;
                    attempt = s.attempt();
                    if (startedAt == null) startedAt = s.at();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.STARTED, "attempt " + s.attempt(), s.at(), 0, null));
                }
                case ExecutionResumed r -> {
                    status = Status.RUNNING;
                    attempt = r.attempt();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.RESUMED, "attempt " + r.attempt(), r.at(), 0, null));
                }
                case ExecutionPaused p -> {
                    status = Status.PAUSED;
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.PAUSED, p.reason(), p.at(), 0, null));
                }
                case PromptBuilt p ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.PROMPT, p.promptHash(), p.at(), 0, p.prompt()));
                case CompletionRequested cr -> { /* marker; folded together with CompletionReceived */ }
                case CompletionReceived cr -> {
                    cost = cost.plus(cr.promptTokens(), cr.completionTokens(), cr.costUsd());
                    totalLatencyMillis += cr.latencyMillis();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.MODEL, cr.finishReason(), cr.at(), cr.latencyMillis(), cr.completion()));
                }
                case ToolRequested tr -> {
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.TOOL, tr.toolName(), tr.at(), 0, tr.input()));
                    lastToolStepIndex = timeline.size() - 1;
                }
                case ToolCompleted tc -> {
                    totalLatencyMillis += tc.latencyMillis();
                    // Fold the completion into the single TOOL step for its request (one step per call).
                    if (lastToolStepIndex >= 0) {
                        TimelineStep req = timeline.get(lastToolStepIndex);
                        timeline.set(lastToolStepIndex, new TimelineStep(req.seq(), TimelineStep.Kind.TOOL,
                                req.label(), req.at(), tc.latencyMillis(),
                                tc.error() == null ? tc.output() : null));
                        lastToolStepIndex = -1;
                    } else {
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.TOOL,
                                tc.error() == null ? "ok" : "error", tc.at(), tc.latencyMillis(), tc.output()));
                    }
                }
                case EffectRecorded ef ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.EFFECT, ef.label(), ef.at(), 0, ef.value()));
                case MemoryRead mr ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.MEMORY_READ, mr.key(), mr.at(), 0, mr.value()));
                case MemoryWritten mw ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.MEMORY_WRITE, mw.key(), mw.at(), 0, mw.value()));
                case RetryRequested rr ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.RETRY, rr.cause(), rr.at(), 0, null));
                case ExecutionBranched b ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.BRANCHED, b.parentId(), b.at(), 0, null));
                case ExecutionCompleted c -> {
                    status = Status.COMPLETED;
                    endedAt = c.at();
                    result = c.result();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.COMPLETED, null, c.at(), 0, c.result()));
                }
                case ExecutionFailed f -> {
                    status = Status.FAILED;
                    endedAt = f.at();
                    error = f.error();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.FAILED, f.error(), f.at(), 0, null));
                }
                case ExecutionCancelled c -> {
                    status = Status.CANCELLED;
                    endedAt = c.at();
                    error = c.reason();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.CANCELLED, c.reason(), c.at(), 0, null));
                }
            }
        }

        return new ReducerState(status, taskType, idempotencyKey, attempt, startedAt, endedAt,
                cost, totalLatencyMillis, result, error, lastSeq, lastToolStepIndex, timeline);
    }
}
