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
        Status status = Status.NEW;
        String taskType = null;
        String idempotencyKey = null;
        int attempt = 0;
        Instant startedAt = null;
        Instant endedAt = null;
        Cost cost = Cost.ZERO;
        long totalLatencyMillis = 0;
        com.fasterxml.jackson.databind.JsonNode result = null;
        String error = null;
        long lastSeq = -1;
        List<TimelineStep> timeline = new ArrayList<>();

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
                case ToolRequested tr ->
                        timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.TOOL, tr.toolName(), tr.at(), 0, tr.input()));
                case ToolCompleted tc -> {
                    totalLatencyMillis += tc.latencyMillis();
                    timeline.add(new TimelineStep(se.seq(), TimelineStep.Kind.TOOL, tc.error() == null ? "ok" : "error", tc.at(), tc.latencyMillis(), tc.output()));
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
            }
        }

        return new ExecutionState(id, status, taskType, idempotencyKey, attempt, startedAt, endedAt,
                cost, totalLatencyMillis, result, error, lastSeq, timeline);
    }
}
