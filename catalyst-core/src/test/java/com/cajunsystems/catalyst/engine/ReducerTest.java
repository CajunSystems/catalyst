package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReducerTest {

    private final ExecutionId id = ExecutionId.of("exec-1");

    @Test
    void foldsLifecycleCostAndTimeline() {
        Instant t = Instant.parse("2026-01-01T00:00:00Z");
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", "key-1"),
                new ExecutionStarted(t, 1, "node-0"),
                new CompletionRequested(t, "rq"),
                new CompletionReceived(t, new TextNode("hi"), 10, 4, 25, 0.002, "stop"),
                new ToolRequested(t, "calculator", new TextNode("1+1")),
                new ToolCompleted(t, new TextNode("2"), null, 1),
                new ExecutionCompleted(t, new TextNode("done"))));

        ExecutionState state = Reducer.fold(id, events);

        assertThat(state.status()).isEqualTo(Status.COMPLETED);
        assertThat(state.isTerminal()).isTrue();
        assertThat(state.taskType()).isEqualTo("TaskA");
        assertThat(state.idempotencyKey()).isEqualTo("key-1");
        assertThat(state.cost().promptTokens()).isEqualTo(10);
        assertThat(state.cost().completionTokens()).isEqualTo(4);
        assertThat(state.cost().usd()).isEqualTo(0.002);
        assertThat(state.totalLatencyMillis()).isEqualTo(26);
        assertThat(state.lastSeq()).isEqualTo(6);
        assertThat(state.trajectory()).extracting(TimelineStep::kind)
                .contains(TimelineStep.Kind.CREATED, TimelineStep.Kind.MODEL,
                        TimelineStep.Kind.TOOL, TimelineStep.Kind.COMPLETED);
    }

    @Test
    void foldsFailure() {
        Instant t = Instant.EPOCH;
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", ""),
                new ExecutionStarted(t, 1, "node-0"),
                new ExecutionFailed(t, "boom", 1)));

        ExecutionState state = Reducer.fold(id, events);

        assertThat(state.status()).isEqualTo(Status.FAILED);
        assertThat(state.error()).isEqualTo("boom");
    }

    @Test
    void foldsCancellation() {
        Instant t = Instant.EPOCH;
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", ""),
                new ExecutionStarted(t, 1, "node-0"),
                new ExecutionCancelled(t, "cancelled by request", 1)));

        ExecutionState state = Reducer.fold(id, events);

        assertThat(state.status()).isEqualTo(Status.CANCELLED);
        assertThat(state.isTerminal()).isTrue();
        assertThat(state.error()).isEqualTo("cancelled by request");
        assertThat(state.trajectory()).extracting(TimelineStep::kind)
                .contains(TimelineStep.Kind.CANCELLED);
    }

    @Test
    void foldsRetryAsAttemptWithCrashSafeBudget() {
        Instant t = Instant.EPOCH;
        // A tool fails (recorded with an error), a retry is elected (RetryRequested), and the execution
        // resumes as attempt 2 and completes. `retries` counts the failure retry; `attempt` counts the
        // resume. The two must not be conflated — a crash resume must not burn retry budget.
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", "key-1"),
                new ExecutionStarted(t, 1, "node-0"),
                new ToolRequested(t, "flaky", new TextNode("in")),
                new ToolCompleted(t, null, "boom", 1),
                new RetryRequested(t, "boom", 10, 3),
                new ExecutionResumed(t, 2),
                new ToolRequested(t, "flaky", new TextNode("in")),
                new ToolCompleted(t, new TextNode("ok"), null, 1),
                new ExecutionCompleted(t, new TextNode("done"))));

        ExecutionState state = Reducer.fold(id, events);

        assertThat(state.status()).isEqualTo(Status.COMPLETED);
        assertThat(state.retries()).as("one failure retry folded").isEqualTo(1);
        assertThat(state.attempt()).as("two attempts (start + resume)").isEqualTo(2);
        assertThat(state.trajectory()).extracting(TimelineStep::kind)
                .contains(TimelineStep.Kind.RETRY, TimelineStep.Kind.RESUMED);
    }

    @Test
    void resumeDoesNotBurnRetryBudget() {
        Instant t = Instant.EPOCH;
        // A crash resume (no preceding RetryRequested) advances `attempt` but leaves `retries` at 0.
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", ""),
                new ExecutionStarted(t, 1, "node-0"),
                new ExecutionResumed(t, 2)));

        ExecutionState state = Reducer.fold(id, events);

        assertThat(state.attempt()).isEqualTo(2);
        assertThat(state.retries()).isEqualTo(0);
    }

    @Test
    void foldFromSnapshotEqualsFullFoldAtEverySplit() {
        Instant t = Instant.parse("2026-01-01T00:00:00Z");
        // A stream whose tool request (seq 4) and its completion (seq 5) straddle any split taken
        // between them — the case that exercises the open-tool-step carried on the accumulator.
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "TaskA", "h", "cfg", "key-1"),
                new ExecutionStarted(t, 1, "node-0"),
                new CompletionRequested(t, "rq"),
                new CompletionReceived(t, new TextNode("hi"), 10, 4, 25, 0.002, "stop"),
                new ToolRequested(t, "calculator", new TextNode("1+1")),
                new ToolCompleted(t, new TextNode("2"), null, 1),
                new EffectRecorded(t, "now", new TextNode("2026")),
                new ExecutionCompleted(t, new TextNode("done"))));

        ExecutionState full = Reducer.fold(id, events);

        // Splitting the fold at every boundary and continuing from the accumulator must reproduce the
        // full fold exactly — this is the invariant snapshots rely on.
        for (int split = 0; split <= events.size(); split++) {
            ReducerState base = Reducer.foldFrom(ReducerState.initial(), events.subList(0, split));
            ReducerState resumed = Reducer.foldFrom(base, events.subList(split, events.size()));
            ExecutionState viaSplit = resumed.toExecutionState(id);

            assertThat(viaSplit.status()).as("split@%d status", split).isEqualTo(full.status());
            assertThat(viaSplit.lastSeq()).as("split@%d lastSeq", split).isEqualTo(full.lastSeq());
            assertThat(viaSplit.cost()).as("split@%d cost", split).isEqualTo(full.cost());
            assertThat(viaSplit.totalLatencyMillis()).as("split@%d latency", split)
                    .isEqualTo(full.totalLatencyMillis());
            assertThat(viaSplit.result()).as("split@%d result", split).isEqualTo(full.result());
            assertThat(viaSplit.trajectory()).as("split@%d timeline", split)
                    .isEqualTo(full.trajectory());
        }
    }

    private static List<SequencedEvent> seq(List<CatalystEvent> events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) out.add(new SequencedEvent(i, events.get(i)));
        return out;
    }
}
