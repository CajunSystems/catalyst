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

    private static List<SequencedEvent> seq(List<CatalystEvent> events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) out.add(new SequencedEvent(i, events.get(i)));
        return out;
    }
}
