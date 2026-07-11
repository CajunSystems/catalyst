package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TimelineTest {

    @Test
    void aggregatesModelToolTokensAndCost() {
        Instant t = Instant.EPOCH;
        List<SequencedEvent> events = seq(List.of(
                new ExecutionCreated(t, "T", "h", "cfg", "k"),
                new ExecutionStarted(t, 1, "n"),
                new CompletionRequested(t, "rq"),
                new CompletionReceived(t, new TextNode("hi"), 100, 20, 30, 0.0123, "stop"),
                new ToolRequested(t, "calculator", new TextNode("2+2")),
                new ToolCompleted(t, new TextNode("4"), null, 5),
                new ExecutionCompleted(t, new TextNode("done"))));

        Timeline timeline = Reducer.fold(ExecutionId.of("e"), events).timelineView();

        assertThat(timeline.modelCalls()).isEqualTo(1);
        assertThat(timeline.toolCalls()).isEqualTo(1); // one step per tool call, not two
        assertThat(timeline.promptTokens()).isEqualTo(100);
        assertThat(timeline.completionTokens()).isEqualTo(20);
        assertThat(timeline.totalTokens()).isEqualTo(120);
        assertThat(timeline.totalCostUsd()).isCloseTo(0.0123, within(1e-9));
        assertThat(timeline.totalLatencyMillis()).isEqualTo(35);
    }

    private static List<SequencedEvent> seq(List<CatalystEvent> events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) out.add(new SequencedEvent(i, events.get(i)));
        return out;
    }
}
