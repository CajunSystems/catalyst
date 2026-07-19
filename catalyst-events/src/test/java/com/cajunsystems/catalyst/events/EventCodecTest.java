package com.cajunsystems.catalyst.events;

import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventCodecTest {

    private final EventCodec codec = new EventCodec();

    @Test
    void everyEventTypeRoundTrips() {
        Instant t = Instant.parse("2026-01-02T03:04:05Z");
        List<CatalystEvent> all = List.of(
                new ExecutionCreated(t, "com.acme.SummarizeTask", "abc123", "cfg", "doc:42"),
                new ExecutionStarted(t, 1, "node-0"),
                new ExecutionResumed(t, 2),
                new ExecutionPaused(t, "waiting"),
                new PromptBuilt(t, "hash", new TextNode("prompt")),
                new CompletionRequested(t, "reqhash"),
                new CompletionReceived(t, new TextNode("hello"), 10, 5, 42, 0.001, "stop"),
                new ToolRequested(t, "calculator", new IntNode(7)),
                new ToolCompleted(t, new IntNode(14), null, 3),
                new EffectRecorded(t, "as-of", new TextNode("2026")),
                new MemoryRead(t, "k", new TextNode("v")),
                new MemoryWritten(t, "k", new TextNode("v")),
                new RetryRequested(t, "timeout", 1000, 5),
                new ExecutionBranched(t, "exec-parent", 12, "model"),
                new ExecutionCompleted(t, new TextNode("result")),
                new ExecutionFailed(t, "boom", 9),
                new ExecutionCancelled(t, "cancelled by request", 7));

        for (CatalystEvent e : all) {
            byte[] bytes = codec.encode(e);
            CatalystEvent decoded = codec.decode(bytes);
            assertThat(decoded).isEqualTo(e);
        }
    }

    @Test
    void discriminatorIsStable() {
        byte[] bytes = codec.encode(new ExecutionStarted(Instant.EPOCH, 1, "n"));
        assertThat(new String(bytes)).contains("\"@type\":\"ExecutionStarted\"");
    }
}
