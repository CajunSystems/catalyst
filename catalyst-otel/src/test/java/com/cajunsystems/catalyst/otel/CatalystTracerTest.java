package com.cajunsystems.catalyst.otel;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.fasterxml.jackson.databind.node.TextNode;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CatalystTracerTest {

    private final ExecutionId id = ExecutionId.of("exec-otel-1");
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private static Instant at(int sec) { return T0.plusSeconds(sec); }

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans))
            .build();
    private final Tracer tracer = provider.get("test");

    @Test
    void foldsAnExecutionIntoOneTraceWithSemanticSpansAndAnnotations() {
        // A run with a model call, a tool that fails, a retry, and a second tool call that succeeds.
        List<SequencedEvent> events = seq(
                new ExecutionCreated(at(0), "SearchTask", "h", "cfg", "k"),
                new ExecutionStarted(at(0), 1, "node-0"),
                new PromptBuilt(at(1), "phash", new TextNode("prompt")),
                new CompletionRequested(at(1), "rqhash"),
                new CompletionReceived(at(2), new TextNode("answer"), 10, 5, 1000, 0.001, "stop"),
                new ToolRequested(at(3), "search", new TextNode("q")),
                new ToolCompleted(at(4), null, "boom", 500),          // errored → triggers retry
                new RetryRequested(at(4), "boom", 10, 6),
                new ExecutionResumed(at(5), 2),
                new ToolRequested(at(5), "search", new TextNode("q")),
                new ToolCompleted(at(6), new TextNode("hit"), null, 500),
                new ExecutionCompleted(at(7), new TextNode("done")));

        new CatalystTracer(tracer).export(id, events);

        List<SpanData> all = spans.getFinishedSpanItems();
        assertThat(all).hasSize(4); // root + model + 2 tool

        SpanData root = all.stream().filter(s -> !s.getParentSpanContext().isValid()).findFirst().orElseThrow();
        assertThat(root.getName()).isEqualTo("SearchTask");
        assertThat(root.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(root.getAttributes().get(AttributeKey.longKey("catalyst.attempt"))).isEqualTo(2L);
        assertThat(root.getAttributes().get(AttributeKey.longKey("catalyst.retries"))).isEqualTo(1L);
        assertThat(root.getAttributes().get(AttributeKey.stringKey("catalyst.execution.id"))).isEqualTo("exec-otel-1");
        assertThat(root.getAttributes().get(AttributeKey.longKey("catalyst.tokens.total"))).isEqualTo(15L);
        assertThat(root.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
        // Root span bounds the whole execution.
        assertThat(root.getStartEpochNanos()).isEqualTo(toNanos(at(0)));
        assertThat(root.getEndEpochNanos()).isEqualTo(toNanos(at(7)));

        // Every non-root span is a child of the root.
        List<SpanData> children = all.stream().filter(s -> s.getParentSpanContext().isValid()).toList();
        assertThat(children).allSatisfy(c ->
                assertThat(c.getParentSpanContext().getSpanId()).isEqualTo(root.getSpanContext().getSpanId()));

        // One model span, carrying that call's own token/cost/finish attributes and its real duration.
        SpanData model = children.stream().filter(s -> s.getName().equals("model")).findFirst().orElseThrow();
        assertThat(model.getAttributes().get(AttributeKey.longKey("gen_ai.usage.input_tokens"))).isEqualTo(10L);
        assertThat(model.getAttributes().get(AttributeKey.longKey("gen_ai.usage.output_tokens"))).isEqualTo(5L);
        assertThat(model.getAttributes().get(AttributeKey.stringKey("catalyst.finish_reason"))).isEqualTo("stop");
        assertThat(model.getStartEpochNanos()).isEqualTo(toNanos(at(1)));
        assertThat(model.getEndEpochNanos()).isEqualTo(toNanos(at(2)));

        // Two tool spans; the first errored, the second succeeded.
        List<SpanData> tools = children.stream().filter(s -> s.getName().equals("search")).toList();
        assertThat(tools).hasSize(2);
        assertThat(tools).anySatisfy(s -> assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR));
        assertThat(tools).anySatisfy(s -> assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET));

        // Lifecycle moments are annotations on the root, not spans.
        assertThat(root.getEvents()).extracting(EventData::getName)
                .contains("started", "retry", "resumed");
    }

    @Test
    void aFailedExecutionMarksTheRootSpanError() {
        List<SequencedEvent> events = seq(
                new ExecutionCreated(at(0), "FlakyTask", "h", "cfg", "k"),
                new ExecutionStarted(at(0), 1, "node-0"),
                new ToolRequested(at(1), "search", new TextNode("q")),
                new ToolCompleted(at(2), null, "boom", 100),
                new ExecutionFailed(at(2), "boom", 3));

        new CatalystTracer(tracer).export(id, events);

        SpanData root = spans.getFinishedSpanItems().stream()
                .filter(s -> !s.getParentSpanContext().isValid()).findFirst().orElseThrow();
        assertThat(root.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(root.getStatus().getDescription()).isEqualTo("boom");
        assertThat(root.getEvents()).extracting(EventData::getName).contains("failed");
    }

    @Test
    void emptyEventListExportsNothing() {
        new CatalystTracer(tracer).export(id, List.of());
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    private static long toNanos(Instant i) {
        return i.getEpochSecond() * 1_000_000_000L + i.getNano();
    }

    private static List<SequencedEvent> seq(CatalystEvent... events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.length; i++) out.add(new SequencedEvent(i, events[i]));
        return out;
    }
}
