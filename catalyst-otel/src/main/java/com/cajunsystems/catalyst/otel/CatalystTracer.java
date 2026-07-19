package com.cajunsystems.catalyst.otel;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.time.Instant;
import java.util.List;

/**
 * Folds an execution's event log into an OpenTelemetry trace (spec §12) — the log <em>is</em> the
 * trace, so this is a pure fold, not a separate instrumentation layer. One trace per {@link
 * ExecutionId}: a root span for the whole execution, a child span for each side-effecting boundary
 * (model / tool / effect / memory), and lifecycle moments (started, resumed, paused, retry, branched,
 * terminal) as span events on the root.
 *
 * <p>Read-only and post-hoc: it consumes the events an execution already produced
 * ({@code runtime.log().read(id)}), so it needs no hook into the runtime and can export a live,
 * resumed, or long-finished execution identically. It emits through an application-supplied {@link
 * Tracer}; Catalyst never owns the telemetry pipeline (the app wires the SDK + a real exporter), the
 * same division as the LangChain4j adapter.
 *
 * <p>Not tied to any SDK: only the OpenTelemetry API is used, so a no-op {@code Tracer} makes export a
 * cheap no-op.
 */
public final class CatalystTracer {

    private static final String SCOPE = "com.cajunsystems.catalyst";

    private final Tracer tracer;

    public CatalystTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    /** Builds a tracer from a configured {@link OpenTelemetry} instance (app-supplied SDK). */
    public static CatalystTracer of(OpenTelemetry openTelemetry) {
        return new CatalystTracer(openTelemetry.getTracer(SCOPE));
    }

    /**
     * Exports {@code events} — the full, seq-ordered event list for {@code id} — as one trace. A no-op
     * for an empty list.
     */
    public void export(ExecutionId id, List<SequencedEvent> events) {
        if (events == null || events.isEmpty()) return;
        ExecutionState state = Reducer.fold(id, events);

        Instant start = state.startedAt() != null ? state.startedAt() : events.get(0).event().at();
        Instant end = state.endedAt() != null ? state.endedAt() : events.get(events.size() - 1).event().at();

        Span root = tracer.spanBuilder(state.taskType() != null ? state.taskType() : "execution")
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(start)
                .setAttribute("catalyst.execution.id", id.value())
                .setAttribute("catalyst.attempt", state.attempt())
                .setAttribute("catalyst.retries", state.retries())
                .setAttribute("catalyst.status", state.status().name())
                .setAttribute("catalyst.tokens.total", state.cost().totalTokens())
                .setAttribute("catalyst.cost.usd", state.cost().usd())
                .setAttribute("catalyst.latency.ms", state.totalLatencyMillis())
                .startSpan();
        Context parent = Context.root().with(root);

        // Pair request/completion markers so a span spans its whole boundary. The request event only
        // marks the start; the span is opened and closed when the paired completion arrives.
        Instant modelRequestAt = null;
        ToolRequested pendingTool = null;
        Instant toolRequestAt = null;

        for (SequencedEvent se : events) {
            long seq = se.seq();
            CatalystEvent e = se.event();
            switch (e) {
                case CompletionRequested cr -> modelRequestAt = cr.at();
                case CompletionReceived cr -> {
                    Instant spanEnd = cr.at();
                    Instant spanStart = modelRequestAt != null ? modelRequestAt : minusMillis(spanEnd, cr.latencyMillis());
                    Span s = child("model", parent, spanStart, seq, "MODEL")
                            .setAttribute("gen_ai.usage.input_tokens", cr.promptTokens())
                            .setAttribute("gen_ai.usage.output_tokens", cr.completionTokens())
                            .setAttribute("catalyst.cost.usd", cr.costUsd())
                            .setAttribute("catalyst.finish_reason", cr.finishReason())
                            .setAttribute("catalyst.latency.ms", cr.latencyMillis())
                            .startSpan();
                    s.end(spanEnd);
                    modelRequestAt = null;
                }
                case ToolRequested tr -> {
                    pendingTool = tr;
                    toolRequestAt = tr.at();
                }
                case ToolCompleted tc -> {
                    Instant spanEnd = tc.at();
                    Instant spanStart = toolRequestAt != null ? toolRequestAt : minusMillis(spanEnd, tc.latencyMillis());
                    String name = pendingTool != null ? pendingTool.toolName() : "tool";
                    Span s = child(name, parent, spanStart, seq, "TOOL")
                            .setAttribute("catalyst.tool.name", name)
                            .setAttribute("catalyst.latency.ms", tc.latencyMillis())
                            .startSpan();
                    if (tc.error() != null) s.setStatus(StatusCode.ERROR, tc.error());
                    s.end(spanEnd);
                    pendingTool = null;
                    toolRequestAt = null;
                }
                case EffectRecorded er -> pointSpan("effect", parent, er.at(), seq, "EFFECT", "catalyst.effect.label", er.label());
                case MemoryRead mr -> pointSpan("memory.read", parent, mr.at(), seq, "MEMORY_READ", "catalyst.memory.key", mr.key());
                case MemoryWritten mw -> pointSpan("memory.write", parent, mw.at(), seq, "MEMORY_WRITE", "catalyst.memory.key", mw.key());

                // Lifecycle moments → annotations on the root span.
                case ExecutionStarted s2 -> root.addEvent("started", attr("catalyst.attempt", s2.attempt()), s2.at());
                case ExecutionResumed r -> root.addEvent("resumed", attr("catalyst.attempt", r.attempt()), r.at());
                case ExecutionPaused p -> root.addEvent("paused", attr("catalyst.reason", p.reason()), p.at());
                case RetryRequested rr -> root.addEvent("retry", attr("catalyst.cause", rr.cause()), rr.at());
                case ExecutionBranched b -> root.addEvent("branched", attr("catalyst.parent.id", b.parentId()), b.at());
                case ExecutionCancelled c -> root.addEvent("cancelled", attr("catalyst.reason", c.reason()), c.at());
                case ExecutionFailed f -> root.addEvent("failed", attr("catalyst.error", f.error()), f.at());

                // ExecutionCreated / ExecutionCompleted / PromptBuilt carry no span or annotation of
                // their own: creation + completion bound the root span, and the prompt hash rides the
                // following model span. (Kept explicit so a new event type is a compile error here.)
                case ExecutionCreated ignored -> { }
                case ExecutionCompleted ignored -> { }
                case PromptBuilt ignored -> { }
            }
        }

        if (state.status() == Status.FAILED) {
            root.setStatus(StatusCode.ERROR, state.error() != null ? state.error() : "failed");
        } else if (state.status() == Status.CANCELLED) {
            root.setStatus(StatusCode.ERROR, state.error() != null ? state.error() : "cancelled");
        } else if (state.status() == Status.COMPLETED) {
            root.setStatus(StatusCode.OK);
        }
        root.end(end);
    }

    private SpanBuilder child(String name, Context parent, Instant start, long seq, String kind) {
        return tracer.spanBuilder(name)
                .setParent(parent)
                .setSpanKind(SpanKind.CLIENT)
                .setStartTimestamp(start)
                .setAttribute("catalyst.seq", seq)
                .setAttribute("catalyst.kind", kind);
    }

    private void pointSpan(String name, Context parent, Instant at, long seq, String kind,
                           String attrKey, String attrValue) {
        // A boundary with no recorded duration: a zero-length span at its instant.
        Span s = child(name, parent, at, seq, kind).setAttribute(attrKey, attrValue).startSpan();
        s.end(at);
    }

    private static Attributes attr(String key, String value) {
        return value == null ? Attributes.empty() : Attributes.builder().put(key, value).build();
    }

    private static Attributes attr(String key, long value) {
        return Attributes.builder().put(key, value).build();
    }

    private static Instant minusMillis(Instant end, long millis) {
        return millis > 0 ? end.minusMillis(millis) : end;
    }
}
