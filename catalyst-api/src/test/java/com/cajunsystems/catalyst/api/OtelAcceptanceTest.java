package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.otel.CatalystTracer;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * v0.2 observability exit demo (spec §12): an execution recorded over a durable log folds into one
 * OpenTelemetry trace — a root span for the run, a child span per side-effecting boundary, and
 * lifecycle moments as annotations on the root. Exercised end-to-end through a real {@link
 * CatalystRuntime} into an in-memory span exporter (offline).
 */
class OtelAcceptanceTest {

    record In(String v) {}

    static final class FlakyTool implements Tool<In, String> {
        final AtomicInteger invocations = new AtomicInteger();
        private final int failures;
        FlakyTool(int failures) { this.failures = failures; }
        public String name() { return "search"; }
        public Class<In> inputType() { return In.class; }
        public String apply(In in) {
            if (invocations.incrementAndGet() <= failures) throw new RuntimeException("transient");
            return "hit:" + in.v();
        }
    }

    private static Task<String> modelThenFlaky(FlakyTool tool) {
        return ctx -> {
            String m = ctx.model().complete(
                    CompletionRequest.of(Prompt.builder().user("go").build())).message();
            return m + "|" + ctx.call(tool, new In("x"));
        };
    }

    private InMemorySpanExporter spans;
    private SdkTracerProvider activeProvider;

    @org.junit.jupiter.api.AfterEach
    void closeProvider() {
        if (activeProvider != null) activeProvider.close();
    }

    private CatalystTracer tracerInto(InMemorySpanExporter exporter) {
        activeProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        return new CatalystTracer(activeProvider.get("test"));
    }

    @Test
    void aRetriedExecutionFoldsIntoOneTraceWithSpansAndAnnotations(@TempDir Path dir) {
        spans = InMemorySpanExporter.create();
        CatalystTracer tracer = tracerInto(spans);
        MockModel model = MockModel.alwaysReturn("M");
        FlakyTool tool = new FlakyTool(1); // fail once, then succeed → a retry annotation

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .retryPolicy(RetryPolicy.maxRetries(2, Duration.ofMillis(5)))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("M|hit:x");

            tracer.export(handle.id(), runtime.log().read(handle.id()));
        }

        List<SpanData> all = spans.getFinishedSpanItems();
        SpanData root = all.stream().filter(s -> !s.getParentSpanContext().isValid()).findFirst().orElseThrow();

        // One trace: exactly one root, and every other span is its child.
        assertThat(all.stream().filter(s -> !s.getParentSpanContext().isValid())).hasSize(1);
        assertThat(all.stream().filter(s -> s.getParentSpanContext().isValid()))
                .allSatisfy(c -> assertThat(c.getParentSpanContext().getSpanId())
                        .isEqualTo(root.getSpanContext().getSpanId()));

        assertThat(root.getKind()).isEqualTo(SpanKind.INTERNAL);
        assertThat(root.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
        assertThat(root.getAttributes().get(AttributeKey.longKey("catalyst.retries"))).isEqualTo(1L);

        // One model span (with token attributes) and two tool spans (first errored, then succeeded).
        assertThat(all).filteredOn(s -> s.getName().equals("model")).hasSize(1)
                .allSatisfy(s -> assertThat(s.getAttributes()
                        .get(AttributeKey.longKey("gen_ai.usage.input_tokens"))).isNotNull());
        assertThat(all).filteredOn(s -> s.getName().equals("search")).hasSize(2);

        // The retry is visible as an annotation on the root, not a span.
        assertThat(root.getEvents()).extracting(EventData::getName).contains("retry", "started");
    }

    @Test
    void aFailedExecutionMarksTheRootSpanError(@TempDir Path dir) {
        spans = InMemorySpanExporter.create();
        CatalystTracer tracer = tracerInto(spans);
        MockModel model = MockModel.alwaysReturn("M");
        FlakyTool tool = new FlakyTool(99); // always fails, default policy → terminal

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));
            catchThrowable(handle::result);

            tracer.export(handle.id(), runtime.log().read(handle.id()));
        }

        SpanData root = spans.getFinishedSpanItems().stream()
                .filter(s -> !s.getParentSpanContext().isValid()).findFirst().orElseThrow();
        assertThat(root.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(root.getEvents()).extracting(EventData::getName).contains("failed");
    }
}
