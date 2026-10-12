package com.cajunsystems.catalyst.timeline;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimelineReportTest {

    record In(String v) {}

    /** A tool whose name and output are attacker-controlled as far as the report is concerned. */
    record EchoTool(String name) implements Tool<In, String> {
        @Override public Class<In> inputType() { return In.class; }
        @Override public String apply(In in) { return "echo:" + in.v(); }
    }

    private static ExecutionState runAndInspect(CatalystRuntime runtime, Task<String> task) {
        var handle = runtime.execute(task, ExecutionOptions.withKey("k"));
        handle.result();
        return runtime.inspect(handle.id());
    }

    @Test
    void rendersASelfContainedReportOfTheFoldedTimeline() {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("hello")).build()) {

            Tool<In, String> echo = new EchoTool("echo");
            ExecutionState state = runAndInspect(runtime, ctx -> {
                String m = ctx.model().complete(
                        CompletionRequest.of(Prompt.builder().user("hi").build())).message();
                String t = ctx.call(echo, new In("x"));
                ctx.memory().put("note", "remembered");
                return m + "|" + t;
            });

            String html = TimelineReport.html(state);

            assertThat(html).startsWith("<!doctype html>").endsWith("</html>\n");
            assertThat(html).contains(state.id().value());
            assertThat(html).contains("COMPLETED");
            // Every boundary kind the execution produced shows up as a step.
            assertThat(html).contains(">MODEL<").contains(">TOOL<").contains(">MEMORY_WRITE<");
            assertThat(html).contains("echo");

            // Self-contained: no network references of any kind, and no scripts.
            assertThat(html).doesNotContain("http://").doesNotContain("https://")
                    .doesNotContain("<script").doesNotContain("<link").doesNotContain("<img");
        }
    }

    @Test
    void escapesEverythingItInterpolatesFromTheLog() {
        // Tool names, labels and payloads are log content, and a report gets opened in a browser by
        // someone who did not necessarily produce the execution. None of it may become markup.
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("safe")).build()) {

            Tool<In, String> nasty = new EchoTool("<script>alert('tool')</script>");
            ExecutionState state = runAndInspect(runtime, ctx -> {
                ctx.effect("<img src=x onerror=alert('effect')>", () -> "<b>value</b>");
                return ctx.call(nasty, new In("<svg onload=alert('input')>"));
            });

            String html = TimelineReport.html(state);

            assertThat(html).doesNotContain("<script>alert")
                    .doesNotContain("<img src=x")
                    .doesNotContain("<svg onload");
            assertThat(html).contains("&lt;script&gt;alert(&#39;tool&#39;)&lt;/script&gt;");
            assertThat(html).contains("&lt;img src=x onerror=alert(&#39;effect&#39;)&gt;");
        }
    }

    @Test
    void escapesTheFiveDangerousCharacters() {
        assertThat(TimelineReport.escape("<a href=\"x\">&'</a>"))
                .isEqualTo("&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;");
        assertThat(TimelineReport.escape(null)).isEmpty();
        assertThat(TimelineReport.escape("plain")).isEqualTo("plain");
    }

    @Test
    void reportsAFailedExecutionWithItsError() {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).build()) {

            var handle = runtime.execute((Task<String>) ctx -> {
                throw new IllegalStateException("boom <&>");
            }, ExecutionOptions.withKey("k"));
            try {
                handle.result();
            } catch (Throwable expected) {
                // terminal failure is the point
            }

            String html = TimelineReport.html(runtime.inspect(handle.id()));
            assertThat(html).contains("FAILED").contains("boom &lt;&amp;&gt;");
            assertThat(html).doesNotContain("boom <&>");
        }
    }

    @Test
    void isDeterministicForAGivenLog() {
        // The report is a pure function of a fold, so the same execution renders identically. That is
        // what makes it safe to diff two reports or commit one as a build artifact.
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("hello")).build()) {

            ExecutionState state = runAndInspect(runtime, ctx -> ctx.model().complete(
                    CompletionRequest.of(Prompt.builder().user("hi").build())).message());

            assertThat(TimelineReport.html(state)).isEqualTo(TimelineReport.html(state));
        }
    }

    @Test
    void elidesAnOversizedPayloadInsteadOfInliningIt() {
        String big = "x".repeat(50_000);
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).build()) {

            ExecutionState state = runAndInspect(runtime, ctx -> ctx.effect("big", () -> big));

            String html = TimelineReport.html(state);
            assertThat(html).contains("elided");
            assertThat(html.length()).isLessThan(big.length()); // the page did not swallow the payload
        }
    }

    @Test
    void writesTheReportToAFileCreatingParentDirectories(@TempDir Path dir) throws Exception {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("hello")).build()) {

            ExecutionState state = runAndInspect(runtime, ctx -> ctx.model().complete(
                    CompletionRequest.of(Prompt.builder().user("hi").build())).message());

            Path target = dir.resolve("nested/report.html");
            Path written = TimelineReport.writeTo(state, target);

            assertThat(written).isEqualTo(target);
            assertThat(Files.readString(target)).isEqualTo(TimelineReport.html(state));
        }
    }

    @Test
    void rejectsANullState() {
        assertThatThrownBy(() -> TimelineReport.html(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void elidesOnACodePointBoundarySoNoCharacterIsCorrupted() {
        // The payload is serialized into a JSON envelope before truncation, so the exact padding that
        // puts a surrogate pair across the 2000-char cut is not something to guess at. Sweep a range
        // of lengths around the boundary: at least one lands mid-pair, and none may corrupt.
        // "\uD83D\uDE80" is a rocket — two UTF-16 chars, one code point.
        String rocket = "\uD83D\uDE80";

        for (int pad = 1_950; pad <= 2_010; pad++) {
            String payload = "a".repeat(pad) + rocket.repeat(100);
            try (CatalystRuntime runtime = CatalystRuntime.builder()
                    .log(EventLogs.inMemory()).build()) {

                ExecutionState state = runAndInspect(runtime, ctx -> ctx.effect("emoji", () -> payload));
                String html = TimelineReport.html(state);

                assertThat(html).as("pad=%d should elide", pad).contains("elided");
                assertNoLoneSurrogates(html, pad);
                // A lone surrogate is unmappable in UTF-8, so it would not survive this round-trip.
                byte[] utf8 = html.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                assertThat(new String(utf8, java.nio.charset.StandardCharsets.UTF_8))
                        .as("pad=%d must survive a UTF-8 round-trip", pad).isEqualTo(html);
            }
        }
    }

    private static void assertNoLoneSurrogates(String html, int pad) {
        for (int i = 0; i < html.length(); i++) {
            char c = html.charAt(i);
            if (Character.isHighSurrogate(c)) {
                assertThat(i + 1 < html.length() && Character.isLowSurrogate(html.charAt(i + 1)))
                        .as("pad=%d left a lone high surrogate at %d", pad, i).isTrue();
            } else if (Character.isLowSurrogate(c)) {
                assertThat(i > 0 && Character.isHighSurrogate(html.charAt(i - 1)))
                        .as("pad=%d left a lone low surrogate at %d", pad, i).isTrue();
            }
        }
    }

    // ── Latency waterfall ────────────────────────────────────────────────────

    private static final java.time.Instant T0 = java.time.Instant.parse("2026-01-01T00:00:00Z");

    private static com.cajunsystems.catalyst.engine.TimelineStep step(
            com.cajunsystems.catalyst.engine.TimelineStep.Kind kind, long atMillis, long latency) {
        return new com.cajunsystems.catalyst.engine.TimelineStep(0, kind, "x", T0.plusMillis(atMillis), latency, null);
    }

    /**
     * Geometry, pinned: a MODEL step is stamped when its completion arrived, so its bar ends there; a
     * TOOL step is stamped at its request, so its bar starts there. Over a 1000 ms span, a model call
     * finishing at 600 ms after 400 ms of latency spans 20–60%.
     */
    @Test
    void latencyBarsArePlacedOnTheExecutionsTimeAxis() {
        assertThat(TimelineReport.bar(T0, 1000, step(com.cajunsystems.catalyst.engine.TimelineStep.Kind.MODEL, 600, 400)))
                .contains("left:20.00%;width:40.00%").contains("class=\"model\"").contains("title=\"400 ms\"");
        assertThat(TimelineReport.bar(T0, 1000, step(com.cajunsystems.catalyst.engine.TimelineStep.Kind.TOOL, 100, 200)))
                .contains("left:10.00%;width:20.00%").contains("class=\"tool\"");
        assertThat(TimelineReport.bar(T0, 1000, step(com.cajunsystems.catalyst.engine.TimelineStep.Kind.EFFECT, 500, 0)))
                .as("an instant step is a tick").contains("class=\"tick effect\"").contains("left:50.00%");
        assertThat(TimelineReport.bar(T0, 1000, step(com.cajunsystems.catalyst.engine.TimelineStep.Kind.MODEL, 100, 400)))
                .as("a call that began before the execution's start is clamped to it, and still ends where it ended")
                .contains("left:0.00%;width:10.00%");
        assertThat(TimelineReport.bar(T0, 1000, step(com.cajunsystems.catalyst.engine.TimelineStep.Kind.TOOL, 900, 500)))
                .as("never past the end of the axis").contains("left:90.00%;width:10.00%");
    }

    @Test
    void everyStepOfARealExecutionGetsABar() {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("hello")).build()) {
            Tool<In, String> echo = new EchoTool("echo");
            ExecutionState state = runAndInspect(runtime, ctx -> {
                ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build()));
                return ctx.call(echo, new In("x"));
            });
            String html = TimelineReport.html(state);
            assertThat(html).contains("<th class=\"wfcol\">waterfall</th>");
            assertThat(countOf(html, "<div class=\"wf\"")).isEqualTo(state.timeline().size());
            assertThat(html).doesNotContain("<script").doesNotContain("http://").doesNotContain("https://");
        }
    }

    private static int countOf(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) n++;
        return n;
    }

    // ── Branch comparison ────────────────────────────────────────────────────

    @Test
    void aBranchComparisonShowsWhatChangedWithBothPayloadsEscaped() {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("<b>original</b>")).build()) {
            Tool<In, String> echo = new EchoTool("echo");
            Task<String> task = ctx -> {
                ctx.call(echo, new In("lookup"));
                return ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();
            };
            var handle = runtime.execute(task, ExecutionOptions.none());
            handle.result();
            com.cajunsystems.catalyst.engine.Trajectory base =
                    com.cajunsystems.catalyst.engine.Trajectory.of(runtime.inspect(handle.id()));
            long beforeModel = runtime.log().read(handle.id()).stream()
                    .filter(e -> e.event() instanceof com.cajunsystems.catalyst.events.CatalystEvent.PromptBuilt)
                    .findFirst().orElseThrow().seq() - 1;
            com.cajunsystems.catalyst.engine.Trajectory fork = runtime.branch(handle.id(), beforeModel)
                    .withModel(MockModel.alwaysReturn("<i>forked</i>")).run(task);

            String html = TimelineReport.diffHtml(base, fork);

            assertThat(html).startsWith("<!doctype html>").contains(base.id().value()).contains(fork.id().value());
            assertThat(html).contains("<dt>Steps changed</dt><dd>1</dd>");
            assertThat(html).contains("<span class=\"mark changed\">CHANGED</span>")
                    .contains("<span class=\"mark same\">SAME</span>");
            assertThat(html).contains("&lt;b&gt;original&lt;/b&gt;").contains("&lt;i&gt;forked&lt;/i&gt;")
                    .doesNotContain("<b>original</b>").doesNotContain("<i>forked</i>");
            assertThat(html).doesNotContain("<script").doesNotContain("http://").doesNotContain("https://");
            assertThat(TimelineReport.diffHtml(base, fork)).as("deterministic").isEqualTo(html);
            // The changed row previews what each side said, not just the shared "stop" label.
            assertThat(html).contains("<div class=\"preview\">&lt;b&gt;original&lt;/b&gt;</div>")
                    .contains("<div class=\"preview\">&lt;i&gt;forked&lt;/i&gt;</div>");
            assertThat(html.split("class=\"preview\"", -1).length - 1).as("only differing rows preview").isEqualTo(2);
        }
    }

    @Test
    void writesTheComparisonToAFileAndRejectsMissingSides(@TempDir Path dir) throws Exception {
        var t = com.cajunsystems.catalyst.engine.Trajectory.of(com.cajunsystems.catalyst.ExecutionId.random(),
                com.cajunsystems.catalyst.Status.COMPLETED, java.util.List.of(), null);
        Path file = TimelineReport.writeDiffTo(t, t, dir.resolve("nested/diff.html"));
        assertThat(Files.readString(file)).contains("Neither run recorded a work step");
        assertThatThrownBy(() -> TimelineReport.diffHtml(null, t)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPreviewIsOneClippedLineOfTheReadablePart() {
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(TimelineReport.preview(json.createObjectNode().put("message", "two\n  lines"))).isEqualTo("two lines");
        assertThat(TimelineReport.preview(json.createObjectNode().put("type", "java.lang.String").put("value", "v"))).isEqualTo("v");
        assertThat(TimelineReport.preview(json.createObjectNode().put("message", "x".repeat(400)))).hasSize(161).endsWith("…");
        assertThat(TimelineReport.preview(null)).isEmpty();
    }
}
