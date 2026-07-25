package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import com.cajunsystems.catalyst.timeline.TimelineReport;
import com.cajunsystems.catalyst.tools.FilesystemTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The v0.2 timeline exit criterion (spec §12): an execution's folded state renders to a read-only
 * HTML report — the view over {@code inspect(id).timelineView()} / {@code trajectory()}.
 *
 * <p>Read-only and post-hoc, the same shape as the OTel exporter: the report consumes a fold of the
 * log and installs no runtime hook, so the log stays the single source of truth and any execution —
 * live or long finished — renders without having cooperated in advance.
 */
class TimelineAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().system("summarizer").user("summarize").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("finalize").build());

    @Test
    void aFoldedExecutionRendersToASelfContainedReport(@TempDir Path dir) throws Exception {
        Path logDir = Files.createDirectory(dir.resolve("log"));
        Path sandbox = Files.createDirectory(dir.resolve("sandbox"));
        FilesystemTool fs = new FilesystemTool(sandbox);

        Task<String> mixed = ctx -> {
            String summary = ctx.model().complete(STEP1).message();
            ctx.call(fs, FilesystemTool.Command.write("out.txt", summary));
            ctx.memory().put("summary", summary);
            String stamp = ctx.effect("stamp", () -> "2024-01-01T00:00:00Z");
            return summary + "|" + ctx.model().complete(STEP2).message() + "|" + stamp;
        };

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(logDir))
                .model(MockModel.scripted("SUMMARY", "FINAL"))
                .costModel(CostModel.perMillionTokens(3.0, 15.0))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(mixed, ExecutionOptions.withKey("t:1"));
            handle.result();
            ExecutionState state = runtime.inspect(handle.id());
            assertThat(state.status()).isEqualTo(Status.COMPLETED);

            Path report = TimelineReport.writeTo(state, dir.resolve("timeline.html"));
            String html = Files.readString(report);

            // Identifies the execution and reports its outcome.
            assertThat(html).contains(handle.id().value()).contains("COMPLETED");

            // Every boundary the execution crossed appears as a step.
            assertThat(html).contains(">MODEL<").contains(">TOOL<")
                    .contains(">EFFECT<").contains(">MEMORY_WRITE<").contains(">COMPLETED<");

            // The roll-ups are the same folds inspect() reports — no separate metrics pipeline.
            assertThat(html).contains(String.valueOf(state.timelineView().modelCalls()));
            assertThat(html).contains(String.valueOf(state.timelineView().totalTokens()));

            // Portable: nothing to fetch, nothing to execute.
            assertThat(html).doesNotContain("http://").doesNotContain("https://")
                    .doesNotContain("<script").doesNotContain("<link").doesNotContain("<img");

            // A pure function of the fold, so two renders of one log are byte-identical.
            assertThat(TimelineReport.html(state)).isEqualTo(html);
        }
    }
}
