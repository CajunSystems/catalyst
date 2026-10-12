package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.Timeline;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The README's Quickstart, kept compiling and true. If this test needs a change, the README's
 * Quickstart needs the same change: it is the first code a new developer copies.
 */
class QuickstartTest {

    // ── Verbatim from README "Quickstart" (only the data directory differs) ──────────────────────
    static final class TriageTask implements Task<String> {
        @Override
        public String execute(Context ctx) {
            String summary = ctx.model().complete(CompletionRequest.of(Prompt.builder()
                    .system("You triage support tickets.")
                    .user("Summarize: the printer on floor 3 is offline")
                    .build())).message();
            String receivedAt = ctx.effect("received-at", () -> Instant.now().toString());
            return summary + " (received " + receivedAt + ")";
        }
    }

    static String runOnce(Path dataDir, MockModel model) {
        try (CatalystRuntime runtime = Catalyst.builder(dataDir)
                .model(model)
                .task(new TriageTask())
                .recoverOnStart(true)
                .build()) {

            ExecutionHandle<String> run =
                    runtime.execute(new TriageTask(), ExecutionOptions.withKey("ticket-1042"));
            String result = run.result();
            Timeline timeline = runtime.inspect(run.id()).timelineView();
            System.out.println(result);
            System.out.println(timeline.modelCalls() + " model call(s), " + timeline.totalTokens() + " tokens");
            return result;
        }
    }
    // ─────────────────────────────────────────────────────────────────────────────────────────────

    /** What the README promises: run it twice, same output, and the model is called once. */
    @Test
    void runningTheQuickstartTwiceGivesTheSameRecordedResult(@TempDir Path dir) {
        MockModel model = MockModel.alwaysReturn("Printer offline, floor 3");

        String first = runOnce(dir, model);
        String second = runOnce(dir, model);

        assertThat(first).startsWith("Printer offline, floor 3 (received ");
        assertThat(second).as("same key: attached to the recorded execution, timestamp included").isEqualTo(first);
        assertThat(model.callCount()).as("the second run did not call the model").isEqualTo(1);
    }
}
