package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.Timeline;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The M1 exit demo as an automated test (spec §11): record a full execution to the durable Gumbo
 * log, then replay it with substitution — <strong>zero external calls</strong>, canonical hashes
 * verified — and show that a divergent task is rejected under STRICT.
 */
class M1ReplayAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().system("summarizer").user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    private static final Task<String> TWO_STEP = ctx -> {
        String summary = ctx.model().complete(STEP1).message();
        String finalAnswer = ctx.model().complete(STEP2).message();
        return summary + "|" + finalAnswer;
    };

    private static MockModel model() {
        // Content-based so replay is deterministic; reports token usage so cost folds in.
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().get(req.prompt().messages().size() - 1).content();
            String text = last.contains("summarize") ? "SUMMARY" : "FINAL";
            return new com.cajunsystems.catalyst.model.Completion(text, java.util.List.of(),
                    new com.cajunsystems.catalyst.model.Usage(40, 8), "stop");
        }).build();
    }

    @Test
    void replayIsExactWithZeroExternalCallsAndCatchesDivergence(@TempDir Path dir) {
        MockModel model = model();
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .costModel(CostModel.perMillionTokens(3.0, 15.0))
                .build()) {

            // Record a complete execution.
            ExecutionId id = runtime.execute(TWO_STEP, ExecutionOptions.withKey("doc:1")).id();
            String result = runtime.execute(TWO_STEP, ExecutionOptions.withKey("doc:1")).result();
            assertThat(result).isEqualTo("SUMMARY|FINAL");
            int callsAfterRecord = model.callCount();

            // Typed timeline with token/cost accounting.
            Timeline timeline = runtime.inspect(id).timelineView();
            assertThat(timeline.modelCalls()).isEqualTo(2);
            assertThat(timeline.totalTokens()).isEqualTo((40 + 8) * 2);
            assertThat(timeline.totalCostUsd()).isGreaterThan(0.0);

            // Exact replay: zero external calls, hashes verify, completes.
            ExecutionState replayed = runtime.replay(id, TWO_STEP);
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(callsAfterRecord);

            // A divergent task (different first prompt) is rejected under STRICT.
            Task<String> divergent = ctx ->
                    ctx.model().complete(CompletionRequest.of(Prompt.builder().user("something else").build())).message();
            assertThatThrownBy(() -> runtime.replay(id, divergent))
                    .isInstanceOf(NonDeterministicReplayException.class);
        }
    }
}
