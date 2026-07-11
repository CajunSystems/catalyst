package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The M0 exit demo as an automated test (spec §11): record a task's first step to the durable Gumbo
 * file log, simulate {@code kill -9} (no completion event is ever written), then reopen the log and
 * resume by re-submitting the task with the same idempotency key. The execution resumes at the exact
 * event and completes with <strong>zero duplicate model calls</strong>.
 */
class M0ResumeAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().system("summarizer").user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    /** A two-step AI task: summarize, then finalize. A crash between the steps is the interesting case. */
    private static final Task<String> TWO_STEP = ctx -> {
        String summary = ctx.model().complete(STEP1).message();
        String finalAnswer = ctx.model().complete(STEP2).message();
        return summary + "|" + finalAnswer;
    };

    @Test
    void resumesAtExactEventWithZeroDuplicateModelCalls(@TempDir Path dir) {
        MockModel model = MockModel.scripted("SUMMARY", "FINAL");
        ExecutionId id = ExecutionId.random();
        String key = "doc:42";

        // ── Phase 1: durably record step 1, then "crash" (never append completion) ──
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            Instant t = Instant.now();
            log.putKey(key, id);
            log.append(id, new CatalystEvent.ExecutionCreated(t, TWO_STEP.getClass().getName(), "h", "cfg", key));
            log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));

            ExecutionInfo info = new ExecutionInfo(id, 1, "TwoStep", Map.of());
            ReplayingContext ctx = new ReplayingContext(id, log, model, info, Map.of(),
                    EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                    com.cajunsystems.catalyst.ReplayMode.STRICT, null,
                    Clock.systemUTC(), LoggerFactory.getLogger("phase1"), log.read(id), true);

            String summary = ctx.model().complete(STEP1).message(); // genuinely records step 1 to disk
            assertThat(summary).isEqualTo("SUMMARY");
            assertThat(model.callCount()).isEqualTo(1);
            // kill -9: control leaves here with no ExecutionCompleted written.
        }

        // ── Phase 2: reopen the durable log and resume via the same idempotency key ──
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {

            String result = runtime.execute(TWO_STEP, ExecutionOptions.withKey(key)).result();

            assertThat(result).isEqualTo("SUMMARY|FINAL");
            // Step 1 was substituted from the log; only step 2 ran live → exactly 2 total calls, not 3.
            assertThat(model.callCount()).isEqualTo(2);

            ExecutionState state = runtime.inspect(id);
            assertThat(state.status()).isEqualTo(Status.COMPLETED);
            assertThat(state.cost().totalTokens()).isGreaterThan(0);
        }
    }
}
