package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The v0.2 resume-by-id exit demo as an automated test (roadmap increment ②): record a task's first
 * step to the durable Gumbo log, simulate {@code kill -9} (no completion event), then reopen the log
 * and recover the execution <strong>from its id alone</strong> via a {@code TaskRegistry} — no
 * idempotency key, no re-submitted task instance. The execution resumes at the exact event and
 * completes with zero duplicate model calls.
 */
class ResumeByIdAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().system("summarizer").user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    /** A named two-step task: a stable {@code taskType} the registry can reconstruct across processes. */
    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) throws Exception {
            String summary = ctx.model().complete(STEP1).message();
            String finalAnswer = ctx.model().complete(STEP2).message();
            return summary + "|" + finalAnswer;
        }
    }

    /** Durably records step 1 for a fresh execution, then returns without completing (a "crash"). */
    private ExecutionId recordStepOne(Path dir, MockModel model) {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            Instant t = Instant.now();
            log.append(id, new CatalystEvent.ExecutionCreated(t, TwoStepTask.class.getName(), "h", "cfg", ""));
            log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));

            ExecutionInfo info = new ExecutionInfo(id, 1, TwoStepTask.class.getName(), Map.of());
            ReplayingContext ctx = new ReplayingContext(id, log, model, info, Map.of(),
                    EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                    com.cajunsystems.catalyst.ReplayMode.STRICT, null,
                    Clock.systemUTC(), LoggerFactory.getLogger("record"), log.read(id), true);
            String summary = ctx.model().complete(STEP1).message(); // records step 1 to disk
            assertThat(summary).isEqualTo("SUMMARY");
            assertThat(model.callCount()).isEqualTo(1);
            // kill -9: control leaves here with no ExecutionCompleted written.
        }
        return id;
    }

    @Test
    void recoversFromIdAloneWithZeroDuplicateModelCalls(@TempDir Path dir) {
        MockModel model = MockModel.scripted("SUMMARY", "FINAL");
        ExecutionId id = recordStepOne(dir, model);

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model)
                .task(new TwoStepTask())   // register the type — no key, no re-submitted task needed
                .build()) {

            String result = (String) runtime.resume(id).result();

            assertThat(result).isEqualTo("SUMMARY|FINAL");
            // Step 1 was substituted from the log; only step 2 ran live → exactly 2 total calls, not 3.
            assertThat(model.callCount()).isEqualTo(2);

            ExecutionState state = runtime.inspect(id);
            assertThat(state.status()).isEqualTo(Status.COMPLETED);
            assertThat(state.cost().totalTokens()).isGreaterThan(0);
        }
    }

    @Test
    void resumingAnUnregisteredTypeFailsWithAGuidingMessage(@TempDir Path dir) {
        MockModel model = MockModel.scripted("SUMMARY", "FINAL");
        ExecutionId id = recordStepOne(dir, model);

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {   // no task registered

            assertThatThrownBy(() -> runtime.resume(id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("No task registered")
                    .hasMessageContaining(TwoStepTask.class.getName());
        }
    }

    @Test
    void resumingAnUnknownIdFails(@TempDir Path dir) {
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(MockModel.scripted("X")).build()) {

            assertThatThrownBy(() -> runtime.resume(ExecutionId.random()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown execution");
        }
    }

    @Test
    void resumingATerminalExecutionNeedsNoRegistration(@TempDir Path dir) {
        MockModel model = MockModel.scripted("SUMMARY", "FINAL");
        ExecutionId id = recordStepOne(dir, model);

        // Drive it to completion once (registered)...
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).task(new TwoStepTask()).build()) {
            assertThat((String) runtime.resume(id).result()).isEqualTo("SUMMARY|FINAL");
        }
        int callsAfterCompletion = model.callCount();

        // ...then recover the recorded outcome from a fresh runtime with NO task registered: a terminal
        // execution replays without re-running the task, so it is recoverable from the id alone.
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {
            assertThat((String) runtime.resume(id).result()).isEqualTo("SUMMARY|FINAL");
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(callsAfterCompletion); // no re-run
        }
    }

    @Test
    void resumingAterminalExecutionReplaysItsOutcomeWithoutRerunning(@TempDir Path dir) {
        MockModel model = MockModel.scripted("SUMMARY", "FINAL");
        ExecutionId id = recordStepOne(dir, model);

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model)
                .task(new TwoStepTask())
                .build()) {

            assertThat((String) runtime.resume(id).result()).isEqualTo("SUMMARY|FINAL");
            int callsAfterCompletion = model.callCount();

            // A second resume of the now-terminal execution replays the recorded result, no new calls.
            String again = (String) runtime.resume(id).result();
            assertThat(again).isEqualTo("SUMMARY|FINAL");
            assertThat(model.callCount()).isEqualTo(callsAfterCompletion);
        }
    }
}
