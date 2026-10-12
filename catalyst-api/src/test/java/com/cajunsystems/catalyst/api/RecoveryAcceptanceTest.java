package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import com.cajunsystems.catalyst.runtime.RecoveryReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Deploy-2 exit demo as an automated test: a node dies with several executions in flight and one
 * paused; a fresh runtime over the same durable log, built with {@code recoverOnStart} and holding no
 * ids, finishes the in-flight ones with zero duplicate model calls and leaves the paused one paused.
 */
class RecoveryAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.model().complete(STEP1).message() + "|" + ctx.model().complete(STEP2).message();
        }
    }

    @Test
    void aRestartedNodeFinishesInFlightWorkAndLeavesPausedWorkPaused(@TempDir Path dir) {
        ExecutionId a, b, c, paused;
        MockModel first = MockModel.alwaysReturn("SUMMARY");
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            a = recordStepOne(log, first);
            b = recordStepOne(log, first);
            c = recordStepOne(log, first);
            paused = recordStepOne(log, first);
            log.append(paused, new CatalystEvent.ExecutionPaused(Instant.now(), "held by operator"));
            // the process dies here: nothing past step 1 for any of them
        }

        MockModel second = MockModel.alwaysReturn("FINAL");
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(second)
                .task(new TwoStepTask())
                .recoverOnStart(true)
                .build()) {

            RecoveryReport report = runtime.startupRecovery().orElseThrow();

            assertThat(report.resumed().keySet()).as("in creation order").containsExactly(a, b, c);
            for (ExecutionHandle<?> h : report.resumed().values()) assertThat(h.result()).isEqualTo("SUMMARY|FINAL");
            assertThat(second.callCount()).as("only the three step 2s ran live").isEqualTo(3);
            for (ExecutionId id : report.resumed().keySet()) {
                assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            }

            assertThat(report.leftAlone()).containsExactly(Map.entry(paused, Status.PAUSED));
            assertThat(runtime.inspect(paused).status()).isEqualTo(Status.PAUSED);
            assertThat(report.notRecovered()).isEmpty();
            assertThat(runtime.log().openExecutions()).containsExactly(paused);
        }

        // A second restart finds only the paused execution, and still does not run it.
        MockModel third = MockModel.alwaysReturn("UNUSED");
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(third)
                .task(new TwoStepTask())
                .recoverOnStart(true)
                .build()) {
            RecoveryReport report = runtime.startupRecovery().orElseThrow();
            assertThat(report.resumed()).isEmpty();
            assertThat(report.leftAlone()).containsOnlyKeys(paused);
            assertThat(third.callCount()).isZero();
        }
    }

    private static ExecutionId recordStepOne(GumboEventLog log, MockModel model) {
        ExecutionId id = ExecutionId.random();
        String taskType = TwoStepTask.class.getName();
        log.append(id, new CatalystEvent.ExecutionCreated(Instant.now(), taskType, "h", "cfg", ""));
        log.append(id, new CatalystEvent.ExecutionStarted(Instant.now(), 1, "node-0"));
        ReplayingContext ctx = new ReplayingContext(id, log, model, new ExecutionInfo(id, 1, taskType, Map.of()),
                Map.of(), EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                ReplayMode.STRICT, null, Clock.systemUTC(), LoggerFactory.getLogger("record"), log.read(id), true);
        assertThat(ctx.model().complete(STEP1).message()).isEqualTo("SUMMARY");
        return id;
    }
}
