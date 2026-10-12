package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionAbandonedSignal;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import com.cajunsystems.catalyst.runtime.RecoveryReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Deploy-4 exit demo as an automated test, on the durable log: closing a runtime mid-run leaves
 * the execution RUNNING, releases the directory lock, and a restart on the same directory resumes it
 * with zero duplicate model calls.
 */
class ShutdownAcceptanceTest {

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
    void closingMidRunAbandonsAndARestartResumes(@TempDir Path dir) throws Exception {
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inStep1 = new CountDownLatch(1);
        MockModel slow = MockModel.builder().respond(req -> {
            inStep1.countDown();
            while (!current.get().isShuttingDown()) Thread.onSpinWait();
            return new Completion("SUMMARY", List.of(), new Usage(1, 1), "stop");
        }).build();

        CatalystRuntime first = Catalyst.builder(dir).model(slow).build();
        current.set(first);
        ExecutionHandle<String> handle = first.execute(new TwoStepTask());
        ExecutionId id = handle.id();
        assertThat(inStep1.await(10, TimeUnit.SECONDS)).isTrue();
        first.close();
        assertThatThrownBy(handle::result).isInstanceOf(ExecutionAbandonedSignal.class);

        MockModel model = MockModel.alwaysReturn("FINAL");
        // Opening the same directory in this JVM only works if close() released the directory lock.
        try (CatalystRuntime restarted = Catalyst.builder(dir)
                .model(model).task(new TwoStepTask()).recoverOnStart(true).build()) {
            RecoveryReport report = restarted.startupRecovery().orElseThrow();
            assertThat(report.resumed()).containsOnlyKeys(id);
            assertThat(report.resumed().get(id).result()).isEqualTo("SUMMARY|FINAL");
            assertThat(model.callCount()).as("only step 2 ran live").isEqualTo(1);
            assertThat(restarted.inspect(id).status()).isEqualTo(Status.COMPLETED);
        }
    }
}
