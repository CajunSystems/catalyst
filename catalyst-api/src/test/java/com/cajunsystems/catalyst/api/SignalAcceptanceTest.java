package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Deploy-6 exit demo as an automated test, on the durable log: a task waits for a reviewer with no
 * thread held, the process goes away, and a signal delivered by a fresh runtime resumes it — the
 * draft substituted, only the final step calling the model.
 */
class SignalAcceptanceTest {

    record Approval(String reviewer, boolean approved) {}

    private static final CompletionRequest DRAFT =
            CompletionRequest.of(Prompt.builder().user("summarize the incident").build());
    private static final CompletionRequest FINAL =
            CompletionRequest.of(Prompt.builder().user("write the final report").build());

    static final class ReviewedReport implements Task<String> {
        @Override public String execute(Context ctx) {
            String draft = ctx.model().complete(DRAFT).message();
            Approval approval = ctx.awaitSignal("approval", Approval.class);
            return draft + "|" + ctx.model().complete(FINAL).message() + " by " + approval.reviewer();
        }
    }

    @Test
    void aWaitSurvivesARestartAndASignalFinishesIt(@TempDir Path dir) {
        ExecutionId id;
        MockModel first = MockModel.alwaysReturn("DRAFT");
        try (CatalystRuntime runtime = Catalyst.builder(dir).model(first).task(new ReviewedReport()).build()) {
            ExecutionHandle<String> handle = runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-7"));
            assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);
            id = handle.id();
            assertThat(first.callCount()).isEqualTo(1);
        }

        MockModel second = MockModel.alwaysReturn("FINAL");
        try (CatalystRuntime runtime = Catalyst.builder(dir).model(second).task(new ReviewedReport())
                .recoverOnStart(true).build()) {
            assertThat(runtime.startupRecovery().orElseThrow().leftAlone()).containsEntry(id, Status.WAITING);
            assertThat(runtime.inspect(id).awaitingSignal()).contains("approval");

            assertThat(runtime.signal(id, "approval", new Approval("dana", true)).result())
                    .isEqualTo("DRAFT|FINAL by dana");
            assertThat(second.callCount()).as("only the final step ran live").isEqualTo(1);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(runtime.log().openExecutions()).doesNotContain(id);
        }
    }
}
