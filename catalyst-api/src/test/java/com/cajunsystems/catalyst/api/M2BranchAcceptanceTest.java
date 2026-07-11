package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.engine.TrajectoryDiff;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The M2 exit demo as an automated test (spec §11): record an execution on the durable Gumbo log,
 * rerun it with a different model from step N, and confirm the diff reports exactly the changed step
 * while the prefix is identical.
 */
class M2BranchAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    private static final Task<String> TWO_STEP = ctx -> {
        String s1 = ctx.model().complete(STEP1).message();
        String s2 = ctx.model().complete(STEP2).message();
        return s1 + "|" + s2;
    };

    private static MockModel model(String finalText) {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream()
                    .filter(m -> m.role() == Role.USER)
                    .reduce((a, b) -> b).map(com.cajunsystems.catalyst.model.Message::content).orElse("");
            return Completion.ofText(last.contains("summarize") ? "SUMMARY" : finalText);
        }).build();
    }

    @Test
    void rerunWithADifferentModelFromStepNAndDiff(@TempDir Path dir) {
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model("FINAL"))
                .build()) {

            ExecutionId id = runtime.execute(TWO_STEP, ExecutionOptions.withKey("doc:1")).id();
            String parentResult = runtime.execute(TWO_STEP, ExecutionOptions.withKey("doc:1")).result();
            assertThat(parentResult).isEqualTo("SUMMARY|FINAL");

            ExecutionState parent = runtime.inspect(id);
            long step1Seq = parent.trajectory().stream()
                    .filter(s -> s.kind() == TimelineStep.Kind.MODEL)
                    .findFirst().orElseThrow().seq();

            MockModel modelB = model("FINAL-B");
            Trajectory fork = runtime.branch(id, step1Seq).withModel(modelB).run(TWO_STEP);

            assertThat(fork.status()).isEqualTo(Status.COMPLETED);
            assertThat(modelB.callCount()).isEqualTo(1); // step 1 substituted; only step 2 ran live

            TrajectoryDiff diff = Trajectory.diff(Trajectory.of(parent), fork);
            assertThat(diff.hasChanges()).isTrue();
            assertThat(diff.changedCount()).isEqualTo(1);

            // The fork is a durable child execution beginning with ExecutionBranched.
            assertThat(runtime.log().read(fork.id()).get(0).event())
                    .isInstanceOf(CatalystEvent.ExecutionBranched.class);
        }
    }
}
