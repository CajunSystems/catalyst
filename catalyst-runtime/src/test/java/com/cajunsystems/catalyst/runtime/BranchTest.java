package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.engine.TrajectoryDiff;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BranchTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("finalize the answer").build());

    private static final Task<String> TWO_STEP = ctx -> {
        String s1 = ctx.model().complete(STEP1).message();
        String s2 = ctx.model().complete(STEP2).message();
        return s1 + "|" + s2;
    };

    private static String lastUser(CompletionRequest req) {
        return req.prompt().messages().stream()
                .filter(m -> m.role() == Role.USER)
                .reduce((a, b) -> b).map(com.cajunsystems.catalyst.model.Message::content).orElse("");
    }

    /** A content-based model: "summarize" → SUMMARY, otherwise the given final text. */
    private static MockModel twoStepModel(String finalText) {
        return MockModel.builder().respond(req ->
                Completion.ofText(lastUser(req).contains("summarize") ? "SUMMARY" : finalText)).build();
    }

    @Test
    void branchWithModelSwapDiffersOnlyAfterTheBranchPoint() {
        MockModel modelA = twoStepModel("FINAL");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(modelA).build()) {

            ExecutionHandle<String> handle = runtime.execute(TWO_STEP, ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("SUMMARY|FINAL");
            ExecutionId id = handle.id();

            ExecutionState parent = runtime.inspect(id);
            long step1Seq = parent.trajectory().stream()
                    .filter(s -> s.kind() == TimelineStep.Kind.MODEL)
                    .findFirst().orElseThrow().seq();

            // Branch after step 1 with a different model; step 2 runs live under the new model.
            MockModel modelB = twoStepModel("FINAL-B");
            Trajectory fork = runtime.branch(id, step1Seq).withModel(modelB).run(TWO_STEP);

            assertThat(fork.status()).isEqualTo(Status.COMPLETED);
            assertThat(modelB.callCount()).isEqualTo(1); // step 1 substituted; only step 2 ran live

            TrajectoryDiff diff = Trajectory.diff(Trajectory.of(parent), fork);
            assertThat(diff.hasChanges()).isTrue();
            assertThat(diff.changedCount()).isEqualTo(1); // exactly the second model call changed

            // The fork is a real child execution whose log begins with ExecutionBranched.
            assertThat(runtime.log().read(fork.id()).get(0).event())
                    .isInstanceOf(CatalystEvent.ExecutionBranched.class);
        }
    }

    // A tool whose result feeds a later prompt, so a counterfactual propagates forward.
    record Query(String key) {}

    private static final Tool<Query, String> LOOKUP = new Tool<>() {
        public String name() { return "lookup"; }
        public Class<Query> inputType() { return Query.class; }
        public String apply(Query q) { return "ACTIVE"; }
    };

    private static final Task<String> LOOKUP_THEN_MODEL = ctx -> {
        String status = ctx.call(LOOKUP, new Query("user"));
        return ctx.model().complete(
                CompletionRequest.of(Prompt.builder().user("status is " + status).build())).message();
    };

    @Test
    void counterfactualToolResultPropagatesThroughALiveModelCall() {
        MockModel model = MockModel.builder().respond(req ->
                Completion.ofText("OK-" + (lastUser(req).contains("ACTIVE") ? "ACTIVE" : "LAPSED"))).build();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(LOOKUP_THEN_MODEL, ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("OK-ACTIVE");
            ExecutionId id = handle.id();

            // Swap the recorded tool result; the changed data flow diverges the later model prompt,
            // which under BRANCH forks to a live model call rather than throwing.
            Trajectory fork = runtime.branch(id, Long.MAX_VALUE)
                    .withRecordedToolResult("lookup", "LAPSED")
                    .run(LOOKUP_THEN_MODEL);

            ExecutionState child = runtime.inspect(fork.id());
            assertThat(child.status()).isEqualTo(Status.COMPLETED);
            assertThat(child.result().toString()).contains("LAPSED");
            assertThat(model.callCount()).isEqualTo(2); // 1 original + 1 live call in the branch
            assertThat(runtime.log().read(fork.id()).get(0).event())
                    .isInstanceOf(CatalystEvent.ExecutionBranched.class);
        }
    }
}
