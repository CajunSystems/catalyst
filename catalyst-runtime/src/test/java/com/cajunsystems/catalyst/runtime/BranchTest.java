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
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.mock.MockModel;
import com.fasterxml.jackson.databind.node.NullNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void forkClearsInDoubtStateSoLaterToolsRunLive() {
        // A log that crashed mid-tool: it ends with a dangling ToolRequested (in-doubt).
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, "T", "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "n"));
        log.append(id, new CatalystEvent.CompletionRequested(t, "HASH_OF_SAY_A"));
        log.append(id, new CatalystEvent.CompletionReceived(t, NullNode.getInstance(), 0, 0, 0, 0.0, "stop"));
        log.append(id, new CatalystEvent.ToolRequested(t, "oldtool", NullNode.getInstance())); // dangling

        // Under BRANCH, the task's divergent prompt forks; the later tool must run live — not be
        // mis-routed into in-doubt handling for the stale dangling tool.
        Task<String> task = ctx -> {
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("say B").build())); // diverges → fork
            return ctx.call(LOOKUP, new Query("x"));
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("live"))
                .replayMode(ReplayMode.BRANCH).inDoubtPolicy(InDoubtPolicy.FAIL).build()) {

            ExecutionHandle<String> handle = runtime.execute(task, ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("ACTIVE"); // LOOKUP ran live after the fork
        }
    }

    @Test
    void branchLeavesTheChildPausedNotFailedOnInDoubtAsk() {
        // Records every append so we can inspect the child's final event.
        InMemoryEventLog delegate = new InMemoryEventLog();
        List<Map.Entry<ExecutionId, CatalystEvent>> appends = new ArrayList<>();
        EventLog log = new EventLog() {
            public long append(ExecutionId id, CatalystEvent event) {
                appends.add(Map.entry(id, event));
                return delegate.append(id, event);
            }
            public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
            public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
            public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
            public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
            public void close() { delegate.close(); }
        };

        // Parent crashed mid-tool: its log ends with a dangling ToolRequested for "lookup".
        ExecutionId parentId = ExecutionId.random();
        java.time.Instant t = java.time.Instant.now();
        log.append(parentId, new CatalystEvent.ExecutionCreated(t, "T", "h", "cfg", ""));
        log.append(parentId, new CatalystEvent.ExecutionStarted(t, 1, "n"));
        log.append(parentId, new CatalystEvent.ToolRequested(t, "lookup", NullNode.getInstance()));

        Task<String> task = ctx -> ctx.call(LOOKUP, new Query("x"));
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("m")).inDoubtPolicy(InDoubtPolicy.ASK).build()) {

            assertThatThrownBy(() -> runtime.branch(parentId, Long.MAX_VALUE).run(task))
                    .isInstanceOf(ExecutionPausedSignal.class);

            ExecutionId childId = appends.stream().map(Map.Entry::getKey)
                    .filter(x -> !x.equals(parentId)).findFirst().orElseThrow();
            List<CatalystEvent> childEvents = appends.stream()
                    .filter(e -> e.getKey().equals(childId)).map(Map.Entry::getValue).toList();
            // The child must end PAUSED — no spurious ExecutionFailed appended after ExecutionPaused.
            assertThat(childEvents.get(childEvents.size() - 1))
                    .isInstanceOf(CatalystEvent.ExecutionPaused.class);
        }
    }

    /** A side-effecting tool that counts its real invocations. */
    static final class SendTool implements Tool<String, String> {
        final java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger();
        @Override public String name() { return "send"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String to) { sent.incrementAndGet(); return "sent to " + to; }
    }

    /** Model call first, then a tool whose input depends on the model's answer: a branch forks before the tool. */
    private static Task<String> notify(SendTool send) {
        return ctx -> {
            String who = ctx.model().complete(STEP1).message();
            return ctx.call(send, who);
        };
    }

    @Test
    void pastTheForkALiveBranchRunsToolsForRealByDefault() {
        SendTool send = new SendTool();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(MockModel.alwaysReturn("ops")).build()) {
            var parent = runtime.execute(notify(send), com.cajunsystems.catalyst.ExecutionOptions.none());
            assertThat(parent.result()).isEqualTo("sent to ops");
            runtime.branch(parent.id(), 1).withModel(MockModel.alwaysReturn("ops")).run(notify(send));
            assertThat(send.sent).as("unchanged M2 behaviour: the fork is live").hasValue(2);
        }
    }

    @Test
    void aRecordedOnlyBranchAnswersRepeatedToolCallsFromTheParentAndRefusesNewOnes() {
        SendTool send = new SendTool();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(MockModel.alwaysReturn("ops")).build()) {
            var parent = runtime.execute(notify(send), com.cajunsystems.catalyst.ExecutionOptions.none());
            parent.result();

            // Same model answer → the identical tool call → the parent's recorded output, nothing sent.
            var same = runtime.branch(parent.id(), 1).withModel(MockModel.alwaysReturn("ops"))
                    .toolReplay(com.cajunsystems.catalyst.engine.BranchSpec.ToolReplay.RECORDED_ONLY).run(notify(send));
            assertThat(same.status()).isEqualTo(com.cajunsystems.catalyst.Status.COMPLETED);
            assertThat(send.sent).hasValue(1);
            assertThat(runtime.inspect(same.id()).trajectory()).extracting(s -> s.kind())
                    .as("the substituted call is still a recorded step of the branch")
                    .contains(com.cajunsystems.catalyst.engine.TimelineStep.Kind.TOOL);

            // A different answer → a call the parent never made → refused, nothing sent.
            assertThatThrownBy(() -> runtime.branch(parent.id(), 1).withModel(MockModel.alwaysReturn("everyone"))
                    .toolReplay(com.cajunsystems.catalyst.engine.BranchSpec.ToolReplay.RECORDED_ONLY).run(notify(send)))
                    .isInstanceOf(com.cajunsystems.catalyst.engine.UnrecordedToolCallException.class)
                    .hasMessageContaining("send");
            assertThat(send.sent).hasValue(1);

            // RECORDED_OR_LIVE runs the new call for real.
            runtime.branch(parent.id(), 1).withModel(MockModel.alwaysReturn("everyone"))
                    .toolReplay(com.cajunsystems.catalyst.engine.BranchSpec.ToolReplay.RECORDED_OR_LIVE).run(notify(send));
            assertThat(send.sent).hasValue(2);
        }
    }

    /** Only calls past the fork are in the table; repeated identical ones are answered in recorded order. */
    @Test
    void recordedAnswersArePastTheForkAndConsumedInOrder() {
        java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        Tool<String, Integer> counter = new Tool<>() {
            @Override public String name() { return "counter"; }
            @Override public Class<String> inputType() { return String.class; }
            @Override public Integer apply(String in) { return reads.incrementAndGet(); }
        };
        Task<String> task = ctx -> {
            int before = ctx.call(counter, "x");                       // before the fork: replayed
            String m = ctx.model().complete(STEP1).message();
            return before + "," + ctx.call(counter, "x") + "," + ctx.call(counter, "x") + "," + m;
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(MockModel.alwaysReturn("m")).build()) {
            var parent = runtime.execute(task, com.cajunsystems.catalyst.ExecutionOptions.none());
            assertThat(parent.result()).isEqualTo("1,2,3,m");
            long fork = runtime.log().read(parent.id()).stream()
                    .filter(e -> e.event() instanceof com.cajunsystems.catalyst.events.CatalystEvent.PromptBuilt)
                    .findFirst().orElseThrow().seq() - 1;
            var child = runtime.branch(parent.id(), fork).withModel(MockModel.alwaysReturn("m"))
                    .toolReplay(com.cajunsystems.catalyst.engine.BranchSpec.ToolReplay.RECORDED_ONLY).run(task);
            assertThat(runtime.inspect(child.id()).result().toString()).contains("1,2,3,m");
            assertThat(reads).as("nothing re-run").hasValue(3);
        }
    }
}
