package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.Usage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Signals: a task parks on {@code ctx.awaitSignal}, holding no thread, until {@code runtime.signal}
 * delivers a payload — the human-in-the-loop boundary. The payload is recorded like any boundary
 * result, so everything before the wait is substituted when the task resumes.
 */
class SignalTest {

    record Approval(String reviewer, boolean approved) {}

    private static final CompletionRequest DRAFT =
            CompletionRequest.of(Prompt.builder().user("summarize the incident").build());
    private static final CompletionRequest FINAL =
            CompletionRequest.of(Prompt.builder().user("write the final report").build());

    /** Draft with the model, wait for a reviewer, then finish — or stop if rejected. */
    static final class ReviewedReport implements Task<String> {
        @Override public String execute(Context ctx) {
            String draft = ctx.model().complete(DRAFT).message();
            Approval approval = ctx.awaitSignal("approval", Approval.class);
            if (!approval.approved()) return "rejected by " + approval.reviewer();
            return draft + "|" + ctx.model().complete(FINAL).message() + " (approved by " + approval.reviewer() + ")";
        }
    }

    private static MockModel model() {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream().filter(m -> m.role() == Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            return new Completion(last.contains("summarize") ? "DRAFT" : "FINAL", List.of(), new Usage(1, 1), "stop");
        }).build();
    }

    private static CatalystRuntime runtime(InMemoryEventLog log, MockModel model) {
        return CatalystRuntime.builder().log(log).model(model).task(new ReviewedReport()).build();
    }

    @Test
    void aTaskParksOnItsSignalAndASignalResumesItWithThePrefixSubstituted() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = runtime(log, model)) {
            ExecutionHandle<String> handle = runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-7"));
            assertThatThrownBy(handle::result)
                    .isInstanceOfSatisfying(ExecutionWaitingSignal.class, w -> assertThat(w.signalName()).isEqualTo("approval"));
            ExecutionId id = handle.id();
            ExecutionState waiting = runtime.inspect(id);
            assertThat(waiting.status()).isEqualTo(Status.WAITING);
            assertThat(waiting.awaitingSignal()).contains("approval");
            assertThat(model.callCount()).isEqualTo(1);

            Object result = runtime.signal(id, "approval", new Approval("dana", true)).result();

            assertThat(result).isEqualTo("DRAFT|FINAL (approved by dana)");
            assertThat(model.callCount()).as("the draft was substituted; only the final call ran").isEqualTo(2);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(runtime.inspect(id).awaitingSignal()).isEmpty();
        }
    }

    @Test
    void theSignalsPayloadDrivesTheTask() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = runtime(log, model)) {
            ExecutionId id = parked(runtime);
            assertThat(runtime.signal(id, "approval", new Approval("lee", false)).result()).isEqualTo("rejected by lee");
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    /** Only a waiting execution, waiting for exactly that signal, accepts it — and a refusal writes nothing. */
    @Test
    void aSignalIsRefusedUnlessTheExecutionIsWaitingForIt() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = runtime(log, model())) {
            ExecutionId id = parked(runtime);
            int before = log.read(id).size();
            assertThatThrownBy(() -> runtime.signal(id, "go-live", "x"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("waiting for signal 'approval'");
            assertThat(log.read(id)).hasSize(before);

            runtime.signal(id, "approval", new Approval("dana", true)).result();
            assertThatThrownBy(() -> runtime.signal(id, "approval", new Approval("dana", true)))
                    .as("already delivered: the execution is no longer waiting")
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("COMPLETED");
            assertThatThrownBy(() -> runtime.signal(ExecutionId.random(), "approval", "x"))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /**
     * Re-submitting or resuming a still-waiting execution runs no attempt: it would only replay to the
     * wait and park again, and its ExecutionResumed would fold the status to RUNNING — at which point
     * startup recovery would treat a waiting execution as crashed.
     */
    @Test
    void resumingOrReSubmittingAWaitingExecutionRunsNothing() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = runtime(log, model)) {
            ExecutionId id = parked(runtime);
            int before = log.read(id).size();

            assertThatThrownBy(() -> runtime.resume(id).result()).isInstanceOf(ExecutionWaitingSignal.class);
            assertThatThrownBy(() -> runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-7")).result())
                    .isInstanceOf(ExecutionWaitingSignal.class);

            assertThat(log.read(id)).as("no events appended").hasSize(before);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.WAITING);
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    /**
     * The crash this design is shaped around: the signal is recorded, and the process dies before the
     * resume. SignalReceived folds the execution to RESUMING, which startup recovery resumes — instead
     * of leaving a WAITING execution with a delivered signal nothing would ever consume.
     */
    @Test
    void aSignalDeliveredJustBeforeACrashIsFinishedByStartupRecovery() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        ExecutionId id;
        try (CatalystRuntime runtime = runtime(log, model)) {
            id = parked(runtime);
        }
        // signal() = append SignalReceived, then resume. Crash between the two:
        log.append(id, new CatalystEvent.SignalReceived(Instant.now(), "approval",
                new PayloadCodec().toTree(new Approval("dana", true))));
        assertThat(com.cajunsystems.catalyst.engine.Reducer.fold(id, log.read(id)).status()).isEqualTo(Status.RESUMING);

        try (CatalystRuntime restarted = CatalystRuntime.builder().log(log).model(model)
                .task(new ReviewedReport()).recoverOnStart(true).build()) {
            RecoveryReport report = restarted.startupRecovery().orElseThrow();
            assertThat(report.resumed()).containsOnlyKeys(id);
            assertThat(report.resumed().get(id).result()).isEqualTo("DRAFT|FINAL (approved by dana)");
            assertThat(model.callCount()).isEqualTo(2);
        }
    }

    @Test
    void startupRecoveryLeavesAWaitingExecutionWaiting() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        ExecutionId id;
        try (CatalystRuntime runtime = runtime(log, model)) {
            id = parked(runtime);
        }
        try (CatalystRuntime restarted = CatalystRuntime.builder().log(log).model(model)
                .task(new ReviewedReport()).recoverOnStart(true).build()) {
            RecoveryReport report = restarted.startupRecovery().orElseThrow();
            assertThat(report.resumed()).isEmpty();
            assertThat(report.leftAlone()).containsEntry(id, Status.WAITING);
            assertThat(model.callCount()).isEqualTo(1);
            // ...and it still accepts its signal after the restart.
            assertThat(restarted.signal(id, "approval", new Approval("dana", true)).result())
                    .isEqualTo("DRAFT|FINAL (approved by dana)");
        }
    }

    /** A strict replay substitutes the signal like any boundary, and a replay of a waiting execution ends at the wait. */
    @Test
    void replaySubstitutesTheSignalAndEndsCleanlyAtAnUnansweredWait() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = runtime(log, model)) {
            ExecutionId waiting = parked(runtime);
            assertThat(runtime.replay(waiting, new ReviewedReport()).status()).isEqualTo(Status.WAITING);

            runtime.signal(waiting, "approval", new Approval("dana", true)).result();
            int calls = model.callCount();
            assertThat(runtime.replay(waiting, new ReviewedReport()).status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).as("zero external calls on replay").isEqualTo(calls);
        }
    }

    /**
     * The swallowed-signal rule applies to waiting too: a task that catches the waiting signal and
     * returns must not be recorded as completed with a result that skipped its wait.
     */
    @Test
    void aSwallowedWaitIsStillAWait() {
        InMemoryEventLog log = new InMemoryEventLog();
        Task<String> swallowing = ctx -> {
            try {
                return ctx.awaitSignal("approval", String.class);
            } catch (RuntimeException e) {
                return "skipped the wait";
            }
        };
        try (CatalystRuntime runtime = runtime(log, model())) {
            ExecutionHandle<String> handle = runtime.execute(swallowing);
            assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.WAITING);
        }
    }

    /**
     * Pausing a waiter, then resuming it, must leave it waiting — not RUNNING. The resume's attempt
     * folds the status to RUNNING before the task re-parks, so the re-park re-records the wait;
     * otherwise signal() would refuse it forever and every restart would re-resume it.
     */
    @Test
    void aPausedThenResumedWaiterIsStillWaitingAndStillAcceptsItsSignal() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = runtime(log, model)) {
            ExecutionId id = parked(runtime);
            runtime.pause(id);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.PAUSED);

            assertThatThrownBy(() -> runtime.resume(id).result()).isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.WAITING);
            assertThat(runtime.inspect(id).awaitingSignal()).contains("approval");

            assertThat(runtime.signal(id, "approval", new Approval("dana", true)).result())
                    .isEqualTo("DRAFT|FINAL (approved by dana)");
            assertThat(model.callCount()).isEqualTo(2);
        }
        try (CatalystRuntime restarted = CatalystRuntime.builder().log(log).model(model)
                .task(new ReviewedReport()).recoverOnStart(true).build()) {
            assertThat(restarted.startupRecovery().orElseThrow().resumed()).as("nothing to re-resume").isEmpty();
        }
    }

    /**
     * The documented flow — result() reports the wait, then signal() at once — repeated back to back.
     * signal() treats a completed attempt as not in flight, because the in-flight entry is cleared by a
     * whenComplete that runs just after the handle completes. That interleaving is too narrow to hit
     * reliably here (this test passes without the fix too), so read it as a check of the flow, not a
     * regression test for the race.
     */
    @Test
    void signallingImmediatelyAfterTheWaitIsReportedIsNeverRefused() {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = runtime(log, model())) {
            for (int i = 0; i < 200; i++) {
                ExecutionHandle<String> handle = runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("k-" + i));
                assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);
                assertThat(runtime.signal(handle.id(), "approval", new Approval("dana", true)).result())
                        .isEqualTo("DRAFT|FINAL (approved by dana)");
            }
        }
    }

    /**
     * A task that swallows the wait and then calls the model must record nothing after the wait — or
     * the delivered signal could never be substituted where the task waits, and the resume would
     * diverge. The fallback's call re-raises the wait instead of running.
     */
    @Test
    void aSwallowedWaitRecordsNothingAfterItAndStillResumes() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        Task<String> fallback = new SwallowingFallback();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .task(SwallowingFallback.class, SwallowingFallback::new).build()) {
            ExecutionHandle<String> handle = runtime.execute(fallback);
            assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(log.read(handle.id()).getLast().event()).isInstanceOf(CatalystEvent.SignalAwaited.class);
            assertThat(model.callCount()).as("the fallback's model call never ran").isZero();

            assertThat(runtime.signal(handle.id(), "approval", new Approval("dana", true)).result())
                    .isEqualTo("approved by dana");
        }
    }

    /** Catches the wait and tries to carry on with the model. */
    static final class SwallowingFallback implements Task<String> {
        @Override public String execute(Context ctx) {
            try {
                return "approved by " + ctx.awaitSignal("approval", Approval.class).reviewer();
            } catch (RuntimeException e) {
                return ctx.model().complete(DRAFT).message();
            }
        }
    }

    @Test
    void aWaitingExecutionCanBeCancelled() {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = runtime(log, model())) {
            ExecutionId id = parked(runtime);
            runtime.cancel(id);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.CANCELLED);
            assertThat(log.openExecutions()).doesNotContain(id);
        }
    }

    /** Without the task type registered the signal is still recorded; the key resumes it. */
    @Test
    void anUnregisteredTaskIsSignalledThenResumedByItsKey() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = model();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build()) {
            ExecutionHandle<String> handle = runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-9"));
            assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);

            assertThatThrownBy(() -> runtime.signal(handle.id(), "approval", new Approval("dana", true)).result())
                    .hasMessageContaining("no task is registered");
            assertThat(runtime.inspect(handle.id()).status()).as("the signal was recorded").isEqualTo(Status.RESUMING);

            assertThat(runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-9")).result())
                    .isEqualTo("DRAFT|FINAL (approved by dana)");
        }
    }

    private static ExecutionId parked(CatalystRuntime runtime) {
        ExecutionHandle<String> handle = runtime.execute(new ReviewedReport(), ExecutionOptions.withKey("incident-7"));
        assertThatThrownBy(handle::result).isInstanceOf(ExecutionWaitingSignal.class);
        return handle.id();
    }
}
