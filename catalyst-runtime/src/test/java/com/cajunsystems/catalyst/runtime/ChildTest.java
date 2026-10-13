package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ChildExecutionFailedException;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Child executions: {@code ctx.child(task)} runs a task as its own durable execution and returns its
 * result. The parent waits with no thread held, is resumed when the child finishes, and records the
 * outcome itself. Each run is a separate stream that resumes, replays and evaluates on its own.
 */
@Timeout(30)
class ChildTest {

    private static final CompletionRequest PLAN = CompletionRequest.of(Prompt.builder().user("plan").build());
    private static final CompletionRequest RESEARCH = CompletionRequest.of(Prompt.builder().user("research").build());

    /** The sub-task: one model call, optionally after a sleep, optionally failing. */
    static final class Research implements Task<String> {
        static volatile Duration sleep = Duration.ZERO;
        static volatile boolean fail = false;
        static volatile java.util.concurrent.CountDownLatch gate = null;
        @Override public String execute(Context ctx) {
            if (!sleep.isZero()) ctx.sleep(sleep);
            java.util.concurrent.CountDownLatch g = gate;
            if (g != null) {
                try {
                    g.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (fail) throw new IllegalStateException("source unavailable");
            return "found:" + ctx.model().complete(RESEARCH).message();
        }
    }

    /** The coordinator: plans, delegates to a child, and uses (or recovers from) its outcome. */
    static final class Coordinator implements Task<String> {
        @Override public String execute(Context ctx) {
            String plan = ctx.model().complete(PLAN).message();
            try {
                return plan + "|" + ctx.child(new Research());
            } catch (ChildExecutionFailedException e) {
                return plan + "|recovered from " + e.status() + ": " + e.error();
            }
        }
    }

    /** Lets a child's failure escape, and delegates to the flag (to test a changed delegation). */
    static final class Uncaught implements Task<String> {
        static volatile boolean other = false;
        @Override public String execute(Context ctx) {
            return other ? ctx.child(new OtherResearch()) : ctx.child(new Research());
        }
    }

    static final class OtherResearch implements Task<String> {
        @Override public String execute(Context ctx) {
            return "other";
        }
    }

    private static CatalystRuntime runtime(EventLog log, MockModel model, java.time.Clock clock) {
        return CatalystRuntime.builder().log(log).model(model).clock(clock)
                .task(Coordinator.class, Coordinator::new).task(Research.class, Research::new).build();
    }

    private static ExecutionId childOf(EventLog log, ExecutionId parent) {
        return log.read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildStarted)
                .map(e -> ExecutionId.of(((CatalystEvent.ChildStarted) e).childId())).findFirst().orElseThrow();
    }

    @Test
    void aParentGetsItsChildsResultAndEachRunHasItsOwnStream() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        MockModel model = MockModel.alwaysReturn("M");
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, model, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("M|found:M");

            ExecutionId child = childOf(log, run.id());
            ExecutionState childState = runtime.inspect(child);
            assertThat(childState.status()).isEqualTo(Status.COMPLETED);
            assertThat(log.read(child).get(0).event()).isInstanceOfSatisfying(CatalystEvent.ExecutionCreated.class,
                    c -> assertThat(c.idempotencyKey()).as("linked to its parent").isEqualTo("child:" + run.id().value() + ":child-1"));
            assertThat(log.read(run.id())).extracting(se -> se.event().getClass().getSimpleName())
                    .containsSubsequence("ChildStarted", "ChildCompleted", "ExecutionCompleted");
            assertThat(model.callCount()).isEqualTo(2);

            assertThat(runtime.replay(run.id(), new Coordinator()).status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).as("the parent's replay substitutes the child's result").isEqualTo(2);
        }
    }

    @Test
    void aFailedChildIsARecordedOutcomeTheParentCanHandle() {
        Research.sleep = Duration.ZERO;
        Research.fail = true;
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), MockModel.alwaysReturn("M"), java.time.Clock.systemUTC())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result())
                    .isEqualTo("M|recovered from FAILED: java.lang.IllegalStateException: source unavailable");
            assertThat(runtime.replay(run.id(), new Coordinator()).status()).isEqualTo(Status.COMPLETED);
        } finally {
            Research.fail = false;
        }
    }

    /** While the child runs (here, sleeps), the parent waits as WAITING, holds no thread, and refuses signals. */
    @Test
    void aParentWaitsWithNoThreadHeldWhileItsChildRuns() {
        Research.sleep = Duration.ofHours(1);
        Research.fail = false;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), new LeaseTest.TestClock())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5)))
                    .isInstanceOfSatisfying(ExecutionWaitingSignal.class, w -> assertThat(w.child()).isNotNull());
            ExecutionId child = childOf(log, run.id());
            await(() -> runtime.inspect(child).status() == Status.WAITING);

            ExecutionState parent = runtime.inspect(run.id());
            assertThat(parent.status()).isEqualTo(Status.WAITING);
            assertThat(parent.awaitingChild()).contains(child);
            assertThat(parent.awaitingTimer()).isEmpty();
            assertThat(parent.awaitingSignal()).isEmpty();
            assertThatThrownBy(() -> runtime.signal(run.id(), "x", "y")).isInstanceOf(IllegalStateException.class);
        } finally {
            Research.sleep = Duration.ZERO;
        }
    }

    /**
     * The process dies while the child sleeps. The next one recovers the child when its timer is due,
     * and the child finishing wakes the parent, with nothing repeated.
     */
    @Test
    void afterARestartTheChildFinishesAndWakesItsParent() {
        Research.sleep = Duration.ofHours(1);
        Research.fail = false;
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        MockModel first = MockModel.alwaysReturn("A");
        ExecutionId parentId;
        try (CatalystRuntime dying = runtime(log, first, clock)) {
            var run = dying.execute(new Coordinator(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            parentId = run.id();
            ExecutionId child = childOf(log, parentId);
            await(() -> dying.inspect(child).status() == Status.WAITING);
        }
        MockModel second = MockModel.alwaysReturn("B");
        try (CatalystRuntime next = runtime(log, second, clock)) {
            clock.advance(Duration.ofHours(1));
            next.recoverAll(); // the parent's child link is remembered; the child's due timer resumes it
            ExecutionId p = parentId;
            await(() -> next.inspect(p).status() == Status.COMPLETED);
            assertThat(next.resume(parentId).result()).isEqualTo("A|found:B");
        } finally {
            Research.sleep = Duration.ZERO;
        }
        assertThat(first.callCount()).as("the plan, once").isEqualTo(1);
        assertThat(second.callCount()).as("the research, once").isEqualTo(1);
    }

    /** Paused while waiting, then resumed before the child finishes: it records the wait again and still reads WAITING. */
    @Test
    void aParentResumedBeforeItsChildFinishesParksAgain() {
        Research.sleep = Duration.ofHours(1);
        Research.fail = false;
        EventLog log = EventLogs.inMemory();
        MockModel model = MockModel.alwaysReturn("M");
        try (CatalystRuntime runtime = runtime(log, model, new LeaseTest.TestClock())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            ExecutionId child = childOf(log, run.id());
            runtime.pause(run.id());
            assertThatThrownBy(() -> runtime.resume(run.id()).result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(runtime.inspect(run.id()).status()).isEqualTo(Status.WAITING);
            assertThat(runtime.inspect(run.id()).awaitingChild()).contains(child);
            assertThat(log.read(run.id()).stream().filter(se -> se.event() instanceof CatalystEvent.ExecutionCreated).count())
                    .isEqualTo(1);
            assertThat(log.read(child).stream().filter(se -> se.event() instanceof CatalystEvent.ExecutionCreated).count())
                    .as("the child was created once, not again on the resume").isEqualTo(1);
            assertThat(model.callCount()).isEqualTo(1);
        } finally {
            Research.sleep = Duration.ZERO;
        }
    }

    /** A parent cancelled while its child runs stays cancelled when the child ends (cancelled by the cascade). */
    @Test
    void aCancelledParentIsNotWokenByItsChild() {
        Research.sleep = Duration.ofMillis(300);
        Research.fail = false;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), java.time.Clock.systemUTC())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            runtime.cancel(run.id());
            ExecutionId child = childOf(log, run.id());
            await(() -> runtime.inspect(child).isTerminal());
            sleep(200);
            assertThat(runtime.inspect(run.id()).status()).isEqualTo(Status.CANCELLED);
        } finally {
            Research.sleep = Duration.ZERO;
        }
    }

    /** A branch cannot start a child live: it fails rather than run one. A recorded child result is still substituted before the fork. */
    @Test
    void aBranchThatReachesAChildLiveFails() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), MockModel.alwaysReturn("M"), java.time.Clock.systemUTC())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            long beforePlan = runtime.log().read(run.id()).stream()
                    .filter(e -> e.event() instanceof CatalystEvent.PromptBuilt).findFirst().orElseThrow().seq() - 1;
            assertThatThrownBy(() -> runtime.branch(run.id(), beforePlan).withModel(MockModel.alwaysReturn("other")).run(new Coordinator()))
                    .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("child-1");
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within 10s");
            sleep(20);
        }
    }
    @Test
    void aFailedChildIsNotRetriedThroughItsParent() {
        Research.sleep = Duration.ZERO;
        Research.fail = true;
        Research.gate = null;
        Uncaught.other = false;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M"))
                .retryPolicy(com.cajunsystems.catalyst.engine.RetryPolicy.maxRetries(3, Duration.ofMillis(5)))
                .task(Uncaught.class, Uncaught::new).task(Research.class, Research::new).build()) {
            var run = runtime.execute(new Uncaught(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.FAILED);
            // The recorded failure is substituted into every retry, so a retry could only meet it again.
            assertThat(runtime.inspect(run.id()).retries()).isZero();
        } finally {
            Research.fail = false;
        }
    }

    @Test
    void delegatingToAnotherTaskTypeIsADivergence() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = null;
        Uncaught.other = false;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M"))
                .task(Uncaught.class, Uncaught::new).task(Research.class, Research::new).build()) {
            var run = runtime.execute(new Uncaught(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.replay(run.id(), new Uncaught()).status()).isEqualTo(Status.COMPLETED);
            Uncaught.other = true;
            assertThatThrownBy(() -> runtime.replay(run.id(), new Uncaught()))
                    .isInstanceOf(com.cajunsystems.catalyst.engine.NonDeterministicReplayException.class)
                    .hasMessageContaining(OtherResearch.class.getName());
        } finally {
            Uncaught.other = false;
        }
    }

    @Test
    void aParentWhoseChildRunsOnAnotherNodeWaitsInsteadOfFailing() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = new java.util.concurrent.CountDownLatch(1);
        SharedLog log = SharedLog.distributable();
        java.util.function.Function<String, CatalystRuntime> node = id -> CatalystRuntime.builder().log(log)
                .model(MockModel.alwaysReturn("M")).nodeId(id).distributed()
                .lease(Duration.ofSeconds(30), Duration.ofMillis(50)).claimInterval(Duration.ofMillis(50))
                .task(Coordinator.class, Coordinator::new).task(Research.class, Research::new).build();
        try (CatalystRuntime a = node.apply("node-a"); CatalystRuntime b = node.apply("node-b")) {
            var run = a.execute(new Coordinator(), ExecutionOptions.withKey("lease-held-child"));
            await(() -> a.inspect(run.id()).status() == Status.WAITING);
            ExecutionId child = childOf(log, run.id());
            await(() -> a.inspect(child).status() == Status.RUNNING); // held by node A, blocked on the gate
            // Node B pauses and resumes the parent: its attempt finds the child running under A's lease.
            b.pause(run.id());
            await(() -> b.inspect(run.id()).status() == Status.PAUSED);
            try {
                b.resume(run.id()).result(Duration.ofSeconds(10));
            } catch (ExecutionWaitingSignal parked) {
                // expected: the parent waits for its child again
            }
            assertThat(b.inspect(run.id()).status()).isEqualTo(Status.WAITING);
            Research.gate.countDown();
            await(() -> a.inspect(run.id()).status() == Status.COMPLETED);
        } finally {
            Research.gate.countDown();
            Research.gate = null;
        }
    }
    /** A crash after the parent recorded ChildStarted but before the child existed: recovery creates the child. */
    @Test
    void aCrashBeforeTheChildExistedIsRecovered() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = null;
        Uncaught.other = false;
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, Uncaught.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildStarted(t, "child-1", ExecutionId.random().value(), Research.class.getName()));
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M"))
                .task(Uncaught.class, Uncaught::new).task(Research.class, Research::new).build()) {
            assertThat(runtime.inspect(parent).status()).isEqualTo(Status.WAITING);
            runtime.recoverAll();
            await(() -> runtime.inspect(parent).status() == Status.COMPLETED);
            assertThat(runtime.resume(parent).result()).isEqualTo("found:M");
        }
    }
    /** A parked child cancelled by id: no attempt ends, yet its parent is woken and sees the cancel. */
    @Test
    void cancellingAParkedChildWakesItsParent() {
        Research.sleep = Duration.ofHours(1);
        Research.fail = false;
        Research.gate = null;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), new LeaseTest.TestClock())) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            ExecutionId child = childOf(log, run.id());
            await(() -> runtime.inspect(child).status() == Status.WAITING);
            runtime.cancel(child);
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat((String) runtime.resume(run.id()).result()).startsWith("M|recovered from CANCELLED");
        } finally {
            Research.sleep = Duration.ZERO;
        }
    }
    // ── Cancel cascade ─────────────────────────────────────────────────────────

    /** A middle layer, so a cascade has a grandchild to reach. */
    static final class Middle implements Task<String> {
        @Override public String execute(Context ctx) {
            return "middle:" + ctx.child(new Research());
        }
    }

    static final class Top implements Task<String> {
        @Override public String execute(Context ctx) {
            return "top:" + ctx.child(new Middle());
        }
    }

    private static CatalystRuntime cascadeRuntime(EventLog log, MockModel model) {
        return CatalystRuntime.builder().log(log).model(model).clock(new LeaseTest.TestClock())
                .task(Coordinator.class, Coordinator::new).task(Research.class, Research::new)
                .task(Middle.class, Middle::new).task(Top.class, Top::new).task(Uncaught.class, Uncaught::new).build();
    }

    @Test
    void cancellingAParentCancelsItsRunningChild() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = new java.util.concurrent.CountDownLatch(1);
        EventLog log = EventLogs.inMemory();
        MockModel model = MockModel.alwaysReturn("M");
        try (CatalystRuntime runtime = cascadeRuntime(log, model)) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.WAITING);
            ExecutionId child = childOf(log, run.id());
            await(() -> runtime.inspect(child).status() == Status.RUNNING); // blocked on the gate
            runtime.cancel(run.id());
            await(() -> runtime.inspect(child).status() == Status.CANCELLED);
            assertThat(runtime.inspect(run.id()).status()).isEqualTo(Status.CANCELLED);
            assertThat(model.callCount()).as("the plan only; the child's call never ran").isEqualTo(1);
        } finally {
            Research.gate.countDown();
            Research.gate = null;
        }
    }

    @Test
    void cancellingAParentCancelsEveryGenerationBelowIt() {
        Research.sleep = Duration.ofHours(1);
        Research.fail = false;
        Research.gate = null;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = cascadeRuntime(log, MockModel.alwaysReturn("M"))) {
            var run = runtime.execute(new Top(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.WAITING);
            await(() -> log.read(run.id()).stream().anyMatch(se -> se.event() instanceof CatalystEvent.ChildStarted));
            ExecutionId middle = childOf(log, run.id());
            await(() -> runtime.inspect(middle).status() == Status.WAITING);
            ExecutionId leaf = childOf(log, middle);
            await(() -> runtime.inspect(leaf).status() == Status.WAITING); // asleep
            runtime.cancel(run.id());
            await(() -> runtime.inspect(leaf).status() == Status.CANCELLED);
            assertThat(runtime.inspect(middle).status()).isEqualTo(Status.CANCELLED);
            assertThat(runtime.inspect(run.id()).status()).isEqualTo(Status.CANCELLED);
        } finally {
            Research.sleep = Duration.ZERO;
        }
    }

    /** The process stopped between the parent's cancel and the cascade: recovery cancels the child instead of resuming it. */
    @Test
    void recoveryCancelsAChildWhoseParentWasCancelled() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = null;
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        ExecutionId child = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, Coordinator.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildStarted(t, "child-1", child.value(), Research.class.getName()));
        log.append(child, new CatalystEvent.ExecutionCreated(t, Research.class.getName(), "novars", "cfg",
                "child:" + parent.value() + ":child-1"));
        log.append(child, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ExecutionCancelled(t, "cancelled by request", 2));
        MockModel model = MockModel.alwaysReturn("M");
        try (CatalystRuntime runtime = cascadeRuntime(log, model)) {
            RecoveryReport report = runtime.recoverAll();
            assertThat(report.resumed()).doesNotContainKey(child);
            assertThat(runtime.inspect(child).status()).isEqualTo(Status.CANCELLED);
            assertThat(model.callCount()).isZero();
        }
    }
    /** Swallows the child wait and blocks, so it is still in flight when cancelled. */
    static final class Lingering implements Task<String> {
        static final java.util.concurrent.CountDownLatch blocked = new java.util.concurrent.CountDownLatch(1);
        @Override public String execute(Context ctx) {
            try {
                return ctx.child(new Research());
            } catch (ExecutionWaitingSignal swallowed) {
                blocked.countDown();
                try {
                    Thread.sleep(Duration.ofSeconds(20).toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return ctx.effect("after", () -> "late"); // unwinds here, with the cancel
            }
        }
    }

    /** The parent's own attempt records the cancel (it was in flight): the cascade still runs. */
    @Test
    void aParentCancelledInFlightCancelsItsChild() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = new java.util.concurrent.CountDownLatch(1);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M"))
                .task(Lingering.class, Lingering::new).task(Research.class, Research::new).build()) {
            var run = runtime.execute(new Lingering(), ExecutionOptions.none());
            assertThat(awaitLatch(Lingering.blocked)).isTrue();
            ExecutionId child = childOf(log, run.id());
            await(() -> runtime.inspect(child).status() == Status.RUNNING);
            runtime.cancel(run.id()); // trips the parent's attempt, which records ExecutionCancelled itself
            await(() -> runtime.inspect(run.id()).status() == Status.CANCELLED);
            await(() -> runtime.inspect(child).status() == Status.CANCELLED);
        } finally {
            Research.gate.countDown();
            Research.gate = null;
        }
    }

    private static boolean awaitLatch(java.util.concurrent.CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    /** Caller keys are not namespaced: a key that only looks like a child's never gets an execution cancelled. */
    @Test
    void recoveryDoesNotCancelAnExecutionWhoseKeyOnlyLooksLikeAChilds() {
        Research.sleep = Duration.ZERO;
        Research.fail = false;
        Research.gate = null;
        EventLog log = EventLogs.inMemory();
        ExecutionId cancelled = ExecutionId.random();
        ExecutionId unrelated = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(cancelled, new CatalystEvent.ExecutionCreated(t, Coordinator.class.getName(), "novars", "cfg", ""));
        log.append(cancelled, new CatalystEvent.ExecutionCancelled(t, "cancelled by request", 0));
        log.append(unrelated, new CatalystEvent.ExecutionCreated(t, Research.class.getName(), "novars", "cfg",
                "child:" + cancelled.value() + ":mine"));
        log.append(unrelated, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        try (CatalystRuntime runtime = cascadeRuntime(log, MockModel.alwaysReturn("M"))) {
            runtime.recoverAll();
            await(() -> runtime.inspect(unrelated).status() == Status.COMPLETED);
        }
    }
    /** A child the parent recorded but never created: the cascade leaves its stream empty. */
    @Test
    void cancellingAParentDoesNotWriteAStreamForAChildNeverCreated() {
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        ExecutionId child = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, Coordinator.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildStarted(t, "child-1", child.value(), Research.class.getName()));
        try (CatalystRuntime runtime = cascadeRuntime(log, MockModel.alwaysReturn("M"))) {
            runtime.cancel(parent);
            assertThat(runtime.inspect(parent).status()).isEqualTo(Status.CANCELLED);
            sleep(300); // the cascade runs on the timer thread
            assertThat(log.read(child)).isEmpty();
        }
    }
    /** A durable log decodes a failed child's absent result as a JSON null node: it is still a failure. */
    @Test
    void aRecordedFailureWithANullNodeResultIsStillAFailure() {
        Uncaught.other = false;
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        ExecutionId child = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, Uncaught.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildStarted(t, "child-1", child.value(), Research.class.getName()));
        log.append(parent, new CatalystEvent.ChildCompleted(t, "child-1", child.value(), "FAILED",
                com.fasterxml.jackson.databind.node.NullNode.getInstance(), "boom"));
        try (CatalystRuntime runtime = cascadeRuntime(log, MockModel.alwaysReturn("M"))) {
            runtime.recoverAll();
            await(() -> runtime.inspect(parent).isTerminal());
            assertThat(runtime.inspect(parent).status()).isEqualTo(Status.FAILED);
        }
    }
}
