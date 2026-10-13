package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
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
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Durable timers: {@code ctx.sleep(d)} records when to wake, ends the attempt holding no thread, and
 * the execution waits as WAITING until the timer comes due. It is then resumed, in this process or
 * after a restart, and only what was left is waited.
 */
@Timeout(30)
class TimerTest {

    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    /** A follow-up: step 1, sleep, step 2. A named class so a runtime that did not start it can resume it. */
    static final class FollowUp implements Task<String> {
        static volatile Duration sleep = Duration.ofHours(1);
        @Override public String execute(Context ctx) {
            String one = ctx.model().complete(STEP1).message();
            ctx.sleep(sleep);
            return one + "|" + ctx.model().complete(STEP2).message();
        }
    }

    private static CatalystRuntime runtime(EventLog log, MockModel model, java.time.Clock clock) {
        return CatalystRuntime.builder().log(log).model(model).clock(clock).task(FollowUp.class, FollowUp::new).build();
    }

    @Test
    void aSleepParksWithNoThreadHeldAndWakesInProcess() {
        FollowUp.sleep = Duration.ofMillis(300);
        MockModel model = MockModel.alwaysReturn("A");
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), model, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new FollowUp(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5)))
                    .isInstanceOfSatisfying(ExecutionWaitingSignal.class, w -> assertThat(w.wakeAt()).isNotNull());
            ExecutionState sleeping = runtime.inspect(run.id());
            assertThat(sleeping.status()).isEqualTo(Status.WAITING);
            assertThat(sleeping.awaitingTimer()).isPresent();
            assertThat(sleeping.awaitingSignal()).as("a timer wait is not a signal wait").isEmpty();
            assertThat(model.callCount()).isEqualTo(1);

            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("A|A");
            assertThat(model.callCount()).as("step 1 substituted on wake; step 2 once").isEqualTo(2);
            assertThat(runtime.replay(run.id(), new FollowUp()).status()).as("and it replays exactly").isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(2);
        }
    }

    /** A process that dies while an execution sleeps: the next one finds it, waits out the rest, and finishes it. */
    @Test
    void aTimerSurvivesARestartAndFiresFromRecovery() {
        FollowUp.sleep = Duration.ofHours(1);
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        MockModel first = MockModel.alwaysReturn("A");
        ExecutionId id;
        try (CatalystRuntime dying = runtime(log, first, clock)) {
            var run = dying.execute(new FollowUp(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            id = run.id();
        }
        Instant wakeAt = clock.instant().plus(Duration.ofHours(1));

        MockModel second = MockModel.alwaysReturn("B");
        try (CatalystRuntime next = runtime(log, second, clock)) {
            clock.advance(Duration.ofMinutes(40));
            RecoveryReport early = next.recoverAll();
            assertThat(early.leftAlone()).containsEntry(id, Status.WAITING);
            assertThat(next.inspect(id).awaitingTimer()).as("the wake time is fixed; only the rest is waited").contains(wakeAt);

            clock.advance(Duration.ofMinutes(20));
            RecoveryReport due = next.recoverAll();
            assertThat(due.resumed()).containsOnlyKeys(id);
            assertThat(due.resumed().get(id).result()).isEqualTo("A|B");
        }
        assertThat(first.callCount()).isEqualTo(1);
        assertThat(second.callCount()).as("only step 2 after the wake").isEqualTo(1);
    }

    /**
     * Paused while sleeping, then resumed before the timer is due: the attempt replays to the sleep and
     * parks again. It records the wait again, with the same wake time, so the execution reads WAITING,
     * not RUNNING, and recovery does not re-resume it early.
     */
    @Test
    void aResumeBeforeTheTimerIsDueParksAgainAndStillReadsWaiting() {
        FollowUp.sleep = Duration.ofHours(1);
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        MockModel model = MockModel.alwaysReturn("A");
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), model, clock)) {
            var run = runtime.execute(new FollowUp(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            Instant wakeAt = runtime.inspect(run.id()).awaitingTimer().orElseThrow();

            runtime.pause(run.id());
            clock.advance(Duration.ofMinutes(30));
            assertThatThrownBy(() -> runtime.resume(run.id()).result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);

            ExecutionState again = runtime.inspect(run.id());
            assertThat(again.status()).isEqualTo(Status.WAITING);
            assertThat(again.awaitingTimer()).contains(wakeAt);
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    /** A wake only ever resumes a sleeper whose timer is due: never one that was cancelled or paused. */
    @Test
    void aCancelledOrPausedSleeperIsNotWoken() {
        FollowUp.sleep = Duration.ofHours(1);
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        MockModel model = MockModel.alwaysReturn("A");
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), model, clock)) {
            var cancelled = runtime.execute(new FollowUp(), ExecutionOptions.none());
            var paused = runtime.execute(new FollowUp(), ExecutionOptions.none());
            for (var h : java.util.List.of(cancelled, paused)) {
                assertThatThrownBy(() -> h.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            }
            runtime.cancel(cancelled.id());
            runtime.pause(paused.id());

            clock.advance(Duration.ofHours(2));
            runtime.recoverAll();
            assertThat(runtime.inspect(cancelled.id()).status()).isEqualTo(Status.CANCELLED);
            assertThat(runtime.inspect(paused.id()).status()).isEqualTo(Status.PAUSED);
            assertThat(model.callCount()).as("neither ran step 2").isEqualTo(2);
        }
    }

    @Test
    void aTimerWaitRefusesSignals() {
        FollowUp.sleep = Duration.ofHours(1);
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), MockModel.alwaysReturn("A"), new LeaseTest.TestClock())) {
            var run = runtime.execute(new FollowUp(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            assertThatThrownBy(() -> runtime.signal(run.id(), "approve", "x"))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not waiting for signal 'approve'");
        }
    }

    /** Waited for a signal, got it, now sleeps: that earlier signal must not be accepted again. */
    static final class ApproveThenSleep implements Task<String> {
        @Override public String execute(Context ctx) {
            String who = ctx.awaitSignal("approve", String.class);
            ctx.sleep(Duration.ofHours(1));
            return who;
        }
    }

    @Test
    void anEarlierSignalWaitIsNotMistakenForTheCurrentSleep() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(MockModel.alwaysReturn("A"))
                .clock(new LeaseTest.TestClock()).task(new ApproveThenSleep()).build()) {
            var run = runtime.execute(new ApproveThenSleep(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            assertThatThrownBy(() -> runtime.signal(run.id(), "approve", "ops").result(Duration.ofSeconds(5)))
                    .as("now sleeping").isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(runtime.inspect(run.id()).awaitingSignal()).isEmpty();
            assertThatThrownBy(() -> runtime.signal(run.id(), "approve", "again")).isInstanceOf(IllegalStateException.class);
        }
    }

    /** A zero sleep is already due: recorded, fired, and the task runs straight through. */
    @Test
    void aZeroSleepDoesNotPark() {
        FollowUp.sleep = Duration.ZERO;
        MockModel model = MockModel.alwaysReturn("A");
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), model, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new FollowUp(), ExecutionOptions.none());
            assertThat(run.result()).isEqualTo("A|A");
            assertThat(runtime.log().read(run.id())).extracting(se -> se.event().getClass().getSimpleName())
                    .contains("TimerStarted", "TimerFired");
        }
    }

    /** Distributed: a timer started on one node comes due on another node's claim loop. */
    @Test
    void anotherNodesClaimLoopWakesADueTimer() {
        FollowUp.sleep = Duration.ofHours(1);
        SharedLog log = SharedLog.distributable();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        MockModel modelA = MockModel.alwaysReturn("A");
        ExecutionId id;
        try (CatalystRuntime nodeA = CatalystRuntime.builder().log(log).model(modelA).clock(clock)
                .nodeId("node-a").distributed().task(FollowUp.class, FollowUp::new).build()) {
            var run = nodeA.execute(new FollowUp(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            id = run.id();
            assertThat(log.readLease(id)).as("a sleeping execution holds no lease").isEmpty();
        }
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = CatalystRuntime.builder().log(log).model(modelB).clock(clock)
                .nodeId("node-b").distributed().claimInterval(Duration.ofMillis(50))
                .task(FollowUp.class, FollowUp::new).build()) {
            clock.advance(Duration.ofHours(1));
            ExecutionId sleeper = id;
            await(() -> nodeB.inspect(sleeper).status() == Status.COMPLETED);
            assertThat(modelB.callCount()).isEqualTo(1);
        }
    }

    @Test
    void aContextWithoutDurableSleepRefusesIt() {
        Context bare = new Context() {
            @Override public com.cajunsystems.catalyst.model.Model model() { return null; }
            @Override public <I, O> O call(com.cajunsystems.catalyst.Tool<I, O> tool, I input) { return null; }
            @Override public <T> T effect(String label, java.util.function.Supplier<T> supplier) { return null; }
            @Override public com.cajunsystems.catalyst.Memory memory() { return null; }
            @Override public com.cajunsystems.catalyst.ExecutionInfo info() { return null; }
            @Override public <T> T var(String name) { return null; }
            @Override public org.slf4j.Logger log() { return null; }
        };
        assertThatThrownBy(() -> bare.sleep(Duration.ofSeconds(1))).isInstanceOf(UnsupportedOperationException.class);
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within 10s");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
