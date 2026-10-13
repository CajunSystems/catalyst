package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ChildExecutionFailedException;
import com.cajunsystems.catalyst.engine.Supervision;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One-for-one supervision of child executions: {@code ctx.child(task, supervision)} restarts a
 * failed child as a fresh execution, within a restart limit measured on recorded times, after a
 * durable backoff, and escalates the last failure when restarts run out. Every decision is made from
 * the parent's recorded outcomes, so a replay walks the same restarts.
 */
@Timeout(30)
class SupervisionTest {

    private static final CompletionRequest WORK = CompletionRequest.of(Prompt.builder().user("work").build());

    static final AtomicInteger runs = new AtomicInteger();
    static volatile int failFirst = 0;
    static volatile Duration childSleep = Duration.ZERO;
    static volatile Supervision policy = Supervision.escalate();

    /** Fails on its first {@code failFirst} runs (across executions), then does one model call. */
    static final class Flaky implements Task<String> {
        @Override public String execute(Context ctx) {
            if (!childSleep.isZero()) ctx.sleep(childSleep);
            int n = runs.incrementAndGet();
            if (n <= failFirst) throw new IllegalStateException("flake " + n);
            return "ok:" + ctx.model().complete(WORK).message();
        }
    }

    /** Lets an escalated failure end the parent. */
    static final class Parent implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.child(new Flaky(), policy);
        }
    }

    /** Handles an escalated failure. */
    static final class Recovering implements Task<String> {
        @Override public String execute(Context ctx) {
            try {
                return ctx.child(new Flaky(), policy);
            } catch (ChildExecutionFailedException e) {
                return "gave up after " + e.restarts() + ": " + e.status() + " " + e.error();
            }
        }
    }

    @BeforeEach
    void reset() {
        runs.set(0);
        failFirst = 0;
        childSleep = Duration.ZERO;
        policy = Supervision.escalate();
    }

    private static CatalystRuntime runtime(EventLog log, MockModel model, Clock clock) {
        return CatalystRuntime.builder().log(log).model(model).clock(clock)
                .task(Parent.class, Parent::new).task(Recovering.class, Recovering::new).task(Flaky.class, Flaky::new).build();
    }

    private static List<CatalystEvent.ChildStarted> started(EventLog log, ExecutionId parent) {
        return log.read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildStarted).map(e -> (CatalystEvent.ChildStarted) e).toList();
    }

    @Test
    void aFailedChildIsRestartedAsAFreshExecution() {
        failFirst = 2;
        policy = Supervision.restart(3);
        EventLog log = EventLogs.inMemory();
        MockModel model = MockModel.alwaysReturn("M");
        try (CatalystRuntime runtime = runtime(log, model, Clock.systemUTC())) {
            var run = runtime.execute(new Parent(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("ok:M");
            List<CatalystEvent.ChildStarted> children = started(log, run.id());
            assertThat(children).extracting(CatalystEvent.ChildStarted::childRef).containsExactly("child-1", "child-1#2", "child-1#3");
            assertThat(children).extracting(CatalystEvent.ChildStarted::childId).doesNotHaveDuplicates();
            assertThat(children).extracting(c -> runtime.inspect(ExecutionId.of(c.childId())).status())
                    .containsExactly(Status.FAILED, Status.FAILED, Status.COMPLETED);
            // The restarts are recorded: a replay walks them without running a child or a model call.
            assertThat(runtime.replay(run.id(), new Parent()).status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(1);
            assertThat(runs.get()).isEqualTo(3);
        }
    }

    @Test
    void whenRestartsRunOutTheLastFailureEscalates() {
        failFirst = 100;
        policy = Supervision.restart(2);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), Clock.systemUTC())) {
            var run = runtime.execute(new Recovering(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat((String) runtime.resume(run.id()).result()).startsWith("gave up after 2: FAILED").contains("flake 3");
            assertThat(started(log, run.id())).hasSize(3);
            assertThat(runtime.replay(run.id(), new Recovering()).status()).isEqualTo(Status.COMPLETED);
            assertThat(runs.get()).isEqualTo(3);
        }
    }

    @Test
    void anEscalatedFailureEndsAParentThatDoesNotHandleIt() {
        failFirst = 100;
        policy = Supervision.restart(1);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), Clock.systemUTC())) {
            var run = runtime.execute(new Parent(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.FAILED);
            assertThat(runtime.inspect(run.id()).error()).contains("after 1 restart");
            assertThat(started(log, run.id())).hasSize(2);
        }
    }

    @Test
    void aCancelledChildIsNotRestarted() {
        childSleep = Duration.ofHours(1);
        policy = Supervision.restart(3);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), new LeaseTest.TestClock())) {
            var run = runtime.execute(new Recovering(), ExecutionOptions.none());
            await(() -> !started(log, run.id()).isEmpty());
            ExecutionId child = ExecutionId.of(started(log, run.id()).get(0).childId());
            await(() -> runtime.inspect(child).status() == Status.WAITING);
            runtime.cancel(child);
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat((String) runtime.resume(run.id()).result()).startsWith("gave up after 0: CANCELLED");
            assertThat(started(log, run.id())).hasSize(1);
        }
    }

    /** Failures spaced further apart than the window are each restarted; without it the same failures escalate. */
    @Test
    void theRestartWindowIsMeasuredOnRecordedTimes() {
        failFirst = 2;
        policy = Supervision.restart(1).within(Duration.ofHours(1)).backoff(Duration.ofHours(2));
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), clock)) {
            var run = runtime.execute(new Recovering(), ExecutionOptions.none());
            for (int restart = 1; restart <= 2; restart++) {
                java.time.Instant due = clock.instant().plus(Duration.ofHours(2));
                await(() -> runtime.inspect(run.id()).status() == Status.WAITING
                        && runtime.inspect(run.id()).awaitingTimer().filter(due::equals).isPresent());
                clock.advance(Duration.ofHours(2)); // the backoff is a durable sleep
                runtime.recoverAll();
            }
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("ok:M");
            assertThat(started(log, run.id())).hasSize(3);
            assertThat(runtime.replay(run.id(), new Recovering()).status()).isEqualTo(Status.COMPLETED);
        }
        runs.set(0);
        policy = Supervision.restart(1).backoff(Duration.ofHours(2));
        EventLog other = EventLogs.inMemory();
        LeaseTest.TestClock clock2 = new LeaseTest.TestClock();
        try (CatalystRuntime runtime = runtime(other, MockModel.alwaysReturn("M"), clock2)) {
            var run = runtime.execute(new Recovering(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.WAITING && runtime.inspect(run.id()).awaitingTimer().isPresent());
            clock2.advance(Duration.ofHours(2));
            runtime.recoverAll();
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat((String) runtime.resume(run.id()).result()).startsWith("gave up after 1: FAILED");
        }
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
