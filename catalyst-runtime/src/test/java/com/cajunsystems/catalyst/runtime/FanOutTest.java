package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ChildExecutionFailedException;
import com.cajunsystems.catalyst.engine.ChildHandle;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fan-out: {@code ctx.spawn} starts children that run at the same time, and {@code ctx.await},
 * {@code awaitAll} and {@code awaitAny} collect their outcomes. Each outcome (and which child won an
 * awaitAny) is recorded, so a replay returns the same. A child the parent never awaited is cancelled
 * when the parent ends.
 */
@Timeout(30)
class FanOutTest {

    /** Per-name behaviour for {@link Worker}: a sleep, a gate to block on, or a failure. */
    static final Map<String, Duration> sleeps = new ConcurrentHashMap<>();
    static final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();
    static final Map<String, Boolean> failing = new ConcurrentHashMap<>();
    static volatile CyclicBarrier barrier = null;

    static final class Worker implements Task<String> {
        private final String name;
        Worker(String name) { this.name = name; }
        Worker() { this("?"); }
        @Override public String execute(Context ctx) {
            Duration sleep = sleeps.get(name);
            if (sleep != null) ctx.sleep(sleep);
            CyclicBarrier b = barrier;
            if (b != null) {
                try {
                    b.await(5, TimeUnit.SECONDS); // passes only if every worker runs at the same time
                } catch (Exception e) {
                    throw new IllegalStateException("workers did not run concurrently", e);
                }
            }
            CountDownLatch gate = gates.get(name);
            if (gate != null) {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (failing.getOrDefault(name, false)) throw new IllegalStateException(name + " broke");
            return name + ":" + ctx.effect("work", () -> name.toUpperCase());
        }
    }

    // The worker's name must survive a resume by id, so each name gets its own task type.
    static final class A extends WorkerOf { A() { super("a"); } }
    static final class B extends WorkerOf { B() { super("b"); } }
    static final class C extends WorkerOf { C() { super("c"); } }
    static class WorkerOf implements Task<String> {
        private final Worker worker;
        WorkerOf(String name) { this.worker = new Worker(name); }
        @Override public String execute(Context ctx) { return worker.execute(ctx); }
    }

    static final class All implements Task<String> {
        @Override public String execute(Context ctx) {
            List<ChildHandle<String>> hs = List.of(ctx.spawn(new A()), ctx.spawn(new B()), ctx.spawn(new C()));
            return String.join(",", ctx.awaitAll(hs));
        }
    }

    static final class AllRecovering implements Task<String> {
        @Override public String execute(Context ctx) {
            List<ChildHandle<String>> hs = List.of(ctx.spawn(new A()), ctx.spawn(new B()), ctx.spawn(new C()));
            try {
                return String.join(",", ctx.awaitAll(hs));
            } catch (ChildExecutionFailedException e) {
                return "failed: " + e.error();
            }
        }
    }

    static final class Any implements Task<String> {
        @Override public String execute(Context ctx) {
            List<ChildHandle<String>> hs = List.of(ctx.spawn(new A()), ctx.spawn(new B()));
            ChildHandle<String> first = ctx.awaitAny(hs);
            return "first=" + ctx.await(first);
        }
    }

    /** Awaits b before a: with a barrier between them, that needs both running at once. */
    static final class BThenA implements Task<String> {
        @Override public String execute(Context ctx) {
            ChildHandle<String> a = ctx.spawn(new A());
            ChildHandle<String> b = ctx.spawn(new B());
            String second = ctx.await(b);
            return ctx.await(a) + "," + second;
        }
    }

    @BeforeEach
    void reset() {
        sleeps.clear();
        gates.values().forEach(CountDownLatch::countDown);
        gates.clear();
        failing.clear();
        barrier = null;
    }

    private static CatalystRuntime runtime(EventLog log, java.time.Clock clock) {
        return CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M")).clock(clock)
                .task(All.class, All::new).task(AllRecovering.class, AllRecovering::new).task(Any.class, Any::new).task(BThenA.class, BThenA::new)
                .task(A.class, A::new).task(B.class, B::new).task(C.class, C::new).build();
    }

    private static List<ExecutionId> spawned(EventLog log, ExecutionId parent) {
        return log.read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildSpawned)
                .map(e -> ExecutionId.of(((CatalystEvent.ChildSpawned) e).childId())).toList();
    }

    @Test
    void spawnedChildrenRunAtTheSameTimeAndAwaitAllCollectsThemInOrder() {
        barrier = new CyclicBarrier(3);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new All(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("a:A,b:B,c:C");
            List<ExecutionId> children = spawned(log, run.id());
            assertThat(children).hasSize(3).doesNotHaveDuplicates();
            children.forEach(c -> assertThat(runtime.inspect(c).status()).isEqualTo(Status.COMPLETED));
            barrier = null;
            assertThat(runtime.replay(run.id(), new All()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void aParentWaitingOnSeveralChildrenHoldsNoThread() {
        gates.put("a", new CountDownLatch(1));
        gates.put("b", new CountDownLatch(1));
        gates.put("c", new CountDownLatch(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new All(), ExecutionOptions.none());
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            List<ExecutionId> children = spawned(log, run.id());
            assertThat(runtime.inspect(run.id()).awaitingChildren()).containsExactly(children.get(0));
            gates.get("c").countDown();
            gates.get("b").countDown();
            gates.get("a").countDown();
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("a:A,b:B,c:C");
        }
    }

    @Test
    void awaitAnyRecordsTheWinnerAndAChildNeverAwaitedIsCancelledWhenTheParentEnds() {
        gates.put("a", new CountDownLatch(1)); // a blocks; b finishes first
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new Any(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("first=b:B");
            ExecutionId a = spawned(log, run.id()).get(0);
            await(() -> runtime.inspect(a).status() == Status.CANCELLED); // the parent ended without it
            gates.get("a").countDown();
            assertThat(runtime.replay(run.id(), new Any()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void awaitAnyWakesOnWhicheverChildEndsFirst() {
        sleeps.put("a", Duration.ofHours(3));
        sleeps.put("b", Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        try (CatalystRuntime runtime = runtime(log, clock)) {
            var run = runtime.execute(new Any(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).awaitingChildren().size() == 2);
            List<ExecutionId> children = spawned(log, run.id());
            await(() -> children.stream().allMatch(c -> runtime.inspect(c).status() == Status.WAITING));
            clock.advance(Duration.ofHours(1));
            runtime.recoverAll(); // b's timer is due; b finishing wakes the parent
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(runtime.resume(run.id()).result()).isEqualTo("first=b:B");
            await(() -> runtime.inspect(children.get(0)).status() == Status.CANCELLED);
        }
    }

    @Test
    void awaitAllSurfacesTheFirstFailureInOrderAndCancelsTheRest() {
        failing.put("b", true);
        gates.put("c", new CountDownLatch(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            var run = runtime.execute(new AllRecovering(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
            assertThat((String) runtime.resume(run.id()).result()).contains("b broke");
            ExecutionId c = spawned(log, run.id()).get(2);
            await(() -> runtime.inspect(c).status() == Status.CANCELLED);
        }
    }

    @Test
    void afterARestartSpawnedChildrenFinishAndWakeTheirParent() {
        sleeps.put("a", Duration.ofHours(1));
        sleeps.put("b", Duration.ofHours(1));
        sleeps.put("c", Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        ExecutionId parent;
        try (CatalystRuntime dying = runtime(log, clock)) {
            var run = dying.execute(new All(), ExecutionOptions.none());
            parent = run.id();
            await(() -> spawned(log, run.id()).size() == 3
                    && spawned(log, run.id()).stream().allMatch(c -> dying.inspect(c).status() == Status.WAITING)
                    && dying.inspect(run.id()).status() == Status.WAITING);
        }
        try (CatalystRuntime next = runtime(log, clock)) {
            clock.advance(Duration.ofHours(1));
            next.recoverAll();
            ExecutionId p = parent;
            await(() -> next.inspect(p).status() == Status.COMPLETED);
            assertThat(next.resume(parent).result()).isEqualTo("a:A,b:B,c:C");
        }
    }

    /** A crash between recording a spawn and creating the child: the resumed parent creates it. */
    @Test
    void aSpawnRecordedButNeverCreatedIsCreatedOnResume() {
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, All.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildSpawned(t, "child-1", ExecutionId.random().value(), A.class.getName()));
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            runtime.recoverAll();
            await(() -> runtime.inspect(parent).status() == Status.COMPLETED);
            assertThat(runtime.resume(parent).result()).isEqualTo("a:A,b:B,c:C");
        }
    }

    /** Paused while waiting, then resumed before any child ends: it records the wait again and reads WAITING. */
    @Test
    void aParentResumedBeforeAnyChildEndsParksAgain() {
        sleeps.put("a", Duration.ofHours(1));
        sleeps.put("b", Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, new LeaseTest.TestClock())) {
            var run = runtime.execute(new Any(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).awaitingChildren().size() == 2);
            runtime.pause(run.id());
            assertThatThrownBy(() -> runtime.resume(run.id()).result(Duration.ofSeconds(5))).isInstanceOf(ExecutionWaitingSignal.class);
            assertThat(runtime.inspect(run.id()).status()).isEqualTo(Status.WAITING);
            assertThat(runtime.inspect(run.id()).awaitingChildren()).hasSize(2);
        }
    }

    /** The resumed parent re-runs its spawns: a recorded child it never created is started then, not when awaited. */
    @Test
    void aResumedParentStartsARecordedChildAtItsSpawnNotAtItsAwait() {
        barrier = new CyclicBarrier(2); // a and b must run at the same time
        EventLog log = EventLogs.inMemory();
        ExecutionId parent = ExecutionId.random();
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        log.append(parent, new CatalystEvent.ExecutionCreated(t, BThenA.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildSpawned(t, "child-1", ExecutionId.random().value(), A.class.getName()));
        try (CatalystRuntime runtime = runtime(log, java.time.Clock.systemUTC())) {
            runtime.recoverAll();
            await(() -> runtime.inspect(parent).isTerminal());
            assertThat(runtime.inspect(parent).status()).isEqualTo(Status.COMPLETED);
            assertThat(runtime.resume(parent).result()).isEqualTo("a:A,b:B");
        }
    }

    /** After a restart, any of the awaited children ending wakes the parent: recovery links every one. */
    @Test
    void afterARestartAnyAwaitedChildWakesItsParent() {
        sleeps.put("a", Duration.ofHours(3));
        sleeps.put("b", Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        LeaseTest.TestClock clock = new LeaseTest.TestClock();
        ExecutionId parent;
        try (CatalystRuntime dying = runtime(log, clock)) {
            var run = dying.execute(new Any(), ExecutionOptions.none());
            parent = run.id();
            await(() -> dying.inspect(run.id()).awaitingChildren().size() == 2
                    && spawned(log, run.id()).stream().allMatch(c -> dying.inspect(c).status() == Status.WAITING));
        }
        try (CatalystRuntime next = runtime(log, clock)) {
            clock.advance(Duration.ofHours(1));
            next.recoverAll(); // resumes b (due); the parent is still waiting, so only its links are noted
            ExecutionId p = parent;
            await(() -> next.inspect(p).status() == Status.COMPLETED);
            assertThat(next.resume(parent).result()).isEqualTo("first=b:B");
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
