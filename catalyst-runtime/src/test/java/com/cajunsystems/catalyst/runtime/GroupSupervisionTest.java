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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Supervised groups: {@code ctx.supervise(supervision, tasks)} runs children at the same time and
 * restarts on failure by strategy: only the failed child (one-for-one), every child (one-for-all),
 * or the failed child and those after it (rest-for-one). Restarted children still running are
 * cancelled first; giving up cancels the rest. A replay walks the same restarts.
 */
@Timeout(30)
class GroupSupervisionTest {

    /** Per name: how many runs fail, and a gate the first run blocks on. Runs are counted when they start. */
    static final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
    static final Map<String, Integer> failRuns = new ConcurrentHashMap<>();
    static final Map<String, CountDownLatch> firstRunGate = new ConcurrentHashMap<>();
    static volatile Supervision policy = Supervision.escalate();

    static class Worker implements Task<String> {
        private final String name;
        Worker(String name) { this.name = name; }
        @Override public String execute(Context ctx) {
            int run = runs.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            CountDownLatch gate = firstRunGate.get(name);
            if (gate != null && run == 1) {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // cancelled: unwinds at the next boundary
                }
            }
            if (run <= failRuns.getOrDefault(name, 0)) throw new IllegalStateException(name + " broke on run " + run);
            return ctx.effect("work", () -> name + run);
        }
    }

    static final class A extends Worker { A() { super("a"); } }
    static final class B extends Worker { B() { super("b"); } }
    static final class C extends Worker { C() { super("c"); } }

    static final class Group implements Task<String> {
        @Override public String execute(Context ctx) {
            return String.join(",", ctx.supervise(policy, List.of(new A(), new B(), new C())));
        }
    }

    /** Catches an escalated failure, then waits a long time, so it has not ended when the test looks. */
    static final class GivingUp implements Task<String> {
        @Override public String execute(Context ctx) {
            try {
                return String.join(",", ctx.supervise(policy, List.of(new A(), new B(), new C())));
            } catch (ChildExecutionFailedException e) {
                ctx.sleep(Duration.ofHours(1));
                return "gave up: " + e.error();
            }
        }
    }

    @BeforeEach
    void reset() {
        runs.clear();
        failRuns.clear();
        firstRunGate.values().forEach(CountDownLatch::countDown);
        firstRunGate.clear();
        policy = Supervision.escalate();
    }

    private static CatalystRuntime runtime(EventLog log) {
        return CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M")).clock(new LeaseTest.TestClock())
                .task(Group.class, Group::new).task(GivingUp.class, GivingUp::new)
                .task(A.class, A::new).task(B.class, B::new).task(C.class, C::new).build();
    }

    private static List<CatalystEvent.ChildSpawned> spawned(EventLog log, ExecutionId parent) {
        return log.read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildSpawned).map(e -> (CatalystEvent.ChildSpawned) e).toList();
    }

    private static int runsOf(String name) {
        return runs.getOrDefault(name, new AtomicInteger()).get();
    }

    private static String runGroup(CatalystRuntime runtime, EventLog log, Task<String> task, ExecutionId[] id) {
        var run = runtime.execute(task, ExecutionOptions.none());
        id[0] = run.id();
        await(() -> runtime.inspect(run.id()).status() == Status.COMPLETED);
        return (String) runtime.resume(run.id()).result();
    }

    @Test
    void oneForOneRestartsOnlyTheFailedChild() {
        policy = Supervision.restart(2);
        failRuns.put("b", 1);
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log)) {
            ExecutionId[] id = new ExecutionId[1];
            assertThat(runGroup(runtime, log, new Group(), id)).isEqualTo("a1,b2,c1");
            assertThat(spawned(log, id[0])).hasSize(4);
            assertThat(List.of(runsOf("a"), runsOf("b"), runsOf("c"))).containsExactly(1, 2, 1);
            assertThat(runtime.replay(id[0], new Group()).status()).isEqualTo(Status.COMPLETED);
            assertThat(List.of(runsOf("a"), runsOf("b"), runsOf("c"))).as("a replay runs nothing").containsExactly(1, 2, 1);
        }
    }

    @Test
    void oneForAllCancelsAndRestartsEveryChild() {
        policy = Supervision.restart(2).oneForAll();
        failRuns.put("b", 1);
        firstRunGate.put("a", new CountDownLatch(1)); // a's first run is still going when b fails
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log)) {
            ExecutionId[] id = new ExecutionId[1];
            assertThat(runGroup(runtime, log, new Group(), id)).isEqualTo("a2,b2,c2");
            List<CatalystEvent.ChildSpawned> children = spawned(log, id[0]);
            assertThat(children).hasSize(6);
            assertThat(runtime.inspect(ExecutionId.of(children.get(0).childId())).status())
                    .as("the first a, still running when b failed").isEqualTo(Status.CANCELLED);
            assertThat(List.of(runsOf("a"), runsOf("b"), runsOf("c"))).containsExactly(2, 2, 2);
            assertThat(runtime.replay(id[0], new Group()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void restForOneRestartsTheFailedChildAndThoseAfterIt() {
        policy = Supervision.restart(2).restForOne();
        failRuns.put("b", 1);
        firstRunGate.put("c", new CountDownLatch(1)); // c's first run is still going when b fails
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log)) {
            ExecutionId[] id = new ExecutionId[1];
            assertThat(runGroup(runtime, log, new Group(), id)).isEqualTo("a1,b2,c2");
            List<CatalystEvent.ChildSpawned> children = spawned(log, id[0]);
            assertThat(children).hasSize(5);
            assertThat(runtime.inspect(ExecutionId.of(children.get(2).childId())).status()).isEqualTo(Status.CANCELLED);
            assertThat(List.of(runsOf("a"), runsOf("b"), runsOf("c"))).containsExactly(1, 2, 2);
            assertThat(runtime.replay(id[0], new Group()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void givingUpCancelsTheChildrenStillRunning() {
        policy = Supervision.restart(1).oneForAll();
        failRuns.put("b", 100);
        firstRunGate.put("a", new CountDownLatch(1));
        firstRunGate.put("c", new CountDownLatch(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log)) {
            var run = runtime.execute(new GivingUp(), ExecutionOptions.none());
            // Gave up and is now asleep: it has not ended, so these cancels are the group's, not the end-of-parent cascade.
            await(() -> runtime.inspect(run.id()).awaitingTimer().isPresent());
            List<CatalystEvent.ChildSpawned> children = spawned(log, run.id());
            assertThat(children).hasSize(6);
            // The second generation of a and c ran to completion (their gates only hold first runs), so
            // check the first generation, cancelled by the restart, and that no child is left running.
            await(() -> children.stream().allMatch(c -> runtime.inspect(ExecutionId.of(c.childId())).isTerminal()));
            assertThat(runtime.inspect(ExecutionId.of(children.get(0).childId())).status()).isEqualTo(Status.CANCELLED);
        }
    }

    @Test
    void givingUpCancelsAChildStillBlocked() {
        policy = Supervision.escalate();
        failRuns.put("b", 100);
        firstRunGate.put("a", new CountDownLatch(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log)) {
            var run = runtime.execute(new GivingUp(), ExecutionOptions.none());
            await(() -> runtime.inspect(run.id()).awaitingTimer().isPresent());
            ExecutionId a = ExecutionId.of(spawned(log, run.id()).get(0).childId());
            await(() -> runtime.inspect(a).status() == Status.CANCELLED);
            assertThat(runtime.inspect(run.id()).status()).as("the parent has not ended").isEqualTo(Status.WAITING);
        }
    }

    /** The log a crash leaves after the group recorded discarding a and c (b failed) but before they unwound. */
    private static ExecutionId[] crashedAfterDiscarding(EventLog log) {
        java.time.Instant t = java.time.Instant.parse("2026-01-01T00:00:00Z");
        ExecutionId parent = ExecutionId.random();
        ExecutionId a = ExecutionId.random(), b = ExecutionId.random(), c = ExecutionId.random();
        log.append(parent, new CatalystEvent.ExecutionCreated(t, Group.class.getName(), "novars", "cfg", ""));
        log.append(parent, new CatalystEvent.ExecutionStarted(t, 1, "node-dead"));
        log.append(parent, new CatalystEvent.ChildSpawned(t, "child-1", a.value(), A.class.getName()));
        log.append(parent, new CatalystEvent.ChildSpawned(t, "child-2", b.value(), B.class.getName()));
        log.append(parent, new CatalystEvent.ChildSpawned(t, "child-3", c.value(), C.class.getName()));
        String[][] kids = {{a.value(), A.class.getName(), "child-1"}, {b.value(), B.class.getName(), "child-2"},
                {c.value(), C.class.getName(), "child-3"}};
        for (String[] kid : kids) {
            ExecutionId child = ExecutionId.of(kid[0]);
            log.append(child, new CatalystEvent.ExecutionCreated(t, kid[1], "novars", "cfg", "child:" + parent.value() + ":" + kid[2]));
            log.append(child, new CatalystEvent.ExecutionStarted(t, 1, "node-dead")); // left RUNNING by the crash
        }
        log.append(b, new CatalystEvent.ExecutionFailed(t, "b broke", -1));
        log.append(parent, new CatalystEvent.ChildCompleted(t, "child-2", b.value(), "FAILED", null, "b broke"));
        log.append(parent, new CatalystEvent.ChildDiscarded(t, "child-1", a.value()));
        log.append(parent, new CatalystEvent.ChildDiscarded(t, "child-3", c.value()));
        log.append(parent, new CatalystEvent.TimerStarted(t, "timer-1", t.plus(Duration.ofHours(1)))); // long due
        return new ExecutionId[] {parent, a, c};
    }

    private static CatalystRuntime systemClockRuntime(EventLog log) {
        return CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("M"))
                .task(Group.class, Group::new).task(A.class, A::new).task(B.class, B::new).task(C.class, C::new).build();
    }

    /** Recovery cancels a discarded child it finds RUNNING, rather than resuming it. */
    @Test
    void recoveryCancelsADiscardedChildInsteadOfResumingIt() {
        policy = Supervision.restart(2).oneForAll().backoff(Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        ExecutionId[] ids = crashedAfterDiscarding(log);
        try (CatalystRuntime runtime = systemClockRuntime(log)) {
            runtime.recoverAll();
            await(() -> runtime.inspect(ids[0]).status() == Status.COMPLETED);
            assertThat(runtime.resume(ids[0]).result()).isEqualTo("a1,b1,c1");
            await(() -> runtime.inspect(ids[1]).status() == Status.CANCELLED && runtime.inspect(ids[2]).status() == Status.CANCELLED);
            assertThat(List.of(runsOf("a"), runsOf("c"))).as("only the restarted generation ran").containsExactly(1, 1);
        }
    }

    /** The parent's own replay re-runs its spawns, but does not revive a child it recorded discarding. */
    @Test
    void theParentsReplayDoesNotReviveADiscardedChild() {
        policy = Supervision.restart(2).oneForAll().backoff(Duration.ofHours(1));
        EventLog log = EventLogs.inMemory();
        ExecutionId[] ids = crashedAfterDiscarding(log);
        try (CatalystRuntime runtime = systemClockRuntime(log)) {
            runtime.resume(ids[0]); // no recovery scan: only the parent's replay could touch a and c
            await(() -> runtime.inspect(ids[0]).status() == Status.COMPLETED);
            assertThat(runtime.resume(ids[0]).result()).isEqualTo("a1,b1,c1");
            // The parent ended with a and c outstanding: the cascade cancels them, never having run them.
            await(() -> runtime.inspect(ids[1]).status() == Status.CANCELLED && runtime.inspect(ids[2]).status() == Status.CANCELLED);
            assertThat(List.of(runsOf("a"), runsOf("c"))).as("only the restarted generation ran").containsExactly(1, 1);
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
