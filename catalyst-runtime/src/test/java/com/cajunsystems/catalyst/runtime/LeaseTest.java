package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.Lease;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-3: a distributed runtime runs an execution only while holding its lease. Each test shares one
 * {@link SharedLog} between runtimes, so each runtime is a node. A dead node is simulated by writing
 * its lease row directly: nothing renews it, so it lapses when the test clock passes it.
 */
class LeaseTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");
    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    /** A clock tests move by hand; lease expiry is read from it, renewal timing is real. */
    static final class TestClock extends Clock {
        private volatile Instant now = T;
        void advance(Duration d) { now = now.plus(d); }
        @Override public Instant instant() { return now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
    }

    /** Records step 1, optionally parks at a gate, then records step 2. */
    static final class GatedTask implements Task<String> {
        private final CountDownLatch reachedGate;
        private final CountDownLatch openGate;

        GatedTask(CountDownLatch reachedGate, CountDownLatch openGate) {
            this.reachedGate = reachedGate;
            this.openGate = openGate;
        }

        GatedTask() {
            this(null, null);
        }

        @Override public String execute(Context ctx) throws Exception {
            String one = ctx.model().complete(STEP1).message();
            if (openGate != null) {
                reachedGate.countDown();
                openGate.await();
            }
            return one + "|" + ctx.model().complete(STEP2).message();
        }
    }

    /** Parks on a signal, then finishes with it. */
    static final class ApprovalTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return "approved by " + ctx.awaitSignal("approve", String.class);
        }
    }

    private final SharedLog log = SharedLog.distributable();
    private final TestClock clock = new TestClock();

    private CatalystRuntime node(String id, MockModel model) {
        return node(id, model, Duration.ofSeconds(30), Duration.ofSeconds(10));
    }

    private CatalystRuntime node(String id, MockModel model, Duration lease, Duration heartbeat) {
        return CatalystRuntime.builder().log(log).model(model).clock(clock)
                .nodeId(id).distributed().lease(lease, heartbeat)
                .task(GatedTask.class, GatedTask::new).task(new ApprovalTask())
                .build();
    }

    /** An execution a node left mid-run: created and started, step 1 not yet recorded. */
    private ExecutionId crashInterrupted() {
        ExecutionId id = ExecutionId.random();
        log.append(id, new CatalystEvent.ExecutionCreated(T, GatedTask.class.getName(), "novars", "cfg", ""), 0);
        log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-dead"), 1);
        return id;
    }

    /** Writes the lease a node that then died would have left behind. */
    private void leasedByDeadNode(ExecutionId id, Duration remaining) {
        assertThat(log.compareAndSetLease(id, null, new Lease("node-dead", clock.instant().plus(remaining).toEpochMilli()))).isTrue();
    }

    @Test
    void aRunningExecutionIsLeasedToItsNodeAndReleasedWhenItEnds() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        try (CatalystRuntime nodeA = node("node-a", MockModel.alwaysReturn("A"))) {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.none());
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();

            Lease lease = log.readLease(run.id()).orElseThrow();
            assertThat(lease.holder()).isEqualTo("node-a");
            assertThat(lease.expiresAt()).isEqualTo(clock.instant().plus(Duration.ofSeconds(30)));

            open.countDown();
            assertThat(run.result()).isEqualTo("A|A");
            assertThat(log.readLease(run.id())).as("released when the attempt ended").isEmpty();
        } finally {
            open.countDown();
        }
    }

    /** Every way into a running execution from another node is refused, before writing anything. */
    @Test
    void anotherNodeIsRefusedWhileTheLeaseIsLive() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeA = node("node-a", MockModel.alwaysReturn("A"));
             CatalystRuntime nodeB = node("node-b", modelB)) {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.withKey("job-1"));
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = run.id();
            List<SequencedEvent> before = log.read(id);

            assertThatThrownBy(() -> nodeB.resume(id))
                    .isInstanceOfSatisfying(LeaseHeldException.class, e -> assertThat(e.lease().holder()).isEqualTo("node-a"));
            assertThatThrownBy(() -> nodeB.execute(new GatedTask(), ExecutionOptions.withKey("job-1")))
                    .as("a re-submitted key resolves to the running execution").isInstanceOf(LeaseHeldException.class);
            assertThatThrownBy(() -> nodeB.cancel(id)).isInstanceOf(LeaseHeldException.class);
            assertThatThrownBy(() -> nodeB.pause(id)).isInstanceOf(LeaseHeldException.class);

            assertThat(log.read(id)).as("nothing written").isEqualTo(before);
            assertThat(modelB.callCount()).isZero();
            open.countDown();
            assertThat(run.result()).isEqualTo("A|A");
        } finally {
            open.countDown();
        }
    }

    @Test
    void aDeadNodesLeaseLapsesAndAnotherNodeTakesOver() {
        ExecutionId id = crashInterrupted();
        leasedByDeadNode(id, Duration.ofSeconds(30));
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            assertThatThrownBy(() -> nodeB.resume(id)).isInstanceOf(LeaseHeldException.class);

            clock.advance(Duration.ofSeconds(31));
            assertThat(nodeB.resume(id).result()).isEqualTo("B|B");
            assertThat(nodeB.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /** The startup scan on a shared index: skips what live nodes hold, takes over what dead ones left. */
    @Test
    void recoveryLeavesLiveLeasesAloneAndTakesOverLapsedOnes() {
        ExecutionId held = crashInterrupted();
        leasedByDeadNode(held, Duration.ofSeconds(30));
        ExecutionId lapsed = crashInterrupted();
        leasedByDeadNode(lapsed, Duration.ofSeconds(-1));
        ExecutionId unleased = crashInterrupted();

        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            RecoveryReport report = nodeB.recoverAll();
            assertThat(report.resumed()).containsOnlyKeys(lapsed, unleased);
            assertThat(report.leftAlone()).containsEntry(held, Status.RUNNING);
            assertThat(report.notRecovered()).isEmpty();
            report.resumed().values().forEach(h -> assertThat(h.result()).isEqualTo("B|B"));
        }
    }

    @Test
    void theHeartbeatRenewsHeldLeases() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        try (CatalystRuntime nodeA = node("node-a", MockModel.alwaysReturn("A"), Duration.ofSeconds(30), Duration.ofMillis(50))) {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.none());
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();

            clock.advance(Duration.ofSeconds(20));
            Instant renewedTo = clock.instant().plus(Duration.ofSeconds(30));
            await(() -> log.readLease(run.id()).map(l -> l.expiresAt().equals(renewedTo)).orElse(false));
            open.countDown();
            assertThat(run.result()).isEqualTo("A|A");
        } finally {
            open.countDown();
        }
    }

    /**
     * A renewal that finds the row changed means another node took over: the attempt stops at its
     * next boundary and writes nothing, as on shutdown, and its handle says why.
     */
    @Test
    void losingTheLeaseStopsTheAttemptBeforeItWritesAgain() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        MockModel modelA = MockModel.alwaysReturn("A");
        try (CatalystRuntime nodeA = node("node-a", modelA, Duration.ofSeconds(30), Duration.ofMillis(50))) {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.none());
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = run.id();

            Lease mine = log.readLease(id).orElseThrow();
            Lease theirs = new Lease("node-x", clock.instant().plus(Duration.ofMinutes(5)).toEpochMilli());
            assertThat(log.compareAndSetLease(id, mine, theirs)).isTrue();
            List<SequencedEvent> before = log.read(id);
            Thread.sleep(500); // several heartbeats: one of them finds the row changed

            open.countDown();
            assertThatThrownBy(run::result).isInstanceOf(LeaseLostException.class);
            assertThat(log.read(id)).as("nothing written after the lease was lost").isEqualTo(before);
            assertThat(modelA.callCount()).as("step 2 never called").isEqualTo(1);
            assertThat(log.readLease(id)).as("the new holder's lease is untouched").contains(theirs);
        } finally {
            open.countDown();
        }
    }

    /**
     * Claim, then decide. Node B folds the execution as resumable, and before its claim lands another
     * node finishes it and releases the lease. B must re-read after claiming and replay the recorded
     * outcome. Appending {@code ExecutionResumed} after the terminal event would re-run a finished task.
     */
    @Test
    void anExecutionFinishedElsewhereDuringTheClaimIsNotResumed() {
        ExecutionId id = crashInterrupted();
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = node("node-b", modelB)) {
            log.beforeReadLease = leased -> {
                log.beforeReadLease = x -> { };
                log.append(leased, new CatalystEvent.ExecutionCompleted(T, new com.cajunsystems.catalyst.engine.PayloadCodec().toTree("done elsewhere")), 2);
            };
            assertThat(nodeB.resume(id).result()).isEqualTo("done elsewhere");
            assertThat(log.read(id)).as("nothing appended after the terminal event").hasSize(3);
            assertThat(modelB.callCount()).isZero();
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /**
     * The recovery scan's version of the window: another node pauses the execution and releases
     * before the scan's claim lands. Recovery must leave a deliberate pause alone (Deploy-2), so it
     * has to decide from a fold read after claiming.
     */
    @Test
    void anExecutionPausedElsewhereDuringTheScansClaimIsLeftAlone() {
        ExecutionId id = crashInterrupted();
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = node("node-b", modelB)) {
            log.beforeReadLease = leased -> {
                log.beforeReadLease = x -> { };
                log.append(leased, new CatalystEvent.ExecutionPaused(T, "operator"), 2);
            };
            RecoveryReport report = nodeB.recoverAll();
            assertThat(report.resumed()).isEmpty();
            assertThat(report.leftAlone()).containsEntry(id, Status.PAUSED);
            assertThat(log.read(id)).hasSize(3);
            assertThat(modelB.callCount()).isZero();
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /** The same window, landing on a wait: the execution must stay WAITING, not be folded back to RUNNING. */
    @Test
    void anExecutionThatStartedWaitingDuringTheClaimIsNotResumed() {
        ExecutionId id = ExecutionId.random();
        log.append(id, new CatalystEvent.ExecutionCreated(T, ApprovalTask.class.getName(), "novars", "cfg", ""), 0);
        log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-dead"), 1);
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            log.beforeReadLease = leased -> {
                log.beforeReadLease = x -> { };
                log.append(leased, new CatalystEvent.SignalAwaited(T, "approve", String.class.getName()), 2);
            };
            assertThatThrownBy(() -> nodeB.resume(id).result()).isInstanceOf(com.cajunsystems.catalyst.engine.ExecutionWaitingSignal.class);
            assertThat(log.read(id)).hasSize(3);
            assertThat(nodeB.inspect(id).status()).isEqualTo(Status.WAITING);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /** A waiting execution holds no lease, so a signal can arrive at any node. */
    @Test
    void aWaitingExecutionReleasesItsLeaseAndASignalOnAnotherNodeResumesIt() {
        try (CatalystRuntime nodeA = node("node-a", MockModel.alwaysReturn("A"));
             CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            ExecutionHandle<String> run = nodeA.execute(new ApprovalTask(), ExecutionOptions.none());
            await(() -> nodeA.inspect(run.id()).status() == Status.WAITING && log.readLease(run.id()).isEmpty());

            assertThat(nodeB.signal(run.id(), "approve", "ops").result()).isEqualTo("approved by ops");
            assertThat(log.readLease(run.id())).isEmpty();
        }
    }

    /** A node restarted under its own id need not wait out its predecessor's leases. */
    @Test
    void aNodeRestartedUnderTheSameIdTakesItsLeasesBack() {
        ExecutionId id = crashInterrupted();
        assertThat(log.compareAndSetLease(id, null,
                new Lease("node-a", clock.instant().plus(Duration.ofSeconds(30)).toEpochMilli()))).isTrue();
        try (CatalystRuntime restarted = node("node-a", MockModel.alwaysReturn("A"))) {
            RecoveryReport report = restarted.recoverAll();
            assertThat(report.resumed()).containsOnlyKeys(id);
            assertThat(report.resumed().get(id).result()).isEqualTo("A|A");
            assertThat(restarted.inspect(id).status()).isEqualTo(Status.COMPLETED);
        }
    }

    /** Control of an execution no node is running claims its lease for the write, then gives it back. */
    @Test
    void cancellingAnExecutionNoNodeRunsTakesAndReleasesItsLease() {
        ExecutionId id = crashInterrupted();
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            nodeB.cancel(id);
            assertThat(nodeB.inspect(id).status()).isEqualTo(Status.CANCELLED);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /** Shutdown releases what it holds, so other nodes take the work over now rather than at expiry. */
    @Test
    void closingANodeReleasesItsLeases() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        CatalystRuntime nodeA = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("A")).clock(clock)
                .nodeId("node-a").distributed().drainTimeout(Duration.ofMillis(100)).build();
        try {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.none());
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
            nodeA.close(); // the task is parked off-boundary, so the drain times out with it still running
            assertThat(log.readLease(run.id())).isEmpty();
        } finally {
            open.countDown();
            nodeA.close();
        }
    }

    private static void await(BooleanSupplier condition) throws RuntimeException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within 5s");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
