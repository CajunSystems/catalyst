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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-4: what makes several nodes on one log behave as one system without a coordinator. The claim
 * loop takes over work no live node holds. A cancel issued on any node lands through the node running
 * the execution. Two nodes taking the same new idempotency key create one execution. As in
 * {@link LeaseTest}, runtimes sharing one {@link SharedLog} are the nodes, and a dead node is a lease
 * row nothing renews.
 */
@org.junit.jupiter.api.Timeout(30) // a lease bug tends to hang (a parked attempt nobody stops), not fail
class CrossNodeTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");
    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    /**
     * Records step 1, optionally parks at a gate, then records step 2. Swallows the interrupt a cancel
     * delivers, like the cancel demo, so the cancel is observed cooperatively at step 2's boundary.
     */
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

        @Override public String execute(Context ctx) {
            String one = ctx.model().complete(STEP1).message();
            if (openGate != null) {
                reachedGate.countDown();
                try {
                    openGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return one + "|" + ctx.model().complete(STEP2).message();
        }
    }

    static final class ApprovalTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return "approved by " + ctx.awaitSignal("approve", String.class);
        }
    }

    private final SharedLog log = SharedLog.distributable();
    private final LeaseTest.TestClock clock = new LeaseTest.TestClock();

    private CatalystRuntime.Builder nodeBuilder(String id, MockModel model) {
        return CatalystRuntime.builder().log(log).model(model).clock(clock)
                .nodeId(id).distributed().lease(Duration.ofSeconds(30), Duration.ofMillis(50))
                .claimInterval(Duration.ofMillis(50));
    }

    private CatalystRuntime node(String id, MockModel model) {
        return nodeBuilder(id, model).task(GatedTask.class, GatedTask::new).task(new ApprovalTask()).build();
    }

    private ExecutionId crashInterrupted() {
        ExecutionId id = ExecutionId.random();
        log.append(id, new CatalystEvent.ExecutionCreated(T, GatedTask.class.getName(), "novars", "cfg", ""), 0);
        log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-dead"), 1);
        return id;
    }

    private void leasedByDeadNode(ExecutionId id, Duration remaining) {
        assertThat(log.compareAndSetLease(id, null,
                new Lease("node-dead", clock.instant().plus(remaining).toEpochMilli()))).isTrue();
    }

    // ── Claim loop ───────────────────────────────────────────────────────────

    /** No one calls recoverAll: the loop notices the lapse and takes the execution over by itself. */
    @Test
    void theClaimLoopTakesOverADeadNodesExecutionOnceItsLeaseLapses() {
        ExecutionId id = crashInterrupted();
        leasedByDeadNode(id, Duration.ofSeconds(30));
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = node("node-b", modelB)) {
            sleep(300); // several ticks while the lease is live: left alone
            assertThat(log.read(id)).hasSize(2);

            clock.advance(Duration.ofSeconds(31));
            await(() -> nodeB.inspect(id).status() == Status.COMPLETED);
            assertThat(modelB.callCount()).isEqualTo(2);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /**
     * Ticking over paused and waiting executions writes nothing for them: they are folded and left,
     * never claimed, so a loop over many parked executions does not churn their lease rows.
     */
    @Test
    void theClaimLoopWritesNoLeaseForParkedExecutions() {
        ExecutionId paused = crashInterrupted();
        log.append(paused, new CatalystEvent.ExecutionPaused(T, "operator"), 2);
        ExecutionId waiting = ExecutionId.random();
        log.append(waiting, new CatalystEvent.ExecutionCreated(T, ApprovalTask.class.getName(), "novars", "cfg", ""), 0);
        log.append(waiting, new CatalystEvent.ExecutionStarted(T, 1, "node-dead"), 1);
        log.append(waiting, new CatalystEvent.SignalAwaited(T, "approve", String.class.getName()), 2);
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            sleep(400);
            assertThat(log.leaseWrites(paused)).isZero();
            assertThat(log.leaseWrites(waiting)).isZero();
            assertThat(nodeB.inspect(paused).status()).isEqualTo(Status.PAUSED);
            assertThat(nodeB.inspect(waiting).status()).isEqualTo(Status.WAITING);
        }
    }

    /**
     * "Its signal re-enqueues it" (Deploy-6): a signal accepted on a node that cannot run the task
     * leaves the execution RESUMING, and a node that can picks it up from the index.
     */
    @Test
    void aSignalledExecutionTheReceivingNodeCannotRunIsPickedUpByOneThatCan() {
        try (CatalystRuntime nodeA = nodeBuilder("node-a", MockModel.alwaysReturn("A")).build(); // no task registrations
             CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            ExecutionHandle<String> run = nodeA.execute(new ApprovalTask(), ExecutionOptions.none());
            // The handle completes only once the attempt is deregistered; the fold says WAITING earlier.
            assertThatThrownBy(() -> run.result(Duration.ofSeconds(5)))
                    .isInstanceOf(com.cajunsystems.catalyst.engine.ExecutionWaitingSignal.class);

            ExecutionHandle<?> here = nodeA.signal(run.id(), "approve", "ops");
            assertThatThrownBy(here::result).hasMessageContaining("no task is registered");
            await(() -> nodeB.inspect(run.id()).status() == Status.COMPLETED);
            assertThat(nodeB.resume(run.id()).result()).isEqualTo("approved by ops");
        }
    }

    // ── Cross-node cancel ────────────────────────────────────────────────────

    /**
     * The roadmap's "cancel issued from the non-owner lands via the owner": node B asks, node A, which
     * is running the execution, finds the request at its next renewal and records the cancel itself.
     * Node B never writes to A's stream.
     */
    @Test
    void aCancelIssuedOnAnotherNodeLandsThroughTheNodeRunningTheExecution() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        MockModel modelA = MockModel.alwaysReturn("A");
        try (CatalystRuntime nodeA = node("node-a", modelA);
             CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            ExecutionHandle<String> run = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.none());
            assertThat(reached.await(5, TimeUnit.SECONDS)).isTrue();
            List<SequencedEvent> before = log.read(run.id());

            nodeB.cancel(run.id());
            assertThat(log.read(run.id())).as("node B wrote nothing to node A's stream").isEqualTo(before);

            assertThatThrownBy(() -> run.result(Duration.ofSeconds(10))).isInstanceOf(CancellationException.class);
            assertThat(nodeA.inspect(run.id()).status()).isEqualTo(Status.CANCELLED);
            assertThat(modelA.callCount()).as("step 2 never called").isEqualTo(1);
            assertThat(log.readLease(run.id())).isEmpty();
        } finally {
            open.countDown();
        }
    }

    /** A request on a dead node's lease is honoured by whichever node takes the execution over. */
    @Test
    void aCancelRequestOutlivesTheNodeThatWasAskedToHonourIt() {
        ExecutionId id = crashInterrupted();
        assertThat(log.compareAndSetLease(id, null, new Lease("node-dead",
                clock.instant().plus(Duration.ofSeconds(30)).toEpochMilli(), "cancelled by request"))).isTrue();
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = node("node-b", modelB)) {
            clock.advance(Duration.ofSeconds(31));
            await(() -> nodeB.inspect(id).status() == Status.CANCELLED);
            assertThat(modelB.callCount()).as("cancelled before running anything").isZero();
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /**
     * The owner parked the execution (paused) before it could honour a cancel request, so its release
     * left the request on a lapsed row. Nothing resumes a paused execution, so the loop records the
     * cancel itself.
     */
    @Test
    void aCancelOrphanedOnAParkedExecutionIsRecordedByTheClaimLoop() {
        ExecutionId id = crashInterrupted();
        log.append(id, new CatalystEvent.ExecutionPaused(T, "operator"), 2);
        assertThat(log.compareAndSetLease(id, null,
                new Lease("node-dead", clock.instant().toEpochMilli(), "cancelled by request"))).isTrue();
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            await(() -> nodeB.inspect(id).status() == Status.CANCELLED);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /** Recording a cancel needs no task: a node that cannot run the type still honours a carried request. */
    @Test
    void aNodeThatCannotRunTheTaskStillHonoursACarriedCancel() {
        ExecutionId id = crashInterrupted();
        assertThat(log.compareAndSetLease(id, null, new Lease("node-dead",
                clock.instant().toEpochMilli() - 1, "cancelled by request"))).isTrue();
        try (CatalystRuntime bare = nodeBuilder("node-bare", MockModel.alwaysReturn("B")).build()) { // no registrations
            await(() -> bare.inspect(id).status() == Status.CANCELLED);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    /**
     * The scan sees a crash-interrupted execution carrying a cancel, but before its claim lands the
     * execution is parked. Left alone, the request must survive for whoever records it, not be
     * dropped because the scan read it.
     */
    @Test
    void aCancelTheScanReadsButDoesNotActOnSurvives() {
        ExecutionId id = crashInterrupted();
        assertThat(log.compareAndSetLease(id, null, new Lease("node-dead",
                clock.instant().toEpochMilli() - 1, "cancelled by request"))).isTrue();
        log.beforeReadLease = leased -> {
            log.beforeReadLease = x -> { };
            log.append(leased, new CatalystEvent.SignalAwaited(T, "approve", String.class.getName()), 2);
        };
        try (CatalystRuntime nodeB = nodeBuilder("node-b", MockModel.alwaysReturn("B"))
                .claimInterval(Duration.ofMinutes(10)).task(GatedTask.class, GatedTask::new).build()) {
            nodeB.recoverAll();
            assertThat(log.readLease(id).flatMap(Lease::cancelRequested)).as("still on the row").contains("cancelled by request");
            nodeB.recoverAll(); // the orphaned-cancel path now records it
            assertThat(nodeB.inspect(id).status()).isEqualTo(Status.CANCELLED);
        }
    }

    /** No node is running it: the cancel is recorded directly, under the lease, as on one node. */
    @Test
    void cancellingAnExecutionNoNodeIsRunningRecordsItDirectly() {
        ExecutionId id = crashInterrupted();
        log.append(id, new CatalystEvent.ExecutionPaused(T, "operator"), 2);
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            nodeB.cancel(id);
            assertThat(nodeB.inspect(id).status()).isEqualTo(Status.CANCELLED);
            assertThat(log.readLease(id)).isEmpty();
        }
    }

    // ── Keyed submission ─────────────────────────────────────────────────────

    /**
     * Two nodes take the same new key at once. Node A wins the key and is running the execution when
     * node B's claim lands, so B resolves to A's execution and is told it is running there.
     * Previously B would have registered its own id over A's and run the work a second time.
     */
    @Test
    void twoNodesTakingTheSameNewKeyCreateOneExecution() throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeA = node("node-a", MockModel.alwaysReturn("A"));
             CatalystRuntime nodeB = node("node-b", modelB)) {
            ExecutionHandle<String>[] fromA = new ExecutionHandle[1];
            log.beforeClaimKey = key -> {
                log.beforeClaimKey = k -> { };
                fromA[0] = nodeA.execute(new GatedTask(reached, open), ExecutionOptions.withKey(key));
                awaitLatch(reached);
            };
            assertThatThrownBy(() -> nodeB.execute(new GatedTask(), ExecutionOptions.withKey("order-7")))
                    .isInstanceOfSatisfying(LeaseHeldException.class,
                            e -> assertThat(e.executionId()).isEqualTo(fromA[0].id()));
            assertThat(log.findByKey("order-7")).contains(fromA[0].id());
            assertThat(log.openExecutions()).containsExactly(fromA[0].id());
            assertThat(modelB.callCount()).isZero();

            open.countDown();
            assertThat(fromA[0].result()).isEqualTo("A|A");
        } finally {
            open.countDown();
        }
    }

    /** A node that won a key and died before creating the execution: the next submission creates it under that id. */
    @Test
    void aKeyClaimedByANodeThatDiedBeforeCreatingIsCreatedUnderTheClaimedId() {
        ExecutionId claimed = ExecutionId.random();
        assertThat(log.claimKey("order-8", claimed)).isEmpty();
        try (CatalystRuntime nodeB = node("node-b", MockModel.alwaysReturn("B"))) {
            ExecutionHandle<String> run = nodeB.execute(new GatedTask(), ExecutionOptions.withKey("order-8"));
            assertThat(run.id()).isEqualTo(claimed);
            assertThat(run.result()).isEqualTo("B|B");
            assertThat(log.read(claimed).get(0).event())
                    .as("a real creation, not a resume of an empty stream")
                    .isInstanceOfSatisfying(CatalystEvent.ExecutionCreated.class,
                            created -> assertThat(created.idempotencyKey()).isEqualTo("order-8"));
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within 5s");
            sleep(20);
        }
    }
}
