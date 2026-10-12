package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.events.FileBlobStore;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import com.cajunsystems.catalyst.runtime.LeaseHeldException;
import com.cajunsystems.catalyst.runtime.LeaseLostException;
import com.cajunsystems.catalyst.runtime.RecoveryReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Dist-0 and Dist-3, end to end, with runtimes that <strong>each have their own log on one
 * FoundationDB root</strong> — separate processes' worth of connections, sequencers and caches. Node
 * A stalls mid-execution holding its lease. Other nodes find the execution through the shared index,
 * leave it alone while the lease is live, and take it over once it has lapsed, with zero duplicate
 * model calls. Node A's late write is then refused.
 *
 * <p>This is what Dist-4's claim loop will do automatically; here the test drives it. Every runtime
 * is built {@code distributed()}: Dist-2's guard passes on this log, and Dist-3's leases decide who
 * may run the execution. The index, the idempotency keys, the leases, the fence and the blob store
 * all span nodes.
 *
 * <p>Skips without a cluster; fails instead under {@code CATALYST_REQUIRE_FDB=true}, as CI runs it.
 * Locally: {@code scripts/fdb-tests.sh}.
 */
class FoundationDBAcceptanceTest {

    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    /** Records step 1, then (on node A) parks at a gate — node A has, as far as anyone else can tell, died. */
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

    @TempDir Path blobs;
    private String root;

    @BeforeEach
    void cluster() {
        assumeTrue(clusterAvailable(), "FoundationDB cluster not available — skipping");
        root = "catalyst-acceptance-" + UUID.randomUUID();
    }

    private GumboEventLog nodeLog() {
        return GumboEventLog.foundationDB(System.getenv("FDB_CLUSTER_FILE"), root, FileBlobStore.at(blobs));
    }

    /**
     * Node A runs an execution and stalls holding its lease, so node B is refused (Dist-3). Then node
     * B', whose clock runs a minute ahead, sees A's lease as lapsed. That stands in for A pausing
     * longer than its lease, for example in a long GC. B' takes over and finishes with one model call,
     * finding the work through the shared index rather than by holding its id. When A wakes, its next
     * write is refused: by the fence, or, if a heartbeat noticed first, by the lost lease. Either way
     * it records nothing. The coordination layer was wrong and the stream is still correct.
     */
    @Test
    void aSecondNodeTakesOverAStalledExecutionAndTheFirstNodesLateWriteIsRefused() throws Exception {
        CountDownLatch reachedGate = new CountDownLatch(1);
        CountDownLatch openGate = new CountDownLatch(1);
        MockModel modelA = MockModel.alwaysReturn("A");
        MockModel modelB = MockModel.alwaysReturn("B");
        GumboEventLog logA = nodeLog();
        GumboEventLog logB = nodeLog();
        GumboEventLog logSkewed = nodeLog();

        // Every node declares distributed(), so each build() checks the log (Dist-2) and takes leases (Dist-3).
        CatalystRuntime nodeA = Catalyst.builder().log(logA).model(modelA).nodeId("node-a").distributed().build();
        CatalystRuntime nodeB = Catalyst.builder().log(logB).model(modelB).nodeId("node-b").distributed()
                .task(GatedTask.class, GatedTask::new).build();
        CatalystRuntime nodeSkewed = Catalyst.builder().log(logSkewed).model(modelB).nodeId("node-b-skewed")
                .clock(Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1))).distributed()
                .task(GatedTask.class, GatedTask::new).build();
        try {
            ExecutionHandle<String> stalled = nodeA.execute(new GatedTask(reachedGate, openGate),
                    ExecutionOptions.withKey("ticket-1042"));
            assertThat(reachedGate.await(10, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = stalled.id();
            assertThat(logB.findByKey("ticket-1042")).contains(id);
            assertThat(logB.readLease(id).orElseThrow().holder()).isEqualTo("node-a");

            // A holds a live lease: B's scan leaves the execution alone, and B's direct resume is refused.
            RecoveryReport report = nodeB.recoverAll();
            assertThat(report.leftAlone()).containsEntry(id, Status.RUNNING);
            assertThat(report.resumed()).isEmpty();
            assertThatThrownBy(() -> nodeB.resume(id)).isInstanceOf(LeaseHeldException.class);
            assertThat(modelB.callCount()).isZero();

            // To the skewed node A's lease has lapsed, so its scan takes the execution over.
            RecoveryReport takeover = nodeSkewed.recoverAll();
            assertThat(takeover.resumed()).containsOnlyKeys(id);
            assertThat(takeover.resumed().get(id).result()).isEqualTo("A|B");
            assertThat(modelB.callCount()).as("step 1 substituted from node A's record; only step 2 ran").isEqualTo(1);
            List<SequencedEvent> history = logB.read(id);

            openGate.countDown();
            assertThatThrownBy(stalled::result)
                    .satisfies(e -> assertThat(e).isInstanceOfAny(StaleWriterException.class, LeaseLostException.class));
            assertThat(modelA.callCount()).as("node A was stopped before its step-2 call").isEqualTo(1);

            assertThat(logA.read(id)).as("node A recorded nothing after the takeover").isEqualTo(history);
            assertThat(nodeA.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(logA.openExecutions()).doesNotContain(id);
        } finally {
            openGate.countDown();
            nodeSkewed.close();
            nodeB.close();
            nodeA.close();
        }
    }

    // --- cluster discovery: FdbSupport's rule (catalyst-gumbo tests), under the same variable --------

    private static Boolean available;

    private static synchronized boolean clusterAvailable() {
        if (available == null) available = probe();
        return available;
    }

    /** Time-bounded: a client facing an unreachable cluster retries forever rather than failing. */
    private static boolean probe() {
        try {
            CompletableFuture.runAsync(() -> GumboEventLog.foundationDB(System.getenv("FDB_CLUSTER_FILE"),
                    "catalyst-probe-" + UUID.randomUUID(), BlobStore.inMemory()).close()).get(20, TimeUnit.SECONDS);
            return true;
        } catch (Throwable e) {
            if ("true".equalsIgnoreCase(System.getenv("CATALYST_REQUIRE_FDB"))) {
                throw new AssertionError("CATALYST_REQUIRE_FDB=true but no FoundationDB cluster is reachable"
                        + " (FDB_CLUSTER_FILE=" + System.getenv("FDB_CLUSTER_FILE") + "); refusing to skip", e);
            }
            return false;
        }
    }
}
