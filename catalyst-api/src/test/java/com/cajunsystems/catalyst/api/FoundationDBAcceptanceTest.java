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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Dist-0 exit criterion, end to end: two runtimes, <strong>each with its own log on one FoundationDB
 * root</strong> — two processes' worth of connections, sequencers and caches. Node A stalls
 * mid-execution; node B finds the execution through the shared index, resumes it with zero duplicate
 * model calls, and node A's late write is refused by the storage-side fence.
 *
 * <p>This is what Dist-4's claim loop will do automatically; here the test is the claim loop. Both
 * runtimes are built {@code distributed()}, so Dist-2's guard passes on this log. It
 * shows the log is ready for it: the index, the idempotency keys, the fence and the blob store all
 * span nodes.
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

    @Test
    void aSecondNodeRecoversAStalledExecutionAndTheFirstNodesLateWriteIsRefused() throws Exception {
        CountDownLatch reachedGate = new CountDownLatch(1);
        CountDownLatch openGate = new CountDownLatch(1);
        MockModel modelA = MockModel.alwaysReturn("A");
        MockModel modelB = MockModel.alwaysReturn("B");
        GumboEventLog logA = nodeLog();
        GumboEventLog logB = nodeLog();

        // Both nodes declare distributed(), so each build() checks the log is fenced and multi-writer (Dist-2).
        CatalystRuntime nodeA = Catalyst.builder().log(logA).model(modelA).nodeId("node-a").distributed().build();
        CatalystRuntime nodeB = Catalyst.builder().log(logB).model(modelB).nodeId("node-b").distributed()
                .task(GatedTask.class, GatedTask::new).build();
        assertThat(nodeA.isDistributed() && nodeB.isDistributed()).isTrue();
        try {
            ExecutionHandle<String> stalled = nodeA.execute(new GatedTask(reachedGate, openGate),
                    ExecutionOptions.withKey("ticket-1042"));
            assertThat(reachedGate.await(10, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = stalled.id();

            // Node B holds no id: it finds the work through the index node A wrote, and the key too.
            // This test plays the claim loop (Dist-4). A distributed runtime refuses recoverAll() until
            // leases exist, so B resumes the one open execution by id.
            assertThat(logB.findByKey("ticket-1042")).contains(id);
            assertThat(logB.openExecutions()).containsExactly(id);
            assertThat(nodeB.resume(id).result()).isEqualTo("A|B");
            assertThat(modelB.callCount()).as("step 1 substituted from node A's record; only step 2 ran").isEqualTo(1);
            List<SequencedEvent> history = logB.read(id);

            openGate.countDown();
            assertThatThrownBy(stalled::result).isInstanceOf(StaleWriterException.class);
            assertThat(modelA.callCount()).as("the fence refused node A before its step-2 call").isEqualTo(1);

            assertThat(logA.read(id)).as("node A's late write was refused by storage").isEqualTo(history);
            assertThat(nodeA.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(logA.openExecutions()).doesNotContain(id);
        } finally {
            openGate.countDown();
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
