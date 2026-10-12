package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.events.FileBlobStore;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import com.cajunsystems.catalyst.runtime.LeaseLostException;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The Dist-4 exit demo: several Catalyst nodes on one FoundationDB-backed log behave as one system,
 * with no coordinator. Two processes, like M0's crash demo:
 *
 * <pre>
 *   Demo &lt;root&gt; distributed-a   # node A: records step 1, then dies (kill -9, exit 137)
 *   Demo &lt;root&gt; distributed-b   # node B and friends: takeover, zombie, cross-node cancel
 * </pre>
 *
 * {@code <root>} is the FoundationDB subspace both processes share; the cluster file comes from
 * {@code FDB_CLUSTER_FILE}. Payloads offload to a blob directory both processes can read (one machine
 * here; shared storage in production). Every node opens its own log, so each is a separate client of
 * the cluster even when two share a process.
 */
final class DistributedDemo {

    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());
    private static final String KEY = "ticket-1042";
    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration HEARTBEAT = Duration.ofSeconds(1);

    private DistributedDemo() {}

    /**
     * Two model steps. In node A's process {@link #afterStepOne} kills the JVM between them. A gated
     * instance parks there instead, swallowing the interrupt a cancel delivers so the cancel is
     * observed at step 2's boundary. Instances built from the registry are never gated.
     */
    static final class TwoStepTask implements Task<String> {
        static volatile Runnable afterStepOne = () -> { };
        private final CountDownLatch reachedGate;
        private final CountDownLatch openGate;

        TwoStepTask(CountDownLatch reachedGate, CountDownLatch openGate) {
            this.reachedGate = reachedGate;
            this.openGate = openGate;
        }

        TwoStepTask() {
            this(null, null);
        }

        @Override public String execute(Context ctx) {
            String one = ctx.model().complete(STEP1).message();
            afterStepOne.run();
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

    private static GumboEventLog log(String root) {
        Path blobs = Path.of(System.getProperty("java.io.tmpdir"), "catalyst-distributed-demo-" + root);
        return GumboEventLog.foundationDB(System.getenv("FDB_CLUSTER_FILE"), root, FileBlobStore.at(blobs));
    }

    private static CatalystRuntime.Builder node(String root, String id, MockModel model, Clock clock) {
        return Catalyst.builder().log(log(root)).model(model).clock(clock)
                .nodeId(id).distributed()
                .lease(LEASE, HEARTBEAT).claimInterval(Duration.ofMillis(500))
                .task(TwoStepTask.class, TwoStepTask::new);
    }

    /** Process 1: node A takes the job, records step 1, and dies before step 2. */
    static void nodeA(String root) {
        MockModel model = MockModel.alwaysReturn("A");
        TwoStepTask.afterStepOne = () -> {
            System.out.println("[node-a] step 1 recorded (model calls: " + model.callCount() + "); dying (kill -9)...");
            Runtime.getRuntime().halt(137);
        };
        CatalystRuntime nodeA = node(root, "node-a", model, Clock.systemUTC()).build();
        nodeA.execute(new TwoStepTask(), ExecutionOptions.withKey(KEY)).result();
        throw new AssertionError("node A should have died after step 1");
    }

    /** Process 2: the rest of the cluster. */
    static void rest(String root) throws Exception {
        MockModel modelB = MockModel.alwaysReturn("B");
        try (CatalystRuntime nodeB = node(root, "node-b", modelB, Clock.systemUTC()).build()) {
            takeover(nodeB, modelB);
            zombie(root);
            crossNodeCancel(root, nodeB);
        }
        System.out.println("[distributed] Dist-4 exit criterion holds: a dead node's work was taken over with zero"
                + " duplicate model calls, a stale node's late write was refused, and a cancel issued on one node"
                + " landed through the node running the execution.");
    }

    /** Nobody calls recoverAll: node B's claim loop notices node A's lease lapse and takes the job. */
    private static void takeover(CatalystRuntime nodeB, MockModel modelB) {
        ExecutionId id = nodeB.log().findByKey(KEY).orElseThrow(() -> new AssertionError("node A recorded nothing"));
        System.out.println("[node-b] found " + id.value() + " in the shared index, leased by '"
                + nodeB.log().readLease(id).map(l -> l.holder()).orElse("nobody") + "'; waiting for its lease to lapse");
        await(Duration.ofSeconds(30), () -> nodeB.inspect(id).status() == Status.COMPLETED);
        String result = String.valueOf(nodeB.resume(id).result());
        System.out.println("[node-b] took over and finished it: result " + result);
        System.out.println("[node-b] model calls on node-b: " + modelB.callCount()
                + " (step 1 was substituted from node A's record; only step 2 ran)");
        check(result.equals("A|B"), "expected A|B, got " + result);
        check(modelB.callCount() == 1, "expected exactly one model call on node-b");
    }

    /**
     * Node Z stalls holding its lease. Node C's clock runs a minute ahead, so to C the lease has
     * lapsed: the same as Z pausing past it. C takes the job over. When Z wakes, its write is refused.
     */
    private static void zombie(String root) throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        MockModel modelZ = MockModel.alwaysReturn("Z");
        try (CatalystRuntime nodeZ = node(root, "node-z", modelZ, Clock.systemUTC()).build()) {
            ExecutionHandle<String> stalled = nodeZ.execute(new TwoStepTask(reached, open), ExecutionOptions.withKey("ticket-2"));
            check(reached.await(10, TimeUnit.SECONDS), "node Z never reached its stall");
            List<SequencedEvent> history;
            try (CatalystRuntime nodeC = node(root, "node-c", MockModel.alwaysReturn("C"),
                    Clock.offset(Clock.systemUTC(), Duration.ofMinutes(1))).build()) {
                await(Duration.ofSeconds(15), () -> nodeC.inspect(stalled.id()).status() == Status.COMPLETED);
                history = nodeC.log().read(stalled.id());
                System.out.println("[node-c] took over the stalled job: " + nodeC.resume(stalled.id()).result());
            }
            open.countDown();
            String refusal;
            try {
                stalled.result(Duration.ofSeconds(15));
                throw new AssertionError("node Z's late write was accepted");
            } catch (StaleWriterException | LeaseLostException e) {
                refusal = e.getClass().getSimpleName();
            }
            check(nodeZ.log().read(stalled.id()).equals(history), "node Z changed the history after the takeover");
            check(modelZ.callCount() == 1, "node Z made its step-2 call");
            System.out.println("[node-z] woke up; its late write was refused (" + refusal + "), history unchanged");
        } finally {
            open.countDown();
        }
    }

    /** Node X is running a job; node B cancels it. X records the cancel; B never writes to X's stream. */
    private static void crossNodeCancel(String root, CatalystRuntime nodeB) throws Exception {
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        try (CatalystRuntime nodeX = node(root, "node-x", MockModel.alwaysReturn("X"), Clock.systemUTC()).build()) {
            ExecutionHandle<String> running = nodeX.execute(new TwoStepTask(reached, open), ExecutionOptions.withKey("ticket-3"));
            check(reached.await(10, TimeUnit.SECONDS), "node X never started");
            int before = nodeB.log().read(running.id()).size();
            nodeB.cancel(running.id());
            check(nodeB.log().read(running.id()).size() == before, "node B wrote to node X's stream");
            try {
                running.result(Duration.ofSeconds(15));
                throw new AssertionError("the cancel did not land");
            } catch (CancellationException expected) {
                // recorded by node X at its next renewal
            }
            Status status = nodeB.inspect(running.id()).status();
            check(status == Status.CANCELLED, "expected CANCELLED, got " + status);
            System.out.println("[distributed] cancel from node-b landed via node-x: " + status);
        } finally {
            open.countDown();
        }
    }

    private static void await(Duration limit, BooleanSupplier condition) throws RuntimeException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!condition.getAsBoolean()) {
            check(System.nanoTime() < deadline, "timed out after " + limit);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static void check(boolean condition, String failure) {
        if (!condition) throw new AssertionError("FAIL: " + failure);
    }
}
