package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.FencedEventLog;
import com.cajunsystems.catalyst.log.Lease;
import com.cajunsystems.catalyst.log.Snapshot;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.log.WorkQueue;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.Worker;
import com.cajunsystems.catalyst.runtime.WorkerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The v1 distributed-execution exit demo: work is <em>submitted</em> to a shared queue rather than
 * run in the submitting process, and independent workers claim it, run it, and take over each
 * other's executions when one dies.
 *
 * <p>Each test pins one of the three properties the design actually rests on:
 *
 * <ol>
 *   <li>competing workers run each submitted execution <strong>once</strong>;
 *   <li>a worker that lost its claim is <strong>rejected by storage</strong>, not merely asked not to
 *       write;
 *   <li>a dead worker's execution is <strong>resumed</strong>, not restarted.
 * </ol>
 *
 * <p>The two workers are given <em>separate runtimes</em> over one shared log. That is the point of
 * the fixture rather than an incidental detail: a single runtime already prevents a second
 * concurrent attempt with {@code KeyedLock} plus its in-flight set, so two workers inside one
 * runtime would pass property 1 with the lease removed entirely and prove nothing about it. Separate
 * runtimes have separate in-flight sets, which is as close as one JVM gets to two processes, and it
 * is the configuration in which the claim is the only thing standing between the workers.
 */
class DistributedAcceptanceTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("step two").build());

    /** A named two-step task, so a worker can reconstruct it from the recorded task type. */
    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) throws Exception {
            String a = ctx.model().complete(STEP1).message();
            String b = ctx.model().complete(STEP2).message();
            return a + "|" + b;
        }
    }

    // ---------------------------------------------------------------- property 1

    @Test
    void twoWorkersRunEachSubmittedExecutionExactlyOnce(@TempDir Path dir) throws Exception {
        MockModel model = MockModel.alwaysReturn("OK");
        try (GumboEventLog shared = GumboEventLog.at(dir)) {
            CatalystRuntime a = runtimeOver(shared, model);
            CatalystRuntime b = runtimeOver(shared, model);

            List<ExecutionId> submitted = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                submitted.add(a.submit("default", new TwoStepTask()));
            }

            // Nothing has run: submitting records the execution, it does not execute it.
            assertThat(model.callCount()).isZero();
            for (ExecutionId id : submitted) {
                assertThat(a.inspect(id).status()).isNotEqualTo(Status.COMPLETED);
            }

            try (Worker wa = a.worker("default", fast("node-a")).start();
                 Worker wb = b.worker("default", fast("node-b")).start()) {
                for (ExecutionId id : submitted) {
                    awaitTerminal(a, shared, id);
                }
            }

            for (ExecutionId id : submitted) {
                ExecutionState state = a.inspect(id);
                assertThat(state.status()).isEqualTo(Status.COMPLETED);

                // The load-bearing assertion. Each worker that runs an execution appends exactly one
                // ExecutionResumed, so a second worker having also run it is visible as a second one
                // -- which is what a lease that did not exclude, or excluded only by luck, would
                // leave behind. Counting model calls alone would not catch it: a duplicate run
                // substitutes the recorded prefix and can complete without calling the model again.
                assertThat(startsOf(shared, id))
                        .as("execution %s should have been run by exactly one worker", id)
                        .isEqualTo(1);
            }

            // Two steps per execution, run once each. Any duplicate live run shows up here too.
            assertThat(model.callCount()).isEqualTo(8 * 2);
        }
    }

    // ---------------------------------------------------------------- property 2

    /**
     * The guarantee the whole design leans on: a worker whose view of the stream is stale is refused
     * by storage. Asserted at the seam rather than by racing two workers, because a race that
     * happens to interleave the right way on one run is not evidence -- this is deterministic, and
     * it fails if the fence is ever quietly dropped.
     */
    @Test
    void aWorkerThatLostItsClaimIsRejectedByStorage(@TempDir Path dir) throws Exception {
        MockModel model = MockModel.alwaysReturn("OK");
        try (GumboEventLog shared = GumboEventLog.at(dir)) {
            CatalystRuntime a = runtimeOver(shared, model);
            ExecutionId id = a.submit("default", new TwoStepTask());

            // A worker attaches at the tip it can see: one event, so its next append is seq 1.
            EventLog zombie = FencedEventLog.forAttempt(shared, id, /* tipSeq */ 0);

            // Meanwhile the execution is claimed and run to completion by somebody else.
            try (Worker w = a.worker("default", fast("node-live")).start()) {
                awaitTerminal(a, id);
            }
            assertThat(a.inspect(id).status()).isEqualTo(Status.COMPLETED);

            long tipAfter = shared.latestSeq(id);
            assertThat(tipAfter).isGreaterThan(0);

            // The zombie now writes, still believing the stream is where it left it.
            assertThatThrownBy(() -> zombie.append(id, new CatalystEvent.ExecutionResumed(Instant.now(), 99)))
                    .isInstanceOf(StaleWriterException.class)
                    .satisfies(e -> {
                        StaleWriterException s = (StaleWriterException) e;
                        assertThat(s.executionId()).isEqualTo(id);
                        assertThat(s.expectedSeq()).isEqualTo(1);
                    });

            // Rejected means rejected: the stream is untouched, so it still folds to the real outcome.
            assertThat(shared.latestSeq(id)).isEqualTo(tipAfter);
            assertThat(a.inspect(id).status()).isEqualTo(Status.COMPLETED);
        }
    }

    // ---------------------------------------------------------------- property 3

    /**
     * Reclaim, and specifically that it is a <em>resume</em>. A conventional queue redelivers a dead
     * worker's message and the handler starts from the top; Catalyst substitutes the recorded prefix
     * and continues at the boundary it reached. The evidence is the model call count: the first step
     * was already recorded before the "dead" worker stopped, so a reclaiming worker that restarted
     * the task would call the model twice more, and one that resumed calls it once.
     */
    @Test
    void aDeadWorkersExecutionIsReclaimedAndResumedRatherThanRestarted(@TempDir Path dir) throws Exception {
        MockModel model = MockModel.alwaysReturn("OK");
        try (GumboEventLog shared = GumboEventLog.at(dir)) {
            CatalystRuntime a = runtimeOver(shared, model);
            ExecutionId id = a.submit("default", new TwoStepTask());

            WorkQueue queue = shared.workQueue("default").orElseThrow();

            // A worker claims the execution and gets one step done, then dies -- no release, no
            // renewal, exactly what a killed process leaves behind.
            Duration ttl = Duration.ofMillis(400);
            Optional<Lease> deadClaim = queue.claim(id, "node-dead", ttl, Instant.now());
            assertThat(deadClaim).isPresent();
            recordFirstStepOf(shared, model, id);
            assertThat(model.callCount()).isEqualTo(1);

            // While that lease is live, another worker must not touch it.
            assertThat(queue.claim(id, "node-live", ttl, Instant.now())).isEmpty();

            try (Worker live = a.worker("default",
                    fast("node-live").withLease(ttl, ttl.dividedBy(4))).start()) {
                awaitTerminal(a, id);
            }

            assertThat(a.inspect(id).status()).isEqualTo(Status.COMPLETED);
            // Resumed, not restarted: step 1 was substituted from the log, only step 2 ran live.
            assertThat(model.callCount())
                    .as("a reclaimed execution resumes at its recorded boundary")
                    .isEqualTo(2);
        }
    }

    // ---------------------------------------------------------------- fixture

    /**
     * Drives the task's first step through the real recording path and then stops, leaving the
     * execution durably mid-flight -- recorded, non-terminal, and nobody running it.
     *
     * <p>It uses {@code ReplayingContext} directly rather than the runtime because the runtime has
     * no way to stop in the middle: every exit it offers is terminal. A task that threw would record
     * {@code ExecutionFailed} and the execution would be finished, not stranded, and property 3
     * would then be testing that a terminal execution stays terminal. What a killed process actually
     * leaves is a prefix with no ending, which is what this writes.
     */
    private void recordFirstStepOf(EventLog log, MockModel model, ExecutionId id) {
        log.append(id, new CatalystEvent.ExecutionStarted(Instant.now(), 1, "node-dead"));
        ExecutionInfo info = new ExecutionInfo(id, 1, TwoStepTask.class.getName(), Map.of());
        ReplayingContext ctx = new ReplayingContext(id, log, model, info, Map.of(),
                EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                ReplayMode.STRICT, null, Clock.systemUTC(),
                LoggerFactory.getLogger("dead-node"), log.read(id), true);
        ctx.model().complete(STEP1);
    }

    private static WorkerConfig fast(String nodeId) {
        return WorkerConfig.defaults()
                .withNodeId(nodeId)
                .withLease(Duration.ofSeconds(5), Duration.ofMillis(500))
                .withPollInterval(Duration.ofMillis(20));
    }

    private CatalystRuntime runtimeOver(GumboEventLog shared, MockModel model) {
        return Catalyst.builder()
                .log(new SharedLog(shared))
                .model(model)
                .task(new TwoStepTask())
                .build();
    }

    private static void awaitTerminal(CatalystRuntime runtime, ExecutionId id) throws Exception {
        awaitTerminal(runtime, null, id);
    }

    /**
     * Waits for {@code id} to finish, and on timeout reports the execution's recorded events rather
     * than just its status. A stuck execution is diagnosed almost entirely from where its stream
     * stops, and "did not reach a terminal state: STARTING" on its own sends you back to reproduce
     * the run just to find that out.
     */
    private static void awaitTerminal(CatalystRuntime runtime, EventLog log, ExecutionId id)
            throws Exception {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            if (runtime.inspect(id).isTerminal()) return;
            Thread.sleep(10);
        }
        String events = log == null ? "" : log.read(id).stream()
                .map(se -> se.seq() + ":" + se.event().getClass().getSimpleName())
                .collect(Collectors.joining(", ", " [", "]"));
        throw new AssertionError("execution " + id + " did not reach a terminal state: "
                + runtime.inspect(id).status() + events);
    }

    /** How many times an execution was started or resumed -- i.e. how many workers ran it. */
    private static int startsOf(EventLog log, ExecutionId id) {
        int n = 0;
        for (SequencedEvent se : log.read(id)) {
            if (se.event() instanceof CatalystEvent.ExecutionStarted
                    || se.event() instanceof CatalystEvent.ExecutionResumed) {
                n++;
            }
        }
        return n;
    }

    /**
     * The shared log, minus ownership of it.
     *
     * <p>Two runtimes are pointed at one log here, and each would otherwise close it when it closed
     * -- so the first runtime to shut down would pull the store out from under the second. The test
     * owns the real log in its try-with-resources and hands each runtime a view that cannot close it.
     */
    private record SharedLog(EventLog delegate) implements EventLog {
        @Override public long append(ExecutionId id, CatalystEvent e) { return delegate.append(id, e); }
        @Override public long append(ExecutionId id, CatalystEvent e, long expectedSeq) {
            return delegate.append(id, e, expectedSeq);
        }
        @Override public boolean supportsConditionalAppend() { return delegate.supportsConditionalAppend(); }
        @Override public boolean supportsMultiWriter() { return delegate.supportsMultiWriter(); }
        @Override public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        @Override public List<SequencedEvent> readFrom(ExecutionId id, long after) {
            return delegate.readFrom(id, after);
        }
        @Override public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        @Override public Optional<Snapshot> readSnapshot(ExecutionId id) { return delegate.readSnapshot(id); }
        @Override public void writeSnapshot(ExecutionId id, Snapshot s) { delegate.writeSnapshot(id, s); }
        @Override public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        @Override public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        @Override public Optional<WorkQueue> workQueue(String name) { return delegate.workQueue(name); }
        @Override public void close() { /* the test owns the log */ }
    }
}
