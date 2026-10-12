package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionAbandonedSignal;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.Usage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Graceful shutdown: {@code close()} abandons running attempts rather than failing them, so the next
 * start resumes them. The property under test throughout is what the log is left holding — nothing
 * terminal, and no dangling request.
 */
class ShutdownTest {

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.model().complete(STEP1).message() + "|" + ctx.model().complete(STEP2).message();
        }
    }

    /**
     * The worker is inside step 1's model call when shutdown begins. It is allowed to finish that call
     * and record it, then unwinds at step 2's boundary before recording anything — so the restart
     * substitutes step 1 and makes exactly one more call.
     */
    @Test
    void aRunningExecutionIsAbandonedAtItsNextBoundaryAndResumedOnRestart() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog(); // close() is a no-op, so it survives the "restart"
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inStep1 = new CountDownLatch(1);
        MockModel model = model(() -> {
            inStep1.countDown();
            // Hold the call open until shutdown has begun, so the boundary straddles it.
            while (!current.get().isShuttingDown()) Thread.onSpinWait();
        });

        CatalystRuntime first = CatalystRuntime.builder().log(log).model(model).build();
        current.set(first);
        ExecutionHandle<String> handle = first.execute(new TwoStepTask());
        assertThat(inStep1.await(5, TimeUnit.SECONDS)).isTrue();
        first.close();

        assertThatThrownBy(handle::result).isInstanceOf(ExecutionAbandonedSignal.class);
        ExecutionId id = handle.id();
        assertThat(model.callCount()).as("the in-flight call was allowed to finish").isEqualTo(1);
        assertThat(fold(log, id).status()).as("left non-terminal").isEqualTo(Status.RUNNING);
        assertThat(log.read(id)).extracting(SequencedEvent::event)
                .as("nothing appended on the way out")
                .noneMatch(e -> e instanceof CatalystEvent.ExecutionFailed || e instanceof CatalystEvent.ExecutionCancelled);
        assertThat(log.read(id).getLast().event())
                .as("step 1 completed and recorded; step 2 never requested — nothing in doubt")
                .isInstanceOf(CatalystEvent.CompletionReceived.class);
        assertThat(log.openExecutions()).containsExactly(id);

        try (CatalystRuntime restarted = CatalystRuntime.builder()
                .log(log).model(model).task(new TwoStepTask()).recoverOnStart(true).build()) {
            current.set(restarted);
            RecoveryReport report = restarted.startupRecovery().orElseThrow();
            assertThat(report.resumed()).containsOnlyKeys(id);
            assertThat(report.resumed().get(id).result()).isEqualTo("SUMMARY|FINAL");
            assertThat(model.callCount()).as("step 1 substituted; only step 2 ran live").isEqualTo(2);
        }
    }

    @Test
    void newWorkIsRefusedOnceShutdownBegins() {
        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(new InMemoryEventLog()).model(MockModel.alwaysReturn("x")).task(new TwoStepTask()).build();
        ExecutionId done = runtime.execute(new TwoStepTask()).id();
        runtime.close();

        assertThat(runtime.isShuttingDown()).isTrue();
        assertThatThrownBy(() -> runtime.execute(new TwoStepTask()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("shutting down");
        assertThatThrownBy(() -> runtime.resume(done))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("shutting down");
        runtime.close(); // idempotent
    }

    /**
     * A worker that reaches no boundary cannot be unwound; the drain timeout bounds the wait and the
     * log is closed under it. Its late completion then fails to append, and must not be turned into a
     * terminal failure either.
     */
    @Test
    void theDrainTimeoutBoundsTheWaitAndALateWorkerLeavesNothingTerminal() throws Exception {
        CloseableLog log = new CloseableLog();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Task<String> stuck = ctx -> {
            entered.countDown();
            release.await(); // compute with no boundary: nothing for abandon to observe
            return "late";
        };
        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x")).drainTimeout(Duration.ofMillis(200)).build();
        ExecutionHandle<String> handle = runtime.execute(stuck);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        long t0 = System.nanoTime();
        runtime.close();
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
        assertThat(log.closed).isTrue();

        release.countDown();
        assertThatThrownBy(handle::result).isInstanceOf(ExecutionAbandonedSignal.class);
        assertThat(fold(log.delegate, handle.id()).status()).isEqualTo(Status.RUNNING);
    }

    /**
     * Shutdown during a retry backoff: abandon does not interrupt, so the sleep must notice it on its
     * own. RetryRequested is already durable, so the restart resumes with the retry budget intact.
     */
    @Test
    void anAbandonedBackoffWakesPromptlyAndResumesOnRestart() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch failedOnce = new CountDownLatch(1);
        Task<String> flaky = new FlakyTask(attempts, failedOnce);

        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x"))
                .retryPolicy(RetryPolicy.maxRetries(5, Duration.ofHours(1)))
                .build();
        ExecutionHandle<String> handle = runtime.execute(flaky);
        assertThat(failedOnce.await(5, TimeUnit.SECONDS)).isTrue();
        waitFor(() -> log.read(handle.id()).stream().anyMatch(e -> e.event() instanceof CatalystEvent.RetryRequested));

        long t0 = System.nanoTime();
        runtime.close();
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).as("did not sleep out the hour").isLessThan(Duration.ofSeconds(5));
        assertThatThrownBy(handle::result).isInstanceOf(ExecutionAbandonedSignal.class);
        ExecutionState state = fold(log, handle.id());
        assertThat(state.status()).isEqualTo(Status.RUNNING);
        assertThat(state.retries()).isEqualTo(1);

        try (CatalystRuntime restarted = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x"))
                .task(FlakyTask.class.getName(), () -> new FlakyTask(attempts, failedOnce))
                .recoverOnStart(true).build()) {
            assertThat(restarted.startupRecovery().orElseThrow().resumed().get(handle.id()).result()).isEqualTo("ok");
        }
    }

    /**
     * close() is a barrier for every caller: a second, concurrent close() must not return while the
     * first is still draining with the log open — try-with-resources plus a shutdown hook would
     * otherwise race the directory lock.
     */
    @Test
    void aConcurrentCloseReturnsOnlyOnceTheLogIsClosed() throws Exception {
        CloseableLog log = new CloseableLog();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x")).drainTimeout(Duration.ofSeconds(1)).build();
        runtime.execute(ctx -> { entered.countDown(); release.await(); return "done"; });
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        Thread first = Thread.ofVirtual().start(runtime::close);
        waitFor(runtime::isShuttingDown);
        runtime.close();
        assertThat(log.closed).as("the second close() waited for the first to close the log").isTrue();

        release.countDown();
        first.join(5_000);
    }

    /** A task that closes its own runtime would otherwise wait out the whole drain on itself. */
    @Test
    void closeFromATaskThreadDoesNotWaitOnItself() throws Exception {
        AtomicReference<CatalystRuntime> self = new AtomicReference<>();
        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(new InMemoryEventLog()).model(MockModel.alwaysReturn("x"))
                .drainTimeout(Duration.ofSeconds(30)).build();
        self.set(runtime);
        ExecutionHandle<Long> handle = runtime.execute(ctx -> {
            long t0 = System.nanoTime();
            self.get().close();
            return System.nanoTime() - t0;
        });
        assertThat(Duration.ofNanos(handle.result(Duration.ofSeconds(10))))
                .as("returned without sitting out the 30s drain").isLessThan(Duration.ofSeconds(5));
        assertThat(runtime.isShuttingDown()).isTrue();
    }

    /** Fails its effect on the first attempt only. */
    static final class FlakyTask implements Task<String> {
        private final AtomicInteger attempts;
        private final CountDownLatch failedOnce;
        FlakyTask(AtomicInteger attempts, CountDownLatch failedOnce) { this.attempts = attempts; this.failedOnce = failedOnce; }
        @Override public String execute(Context ctx) {
            return ctx.effect("flaky", () -> {
                if (attempts.getAndIncrement() == 0) {
                    failedOnce.countDown();
                    throw new IllegalStateException("transient");
                }
                return "ok";
            });
        }
    }

    private static ExecutionState fold(EventLog log, ExecutionId id) {
        return com.cajunsystems.catalyst.engine.Reducer.fold(id, log.read(id));
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met in 5s");
            Thread.sleep(10);
        }
    }

    /** Content-based answers; {@code onStep1} runs inside the step-1 call. */
    private static MockModel model(Runnable onStep1) {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream().filter(m -> m.role() == Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            boolean step1 = last.contains("summarize");
            if (step1) onStep1.run();
            return new Completion(step1 ? "SUMMARY" : "FINAL", List.of(), new Usage(1, 1), "stop");
        }).build();
    }

    /** A log that, like a durable one, refuses writes once closed. */
    private static final class CloseableLog implements EventLog {
        final InMemoryEventLog delegate = new InMemoryEventLog();
        volatile boolean closed;

        public long append(ExecutionId id, CatalystEvent event) {
            if (closed) throw new IllegalStateException("log closed");
            return delegate.append(id, event);
        }
        public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        public void close() { closed = true; }
    }
}
