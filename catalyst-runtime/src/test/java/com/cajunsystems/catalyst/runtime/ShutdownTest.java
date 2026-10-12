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
        assertThatThrownBy(handle::result).isNotNull(); // its completion could not be written
        assertThat(fold(log.delegate, handle.id()).status())
                .as("the late worker's failed writes left nothing terminal").isEqualTo(Status.RUNNING);
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

    /**
     * A task that swallows the abandon signal with a {@code catch (RuntimeException)} fallback and
     * returns normally must not be recorded as completed: that result came from the shutdown, and
     * the boundary that raised it was never recorded. It is abandoned like any other.
     */
    @Test
    void aSwallowedAbandonSignalIsNotRecordedAsACompletion() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inStep1 = new CountDownLatch(1);
        MockModel model = model(() -> {
            inStep1.countDown();
            while (!current.get().isShuttingDown()) Thread.onSpinWait();
        });
        Task<String> withFallback = ctx -> {
            String summary = ctx.model().complete(STEP1).message();
            String answer;
            try {
                answer = ctx.model().complete(STEP2).message();
            } catch (RuntimeException e) {
                answer = "fallback";
            }
            return summary + "|" + answer;
        };
        CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build();
        current.set(runtime);
        ExecutionHandle<String> handle = runtime.execute(withFallback);
        assertThat(inStep1.await(5, TimeUnit.SECONDS)).isTrue();
        runtime.close();

        assertThatThrownBy(handle::result).isInstanceOf(ExecutionAbandonedSignal.class);
        assertThat(fold(log, handle.id()).status()).as("not terminal: the next start resumes it").isEqualTo(Status.RUNNING);
        assertThat(log.read(handle.id())).extracting(SequencedEvent::event)
                .noneMatch(e -> e instanceof CatalystEvent.ExecutionCompleted);
    }

    /** The same for cancel: a swallowed cancel still folds to CANCELLED, not COMPLETED. */
    @Test
    void aSwallowedCancelSignalStillFoldsToCancelled() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        CountDownLatch entered = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        Task<String> withFallback = ctx -> {
            entered.countDown();
            // Uninterruptible on purpose: cancel() also interrupts, and the case under test is a
            // task that reaches its boundary and swallows the signal there.
            while (!cancelled.get()) Thread.onSpinWait();
            try {
                return ctx.model().complete(STEP1).message();
            } catch (RuntimeException e) {
                return "fallback";
            }
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x")).build()) {
            ExecutionHandle<String> handle = runtime.execute(withFallback);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            runtime.cancel(handle.id());
            cancelled.set(true);

            assertThatThrownBy(handle::result).isInstanceOf(java.util.concurrent.CancellationException.class);
            assertThat(fold(log, handle.id()).status()).isEqualTo(Status.CANCELLED);
        }
    }

    /**
     * A cancel issued while the runtime is shutting down must stay a cancel. cancel() interrupts the
     * worker; here the interrupt surfaces as an ordinary exception inside an effect boundary, which
     * neither cancel check catches. If shutdown abandoned it, the execution would be left RUNNING and
     * the next start would resume and complete work the user cancelled.
     */
    @Test
    void aCancelDuringShutdownIsNotUndoneByTheNextStart() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        CountDownLatch inEffect = new CountDownLatch(1);
        Task<String> blocking = ctx -> ctx.effect("slow-io", () -> {
            inEffect.countDown();
            try {
                Thread.sleep(Duration.ofMinutes(5));
            } catch (InterruptedException e) {
                throw new IllegalStateException("I/O interrupted", e);
            }
            return "never";
        });
        CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("x")).drainTimeout(Duration.ofSeconds(10)).build();
        ExecutionHandle<String> handle = runtime.execute(blocking);
        assertThat(inEffect.await(5, TimeUnit.SECONDS)).isTrue();

        Thread closer = Thread.ofVirtual().start(runtime::close);
        waitFor(runtime::isShuttingDown);
        runtime.cancel(handle.id());
        closer.join(10_000);

        assertThatThrownBy(handle::result).isNotInstanceOf(ExecutionAbandonedSignal.class);
        assertThat(fold(log, handle.id()).isTerminal()).as("recorded terminal, not left for recovery").isTrue();
        assertThat(log.openExecutions()).doesNotContain(handle.id());
    }

    /**
     * A provider call that fails during the drain is a genuine, known failure — not something to
     * abandon. The model boundary records CompletionRequested before calling the provider; abandoning
     * the failure would leave that request dangling, and the restart would treat it as in doubt and
     * fail the execution terminally under InDoubtPolicy.FAIL. With retry budget, the failure is
     * recorded as RetryRequested and the retry runs on the next start.
     */
    @Test
    void aModelFailureDuringTheDrainIsRecordedNotLeftInDoubt() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inStep2 = new CountDownLatch(1);
        AtomicInteger step2Calls = new AtomicInteger();
        MockModel model = MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream().filter(m -> m.role() == Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            if (last.contains("summarize")) return new Completion("SUMMARY", List.of(), new Usage(1, 1), "stop");
            if (step2Calls.getAndIncrement() == 0) {
                inStep2.countDown();
                while (!current.get().isShuttingDown()) Thread.onSpinWait();
                throw new IllegalStateException("provider 503");   // fails during the drain
            }
            return new Completion("FINAL", List.of(), new Usage(1, 1), "stop");
        }).build();

        CatalystRuntime first = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofSeconds(30))).build();
        current.set(first);
        ExecutionHandle<String> handle = first.execute(new TwoStepTask());
        assertThat(inStep2.await(5, TimeUnit.SECONDS)).isTrue();
        first.close();

        ExecutionId id = handle.id();
        assertThat(log.read(id).getLast().event())
                .as("the failed request is closed by a verdict, not left dangling")
                .isInstanceOf(CatalystEvent.RetryRequested.class);
        assertThat(fold(log, id).status()).isEqualTo(Status.RUNNING);

        try (CatalystRuntime restarted = CatalystRuntime.builder()
                .log(log).model(model).task(new TwoStepTask()).recoverOnStart(true).build()) {
            current.set(restarted);
            assertThat(restarted.startupRecovery().orElseThrow().resumed().get(id).result())
                    .as("no InDoubtException, no pause: the retry runs").isEqualTo("SUMMARY|FINAL");
        }
    }

    /** Without retry budget the same failure is a plain ExecutionFailed with its real cause — not in doubt. */
    @Test
    void aModelFailureDuringTheDrainWithoutRetryBudgetFailsWithItsRealCause() throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inCall = new CountDownLatch(1);
        MockModel model = MockModel.builder().respond(req -> {
            inCall.countDown();
            while (!current.get().isShuttingDown()) Thread.onSpinWait();
            throw new IllegalStateException("provider 503");
        }).build();
        CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build();
        current.set(runtime);
        ExecutionHandle<String> handle = runtime.execute(new TwoStepTask());
        assertThat(inCall.await(5, TimeUnit.SECONDS)).isTrue();
        runtime.close();

        ExecutionState state = fold(log, handle.id());
        assertThat(state.status()).isEqualTo(Status.FAILED);
        assertThat(state.error()).contains("provider 503").doesNotContain("InDoubt");
    }

    /**
     * The other ordering: shutdown's abandon signal is raised first and swallowed by the task's
     * fallback, and the cancel arrives while that fallback runs. Whether the task then returns
     * normally or rethrows, the cancel must win — not be undone by the next start (return path), and
     * not be recorded as FAILED with the abandon message (throw path).
     */
    @Test
    void aCancelAfterASwallowedAbandonSignalStillWinsWhenTheTaskReturns() throws Exception {
        assertCancelAfterSwallowedAbandonWins(false);
    }

    @Test
    void aCancelAfterASwallowedAbandonSignalStillWinsWhenTheTaskRethrows() throws Exception {
        assertCancelAfterSwallowedAbandonWins(true);
    }

    private void assertCancelAfterSwallowedAbandonWins(boolean rethrow) throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        AtomicReference<CatalystRuntime> current = new AtomicReference<>();
        CountDownLatch inStep1 = new CountDownLatch(1);
        CountDownLatch swallowed = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        MockModel model = model(() -> {
            inStep1.countDown();
            while (!current.get().isShuttingDown()) Thread.onSpinWait();
        });
        Task<String> fallback = ctx -> {
            String summary = ctx.model().complete(STEP1).message();
            try {
                return summary + "|" + ctx.model().complete(STEP2).message(); // raises abandon here
            } catch (RuntimeException e) {
                swallowed.countDown();
                while (!cancelled.get()) Thread.onSpinWait(); // the cancel lands during the fallback
                if (rethrow) throw e;
                return summary + "|fallback";
            }
        };
        CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build();
        current.set(runtime);
        ExecutionHandle<String> handle = runtime.execute(fallback);
        assertThat(inStep1.await(5, TimeUnit.SECONDS)).isTrue();

        Thread closer = Thread.ofVirtual().start(runtime::close);
        assertThat(swallowed.await(5, TimeUnit.SECONDS)).isTrue();
        runtime.cancel(handle.id());
        cancelled.set(true);
        closer.join(10_000);

        assertThatThrownBy(handle::result).isInstanceOf(java.util.concurrent.CancellationException.class);
        assertThat(fold(log, handle.id()).status()).isEqualTo(Status.CANCELLED);
        assertThat(log.openExecutions()).doesNotContain(handle.id());
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
