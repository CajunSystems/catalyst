package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
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
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Startup recovery: a restarted runtime finds the executions its predecessor left unfinished and
 * resumes the ones a crash interrupted — and only those.
 */
class RecoveryTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    /** Named, so its recorded task type is stable and the registry can reconstruct it. */
    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.model().complete(STEP1).message() + "|" + ctx.model().complete(STEP2).message();
        }
    }

    @Test
    void resumesCrashedExecutionsAndOnlyThose() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = contentModel();
        ExecutionId crashedA = recordStepOne(log, model);
        ExecutionId crashedB = recordStepOne(log, model);
        ExecutionId paused = recordStepOne(log, model);
        log.append(paused, new CatalystEvent.ExecutionPaused(T, "operator"));
        ExecutionId unregistered = ExecutionId.random();
        log.append(unregistered, new CatalystEvent.ExecutionCreated(T, "com.example.RetiredTask", "h", "cfg", ""));
        log.append(unregistered, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
        ExecutionId completed = recordStepOne(log, model);
        log.append(completed, new CatalystEvent.ExecutionCompleted(T, null));
        int callsBeforeRestart = model.callCount();

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).task(new TwoStepTask()).build()) {

            RecoveryReport report = runtime.recoverAll();

            assertThat(report.resumed()).containsOnlyKeys(crashedA, crashedB);
            for (ExecutionHandle<?> h : report.resumed().values()) assertThat(h.result()).isEqualTo("SUMMARY|FINAL");
            assertThat(model.callCount() - callsBeforeRestart)
                    .as("step 1 substituted for both; only each step 2 ran live")
                    .isEqualTo(2);

            assertThat(report.leftAlone()).containsExactly(Map.entry(paused, Status.PAUSED));
            assertThat(runtime.inspect(paused).status()).as("a restart does not un-pause").isEqualTo(Status.PAUSED);

            assertThat(report.notRecovered()).containsOnlyKeys(unregistered);
            assertThat(report.notRecovered().get(unregistered)).contains("com.example.RetiredTask");

            assertThat(log.openExecutions()).containsExactly(paused, unregistered);
        }
    }

    /** Calling it on a live runtime must not schedule a second attempt for work already running. */
    @Test
    void anExecutionAlreadyRunningHereIsLeftAlone() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Task<String> blocking = ctx -> {
            entered.countDown();
            release.await();
            return ctx.model().complete(STEP1).message();
        };
        MockModel model = contentModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(new InMemoryEventLog()).model(model).task(new TwoStepTask()).build()) {

            ExecutionHandle<String> live = runtime.execute(blocking);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            RecoveryReport report = runtime.recoverAll();
            assertThat(report.resumed()).isEmpty();
            assertThat(report.leftAlone()).containsExactly(Map.entry(live.id(), Status.RUNNING));

            release.countDown();
            assertThat(live.result()).isEqualTo("SUMMARY");
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    @Test
    void recoverOnStartRunsTheScanInBuild() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = contentModel();
        ExecutionId crashed = recordStepOne(log, model);

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).task(new TwoStepTask()).recoverOnStart(true).build()) {

            RecoveryReport report = runtime.startupRecovery().orElseThrow();
            assertThat(report.resumed()).containsOnlyKeys(crashed);
            assertThat(report.resumed().get(crashed).result()).isEqualTo("SUMMARY|FINAL");
        }
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build()) {
            assertThat(runtime.startupRecovery()).as("off by default").isEmpty();
        }
    }

    /** A node that would silently recover nothing is refused at build, not discovered after a crash. */
    @Test
    void recoverOnStartIsRefusedForALogWithoutAnIndex() {
        assertThatThrownBy(() -> CatalystRuntime.builder().log(new UnindexedLog()).recoverOnStart(true).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("execution index");

        try (CatalystRuntime runtime = CatalystRuntime.builder().log(new UnindexedLog()).build()) {
            assertThatThrownBy(runtime::recoverAll)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("UnindexedLog");
        }
    }

    /**
     * recoverAll() on a live runtime, landing between execute()'s ExecutionCreated and its attempt
     * being scheduled. The execution is already open and STARTING with nothing in flight, so a scan
     * that could act in that window would resume it — and execute() would then start a second writer
     * on the same stream. Creating under the id lock closes the window: the scan waits, then finds the
     * attempt in flight and leaves it alone.
     */
    @Test
    void aScanRacingExecuteNeverStartsASecondWriter() throws Exception {
        AtomicReference<CatalystRuntime> runtimeRef = new AtomicReference<>();
        AtomicReference<RecoveryReport> scan = new AtomicReference<>();
        CountDownLatch scanned = new CountDownLatch(1);
        InMemoryEventLog delegate = new InMemoryEventLog();
        EventLog racingLog = new DelegatingLog(delegate) {
            @Override public long append(ExecutionId id, CatalystEvent event) {
                long seq = delegate.append(id, event);
                if (event instanceof CatalystEvent.ExecutionCreated) {
                    Thread.ofVirtual().start(() -> { scan.set(runtimeRef.get().recoverAll()); scanned.countDown(); });
                    sleep(200); // hold the window open: before the fix, the scan resumes in here
                }
                return seq;
            }
        };
        MockModel model = contentModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(racingLog).model(model).task(new TwoStepTask()).build()) {
            runtimeRef.set(runtime);
            ExecutionHandle<String> handle = runtime.execute(new TwoStepTask());
            assertThat(handle.result()).isEqualTo("SUMMARY|FINAL");
            assertThat(scanned.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(scan.get().resumed()).as("the scan never resumed it").doesNotContainKey(handle.id());
            assertThat(model.callCount()).as("one writer: each step called once").isEqualTo(2);
            assertThat(delegate.read(handle.id())).extracting(SequencedEvent::event)
                    .filteredOn(e -> e instanceof CatalystEvent.ExecutionStarted || e instanceof CatalystEvent.ExecutionResumed)
                    .hasSize(1);
        }
    }

    /** A failed startup scan must not close a log the caller supplied — it is theirs to retry with. */
    @Test
    void aFailedStartupScanLeavesACallerSuppliedLogOpen() {
        boolean[] closed = {false};
        EventLog failingIndex = new DelegatingLog(new InMemoryEventLog()) {
            @Override public List<ExecutionId> openExecutions() { throw new IllegalStateException("index unreadable"); }
            @Override public boolean supportsExecutionIndex() { return true; }
            @Override public void close() { closed[0] = true; }
        };
        assertThatThrownBy(() -> CatalystRuntime.builder().log(failingIndex).recoverOnStart(true).build())
                .hasMessageContaining("index unreadable");
        assertThat(closed[0]).isFalse();
    }

    private static void sleep(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static class DelegatingLog implements EventLog {
        final EventLog delegate;
        DelegatingLog(EventLog delegate) { this.delegate = delegate; }
        public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
        public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        @Override public List<ExecutionId> openExecutions() { return delegate.openExecutions(); }
        @Override public boolean supportsExecutionIndex() { return delegate.supportsExecutionIndex(); }
        public void close() { delegate.close(); }
    }

    /** Durably records step 1 of {@link TwoStepTask} and stops, as a process death would leave it. */
    private static ExecutionId recordStepOne(EventLog log, MockModel model) {
        ExecutionId id = ExecutionId.random();
        String taskType = TwoStepTask.class.getName();
        log.append(id, new CatalystEvent.ExecutionCreated(T, taskType, "h", "cfg", ""));
        log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
        ReplayingContext ctx = new ReplayingContext(id, log, model, new ExecutionInfo(id, 1, taskType, Map.of()),
                Map.of(), EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                ReplayMode.STRICT, null, Clock.systemUTC(), LoggerFactory.getLogger("record"), log.read(id), true);
        ctx.model().complete(STEP1);
        return id;
    }

    /** Answers by content, not position, so concurrent resumes get the same answers in any order. */
    private static MockModel contentModel() {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream().filter(m -> m.role() == Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            return new Completion(last.contains("summarize") ? "SUMMARY" : "FINAL", List.of(), new Usage(1, 1), "stop");
        }).build();
    }

    /** A delegating log that hides the index — what a backend without one looks like. */
    private static final class UnindexedLog implements EventLog {
        private final InMemoryEventLog delegate = new InMemoryEventLog();
        public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
        public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        public void close() { }
    }
}
