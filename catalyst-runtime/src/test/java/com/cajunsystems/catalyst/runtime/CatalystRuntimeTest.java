package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.ExecutionFailedException;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalystRuntimeTest {

    private static final Task<String> ONE_SHOT = ctx ->
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();

    @Test
    void executesAndCompletes() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {
            String result = runtime.executeAndWait(ONE_SHOT);
            assertThat(result).isEqualTo("pong");
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    @Test
    void sameKeyAttachesAndSubstitutesWithZeroDuplicateModelCalls() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> first = runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k"));
            assertThat(first.result()).isEqualTo("pong");
            assertThat(model.callCount()).isEqualTo(1);

            // Re-submitting the same key attaches to the completed execution and substitutes the
            // recorded completion — the model is NOT called again.
            ExecutionHandle<String> second = runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k"));
            assertThat(second.result()).isEqualTo("pong");
            assertThat(second.id()).isEqualTo(first.id());
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    // A tool + task used to exercise the in-doubt path.
    record Add(int a, int b) {}

    private static final Tool<Add, Integer> ADD = new Tool<>() {
        public String name() { return "adder"; }
        public Class<Add> inputType() { return Add.class; }
        public Integer apply(Add in) { return in.a() + in.b(); }
    };

    private static final Task<Integer> ADD_TASK = ctx -> ctx.call(ADD, new Add(2, 3));

    /** Seeds a log with a crash between {@code ToolRequested} and {@code ToolCompleted}. */
    private ExecutionId seedInDoubt(InMemoryEventLog log) {
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, ADD_TASK.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ToolRequested(t, "adder", NullNode.getInstance())); // no ToolCompleted → in-doubt
        return id;
    }

    @Test
    void inDoubtRetryCompletesTheTool() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = seedInDoubt(log);
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).inDoubtPolicy(InDoubtPolicy.RETRY).build()) {
            ExecutionHandle<Integer> handle = runtime.execute(ADD_TASK, ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo(5);
            assertThat(handle.id()).isEqualTo(id);
        }
    }

    @Test
    void inDoubtFailRaises() {
        InMemoryEventLog log = new InMemoryEventLog();
        seedInDoubt(log);
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).inDoubtPolicy(InDoubtPolicy.FAIL).build()) {
            ExecutionHandle<Integer> handle = runtime.execute(ADD_TASK, ExecutionOptions.withKey("k"));
            assertThatThrownBy(handle::result).isInstanceOf(InDoubtException.class);
        }
    }

    @Test
    void concurrentSameKeySubmitAttachesToOneAttempt() throws Exception {
        MockModel model = MockModel.alwaysReturn("pong");
        CountDownLatch release = new CountDownLatch(1);
        // A task that blocks after its model call until the test releases it, keeping the first
        // attempt RUNNING while a second same-key submission arrives.
        Task<String> slow = ctx -> {
            String r = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return r;
        };

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> h1 = runtime.execute(slow, ExecutionOptions.withKey("k"));
            ExecutionHandle<String> h2 = runtime.execute(slow, ExecutionOptions.withKey("k"));
            // The second submission attaches to the in-flight attempt rather than scheduling another.
            assertThat(h2.id()).isEqualTo(h1.id());

            release.countDown();
            assertThat(h1.result()).isEqualTo("pong");
            assertThat(h2.result()).isEqualTo("pong");
            // Exactly one attempt ran: one model call, one ExecutionCompleted.
            assertThat(model.callCount()).isEqualTo(1);
            assertThat(runtime.inspect(h1.id()).trajectory())
                    .filteredOn(step -> step.kind() == com.cajunsystems.catalyst.engine.TimelineStep.Kind.COMPLETED)
                    .hasSize(1);
        }
    }

    @Test
    void substitutionDetectsSwappedEffectLabels() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, "T", "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.EffectRecorded(t, "expected-label", new TextNode("v")));

        Task<String> divergent = ctx -> ctx.effect("different-label", () -> "x");
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).build()) {
            ExecutionHandle<String> handle = runtime.execute(divergent, ExecutionOptions.withKey("k"));
            assertThatThrownBy(handle::result).isInstanceOf(NonDeterministicReplayException.class);
        }
    }

    @Test
    void pauseAndCancelAreNoOpsOnTerminalExecution() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("pong");
            int eventsWhenDone = runtime.log().read(handle.id()).size();

            runtime.pause(handle.id());   // must not reopen the completed execution
            runtime.cancel(handle.id());

            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.COMPLETED);
            assertThat(runtime.log().read(handle.id())).hasSize(eventsWhenDone);

            // Re-submitting the key still substitutes cleanly — no duplicate model call.
            assertThat(runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k")).result()).isEqualTo("pong");
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    @Test
    void cancelOutOfBandFoldsToCancelledNotFailed() {
        // An execution that exists but is not running in this process (e.g. crashed/paused, or created
        // by another node) is cancelled by appending ExecutionCancelled directly — it folds to
        // CANCELLED, and attaching to it surfaces a CancellationException (not ExecutionFailedException).
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, ONE_SHOT.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("pong")).build()) {
            runtime.cancel(id);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.CANCELLED);
            assertThat(runtime.log().read(id)).last()
                    .extracting(se -> se.event().getClass().getSimpleName())
                    .isEqualTo("ExecutionCancelled");

            // Re-submitting the key attaches to the cancelled execution: a CancellationException, and
            // the model is never called (the cancelled prefix substitutes cleanly).
            MockModel model = MockModel.alwaysReturn("pong");
            try (CatalystRuntime runtime2 = CatalystRuntime.builder().log(log).model(model).build()) {
                assertThatThrownBy(() -> runtime2.execute(ONE_SHOT, ExecutionOptions.withKey("k")).result())
                        .isInstanceOf(java.util.concurrent.CancellationException.class);
                assertThat(model.callCount()).isEqualTo(0);
            }
        }
    }

    @Test
    void cancelCooperativelyStopsARunningTaskAndFoldsToCancelled() throws Exception {
        MockModel model = MockModel.alwaysReturn("pong");
        CountDownLatch pastFirstBoundary = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // Two boundaries with a park between them. Cancellation is requested while parked; the second
        // boundary must unwind cooperatively rather than run — so the model is called exactly once.
        Task<String> twoStep = ctx -> {
            String first = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("one").build())).message();
            pastFirstBoundary.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user("two").build())).message() + first;
        };

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(twoStep, ExecutionOptions.withKey("k"));
            assertThat(pastFirstBoundary.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

            runtime.cancel(handle.id());  // trips the token + interrupts the parked worker
            release.countDown();          // (belt-and-braces: also let the park return normally)

            assertThatThrownBy(handle::result).isInstanceOf(java.util.concurrent.CancellationException.class);
            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.CANCELLED);
            // Only the first boundary ran live; the second unwound before calling the model.
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    @Test
    void cancelDoesNotMaskARealErrorThrownBeforeTheNextBoundary() {
        // A task that swallows the cancel interrupt, does non-boundary work, and then throws a genuine
        // error before reaching another ctx boundary must fold to FAILED (with the real error), not be
        // masked as a clean CANCELLED just because a cancel was requested.
        MockModel model = MockModel.alwaysReturn("pong");
        CountDownLatch pastFirstBoundary = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        Task<String> throwsAfterCancel = ctx -> {
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("one").build())).message();
            pastFirstBoundary.countDown();
            // Wait until the test has requested cancellation, tolerating (swallowing) the interrupt.
            while (true) {
                try { proceed.await(); break; }
                catch (InterruptedException e) { /* swallow the cancel interrupt and keep going */ }
            }
            throw new IllegalStateException("real bug after cancel");
        };

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(throwsAfterCancel, ExecutionOptions.withKey("k"));
            try {
                assertThat(pastFirstBoundary.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }

            runtime.cancel(handle.id()); // trips the token + interrupts, but the task throws a real error
            proceed.countDown();

            assertThatThrownBy(handle::result)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("real bug after cancel");
            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.FAILED);
            assertThat(runtime.inspect(handle.id()).error()).contains("real bug after cancel");
        }
    }

    @Test
    void memoryGetRejectsWrongType() {
        Task<String> storeThenMisread = ctx -> {
            ctx.memory().put("n", 42); // an Integer
            return ctx.memory().get("n", String.class).orElse("none"); // asks for a String
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(storeThenMisread, ExecutionOptions.withKey("k"));
            assertThatThrownBy(handle::result)
                    .isInstanceOf(ClassCastException.class)
                    .hasMessageContaining("holds a");
        }
    }

    @Test
    void pauseIsRefusedWhileRunningButNoOpOnceTerminal() throws Exception {
        MockModel model = MockModel.alwaysReturn("pong");
        CountDownLatch release = new CountDownLatch(1);
        Task<String> slow = ctx -> {
            String r = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return r;
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(slow, ExecutionOptions.withKey("k"));
            // While the task is in flight, pausing must be refused rather than racing the event stream.
            assertThatThrownBy(() -> runtime.pause(handle.id())).isInstanceOf(IllegalStateException.class);

            release.countDown();
            assertThat(handle.result()).isEqualTo("pong");

            // Once terminal it is a clean no-op.
            runtime.pause(handle.id());
            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void setupFailureCompletesTheFutureInsteadOfHanging() {
        // An event log that fails when the runtime tries to append ExecutionStarted — i.e. during
        // runAttempt setup, before the task runs. The handle must fail fast, not block forever.
        InMemoryEventLog delegate = new InMemoryEventLog();
        EventLog failing = new EventLog() {
            public long append(ExecutionId id, com.cajunsystems.catalyst.events.CatalystEvent event) {
                if (event instanceof com.cajunsystems.catalyst.events.CatalystEvent.ExecutionStarted) {
                    throw new RuntimeException("boom during setup");
                }
                return delegate.append(id, event);
            }
            public java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
            public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
            public java.util.Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
            public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
            public void close() { delegate.close(); }
        };

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(failing).model(MockModel.alwaysReturn("pong")).build()) {
            ExecutionHandle<String> handle = runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k"));
            // result(timeout) proves it completes (exceptionally) rather than hanging.
            assertThatThrownBy(() -> handle.result(java.time.Duration.ofSeconds(5)))
                    .hasMessageContaining("boom during setup");
        }
    }

    @Test
    void inDoubtToolIsDetectedEvenWhenALifecycleEventFollowsTheRequest() {
        // Simulates an ASK-style in-doubt: ToolRequested followed by ExecutionPaused (non-terminal).
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, ADD_TASK.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ToolRequested(t, "adder", NullNode.getInstance()));
        log.append(id, new CatalystEvent.ExecutionPaused(t, "in-doubt tool: adder"));

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).inDoubtPolicy(InDoubtPolicy.FAIL).build()) {
            ExecutionHandle<Integer> handle = runtime.execute(ADD_TASK, ExecutionOptions.withKey("k"));
            // With the dangling tool still detected, the FAIL policy applies (rather than running live).
            assertThatThrownBy(handle::result).isInstanceOf(InDoubtException.class);
        }
    }

    @Test
    void attachingToAFailedExecutionReturnsTheStoredErrorWithoutRerunning() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, "T", "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ExecutionFailed(t, "original boom", 1));

        MockModel model = MockModel.alwaysReturn("x");
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build()) {
            ExecutionHandle<String> handle = runtime.execute(ONE_SHOT, ExecutionOptions.withKey("k"));
            assertThatThrownBy(handle::result)
                    .isInstanceOf(ExecutionFailedException.class)
                    .hasMessageContaining("original boom");
            assertThat(model.callCount()).isEqualTo(0); // the task was not re-executed
        }
    }

    @Test
    void aCorruptSnapshotFallsBackToAFullFoldAndSelfHeals() {
        // A log whose stored snapshot is garbage bytes: inspect must ignore it, fold the full log
        // correctly, and overwrite the bad checkpoint with a valid one (self-heal).
        InMemoryEventLog inner = new InMemoryEventLog();
        boolean[] returnCorrupt = {true};
        EventLog corrupting = new EventLog() {
            @Override public long append(ExecutionId id, CatalystEvent e) { return inner.append(id, e); }
            @Override public java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> read(ExecutionId id) { return inner.read(id); }
            @Override public java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> readFrom(ExecutionId id, long after) { return inner.readFrom(id, after); }
            @Override public long latestSeq(ExecutionId id) { return inner.latestSeq(id); }
            @Override public java.util.Optional<ExecutionId> findByKey(String k) { return inner.findByKey(k); }
            @Override public void putKey(String k, ExecutionId id) { inner.putKey(k, id); }
            @Override public java.util.Optional<com.cajunsystems.catalyst.log.Snapshot> readSnapshot(ExecutionId id) {
                if (returnCorrupt[0]) return java.util.Optional.of(new com.cajunsystems.catalyst.log.Snapshot(0, new byte[]{1, 2, 3}));
                return inner.readSnapshot(id);
            }
            @Override public void writeSnapshot(ExecutionId id, com.cajunsystems.catalyst.log.Snapshot s) { inner.writeSnapshot(id, s); }
            @Override public void close() { inner.close(); }
        };

        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(corrupting).model(model).snapshotInterval(1).build()) {
            ExecutionHandle<String> handle = runtime.execute(ONE_SHOT);
            assertThat(handle.result()).isEqualTo("pong");
            ExecutionId id = handle.id();

            // inspect() sees the corrupt snapshot, must not throw, and folds the intact log correctly.
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);

            // A fresh, valid checkpoint was written over the corrupt one; a later read works normally.
            returnCorrupt[0] = false;
            assertThat(inner.readSnapshot(id)).isPresent();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void serializerFactoryProducesUsableMapper() {
        assertThat(EventJson.newMapper()).isNotNull();
    }
}
