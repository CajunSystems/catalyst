package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.fasterxml.jackson.databind.node.NullNode;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.fasterxml.jackson.databind.node.NullNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class RetryTest {

    record In(String v) {}

    /** A tool that fails its first {@code failures} live invocations, then succeeds. Counts invocations. */
    static final class FlakyTool implements Tool<In, String> {
        final AtomicInteger invocations = new AtomicInteger();
        private final int failures;
        FlakyTool(int failures) { this.failures = failures; }
        public String name() { return "flaky"; }
        public Class<In> inputType() { return In.class; }
        public String apply(In in) {
            int n = invocations.incrementAndGet();
            if (n <= failures) throw new RuntimeException("transient#" + n);
            return "ok:" + in.v();
        }
    }

    /** Calls the model once, then a flaky tool. On retry the model call substitutes; only the tool re-runs. */
    private static Task<String> modelThenFlaky(FlakyTool tool) {
        return ctx -> {
            String m = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();
            String r = ctx.call(tool, new In("x"));
            return m + "|" + r;
        };
    }

    private static long countOf(EventLogReader log, ExecutionId id, Class<? extends CatalystEvent> type) {
        return log.read(id).stream().filter(se -> type.isInstance(se.event())).count();
    }

    // A tiny read-only view so the helper reads without importing the whole EventLog surface.
    private interface EventLogReader {
        java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> read(ExecutionId id);
    }

    @Test
    void defaultPolicyDoesNotRetry() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(1); // fails once
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model).build()) {
            ExecutionHandle<String> h = runtime.execute(modelThenFlaky(tool));
            catchThrowable(h::result); // fails

            assertThat(runtime.inspect(h.id()).status()).isEqualTo(Status.FAILED);
            assertThat(tool.invocations).as("tool ran once, not retried").hasValue(1);
            assertThat(countOf(log::read, h.id(), CatalystEvent.RetryRequested.class)).isZero();
        }
    }

    @Test
    void transientFailureIsRetriedAndTheSuccessfulPrefixIsSubstituted() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(2); // fails twice, then succeeds
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5))).build()) {
            String result = runtime.executeAndWait(modelThenFlaky(tool));

            assertThat(result).isEqualTo("m|ok:x");
            // The tool ran three times (2 failures + 1 success); the model ran ONCE across all retries
            // (its recorded completion is substituted on each resume). This pair is the whole semantics.
            assertThat(tool.invocations).as("failing boundary re-ran live").hasValue(3);
            assertThat(model.callCount()).as("successful prefix substituted, not re-run").isEqualTo(1);
        }
    }

    @Test
    void retriesFoldToTheBudgetAndTimeline() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(2);
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(modelThenFlaky(tool));
            h.result();

            assertThat(runtime.inspect(h.id()).retries()).isEqualTo(2);
            assertThat(countOf(log::read, h.id(), CatalystEvent.RetryRequested.class)).isEqualTo(2);
            assertThat(runtime.inspect(h.id()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void exhaustedBudgetFailsTerminally() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(99); // always fails
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(2, Duration.ofMillis(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(modelThenFlaky(tool));
            catchThrowable(h::result);

            assertThat(runtime.inspect(h.id()).status()).isEqualTo(Status.FAILED);
            assertThat(runtime.inspect(h.id()).isTerminal()).isTrue();
            assertThat(countOf(log::read, h.id(), CatalystEvent.RetryRequested.class))
                    .as("exactly maxRetries retries taken").isEqualTo(2);
            assertThat(tool.invocations).as("initial attempt + 2 retries").hasValue(3);
        }
    }

    @Test
    void inDoubtFailureIsNotRetried() {
        // A dangling tool under InDoubtPolicy.FAIL raises InDoubtException — governed by the in-doubt
        // policy, so the retry gate excludes it even with a retry policy configured.
        InMemoryEventLog log = new InMemoryEventLog();
        Task<Integer> task = ctx -> ctx.call(new Tool<In, Integer>() {
            public String name() { return "adder"; }
            public Class<In> inputType() { return In.class; }
            public Integer apply(In in) { return 1; }
        }, new In("x"));

        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, task.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ToolRequested(t, "adder", NullNode.getInstance())); // no completion → in-doubt

        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log)
                .inDoubtPolicy(InDoubtPolicy.FAIL)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5))).build()) {
            ExecutionHandle<Integer> h = runtime.execute(task, ExecutionOptions.withKey("k"));
            catchThrowable(h::result);

            assertThat(runtime.inspect(id).status()).isEqualTo(Status.FAILED);
            assertThat(countOf(log::read, id, CatalystEvent.RetryRequested.class)).isZero();
        }
    }

    @Test
    void errorIsNotRetried() {
        MockModel model = MockModel.alwaysReturn("m");
        InMemoryEventLog log = new InMemoryEventLog();
        Task<String> boom = ctx -> { throw new Error("fatal"); };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(boom);
            catchThrowable(h::result);

            assertThat(runtime.inspect(h.id()).status()).isEqualTo(Status.FAILED);
            assertThat(countOf(log::read, h.id(), CatalystEvent.RetryRequested.class)).isZero();
        }
    }

    @Test
    void aRetriedExecutionStillReplaysWithZeroExternalCalls() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(2);
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(modelThenFlaky(tool));
            h.result();

            int modelCallsAfterRun = model.callCount();
            int toolInvocationsAfterRun = tool.invocations.get();

            ExecutionState replayed = runtime.replay(h.id(), modelThenFlaky(tool));

            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).as("no live model call during replay").isEqualTo(modelCallsAfterRun);
            assertThat(tool.invocations).as("no live tool call during replay").hasValue(toolInvocationsAfterRun);
        }
    }

    @Test
    void crashBetweenToolFailureAndRetryDoesNotBurnTheBudgetOnResume() {
        // Simulate a crash in the window between recording ToolCompleted(error) and appending
        // RetryRequested: the log ends with a trailing errored tool and no RetryRequested. On resume the
        // failure is in-doubt (default FAIL) — it must fail immediately, not burn the whole retry budget
        // on futile substitution-replay cycles.
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(0); // would succeed if actually re-run
        Task<String> task = ctx -> ctx.call(tool, new In("x"));

        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, task.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ToolRequested(t, "flaky", NullNode.getInstance()));
        log.append(id, new CatalystEvent.ToolCompleted(t, null, "transient", 1)); // crash right here

        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(5, Duration.ofMillis(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(task, ExecutionOptions.withKey("k"));
            catchThrowable(h::result);

            assertThat(runtime.inspect(id).status()).isEqualTo(Status.FAILED);
            assertThat(countOf(log::read, id, CatalystEvent.RetryRequested.class))
                    .as("budget untouched: the in-doubt tail is not a retryable substitution").isZero();
        }
    }

    @Test
    void crashWindowToolIsReRunLiveUnderInDoubtRetry() {
        // The same crash tail, but with InDoubtPolicy.RETRY: the in-doubt tool is genuinely re-run live
        // and (being transient) now succeeds — the recovery the retry budget could never provide.
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(0);
        Task<String> task = ctx -> ctx.call(tool, new In("x"));

        ExecutionId id = ExecutionId.random();
        Instant t = Instant.now();
        log.putKey("k", id);
        log.append(id, new CatalystEvent.ExecutionCreated(t, task.getClass().getName(), "h", "cfg", "k"));
        log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));
        log.append(id, new CatalystEvent.ToolRequested(t, "flaky", NullNode.getInstance()));
        log.append(id, new CatalystEvent.ToolCompleted(t, null, "transient", 1)); // crash right here

        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .inDoubtPolicy(InDoubtPolicy.RETRY).build()) {
            ExecutionHandle<String> h = runtime.execute(task, ExecutionOptions.withKey("k"));

            assertThat(h.result()).isEqualTo("ok:x");
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(tool.invocations).as("the in-doubt tool ran live once").hasValue(1);
        }
    }

    @Test
    void cancelDuringBackoffFoldsToCancelledNotFailed() throws Exception {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(99); // always fails, so it enters backoff
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(5, Duration.ofSeconds(5))).build()) {
            ExecutionHandle<String> h = runtime.execute(modelThenFlaky(tool));

            // Wait until the first failure has been recorded and the worker is in the 5s backoff.
            long deadline = System.currentTimeMillis() + 2000;
            while (countOf(log::read, h.id(), CatalystEvent.RetryRequested.class) < 1
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertThat(countOf(log::read, h.id(), CatalystEvent.RetryRequested.class)).isEqualTo(1);

            runtime.cancel(h.id());
            catchThrowable(h::result);

            assertThat(runtime.inspect(h.id()).status()).isEqualTo(Status.CANCELLED);
        }
    }

    @Test
    void concurrentSameKeySubmitAttachesDuringARetry() {
        MockModel model = MockModel.alwaysReturn("m");
        FlakyTool tool = new FlakyTool(2);
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(log).model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(50))).build()) {
            ExecutionHandle<String> h1 = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));
            ExecutionHandle<String> h2 = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));

            assertThat(h2.id()).as("same execution, not a duplicate").isEqualTo(h1.id());
            assertThat(h1.result()).isEqualTo("m|ok:x");
            assertThat(h2.result()).isEqualTo(h1.result());
            // A single writer: exactly one creation and one completion despite two submissions + retries.
            assertThat(countOf(log::read, h1.id(), CatalystEvent.ExecutionCreated.class)).isEqualTo(1);
            assertThat(countOf(log::read, h1.id(), CatalystEvent.ExecutionCompleted.class)).isEqualTo(1);
        }
    }
}
