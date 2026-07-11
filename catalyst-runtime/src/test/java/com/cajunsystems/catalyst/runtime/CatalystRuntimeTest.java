package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
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
    void serializerFactoryProducesUsableMapper() {
        assertThat(EventJson.newMapper()).isNotNull();
    }
}
