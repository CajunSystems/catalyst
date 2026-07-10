package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.fasterxml.jackson.databind.node.NullNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

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
    void serializerFactoryProducesUsableMapper() {
        assertThat(EventJson.newMapper()).isNotNull();
    }
}
