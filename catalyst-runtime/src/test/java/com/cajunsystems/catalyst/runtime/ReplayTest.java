package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayTest {

    private static Task<String> ask(String userMessage) {
        return ctx -> ctx.model().complete(
                CompletionRequest.of(Prompt.builder().user(userMessage).build())).message();
    }

    @Test
    void replaySubstitutesWithZeroExternalCalls() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(ask("hi"), ExecutionOptions.withKey("k"));
            assertThat(handle.result()).isEqualTo("pong");
            int callsAfterRecord = model.callCount();

            ExecutionState replayed = runtime.replay(handle.id(), ask("hi"));

            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(callsAfterRecord); // zero external calls on replay
        }
    }

    @Test
    void replayDetectsDivergentPromptAtRecordedSeq() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(ask("original"), ExecutionOptions.withKey("k"));
            handle.result();

            NonDeterministicReplayException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    NonDeterministicReplayException.class,
                    () -> runtime.replay(handle.id(), ask("a different prompt")));
            // Events: Created(0) Started(1) PromptBuilt(2) CompletionRequested(3) CompletionReceived(4)
            assertThat(ex.seq()).isEqualTo(4);
        }
    }

    @Test
    void costModelFoldsIntoExecution() {
        MockModel model = MockModel.scripted("answer"); // reports token usage
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model)
                .costModel(CostModel.perMillionTokens(1000.0, 2000.0)).build()) {

            ExecutionHandle<String> handle = runtime.execute(ask("hi"), ExecutionOptions.withKey("k"));
            handle.result();

            Cost cost = runtime.inspect(handle.id()).cost();
            assertThat(cost.totalTokens()).isGreaterThan(0);
            assertThat(cost.usd()).isGreaterThan(0.0);
        }
    }
}
