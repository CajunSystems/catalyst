package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.TokenSink;
import com.cajunsystems.catalyst.model.Usage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Streaming is a delivery concern, not a durability one: whatever the provider does on the wire, the
 * boundary recorded is the assembled completion. These tests pin that — the log a streamed execution
 * writes is the same log a non-streamed one writes, and it replays with no provider contacted.
 */
class StreamingTest {

    private static final CompletionRequest ASK =
            CompletionRequest.of(Prompt.builder().user("write a sentence").build());

    /** Accumulates the stream, so the task's result exists only if the sink was fed. */
    private static Task<String> streamingTask(List<String> chunksSeen) {
        return ctx -> {
            StringBuilder assembled = new StringBuilder();
            ctx.model().stream(ASK, text -> {
                chunksSeen.add(text);
                assembled.append(text);
            });
            return assembled.toString();
        };
    }

    private static Object decoded(ExecutionState state) {
        return new PayloadCodec().fromTree(state.result());
    }

    private static List<String> eventTypes(List<SequencedEvent> events) {
        List<String> types = new ArrayList<>();
        for (SequencedEvent se : events) types.add(se.event().getClass().getSimpleName());
        return types;
    }

    @Test
    void deliversChunksIncrementallyAndReturnsTheAssembledCompletion() {
        List<String> chunks = new ArrayList<>();
        MockModel model = MockModel.alwaysReturn("the quick brown fox");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            String result = runtime.execute(streamingTask(chunks), ExecutionOptions.withKey("k")).result();

            assertThat(chunks).containsExactly("the ", "quick ", "brown ", "fox");
            assertThat(result).isEqualTo("the quick brown fox");
            assertThat(model.callCount()).isEqualTo(1);
        }
    }

    @Test
    void recordsTheAssembledCompletionWithNoExtraEvents() {
        // The whole schema argument: a streamed call writes exactly what a non-streamed call writes.
        List<SequencedEvent> streamed;
        List<SequencedEvent> plain;

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("the quick brown fox")).build()) {
            var handle = runtime.execute(streamingTask(new ArrayList<>()), ExecutionOptions.withKey("k"));
            handle.result();
            streamed = runtime.log().read(handle.id());
        }
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("the quick brown fox")).build()) {
            Task<String> nonStreaming = ctx -> ctx.model().complete(ASK).message();
            var handle = runtime.execute(nonStreaming, ExecutionOptions.withKey("k"));
            handle.result();
            plain = runtime.log().read(handle.id());
        }

        assertThat(eventTypes(streamed)).isEqualTo(eventTypes(plain));
        assertThat(eventTypes(streamed)).containsExactly(
                "ExecutionCreated", "ExecutionStarted", "PromptBuilt",
                "CompletionRequested", "CompletionReceived", "ExecutionCompleted");
    }

    @Test
    void replaySubstitutesTheStreamWithZeroProviderCalls() {
        List<String> recordChunks = new ArrayList<>();
        MockModel model = MockModel.alwaysReturn("the quick brown fox");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            var handle = runtime.execute(streamingTask(recordChunks), ExecutionOptions.withKey("k"));
            String recorded = handle.result();
            int callsAfterRecord = model.callCount();

            List<String> replayChunks = new ArrayList<>();
            ExecutionState replayed = runtime.replay(handle.id(), streamingTask(replayChunks));

            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(callsAfterRecord); // provider never contacted
            assertThat(decoded(replayed)).isEqualTo(recorded);
            // The sink is still fed, or the task could not have rebuilt its result — but from the
            // recorded text, as one chunk: chunk boundaries are deliberately not in the log.
            assertThat(String.join("", replayChunks)).isEqualTo(recorded);
            assertThat(replayChunks).containsExactly("the quick brown fox");
        }
    }

    @Test
    void aStreamedRecordingReplaysAgainstANonStreamingTaskAndViceVersa() {
        // Since the recorded boundary is identical either way, the two are interchangeable on replay.
        MockModel model = MockModel.alwaysReturn("the quick brown fox");
        Task<String> nonStreaming = ctx -> ctx.model().complete(ASK).message();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            var handle = runtime.execute(streamingTask(new ArrayList<>()), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            ExecutionState replayed = runtime.replay(handle.id(), nonStreaming);
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void aDivergentStreamedPromptIsStillCaught() {
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(model).build()) {

            var handle = runtime.execute(streamingTask(new ArrayList<>()), ExecutionOptions.withKey("k"));
            handle.result();

            Task<String> different = ctx -> {
                StringBuilder sb = new StringBuilder();
                ctx.model().stream(CompletionRequest.of(
                        Prompt.builder().user("a DIFFERENT prompt").build()), sb::append);
                return sb.toString();
            };
            assertThatThrownBy(() -> runtime.replay(handle.id(), different))
                    .isInstanceOf(NonDeterministicReplayException.class);
        }
    }

    @Test
    void aModelWithoutStreamingSupportStillFeedsTheSink() {
        // The SPI default: no provider streaming, one chunk at the end, everything else unchanged.
        List<String> chunks = new ArrayList<>();
        var nonStreamingModel = new com.cajunsystems.catalyst.model.Model() {
            @Override
            public Completion complete(CompletionRequest request) {
                return new Completion("all at once", List.of(), new Usage(1, 3), "stop");
            }
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(nonStreamingModel).build()) {

            String result = runtime.execute(streamingTask(chunks), ExecutionOptions.withKey("k")).result();

            assertThat(chunks).containsExactly("all at once");
            assertThat(result).isEqualTo("all at once");
        }
    }

    @Test
    void anEmptyCompletionEmitsNothing() {
        List<String> chunks = new ArrayList<>();
        var emptyModel = (com.cajunsystems.catalyst.model.Model) request -> Completion.ofText("");
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(emptyModel).build()) {

            String result = runtime.execute(streamingTask(chunks), ExecutionOptions.withKey("k")).result();

            assertThat(chunks).isEmpty();
            assertThat(result).isEmpty();
        }
    }

    @Test
    void rejectsANullSink() {
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory()).model(MockModel.alwaysReturn("x")).build()) {

            Task<String> nullSink = ctx -> {
                ctx.model().stream(ASK, (TokenSink) null);
                return "unreachable";
            };
            assertThatThrownBy(() -> runtime.execute(nullSink, ExecutionOptions.withKey("k")).result())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("sink");
        }
    }
}
