package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The v0.2 streaming exit criterion (spec §13.1): a task consumes a completion incrementally and the
 * execution still replays exactly, with the provider never contacted.
 *
 * <p>The claim being pinned is that streaming is a <em>delivery</em> concern. The durable record is
 * the assembled completion — the same event a non-streaming call writes — so nothing about resume,
 * replay or branch had to change to support it.
 */
class StreamingAcceptanceTest {

    private static final String TEXT = "streaming completions arrive one piece at a time";

    private static Task<String> consumeStream(List<String> chunks) {
        return ctx -> {
            StringBuilder assembled = new StringBuilder();
            ctx.model().stream(
                    CompletionRequest.of(Prompt.builder().user("describe streaming").build()),
                    text -> {
                        chunks.add(text);
                        assembled.append(text);
                    });
            return assembled.toString();
        };
    }

    @Test
    void streamedExecutionReplaysExactlyWithZeroProviderCalls(@TempDir Path dir) {
        MockModel model = MockModel.alwaysReturn(TEXT);
        List<String> recordChunks = new ArrayList<>();

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {

            ExecutionHandle<String> handle =
                    runtime.execute(consumeStream(recordChunks), ExecutionOptions.withKey("stream:1"));
            String recorded = handle.result();
            ExecutionId id = handle.id();
            int callsAfterRecord = model.callCount();

            // Live: genuinely incremental.
            assertThat(recordChunks).hasSizeGreaterThan(1);
            assertThat(String.join("", recordChunks)).isEqualTo(TEXT);
            assertThat(recorded).isEqualTo(TEXT);

            // Durable: one completion event, exactly as a non-streaming call would write.
            List<String> types = new ArrayList<>();
            for (var se : runtime.log().read(id)) types.add(se.event().getClass().getSimpleName());
            assertThat(types).containsExactly(
                    "ExecutionCreated", "ExecutionStarted", "PromptBuilt",
                    "CompletionRequested", "CompletionReceived", "ExecutionCompleted");

            // Replay: provider untouched, sink still fed from the log, same answer rebuilt.
            List<String> replayChunks = new ArrayList<>();
            ExecutionState replayed = runtime.replay(id, consumeStream(replayChunks));

            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).isEqualTo(callsAfterRecord);
            assertThat(new PayloadCodec().fromTree(replayed.result())).isEqualTo(recorded);
            assertThat(String.join("", replayChunks)).isEqualTo(recorded);
            // Chunk boundaries are not recorded: the recorded text comes back as one piece.
            assertThat(replayChunks).containsExactly(TEXT);
        }
    }

    @Test
    void aStreamedRecordingIsInterchangeableWithANonStreamingTask(@TempDir Path dir) {
        MockModel model = MockModel.alwaysReturn(TEXT);
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(
                    consumeStream(new ArrayList<>()), ExecutionOptions.withKey("stream:2"));
            String recorded = handle.result();

            // Because the boundary is identical either way, a non-streaming task replays the same log.
            Task<String> nonStreaming = ctx -> ctx.model().complete(
                    CompletionRequest.of(Prompt.builder().user("describe streaming").build())).message();
            ExecutionState replayed = runtime.replay(handle.id(), nonStreaming);

            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(new PayloadCodec().fromTree(replayed.result())).isEqualTo(recorded);
        }
    }
}
