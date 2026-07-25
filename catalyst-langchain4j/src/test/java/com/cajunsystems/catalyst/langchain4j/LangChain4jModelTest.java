package com.cajunsystems.catalyst.langchain4j;

import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.ToolSpec;
import com.cajunsystems.catalyst.model.Usage;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import com.cajunsystems.catalyst.model.TokenSink;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the LangChain4j → Catalyst mapping offline, against a hand-written fake {@link ChatModel}.
 * No provider, no network, no API key — just the adapter's translation logic.
 */
class LangChain4jModelTest {

    /** A fake ChatModel that records the request it received and returns a canned response. */
    private static final class FakeChatModel implements ChatModel {
        ChatRequest received;
        private final ChatResponse response;

        FakeChatModel(ChatResponse response) {
            this.response = response;
        }

        @Override
        public ChatResponse doChat(ChatRequest chatRequest) {
            this.received = chatRequest;
            return response;
        }
    }

    @Test
    void mapsPromptToMessagesAndResponseToCompletion() {
        FakeChatModel fake = new FakeChatModel(ChatResponse.builder()
                .aiMessage(AiMessage.from("hello from the model"))
                .tokenUsage(new TokenUsage(11, 7))
                .finishReason(FinishReason.STOP)
                .build());

        LangChain4jModel model = LangChain4jModel.of(fake);
        Completion completion = model.complete(
                CompletionRequest.of(Prompt.builder().system("you are helpful").user("hi").build()));

        // Response mapping.
        assertThat(completion.message()).isEqualTo("hello from the model");
        assertThat(completion.usage()).isEqualTo(new Usage(11, 7));
        assertThat(completion.finishReason()).isEqualTo("STOP");
        assertThat(completion.hasToolCalls()).isFalse();

        // Request mapping: Catalyst roles → LangChain4j message types, in order.
        assertThat(fake.received.messages()).hasSize(2);
        assertThat(fake.received.messages().get(0)).isInstanceOf(SystemMessage.class);
        assertThat(fake.received.messages().get(1)).isInstanceOf(UserMessage.class);
    }

    @Test
    void forwardsToolSpecificationsToTheChatRequest() {
        FakeChatModel fake = new FakeChatModel(ChatResponse.builder()
                .aiMessage(AiMessage.from("ok")).build());

        ToolSpec spec = new ToolSpec("calculator", "adds two numbers",
                "{\"type\":\"object\",\"properties\":{"
                        + "\"a\":{\"type\":\"integer\"},"
                        + "\"b\":{\"type\":\"integer\",\"description\":\"the second number\"}},"
                        + "\"required\":[\"a\",\"b\"]}");
        CompletionRequest request = CompletionRequest.of(Prompt.builder().user("2 + 3").build())
                .withTools(java.util.List.of(spec));

        LangChain4jModel.of(fake).complete(request);

        assertThat(fake.received.toolSpecifications()).hasSize(1);
        ToolSpecification forwarded = fake.received.toolSpecifications().get(0);
        assertThat(forwarded.name()).isEqualTo("calculator");
        assertThat(forwarded.description()).isEqualTo("adds two numbers");
        assertThat(forwarded.parameters()).isNotNull();
        assertThat(forwarded.parameters().properties()).containsKeys("a", "b");
        assertThat(forwarded.parameters().required()).contains("a", "b");
    }

    @Test
    void noToolsLeavesToolSpecificationsUnset() {
        FakeChatModel fake = new FakeChatModel(ChatResponse.builder()
                .aiMessage(AiMessage.from("ok")).build());
        LangChain4jModel.of(fake).complete(CompletionRequest.of(Prompt.builder().user("hi").build()));
        // No tools requested → none forwarded (null or empty is acceptable).
        assertThat(fake.received.toolSpecifications() == null || fake.received.toolSpecifications().isEmpty())
                .isTrue();
    }

    @Test
    void missingTokenUsageMapsToZero() {
        FakeChatModel fake = new FakeChatModel(ChatResponse.builder()
                .aiMessage(AiMessage.from("no usage reported"))
                .build());

        Completion completion = LangChain4jModel.of(fake).complete(
                CompletionRequest.of(Prompt.builder().user("hi").build()));

        assertThat(completion.usage()).isEqualTo(Usage.ZERO);
        assertThat(completion.message()).isEqualTo("no usage reported");
    }

    /**
     * A fake streaming provider that emits its chunks from <em>another</em> thread, the way a real
     * provider's HTTP event loop does. That is the property the adapter has to cope with.
     */
    private static final class FakeStreamingChatModel implements StreamingChatModel {
        private final List<String> chunks;
        private final ChatResponse response;
        private final RuntimeException failure;
        volatile Thread callbackThread;

        FakeStreamingChatModel(List<String> chunks, ChatResponse response) {
            this(chunks, response, null);
        }

        FakeStreamingChatModel(List<String> chunks, ChatResponse response, RuntimeException failure) {
            this.chunks = chunks;
            this.response = response;
            this.failure = failure;
        }

        @Override
        public void doChat(ChatRequest chatRequest, StreamingChatResponseHandler handler) {
            Thread producer = new Thread(() -> {
                callbackThread = Thread.currentThread();
                for (String chunk : chunks) handler.onPartialResponse(chunk);
                if (failure != null) handler.onError(failure);
                else handler.onCompleteResponse(response);
            }, "fake-provider");
            producer.start();
        }
    }

    private static ChatResponse responseOf(String text) {
        return ChatResponse.builder()
                .aiMessage(AiMessage.from(text))
                .tokenUsage(new TokenUsage(5, 9))
                .finishReason(FinishReason.STOP)
                .build();
    }

    @Test
    void streamsChunksAndReturnsTheAssembledCompletion() {
        FakeStreamingChatModel fake = new FakeStreamingChatModel(
                List.of("the ", "quick ", "brown ", "fox"), responseOf("the quick brown fox"));

        List<String> received = new ArrayList<>();
        Completion completion = LangChain4jModel.streaming(fake)
                .stream(CompletionRequest.of(Prompt.builder().user("go").build()), received::add);

        assertThat(received).containsExactly("the ", "quick ", "brown ", "fox");
        assertThat(completion.message()).isEqualTo("the quick brown fox");
        assertThat(completion.usage()).isEqualTo(new Usage(5, 9));
        assertThat(completion.finishReason()).isEqualTo("STOP");
    }

    @Test
    void deliversChunksOnTheCallingThreadNotTheProviderThread() {
        // The sink is task code: it belongs on the task's thread, where the Context is bound and the
        // determinism contract applies — not on whatever thread the provider happens to call back on.
        FakeStreamingChatModel fake = new FakeStreamingChatModel(List.of("a", "b"), responseOf("ab"));

        List<Thread> sinkThreads = new ArrayList<>();
        LangChain4jModel.streaming(fake).stream(
                CompletionRequest.of(Prompt.builder().user("go").build()),
                text -> sinkThreads.add(Thread.currentThread()));

        assertThat(sinkThreads).isNotEmpty().containsOnly(Thread.currentThread());
        assertThat(fake.callbackThread).isNotNull().isNotEqualTo(Thread.currentThread());
    }

    @Test
    void surfacesAStreamingFailureToTheCaller() {
        FakeStreamingChatModel fake = new FakeStreamingChatModel(
                List.of("partial"), null, new IllegalStateException("provider exploded"));

        assertThatThrownBy(() -> LangChain4jModel.streaming(fake)
                .stream(CompletionRequest.of(Prompt.builder().user("go").build()), TokenSink.discarding()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Streaming completion failed")
                .hasRootCauseMessage("provider exploded");
    }

    @Test
    void completeWorksAgainstAStreamingOnlyProvider() {
        FakeStreamingChatModel fake = new FakeStreamingChatModel(
                List.of("as", "sembled"), responseOf("assembled"));

        Completion completion = LangChain4jModel.streaming(fake)
                .complete(CompletionRequest.of(Prompt.builder().user("go").build()));

        assertThat(completion.message()).isEqualTo("assembled");
    }

    @Test
    void aBlockingOnlyProviderFallsBackToASingleChunk() {
        FakeChatModel fake = new FakeChatModel(responseOf("all at once"));

        List<String> received = new ArrayList<>();
        Completion completion = LangChain4jModel.of(fake)
                .stream(CompletionRequest.of(Prompt.builder().user("go").build()), received::add);

        assertThat(received).containsExactly("all at once");
        assertThat(completion.message()).isEqualTo("all at once");
    }

    @Test
    void refusesAnAdapterWithNoProviderAtAll() {
        assertThatThrownBy(() -> new LangChain4jModel(null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
