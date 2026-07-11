package com.cajunsystems.catalyst.langchain4j;

import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Usage;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
    void missingTokenUsageMapsToZero() {
        FakeChatModel fake = new FakeChatModel(ChatResponse.builder()
                .aiMessage(AiMessage.from("no usage reported"))
                .build());

        Completion completion = LangChain4jModel.of(fake).complete(
                CompletionRequest.of(Prompt.builder().user("hi").build()));

        assertThat(completion.usage()).isEqualTo(Usage.ZERO);
        assertThat(completion.message()).isEqualTo("no usage reported");
    }
}
