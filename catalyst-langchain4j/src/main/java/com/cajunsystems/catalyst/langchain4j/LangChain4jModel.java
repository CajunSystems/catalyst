package com.cajunsystems.catalyst.langchain4j;

import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.Usage;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapts any LangChain4j {@link ChatModel} to Catalyst's {@link Model} SPI (spec §2). This is the
 * one-line bridge that gives Catalyst OpenAI, Anthropic, Gemini, Ollama and local models for free —
 * the application supplies a concrete LangChain4j provider, and Catalyst never maintains a provider
 * HTTP client of its own.
 *
 * <p>The completion this returns flows through {@code ctx.model()} like any other, so it is recorded
 * and substituted on replay exactly like {@code MockModel}.
 */
public final class LangChain4jModel implements Model {

    private final ChatModel chatModel;

    public LangChain4jModel(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    /** Convenience factory. */
    public static LangChain4jModel of(ChatModel chatModel) {
        return new LangChain4jModel(chatModel);
    }

    @Override
    public Completion complete(CompletionRequest request) {
        ChatRequest chatRequest = ChatRequest.builder()
                .messages(toLangChainMessages(request))
                .build();
        ChatResponse response = chatModel.chat(chatRequest);
        return toCompletion(response);
    }

    private static List<ChatMessage> toLangChainMessages(CompletionRequest request) {
        List<ChatMessage> messages = new ArrayList<>();
        for (Message m : request.prompt().messages()) {
            messages.add(switch (m.role()) {
                case SYSTEM -> SystemMessage.from(m.content());
                case ASSISTANT -> AiMessage.from(m.content());
                // Catalyst has no id/name for TOOL results in v0; surface the content as user text.
                case USER, TOOL -> UserMessage.from(m.content());
            });
        }
        return messages;
    }

    private static Completion toCompletion(ChatResponse response) {
        AiMessage ai = response.aiMessage();
        String text = ai != null && ai.text() != null ? ai.text() : "";

        List<ToolCall> toolCalls = new ArrayList<>();
        if (ai != null && ai.toolExecutionRequests() != null) {
            for (ToolExecutionRequest ter : ai.toolExecutionRequests()) {
                toolCalls.add(new ToolCall(ter.id(), ter.name(), ter.arguments()));
            }
        }

        Usage usage = toUsage(response.tokenUsage());
        FinishReason finishReason = response.finishReason();
        String finish = finishReason != null ? finishReason.name() : "stop";

        return new Completion(text, toolCalls, usage, finish);
    }

    private static Usage toUsage(TokenUsage tokenUsage) {
        if (tokenUsage == null) return Usage.ZERO;
        long in = tokenUsage.inputTokenCount() != null ? tokenUsage.inputTokenCount() : 0;
        long out = tokenUsage.outputTokenCount() != null ? tokenUsage.outputTokenCount() : 0;
        return new Usage(in, out);
    }
}
