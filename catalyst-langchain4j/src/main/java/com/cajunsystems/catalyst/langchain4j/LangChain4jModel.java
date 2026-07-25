package com.cajunsystems.catalyst.langchain4j;

import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.TokenSink;
import com.cajunsystems.catalyst.model.ToolSpec;
import com.cajunsystems.catalyst.model.Usage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.output.FinishReason;
import dev.langchain4j.model.output.TokenUsage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapts any LangChain4j {@link ChatModel} to Catalyst's {@link Model} SPI (spec §2). This is the
 * one-line bridge that gives Catalyst OpenAI, Anthropic, Gemini, Ollama and local models for free —
 * the application supplies a concrete LangChain4j provider, and Catalyst never maintains a provider
 * HTTP client of its own.
 *
 * <p>The completion this returns flows through {@code ctx.model()} like any other, so it is recorded
 * and substituted on replay exactly like {@code MockModel}. Tool specs on the request are forwarded
 * so the model can emit tool calls (which the task then dispatches via {@code ctx.call}).
 */
public final class LangChain4jModel implements Model {

    private static final ObjectMapper SCHEMA_MAPPER = new ObjectMapper();

    /** Ends a chunk queue; carries the assembled response or the failure that ended the stream. */
    private record Terminal(ChatResponse response, Throwable error) {}

    private final ChatModel chatModel;
    private final StreamingChatModel streamingChatModel;

    public LangChain4jModel(ChatModel chatModel) {
        this(chatModel, null);
    }

    public LangChain4jModel(ChatModel chatModel, StreamingChatModel streamingChatModel) {
        if (chatModel == null && streamingChatModel == null) {
            throw new IllegalArgumentException("Supply a ChatModel, a StreamingChatModel, or both");
        }
        this.chatModel = chatModel;
        this.streamingChatModel = streamingChatModel;
    }

    /** Convenience factory. */
    public static LangChain4jModel of(ChatModel chatModel) {
        return new LangChain4jModel(chatModel);
    }

    /**
     * Adapts a streaming provider. {@code complete} works too — it consumes the stream and returns the
     * assembled response — so one of these can serve a task whether or not it asks for chunks.
     */
    public static LangChain4jModel streaming(StreamingChatModel streamingChatModel) {
        return new LangChain4jModel(null, streamingChatModel);
    }

    /** Uses the blocking model for {@code complete} and the streaming one for {@code stream}. */
    public static LangChain4jModel of(ChatModel chatModel, StreamingChatModel streamingChatModel) {
        return new LangChain4jModel(chatModel, streamingChatModel);
    }

    @Override
    public Completion complete(CompletionRequest request) {
        if (chatModel == null) {
            return stream(request, TokenSink.discarding()); // streaming-only provider
        }
        ChatResponse response = chatModel.chat(toChatRequest(request));
        return toCompletion(response);
    }

    /**
     * Bridges LangChain4j's callback API onto Catalyst's synchronous {@link Model#stream} contract: the
     * boundary is not recorded until the assembled completion is in hand, so this must block until the
     * provider says it is done.
     *
     * <p>Chunks are handed across a queue rather than passed straight through from the provider's
     * callback. The callbacks arrive on the provider's own thread, and the sink is <em>task</em> code —
     * it belongs on the task's thread, which is where the {@code Context} is bound and where the
     * determinism contract applies. Draining the queue here keeps delivery incremental while keeping
     * the sink on the calling thread.
     */
    @Override
    public Completion stream(CompletionRequest request, TokenSink sink) {
        if (streamingChatModel == null) {
            return Model.super.stream(request, sink); // no streaming provider: one chunk at the end
        }
        BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        // Set once the consumer below stops draining — a throwing sink, or an interrupt. The provider
        // keeps calling back until its stream ends regardless, and without this it would go on
        // enqueueing chunks nobody will ever read. The queue is deliberately unbounded: a bounded one
        // would park the provider's thread in put() with no consumer left to release it, trading a
        // short-lived buffer for a permanently stuck thread. What bounds it in practice is that a
        // completion's text is finite — the engine buffers the whole message anyway to record it.
        AtomicBoolean abandoned = new AtomicBoolean(false);
        streamingChatModel.chat(toChatRequest(request), new StreamingChatResponseHandler() {
            @Override
            public void onPartialResponse(String partial) {
                if (abandoned.get()) return;
                if (partial != null && !partial.isEmpty()) queue.add(partial);
            }

            @Override
            public void onCompleteResponse(ChatResponse response) {
                queue.add(new Terminal(response, null));
            }

            @Override
            public void onError(Throwable error) {
                queue.add(new Terminal(null, error));
            }
        });

        try {
            while (true) {
                Object next = queue.take();
                if (next instanceof Terminal terminal) {
                    if (terminal.error() != null) {
                        throw new RuntimeException("Streaming completion failed", terminal.error());
                    }
                    return toCompletion(terminal.response());
                }
                sink.accept((String) next);
            }
        } catch (InterruptedException e) {
            abandoned.set(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while streaming a completion", e);
        } catch (RuntimeException e) {
            abandoned.set(true); // the sink threw, or the stream failed: nobody is draining any more
            throw e;
        }
    }

    private static ChatRequest toChatRequest(CompletionRequest request) {
        ChatRequest.Builder builder = ChatRequest.builder().messages(toLangChainMessages(request));
        List<ToolSpecification> tools = toToolSpecifications(request.tools());
        if (!tools.isEmpty()) {
            builder.toolSpecifications(tools);
        }
        return builder.build();
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

    // ── Tool specs (Catalyst → LangChain4j) ──────────────────────────────────

    private static List<ToolSpecification> toToolSpecifications(List<ToolSpec> specs) {
        List<ToolSpecification> out = new ArrayList<>(specs.size());
        for (ToolSpec spec : specs) {
            ToolSpecification.Builder b = ToolSpecification.builder().name(spec.name());
            if (spec.description() != null && !spec.description().isBlank()) {
                b.description(spec.description());
            }
            JsonObjectSchema parameters = parseParameters(spec.inputSchema());
            if (parameters != null) {
                b.parameters(parameters);
            }
            out.add(b.build());
        }
        return out;
    }

    /**
     * Best-effort conversion of a JSON-schema string into a {@link JsonObjectSchema}. Handles the
     * common flat object schema ({@code {"type":"object","properties":{...},"required":[...]}});
     * unparseable or absent schemas forward the tool without a parameter schema rather than dropping
     * it.
     */
    private static JsonObjectSchema parseParameters(String schemaJson) {
        if (schemaJson == null || schemaJson.isBlank()) return null;
        try {
            JsonNode root = SCHEMA_MAPPER.readTree(schemaJson);
            JsonObjectSchema.Builder b = JsonObjectSchema.builder();
            if (root.hasNonNull("description")) {
                b.description(root.get("description").asText());
            }
            JsonNode properties = root.get("properties");
            if (properties != null && properties.isObject()) {
                var fields = properties.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    addProperty(b, field.getKey(), field.getValue());
                }
            }
            JsonNode required = root.get("required");
            if (required != null && required.isArray()) {
                List<String> names = new ArrayList<>();
                required.forEach(n -> names.add(n.asText()));
                if (!names.isEmpty()) b.required(names);
            }
            return b.build();
        } catch (Exception e) {
            return null;
        }
    }

    private static void addProperty(JsonObjectSchema.Builder b, String name, JsonNode property) {
        String type = property.path("type").asText("string");
        String description = property.hasNonNull("description") ? property.get("description").asText() : null;
        switch (type) {
            case "integer" -> { if (description != null) b.addIntegerProperty(name, description); else b.addIntegerProperty(name); }
            case "number" -> { if (description != null) b.addNumberProperty(name, description); else b.addNumberProperty(name); }
            case "boolean" -> { if (description != null) b.addBooleanProperty(name, description); else b.addBooleanProperty(name); }
            default -> { if (description != null) b.addStringProperty(name, description); else b.addStringProperty(name); }
        }
    }

    // ── Response (LangChain4j → Catalyst) ────────────────────────────────────

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
