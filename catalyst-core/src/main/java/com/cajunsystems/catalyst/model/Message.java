package com.cajunsystems.catalyst.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * A single message in a {@link Prompt}.
 *
 * <p>Two optional fields carry native tool calling, for a conversation that goes model → tools →
 * model (an agent loop):
 * <ul>
 *   <li>{@code toolCalls}: on an {@link Role#ASSISTANT} message, the tool calls the model asked for
 *       in that turn.</li>
 *   <li>{@code toolCallId} and {@code toolName}: on a {@link Role#TOOL} message, which call this
 *       result answers.</li>
 * </ul>
 * Both are omitted from the recorded JSON when absent, and they contribute to the request hash only
 * when present. So a message without them serializes and hashes exactly as it always has, and logs
 * recorded before they existed replay unchanged.
 */
public record Message(Role role, String content,
                      @JsonInclude(JsonInclude.Include.NON_EMPTY) List<ToolCall> toolCalls,
                      @JsonInclude(JsonInclude.Include.NON_NULL) String toolCallId,
                      @JsonInclude(JsonInclude.Include.NON_NULL) String toolName) {

    public Message {
        if (role == null) throw new IllegalArgumentException("role must not be null");
        if (content == null) throw new IllegalArgumentException("content must not be null");
        toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
        if (!toolCalls.isEmpty() && role != Role.ASSISTANT) {
            throw new IllegalArgumentException("only an ASSISTANT message carries tool calls");
        }
        if (toolCallId != null && role != Role.TOOL) {
            throw new IllegalArgumentException("only a TOOL message answers a tool call");
        }
    }

    public Message(Role role, String content) {
        this(role, content, List.of(), null, null);
    }

    /** The model's turn that asked for {@code calls} (with whatever text accompanied them). */
    public static Message assistantToolCalls(String content, List<ToolCall> calls) {
        return new Message(Role.ASSISTANT, content == null ? "" : content, calls, null, null);
    }

    /** The result of {@code call}, to send back to the model. */
    public static Message toolResult(ToolCall call, String content) {
        Objects.requireNonNull(call, "call");
        return new Message(Role.TOOL, content, List.of(), call.id(), call.name());
    }
}
