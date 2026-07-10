package com.cajunsystems.catalyst.model;

import java.util.List;

/**
 * The result of a model call: the assistant message, any tool-call requests, token usage, and a
 * finish reason. This is the value recorded in {@code CompletionReceived} and substituted on replay.
 */
public record Completion(String message, List<ToolCall> toolCalls, Usage usage, String finishReason) {

    public Completion {
        message = message == null ? "" : message;
        toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
        usage = usage == null ? Usage.ZERO : usage;
        finishReason = finishReason == null ? "stop" : finishReason;
    }

    /** A plain text completion with no tool calls and zero usage. */
    public static Completion ofText(String message) {
        return new Completion(message, List.of(), Usage.ZERO, "stop");
    }

    public boolean hasToolCalls() {
        return !toolCalls.isEmpty();
    }
}
