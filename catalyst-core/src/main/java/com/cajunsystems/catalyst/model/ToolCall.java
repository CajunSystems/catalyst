package com.cajunsystems.catalyst.model;

/** A tool-call request emitted by a model in a {@link Completion}: an id, a tool name, and JSON args. */
public record ToolCall(String id, String name, String argumentsJson) {

    public ToolCall {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("tool name required");
    }
}
