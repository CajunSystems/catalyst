package com.cajunsystems.catalyst.model;

/**
 * The specification of a tool offered to a model: its name, a human description, and a JSON-schema
 * string describing its input. Part of the canonically hashed request.
 */
public record ToolSpec(String name, String description, String inputSchema) {

    public ToolSpec {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("tool name required");
    }
}
