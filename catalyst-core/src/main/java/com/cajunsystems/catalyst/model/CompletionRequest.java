package com.cajunsystems.catalyst.model;

import java.util.List;
import java.util.Map;

/**
 * An immutable request to a {@link Model}: the {@link Prompt}, the tool specs offered, and free-form
 * params (temperature, max tokens, …). No provider-specific concepts leak through here.
 *
 * <p>For replay, only the messages and tool specs are canonically hashed — never model name,
 * temperature defaults, or timestamps — so benign config drift does not shatter replay (spec §6).
 */
public record CompletionRequest(Prompt prompt, List<ToolSpec> tools, Map<String, Object> params) {

    public CompletionRequest {
        if (prompt == null) throw new IllegalArgumentException("prompt must not be null");
        tools = tools == null ? List.of() : List.copyOf(tools);
        params = params == null ? Map.of() : Map.copyOf(params);
    }

    public static CompletionRequest of(Prompt prompt) {
        return new CompletionRequest(prompt, List.of(), Map.of());
    }

    public CompletionRequest withTools(List<ToolSpec> tools) {
        return new CompletionRequest(prompt, tools, params);
    }

    public CompletionRequest withParams(Map<String, Object> params) {
        return new CompletionRequest(prompt, tools, params);
    }
}
