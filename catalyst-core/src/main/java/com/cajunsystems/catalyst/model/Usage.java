package com.cajunsystems.catalyst.model;

/** Token usage reported by a model for one completion. */
public record Usage(long promptTokens, long completionTokens) {

    public static final Usage ZERO = new Usage(0, 0);

    public long totalTokens() {
        return promptTokens + completionTokens;
    }
}
