package com.cajunsystems.catalyst;

/**
 * Accumulated token usage and monetary cost for an execution, folded from {@code CompletionReceived}
 * events. Observability by construction: cost is a fold over events, not a separate metric.
 */
public record Cost(long promptTokens, long completionTokens, double usd) {

    public static final Cost ZERO = new Cost(0, 0, 0.0);

    public long totalTokens() {
        return promptTokens + completionTokens;
    }

    /** Returns a new cost with the given usage added. */
    public Cost plus(long addPromptTokens, long addCompletionTokens, double addUsd) {
        return new Cost(promptTokens + addPromptTokens,
                completionTokens + addCompletionTokens,
                usd + addUsd);
    }
}
