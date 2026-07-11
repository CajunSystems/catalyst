package com.cajunsystems.catalyst.engine;

/**
 * Prices a model completion from its token usage, so cost is recorded on {@code CompletionReceived}
 * and folds into an execution's {@link com.cajunsystems.catalyst.Cost} (spec §5 — cost is a fold
 * over events, not a separate metric). Providers report tokens; how those map to money is a
 * deployment concern, so it is pluggable. The default is {@link #free()} (zero), matching M0.
 */
@FunctionalInterface
public interface CostModel {

    /** The cost, in USD, of a completion with the given prompt and completion token counts. */
    double usd(long promptTokens, long completionTokens);

    /** A cost model that always returns zero. */
    static CostModel free() {
        return (promptTokens, completionTokens) -> 0.0;
    }

    /** A flat per-million-token price for input and output tokens. */
    static CostModel perMillionTokens(double inputUsdPerMillion, double outputUsdPerMillion) {
        return (promptTokens, completionTokens) ->
                (promptTokens * inputUsdPerMillion + completionTokens * outputUsdPerMillion) / 1_000_000.0;
    }
}
