package com.cajunsystems.catalyst.engine;

import java.util.List;

/**
 * A typed, aggregated view over an execution's timeline (spec §5): the ordered steps plus the
 * roll-ups that observability cares about — model/tool call counts, token usage, cost and latency.
 * Every value here is a fold over the same events; there is no separate metrics pipeline.
 */
public record Timeline(
        List<TimelineStep> steps,
        int modelCalls,
        int toolCalls,
        long promptTokens,
        long completionTokens,
        double totalCostUsd,
        long totalLatencyMillis) {

    public Timeline {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public long totalTokens() {
        return promptTokens + completionTokens;
    }

    /** Builds the aggregated view from a folded {@link ExecutionState}. */
    public static Timeline from(ExecutionState state) {
        int models = 0;
        int tools = 0;
        for (TimelineStep step : state.timeline()) {
            switch (step.kind()) {
                case MODEL -> models++;
                case TOOL -> tools++;
                default -> { /* not a billable call */ }
            }
        }
        return new Timeline(state.timeline(), models, tools,
                state.cost().promptTokens(), state.cost().completionTokens(),
                state.cost().usd(), state.totalLatencyMillis());
    }
}
