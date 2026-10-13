package com.cajunsystems.catalyst.agents;

/**
 * How an {@link Agent} run ended.
 *
 * @param answer     the model's final text; on {@link StopReason#STEP_LIMIT}, the last text it produced
 *                   (possibly empty)
 * @param stoppedBy  why the loop ended
 * @param modelTurns how many times the model was called
 * @param toolCalls  how many tool calls the model made, including refused ones (unknown tool, bad arguments)
 */
public record AgentResult(String answer, StopReason stoppedBy, int modelTurns, int toolCalls) {

    public enum StopReason {
        /** The model answered without asking for a tool. */
        ANSWERED,
        /** The model was still asking for tools when the step budget ran out. */
        STEP_LIMIT
    }
}
