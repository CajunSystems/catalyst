package com.cajunsystems.catalyst.engine;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Configures a branch replay (spec §7): fork the parent execution at {@code atSeq}, optionally
 * substituting counterfactual results for named tools during the replayed prefix, then run forward
 * live. Present only when the {@link ReplayingContext} is driving a branch; normal executions and
 * plain replays pass {@code null}.
 *
 * <p>Past the fork everything normally runs live, tools included, which suits an interactive "what
 * if". An evaluation must not re-run side effects (send the email, post the order), so a branch can
 * instead answer tool calls from the parent's recording ({@link ToolReplay}). A call the parent made
 * with the identical input gets the parent's output. One it never made is refused, unless live tools
 * are allowed.
 *
 * @param atSeq               substitute recorded boundaries up to and including this seq, then go live
 * @param parentId            the execution being branched from (for the {@code ExecutionBranched} event)
 * @param toolOverrides       counterfactual outputs by tool name (already serialized to JSON)
 * @param changedComponents   human description of what was swapped, for the event/timeline
 * @param toolReplay          how tool calls past the fork are answered
 * @param recordedToolResults the parent's successful tool outputs, keyed by {@link #toolKey}; used
 *                            unless {@code toolReplay} is {@link ToolReplay#LIVE}
 */
public record BranchSpec(long atSeq, String parentId, Map<String, JsonNode> toolOverrides, String changedComponents,
                         ToolReplay toolReplay, Map<String, JsonNode> recordedToolResults) {

    /** How a branch answers tool calls once it has forked off the recording. */
    public enum ToolReplay {
        /** Run them for real: the branch is a live run from the fork on (the default). */
        LIVE,
        /** Answer from the recording where the parent made the identical call; run the rest live. */
        RECORDED_OR_LIVE,
        /** Answer from the recording, and refuse a call the parent never made ({@link UnrecordedToolCallException}). */
        RECORDED_ONLY
    }

    public BranchSpec {
        toolOverrides = toolOverrides == null ? Map.of() : Map.copyOf(toolOverrides);
        toolReplay = toolReplay == null ? ToolReplay.LIVE : toolReplay;
        recordedToolResults = recordedToolResults == null ? Map.of() : Map.copyOf(recordedToolResults);
    }

    public BranchSpec(long atSeq, String parentId, Map<String, JsonNode> toolOverrides, String changedComponents) {
        this(atSeq, parentId, toolOverrides, changedComponents, ToolReplay.LIVE, Map.of());
    }

    /** The key a recorded tool result is found by: the tool's name and its input's canonical hash. */
    public static String toolKey(String toolName, String inputHash) {
        return toolName + "#" + inputHash;
    }
}
