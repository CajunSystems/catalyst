package com.cajunsystems.catalyst.engine;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
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
 * @param recordedToolResults every successful tool call the parent recorded, keyed by {@link #toolKey},
 *                            each key's outputs in recorded order with the seq they completed at. The
 *                            context answers a live call only from outputs recorded after the point
 *                            replay actually reached. A candidate can fork earlier than
 *                            {@code atSeq}, and the parent's calls from there on are still unused. A
 *                            repeated call (a second clock read, a poll that answered "pending" then
 *                            "done") gets the next output in turn, not the first again. Used unless
 *                            {@code toolReplay} is {@link ToolReplay#LIVE}
 */
public record BranchSpec(long atSeq, String parentId, Map<String, JsonNode> toolOverrides, String changedComponents,
                         ToolReplay toolReplay, Map<String, List<RecordedOutput>> recordedToolResults) {

    /**
     * One recorded tool result and the seq of the {@code ToolCompleted} that recorded it: an output,
     * or, if the call failed, the recorded error. A recorded failure is an answer too. A branch
     * reproduces it as a replay does, so a run that recovered from a failed call (an agent telling
     * the model) can be re-run without re-running the call.
     */
    public record RecordedOutput(long seq, JsonNode output, String error) {
        public RecordedOutput(long seq, JsonNode output) {
            this(seq, output, null);
        }
    }

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
        recordedToolResults = recordedToolResults == null ? Map.of()
                : recordedToolResults.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, e -> List.copyOf(e.getValue())));
    }

    public BranchSpec(long atSeq, String parentId, Map<String, JsonNode> toolOverrides, String changedComponents) {
        this(atSeq, parentId, toolOverrides, changedComponents, ToolReplay.LIVE, Map.of());
    }

    /** The key a recorded tool result is found by: the tool's name and its input's canonical hash. */
    public static String toolKey(String toolName, String inputHash) {
        return toolName + "#" + inputHash;
    }
}
