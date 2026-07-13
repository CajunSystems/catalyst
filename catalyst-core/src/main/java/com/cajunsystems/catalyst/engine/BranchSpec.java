package com.cajunsystems.catalyst.engine;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Configures a branch replay (spec §7): fork the parent execution at {@code atSeq}, optionally
 * substituting counterfactual results for named tools during the replayed prefix, then run forward
 * live. Present only when the {@link ReplayingContext} is driving a branch; normal executions and
 * plain replays pass {@code null}.
 *
 * @param atSeq             substitute recorded boundaries up to and including this seq, then go live
 * @param parentId          the execution being branched from (for the {@code ExecutionBranched} event)
 * @param toolOverrides     counterfactual outputs by tool name (already serialized to JSON)
 * @param changedComponents human description of what was swapped, for the event/timeline
 */
public record BranchSpec(long atSeq, String parentId, Map<String, JsonNode> toolOverrides, String changedComponents) {

    public BranchSpec {
        toolOverrides = toolOverrides == null ? Map.of() : Map.copyOf(toolOverrides);
    }
}
