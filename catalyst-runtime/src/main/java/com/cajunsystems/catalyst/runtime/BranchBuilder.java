package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.model.Model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluent builder for a branch (spec §7): fork a recorded execution at a chosen sequence number,
 * optionally swap the model, substitute counterfactual tool results, or supply task variables, then
 * {@link #run} the task forward to produce a new {@link Trajectory}.
 *
 * <pre>{@code
 * Trajectory fork = runtime.branch(id, 12)
 *         .withModel(otherModel)
 *         .withRecordedToolResult("eligibility", new Eligibility(false, "lapsed"))
 *         .run(task);
 * }</pre>
 */
public final class BranchBuilder {

    private final CatalystRuntime runtime;
    private final ExecutionId parentId;
    private final long atSeq;

    private Model overrideModel;
    private final Map<String, Object> toolOverrides = new LinkedHashMap<>();
    private final Map<String, Object> vars = new LinkedHashMap<>();
    private final List<String> changed = new ArrayList<>();
    private BranchSpec.ToolReplay toolReplay = BranchSpec.ToolReplay.LIVE;
    private final ExecutionId childId;

    BranchBuilder(CatalystRuntime runtime, ExecutionId parentId, long atSeq) {
        this.runtime = runtime;
        this.parentId = parentId;
        this.atSeq = atSeq;
        this.childId = runtime.mintId();
    }

    /**
     * The id the branch's own execution will have, known before {@link #run}, so a caller can find
     * the branch's record even when the run throws. A failed branch still leaves its
     * {@code ExecutionBranched} … {@code ExecutionFailed} stream.
     */
    public ExecutionId childId() {
        return childId;
    }

    /** Run the branch forward with a different model from the branch point. */
    public BranchBuilder withModel(Model model) {
        this.overrideModel = model;
        this.changed.add("model");
        return this;
    }

    /** Substitute a counterfactual result for {@code toolName} during the replayed prefix. */
    public BranchBuilder withRecordedToolResult(String toolName, Object output) {
        this.toolOverrides.put(toolName, output);
        this.changed.add("tool:" + toolName);
        return this;
    }

    /** Provide a task input variable for the branched run. */
    public BranchBuilder withVar(String name, Object value) {
        this.vars.put(name, value);
        return this;
    }

    /**
     * How tool calls are answered once the branch has forked (default {@link BranchSpec.ToolReplay#LIVE}).
     * The recorded modes answer from the parent's log, so a counterfactual run does not repeat
     * the parent's side effects. That is what an evaluation needs.
     */
    public BranchBuilder toolReplay(BranchSpec.ToolReplay toolReplay) {
        this.toolReplay = java.util.Objects.requireNonNull(toolReplay, "toolReplay");
        return this;
    }

    /** Runs {@code task} forward from the branch point and returns the fork's trajectory. */
    public <R> Trajectory run(Task<R> task) {
        return runtime.runBranch(parentId, atSeq, overrideModel, toolOverrides, vars,
                String.join(",", changed), toolReplay, childId, task);
    }
}
