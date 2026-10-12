package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.engine.UnrecordedToolCallException;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.runtime.BranchBuilder;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The eval harness (spec §12): replays recorded executions against candidate models and prompts and
 * scores the results, so the log works as a regression suite.
 *
 * <pre>{@code
 * EvalReport report = Eval.on(runtime)
 *         .cases(recordedIds)
 *         .task(new SummarizeTask())
 *         .candidate(Candidate.model("mini", miniModel))
 *         .candidate(Candidate.task("prompt-v2", new SummarizeTaskV2()))
 *         .scorer(Scorers.sameResult())
 *         .run();
 * System.out.print(report.pretty());
 * }</pre>
 *
 * <p>Each case is a branch (M2) of the recorded execution, taken just before its first model call.
 * Everything recorded before that point is substituted exactly, so the clock reads, ids and lookups
 * the original run made are the ones the candidate sees, and the comparison isolates the change. A
 * candidate task that diverges earlier (a new prompt built before the first call, say) forks where
 * it diverges.
 *
 * <p><strong>Tools are not re-run.</strong> By default ({@link BranchSpec.ToolReplay#RECORDED_ONLY})
 * a tool call the recorded run made with the same input gets its recorded output. One it never made
 * ends the case as {@link CaseResult.Outcome#UNRECORDED_TOOL}, a finding about the candidate. Nothing
 * is sent to the outside world except to the candidate's model. Deterministic tools
 * ({@code @Deterministic}) are re-executed, as in any replay. {@link #toolReplay} can allow live tools
 * for an evaluation that wants them.
 *
 * <p>Every case leaves a branch execution in the log ({@link CaseResult#branchId()}), inspectable,
 * replayable and renderable like any other. Cases run one after another on the calling thread.
 */
public final class Eval {

    private final CatalystRuntime runtime;
    private final List<ExecutionId> cases = new ArrayList<>();
    private final List<Candidate> candidates = new ArrayList<>();
    private final List<Scorer> scorers = new ArrayList<>();
    private Task<?> task;
    private BranchSpec.ToolReplay toolReplay = BranchSpec.ToolReplay.RECORDED_ONLY;
    private final PayloadCodec payloads = new PayloadCodec();

    private Eval(CatalystRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
    }

    /** An evaluation over {@code runtime}'s log; the runtime's default model serves candidates that set none. */
    public static Eval on(CatalystRuntime runtime) {
        return new Eval(runtime);
    }

    /** The recorded executions to evaluate: the dataset. */
    public Eval cases(List<ExecutionId> ids) {
        cases.addAll(ids);
        return this;
    }

    public Eval cases(ExecutionId... ids) {
        return cases(List.of(ids));
    }

    /** The production task the cases recorded: what a candidate runs unless it brings its own. */
    public Eval task(Task<?> task) {
        this.task = Objects.requireNonNull(task, "task");
        return this;
    }

    public Eval candidate(Candidate candidate) {
        candidates.add(Objects.requireNonNull(candidate, "candidate"));
        return this;
    }

    public Eval scorer(Scorer scorer) {
        scorers.add(Objects.requireNonNull(scorer, "scorer"));
        return this;
    }

    /** How tool calls are answered (default {@link BranchSpec.ToolReplay#RECORDED_ONLY}: never run for real). */
    public Eval toolReplay(BranchSpec.ToolReplay toolReplay) {
        this.toolReplay = Objects.requireNonNull(toolReplay, "toolReplay");
        return this;
    }

    /** Runs every candidate over every case, in order, and scores the completed runs. */
    public EvalReport run() {
        if (candidates.isEmpty()) throw new IllegalStateException("No candidates: add at least one with candidate(...)");
        Set<String> names = new LinkedHashSet<>();
        for (Candidate c : candidates) {
            if (!names.add(c.name())) throw new IllegalStateException("Two candidates are named '" + c.name() + "'");
            if (c.task() == null && task == null) {
                throw new IllegalStateException("Candidate '" + c.name() + "' brings no task, and none was given to task(...)");
            }
        }
        List<CaseResult> results = new ArrayList<>();
        for (ExecutionId id : cases) {
            ExecutionState baseline = runtime.inspect(id);
            for (Candidate candidate : candidates) {
                results.add(runCase(id, baseline, candidate));
            }
        }
        return new EvalReport(List.copyOf(names), results);
    }

    private CaseResult runCase(ExecutionId id, ExecutionState baseline, Candidate candidate) {
        if (baseline.status() != Status.COMPLETED) {
            return new CaseResult(id, candidate.name(), CaseResult.Outcome.SKIPPED, null, null, List.of(),
                    "recorded run is " + baseline.status() + ", not COMPLETED");
        }
        BranchBuilder branch = runtime.branch(id, branchPoint(id)).toolReplay(toolReplay);
        if (candidate.model() != null) branch.withModel(candidate.model());
        Task<?> toRun = candidate.task() != null ? candidate.task() : task;

        Trajectory forked;
        try {
            forked = branch.run(toRun);
        } catch (RuntimeException e) {
            UnrecordedToolCallException unrecorded = unrecordedIn(e);
            if (unrecorded != null) {
                return new CaseResult(id, candidate.name(), CaseResult.Outcome.UNRECORDED_TOOL, null, null, List.of(),
                        unrecorded.getMessage());
            }
            return new CaseResult(id, candidate.name(), CaseResult.Outcome.FAILED, null, null, List.of(), String.valueOf(e));
        }

        ExecutionState candidateState = runtime.inspect(forked.id());
        Trajectory baselineTrajectory = Trajectory.of(baseline);
        Sample sample = new Sample(id, candidate.name(),
                decode(baseline.result()), decode(candidateState.result()),
                baselineTrajectory, forked, Trajectory.diff(baselineTrajectory, forked));
        List<Score> scores = new ArrayList<>();
        for (Scorer scorer : scorers) scores.add(scorer.score(sample));
        return new CaseResult(id, candidate.name(), CaseResult.Outcome.COMPLETED, forked.id(), sample, scores, null);
    }

    /**
     * Just before the recorded run's first model call (its {@code PromptBuilt}): everything earlier is substituted exactly, and
     * the candidate takes over from there. A run with no model call is replayed whole.
     */
    private long branchPoint(ExecutionId id) {
        List<SequencedEvent> events = runtime.log().read(id);
        for (SequencedEvent se : events) {
            // A model call's boundary starts at the PromptBuilt it records before CompletionRequested.
            if (se.event() instanceof CatalystEvent.PromptBuilt || se.event() instanceof CatalystEvent.CompletionRequested) {
                return se.seq() - 1;
            }
        }
        return events.isEmpty() ? -1 : events.get(events.size() - 1).seq();
    }

    private Object decode(JsonNode tree) {
        return tree == null || tree.isNull() ? null : payloads.fromTree(tree);
    }

    private static UnrecordedToolCallException unrecordedIn(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof UnrecordedToolCallException u) return u;
            if (c.getCause() == c) break;
        }
        return null;
    }
}
