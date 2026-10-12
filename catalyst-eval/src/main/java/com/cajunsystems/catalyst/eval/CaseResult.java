package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.engine.Trajectory;

import java.util.List;

/**
 * One candidate's run of one recorded case.
 *
 * @param caseId              the recorded execution
 * @param candidate           the candidate's name
 * @param outcome             how the candidate's run ended
 * @param branchId            the branch execution holding the candidate's run (inspectable like any
 *                            other execution, failed runs included), or null if the case was skipped
 * @param sample              what the scorers saw; null unless the outcome is {@link Outcome#COMPLETED}
 * @param scores              one per scorer; empty unless completed
 * @param error               why it did not complete, or null
 */
public record CaseResult(ExecutionId caseId, String candidate, Outcome outcome, ExecutionId branchId,
                         Sample sample, List<Score> scores, String error) {

    public CaseResult {
        scores = scores == null ? List.of() : List.copyOf(scores);
    }

    public enum Outcome {
        /** The candidate finished; it was scored. */
        COMPLETED,
        /** The candidate's run threw: a model error, a task bug, a non-deterministic task. */
        FAILED,
        /**
         * The candidate made a tool call the recording cannot answer (an input the recorded run never
         * used), and the evaluation does not run tools for real. A finding about the candidate: it
         * took a different path.
         */
        UNRECORDED_TOOL,
        /** The recorded run did not complete, so there is nothing to compare against; not run. */
        SKIPPED
    }

    /** Whether every scorer passed. False for anything not completed. */
    public boolean passed() {
        return outcome == Outcome.COMPLETED && scores.stream().allMatch(Score::pass);
    }

    public Trajectory candidateTrajectory() {
        return sample == null ? null : sample.candidateTrajectory();
    }
}
