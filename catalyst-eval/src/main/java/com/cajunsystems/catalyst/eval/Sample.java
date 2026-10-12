package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.engine.TrajectoryDiff;

/**
 * What a {@link Scorer} sees for one case: the recorded run and the candidate's run of the same
 * input, side by side.
 *
 * @param caseId              the recorded execution
 * @param candidate           the candidate's name
 * @param baselineResult      the recorded result, decoded
 * @param candidateResult     the candidate's result, decoded
 * @param baselineTrajectory  the recorded run's steps, tokens and cost
 * @param candidateTrajectory the candidate's steps, tokens and cost (recorded prefix included)
 * @param diff                the step-by-step difference between the two
 */
public record Sample(ExecutionId caseId, String candidate,
                     Object baselineResult, Object candidateResult,
                     Trajectory baselineTrajectory, Trajectory candidateTrajectory,
                     TrajectoryDiff diff) {
}
