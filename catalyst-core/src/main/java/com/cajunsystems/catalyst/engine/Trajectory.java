package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;

import java.util.List;

/**
 * The ordered path an execution took — its timeline steps plus identity and cost (spec §7). A branch
 * produces a new {@code Trajectory}; comparing two with {@link #diff} shows what changed between an
 * original run and a counterfactual one.
 */
public record Trajectory(ExecutionId id, Status status, List<TimelineStep> steps, Cost cost) {

    public Trajectory {
        steps = steps == null ? List.of() : List.copyOf(steps);
        cost = cost == null ? Cost.ZERO : cost;
    }

    /** The trajectory of a folded execution state. */
    public static Trajectory of(ExecutionState state) {
        return new Trajectory(state.id(), state.status(), state.trajectory(), state.cost());
    }

    /** Builds a trajectory from an explicit step list (e.g. a branch's effective prefix + fork). */
    public static Trajectory of(ExecutionId id, Status status, List<TimelineStep> steps, Cost cost) {
        return new Trajectory(id, status, steps, cost);
    }

    /** The step-by-step difference between a base trajectory and a fork of it. */
    public static TrajectoryDiff diff(Trajectory base, Trajectory fork) {
        return TrajectoryDiff.between(base, fork);
    }
}
