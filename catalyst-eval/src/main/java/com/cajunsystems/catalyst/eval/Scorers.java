package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.engine.Trajectory;

import java.util.Objects;

/** Built-in {@link Scorer}s. */
public final class Scorers {

    private Scorers() {}

    /** Passes when the candidate returns exactly what the recorded run returned. */
    public static Scorer sameResult() {
        return s -> Objects.equals(s.baselineResult(), s.candidateResult())
                ? Score.pass("same-result", 1, "")
                : Score.fail("same-result", 0, "recorded " + abbreviate(s.baselineResult())
                        + ", candidate " + abbreviate(s.candidateResult()));
    }

    /**
     * The candidate's cost as a multiple of the recorded run's (value), passing when it is at most
     * {@code ratio}. A recorded run that cost nothing passes only a candidate that also cost nothing.
     */
    public static Scorer costAtMost(double ratio) {
        return s -> ratioScore("cost<=" + ratio + "x", s.baselineTrajectory().cost().usd(),
                s.candidateTrajectory().cost().usd(), ratio);
    }

    /** As {@link #costAtMost}, for total tokens. */
    public static Scorer tokensAtMost(double ratio) {
        return s -> ratioScore("tokens<=" + ratio + "x", tokens(s.baselineTrajectory()),
                tokens(s.candidateTrajectory()), ratio);
    }

    /** How many work steps (model, tool, effect, memory) differ from the recording; passes when none do. */
    public static Scorer sameSteps() {
        return s -> {
            long changed = s.diff().changedCount();
            return changed == 0
                    ? Score.pass("same-steps", 0, "")
                    : Score.fail("same-steps", changed, changed + " step(s) differ");
        };
    }

    private static Score ratioScore(String name, double baseline, double candidate, double ratio) {
        if (baseline == 0) {
            return candidate == 0 ? Score.pass(name, 1, "") : Score.fail(name, Double.POSITIVE_INFINITY, "recorded run cost nothing");
        }
        double value = candidate / baseline;
        String detail = String.format("%.2fx", value);
        return value <= ratio ? Score.pass(name, value, detail) : Score.fail(name, value, detail);
    }

    private static double tokens(Trajectory t) {
        return t.cost().totalTokens();
    }

    private static String abbreviate(Object o) {
        String s = String.valueOf(o);
        return s.length() <= 80 ? "'" + s + "'" : "'" + s.substring(0, 77) + "...'";
    }
}
