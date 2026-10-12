package com.cajunsystems.catalyst.eval;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Judges a candidate's run of one case against the recording. Scorers are pure functions of the
 * {@link Sample}, so a report can be recomputed from the log. {@link Scorers} has the common ones; a
 * domain check is a lambda:
 *
 * <pre>{@code
 * Scorer.check("mentions-floor", s -> String.valueOf(s.candidateResult()).contains("floor"))
 * }</pre>
 */
@FunctionalInterface
public interface Scorer {

    Score score(Sample sample);

    /** A pass/fail check named {@code name}: value 1 when it passes, 0 when it does not. */
    static Scorer check(String name, Predicate<Sample> passes) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(passes, "passes");
        return sample -> passes.test(sample)
                ? Score.pass(name, 1, "")
                : Score.fail(name, 0, "check failed");
    }
}
