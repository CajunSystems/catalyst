package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.model.Model;

import java.util.Objects;

/**
 * What an evaluation tries against the recording: a different model, a different version of the
 * task (new prompts, a new tool strategy), or both. Anything left null is the production one: the
 * runtime's default model, and the task given to {@link Eval#task}.
 *
 * @param name  how the candidate is reported
 * @param model the model to run with, or null for the runtime's default
 * @param task  the task to run, or null for the evaluation's task
 */
public record Candidate(String name, Model model, Task<?> task) {

    public Candidate {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("name must be non-blank");
    }

    /** The production task, run with {@code model}. */
    public static Candidate model(String name, Model model) {
        return new Candidate(name, Objects.requireNonNull(model, "model"), null);
    }

    /** {@code task}, for example with rewritten prompts, run with the runtime's default model. */
    public static Candidate task(String name, Task<?> task) {
        return new Candidate(name, null, Objects.requireNonNull(task, "task"));
    }

    /** {@code task} run with {@code model}. */
    public static Candidate of(String name, Model model, Task<?> task) {
        return new Candidate(name, model, task);
    }
}
