package com.cajunsystems.catalyst;

import com.cajunsystems.catalyst.model.Model;

import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * Everything available during execution, obtained only as the {@code execute} parameter (spec §4).
 * Every method on this interface that touches the outside world is a <em>recorded boundary</em>: on
 * first run it executes and appends an event; on replay/resume it substitutes the recorded result,
 * performing no side effect. That is what makes executions durable, resumable and replayable.
 */
public interface Context {

    /** The execution's model. Completions requested through it are recorded and substitutable. */
    Model model();

    /**
     * Invokes a tool. This is the <em>only</em> way to call a tool so that the invocation is
     * recorded (as {@code ToolRequested} before, {@code ToolCompleted} after), substitutable on
     * replay, and visible in the timeline.
     */
    <I, O> O call(Tool<I, O> tool, I input);

    /** Working memory: a key-value store scoped to this execution, backed by memory events. */
    Memory memory();

    /**
     * Captures nondeterminism at a labelled call site: runs {@code supplier} at most once per run
     * and records its value, substituting the recorded value on replay. Labels are namespaced by
     * call-site sequence so loops are safe (spec §6).
     */
    <T> T effect(String label, Supplier<T> supplier);

    /** A task input variable supplied via {@link ExecutionOptions}. */
    <T> T var(String name);

    /** Identity and metadata for this execution. */
    ExecutionInfo info();

    /** A logger scoped to this execution. */
    Logger log();
}
