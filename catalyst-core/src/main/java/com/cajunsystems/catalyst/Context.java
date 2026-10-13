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

    /**
     * Waits for the named signal and returns its payload — the human-in-the-loop boundary. If the
     * signal has already been delivered (this is a resume or replay), its recorded payload is returned
     * at once. Otherwise the wait is recorded, the execution parks in {@code WAITING} with no thread
     * held, and the call does not return: the task resumes from the top, with everything before this
     * point substituted, once {@code runtime.signal(id, name, payload)} delivers it.
     *
     * <p>Like any boundary, the payload must be a recordable type (a record, enum, string, number,
     * {@code java.time} value, {@code UUID}, or collections of those).
     *
     * @param type the payload's expected type; a delivered payload of another type fails here
     * @throws UnsupportedOperationException if this context cannot wait (e.g. a test double)
     */
    default <T> T awaitSignal(String name, Class<T> type) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support signals");
    }

    /** A task input variable supplied via {@link ExecutionOptions}. */
    <T> T var(String name);

    /** Identity and metadata for this execution. */
    ExecutionInfo info();

    /** A logger scoped to this execution. */
    Logger log();

    /**
     * Sleeps durably for {@code duration}: the timer is recorded, the attempt ends holding no thread,
     * and the execution waits as {@code WAITING} until the timer comes due. It is then resumed, on
     * this node or (distributed) any node, after any restarts, and the sleep returns. On replay or
     * resume of a timer that already fired, it returns at once.
     *
     * <p>The wake time is fixed when the sleep is first recorded, from the runtime's clock, so a
     * resume waits only for what is left. Expect to wake at or shortly after it, not exactly at it:
     * a runtime fires due timers in-process, and also at startup recovery and on each claim-loop
     * tick when distributed.
     *
     * <p>The default throws, so other {@code Context} implementations still compile.
     */
    default void sleep(java.time.Duration duration) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support durable sleep");
    }

    /**
     * Whether {@code failure}, thrown by {@link #call}, is the recorded result of that call: the tool
     * failed, and the failure is in the log as its {@code ToolCompleted}. That covers a live failure, a
     * failure substituted on replay or resume, and one reproduced by a branch. Code that handles tool
     * failures as data (an agent telling the model) should do so only when this is true. Anything else
     * thrown from {@code call}, a failed log write for one, means the call's outcome is not recorded,
     * and carrying on would record later boundaries on top of that gap.
     *
     * <p>The default returns {@code false}, the safe answer for a context that does not track it.
     */
    default boolean isRecordedToolFailure(Throwable failure) {
        return false;
    }
}
