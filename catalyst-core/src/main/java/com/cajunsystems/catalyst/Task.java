package com.cajunsystems.catalyst;

/**
 * A unit of AI work — the thing Catalyst executes durably. There is deliberately no "Agent" concept
 * in v0; an agent is a v1 consumer built <em>on</em> {@code Task}. A reasoning loop is just a Task
 * with a while-loop in it (spec §4).
 *
 * <p>Everything a task needs is obtained from the {@link Context} passed to {@link #execute} — no
 * statics, no thread-locals in user code. Code between recorded boundaries (model calls, tool calls,
 * effects, memory, clock) must be deterministic, the same discipline as a Temporal workflow.
 *
 * @param <R> the task's result type; must be serializable via the configured serializer
 */
@FunctionalInterface
public interface Task<R> {

    /**
     * Runs the task. Called on the first execution and re-invoked on every resume/replay; recorded
     * boundaries are substituted from the log so re-invocation performs no duplicate side effects.
     */
    R execute(Context ctx) throws Exception;
}
