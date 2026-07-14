package com.cajunsystems.catalyst;

/**
 * Reconstructs a {@link Task} instance for a recorded execution so the runtime can
 * {@code resume(id)} it without the caller holding the original task (spec §7, v0.2 increment ②).
 *
 * <p>A task is designed to be re-invoked on every resume/replay — recorded boundaries are
 * substituted from the log — so a factory may safely return a shared, stateless instance or build a
 * fresh one per call. The reconstructed task must be behaviourally identical to the one that was
 * first executed; otherwise the resume diverges from the recorded log and (correctly) trips
 * {@link com.cajunsystems.catalyst.engine.NonDeterministicReplayException} under strict replay.
 */
@FunctionalInterface
public interface TaskFactory {

    /** Produces a task instance to drive a resume. */
    Task<?> create();
}
