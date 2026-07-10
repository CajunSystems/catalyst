package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;

import java.time.Duration;

/**
 * A handle to a running (or resumed) execution. Handle-first API: {@link #result()} blocks in a
 * virtual-thread-friendly way until the execution completes (spec §4).
 *
 * @param <R> the task's result type
 */
public interface ExecutionHandle<R> {

    /** The id of the execution this handle refers to. */
    ExecutionId id();

    /** Blocks until the execution completes and returns its result, or throws its failure cause. */
    R result();

    /** Blocks up to {@code timeout} for the result. */
    R result(Duration timeout);
}
