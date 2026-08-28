package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;

/**
 * One entry read from a {@link WorkQueue}: an execution that was submitted to the queue, together
 * with its position in that queue's own stream.
 *
 * @param cursor    the queue tag's dense position for this entry. Dense <em>in the queue</em>, not in
 *                  the execution — the two streams number independently, which is exactly what
 *                  Gumbo 0.6.0's per-tag versions bought and what makes a worker cursor a single
 *                  {@code long}. Pass the highest one consumed back to {@link WorkQueue#poll} to
 *                  continue.
 * @param execution the execution to claim and run
 */
public record QueuedExecution(long cursor, ExecutionId execution) {
}
