package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Task;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

/**
 * How a {@link ReplayingContext} starts and follows child executions ({@code ctx.child}): the
 * runtime's part of the seam, since only the runtime can create and schedule an execution.
 */
public interface ChildLauncher {

    /** A fresh id for a new child execution. */
    ExecutionId mint();

    /**
     * Makes sure the child exists and will finish. It creates the child if this is the first time
     * (fenced, so it happens once even if two attempts race), or brings back one a crash left
     * behind. It returns the child's outcome if it has already reached a terminal state, and empty
     * while it is still going.
     */
    Optional<Outcome> ensure(ExecutionId parentId, String childRef, ExecutionId childId, Task<?> task);

    /** How a child ended: its recorded result, or its terminal status and error. */
    /**
     * Cancels a child the parent no longer wants (a supervised group restarting it, or giving up).
     * Idempotent: cancelling a child that has ended does nothing.
     */
    default void cancel(ExecutionId childId) {
        throw new UnsupportedOperationException("this launcher cannot cancel children");
    }

    record Outcome(String status, JsonNode result, String error) {
        public boolean completed() {
            return "COMPLETED".equals(status);
        }
    }
}
