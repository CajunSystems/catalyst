package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.log.Lease;

/**
 * A distributed runtime refused to act on an execution because another node holds its live lease,
 * meaning it is running there. Thrown before anything is written.
 *
 * <p>Retry after {@link #lease()}{@code .expiresAt()}: if the holder has died, its lease lapses
 * and the execution can be taken over then.
 */
public class LeaseHeldException extends IllegalStateException {

    private final ExecutionId executionId;
    private final Lease lease;

    public LeaseHeldException(ExecutionId executionId, Lease lease) {
        super("Execution " + executionId.value() + " is leased by node '" + lease.holder() + "' until "
                + lease.expiresAt() + ": it is running there");
        this.executionId = executionId;
        this.lease = lease;
    }

    public ExecutionId executionId() {
        return executionId;
    }

    /** The lease that blocked the call, as read. */
    public Lease lease() {
        return lease;
    }
}
