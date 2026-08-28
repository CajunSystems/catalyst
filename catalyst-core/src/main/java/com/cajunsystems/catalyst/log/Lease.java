package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;

import java.time.Instant;
import java.util.Objects;

/**
 * A node's claim on an execution: "{@code nodeId} intends to run {@code execution} until
 * {@code expiresAt}".
 *
 * <p><strong>A lease is placement, not correctness.</strong> That distinction is the whole reason
 * the distributed design works, and it is worth stating on the type itself so nobody later mistakes
 * this for the guard. A lease can be wrong in every way a distributed lock can be wrong — the holder
 * can stall past {@code expiresAt} in a GC pause and keep believing it owns the execution while
 * another node claims it. What stops the resulting race from corrupting the stream is not this
 * record but conditional append: a stale writer's events are rejected by storage because the stream
 * has moved past the seq it expected. The lease exists only so that two nodes do not routinely burn
 * duplicate provider spend racing the same work.
 *
 * <p>So read {@link #isExpired} as "worth stealing", never as "the holder has stopped".
 *
 * @param execution the execution claimed
 * @param nodeId    the claiming node
 * @param token     a fencing token, incremented on every successful claim of this execution. Not
 *                  consulted on the write path — {@code expectedSeq} is the fence — but it makes a
 *                  contested execution legible in logs: a token climbing without progress is nodes
 *                  stealing from each other.
 * @param expiresAt when the claim lapses and another node may steal it
 */
public record Lease(ExecutionId execution, String nodeId, long token, Instant expiresAt) {

    public Lease {
        Objects.requireNonNull(execution, "execution");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(expiresAt, "expiresAt");
    }

    /** True if {@code now} is at or past the expiry — i.e. another node may claim this execution. */
    public boolean isExpired(Instant now) {
        return !now.isBefore(expiresAt);
    }

    /** This lease renewed to a new expiry, keeping its token (a renewal is not a fresh claim). */
    public Lease renewedUntil(Instant newExpiry) {
        return new Lease(execution, nodeId, token, newExpiry);
    }
}
