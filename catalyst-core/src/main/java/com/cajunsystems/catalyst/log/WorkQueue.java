package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * A shared queue of executions waiting to be run, plus the leases that keep two nodes from routinely
 * running the same one. This is the seam distributed execution claims work through
 * ({@code docs/distribution.md}); a log that cannot provide it simply has no queue, and
 * {@link EventLog#workQueue} returns empty.
 *
 * <p><strong>Submitting is one append, not two.</strong> {@link #submit} writes the execution's
 * first event to the execution's own stream <em>and</em> to this queue in a single atomic append
 * (Gumbo lets one entry carry several tags). That is not a micro-optimisation: a two-phase write
 * would leave a window in which an execution is recorded but not yet claimable — durable, invisible,
 * and run by nobody — and recovering from that window would need exactly the kind of reconciliation
 * scan this design exists to avoid. There is no such window because there is no second write.
 *
 * <p><strong>What this interface does not do is make concurrency safe.</strong> Leases reduce
 * duplicated work; they never prevent a stale writer from corrupting a stream. That guarantee comes
 * from {@link EventLog#append(ExecutionId, CatalystEvent, long) conditional append} and nothing
 * else, which is why a queue is usable only on a log that can fence — see {@link Lease} for why the
 * split matters and why it cannot be closed by making leases stricter.
 */
public interface WorkQueue {

    /** The queue's name, as passed to {@link EventLog#workQueue(String)}. */
    String name();

    /**
     * Records {@code created} as the execution's first event and publishes it to this queue in one
     * atomic append. Returns the event's {@code seq} in the execution's own stream — which is
     * {@code 0}, since this is that stream's first event; the queue's own position for the entry is
     * unrelated and is what {@link #poll} reports.
     */
    long submit(ExecutionId id, CatalystEvent created);

    /**
     * Reads up to {@code limit} entries published after {@code afterCursor}, oldest first. Pass
     * {@code -1} to start from the beginning of the queue.
     *
     * <p>Entries are <em>not</em> removed by reading: this is a cursored stream, not a destructive
     * take, so several workers each see every entry and settle who runs it by claiming
     * ({@link #claim}). That is what lets a restarted worker rewind and pick up work whose owner
     * died — nothing was consumed on its behalf while it was gone.
     */
    List<QueuedExecution> poll(long afterCursor, int limit);

    /**
     * Attempts to claim {@code id} for {@code nodeId} until {@code now + ttl}.
     *
     * <p>Succeeds when the execution is unclaimed, when the existing lease has expired (a steal, with
     * the fencing token incremented), or when {@code nodeId} already holds it (re-entrant, so a
     * worker that crashed and restarted with the same node id is not locked out by its own lease).
     * Returns empty when another node holds an unexpired lease.
     *
     * <p>The read-decide-write is atomic against other claimants — a compare-and-set on the store,
     * not a read followed by a write — because two nodes deciding "expired, therefore mine" from the
     * same stale read is the one race a lease has to get right to be worth having at all.
     */
    Optional<Lease> claim(ExecutionId id, String nodeId, Duration ttl, Instant now);

    /**
     * Extends {@code held} to {@code now + ttl}, returning the renewed lease, or empty if it was
     * lost — stolen after expiry, or released. A worker whose renewal comes back empty has no claim
     * on the execution any more; another node may already be running it.
     */
    Optional<Lease> renew(Lease held, Duration ttl, Instant now);

    /**
     * Releases {@code held} so another node can claim immediately rather than waiting out the TTL.
     * Returns false if the lease was already lost, which is not an error — it is the normal outcome
     * of finishing work whose lease was stolen while it ran.
     */
    boolean release(Lease held);

    /** The current lease on {@code id}, if any. Read-only; for status and diagnostics. */
    Optional<Lease> leaseOf(ExecutionId id);
}
