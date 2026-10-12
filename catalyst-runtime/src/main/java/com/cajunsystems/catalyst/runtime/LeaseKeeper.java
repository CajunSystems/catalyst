package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A distributed runtime's leases (Dist-3): claims them, renews them on a heartbeat, and releases
 * them when this node stops running an execution.
 *
 * <p>Leases decide which node runs an execution. They are not what keeps a stream correct; the
 * append fence does that (see {@link Lease}). A renewal that finds the row changed means another node
 * has taken over, and the attempt is told to stop at its next boundary.
 *
 * <p>Cancels cross nodes on the lease row (Dist-4). {@link #requestCancel} adds the request to the
 * holder's row. The holder's next renewal finds its own row plus a request, keeps the lease, and runs
 * the attempt's cancel callback. A node that takes a lease over carries any request on it
 * ({@link #pendingCancel}), and a release never deletes a request this node did not honour. It
 * leaves the row lapsed instead, for the next claimer. So a request outlives its holder.
 *
 * <p>A lease whose holder is this node's own id is treated as this node's, even if this process did
 * not claim it. That is how a node restarted under the same id takes its executions back without
 * waiting out its predecessor's leases. It is also why node ids must be unique among live nodes.
 */
final class LeaseKeeper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseKeeper.class);
    /** Compare-and-set attempts before a claim gives up to a node that keeps changing the row. */
    private static final int CLAIM_ATTEMPTS = 5;

    /** A lease this process holds. Synchronized on itself so a renewal and a release never cross. */
    private static final class Held {
        Lease lease;
        boolean released;
        Runnable onLost = () -> { };
        /**
         * The running attempt's cancel, or null while none is registered. Returns whether the cancel took
         * effect: false for an attempt that has already finished, so the request is not counted as
         * honoured.
         */
        java.util.function.Predicate<String> onCancel;
        /** Whether the request on {@link #lease} has been honoured; only then may a release drop it. */
        boolean cancelFired;

        Held(Lease lease) {
            this.lease = lease;
        }
    }

    private final EventLog log;
    private final String nodeId;
    private final Clock clock;
    private final Duration duration;
    private final Map<ExecutionId, Held> held = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeat;
    /** Whether an execution has reached a terminal event; a cancel request on a finished one is moot. */
    private final java.util.function.Predicate<ExecutionId> terminal;

    LeaseKeeper(EventLog log, String nodeId, Clock clock, Duration duration, Duration heartbeatInterval,
                java.util.function.Predicate<ExecutionId> terminal) {
        this.log = log;
        this.terminal = terminal;
        this.nodeId = nodeId;
        this.clock = clock;
        this.duration = duration;
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "catalyst-lease-heartbeat-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        long every = heartbeatInterval.toMillis();
        heartbeat.scheduleWithFixedDelay(this::renewAll, every, every, TimeUnit.MILLISECONDS);
    }

    /**
     * Claims the lease on {@code id} for this node. Returns {@code true} if this call claimed it, or
     * {@code false} if this process already held it (claims are reentrant).
     *
     * @throws LeaseHeldException if another node holds a live lease on it
     */
    boolean acquire(ExecutionId id) {
        if (held.containsKey(id)) return false;
        for (int i = 0; i < CLAIM_ATTEMPTS; i++) {
            Optional<Lease> current = log.readLease(id);
            if (current.isPresent() && !current.get().holder().equals(nodeId) && current.get().liveAt(clock.instant())) {
                throw new LeaseHeldException(id, current.get());
            }
            // A takeover carries any cancel request on the row, so it is honoured by the new holder.
            Lease mine = current.flatMap(Lease::cancelRequested).map(fresh()::withCancelRequest).orElseGet(this::fresh);
            if (log.compareAndSetLease(id, current.orElse(null), mine)) {
                held.put(id, new Held(mine));
                return true;
            }
        }
        // Lost every race: other nodes keep changing the row.
        Optional<Lease> latest = log.readLease(id);
        if (latest.isPresent()) throw new LeaseHeldException(id, latest.get());
        throw new IllegalStateException("Could not claim the lease on " + id.value() + ": the row kept changing");
    }

    /** Runs {@code onLost} if a renewal finds the lease on {@code id} taken over. Replaces any earlier callback. */
    void onLost(ExecutionId id, Runnable onLost) {
        Held h = held.get(id);
        if (h != null) {
            synchronized (h) {
                h.onLost = onLost;
            }
        }
    }

    /**
     * Registers the running attempt's cancel for {@code id}. It runs when a renewal finds a cancel
     * request on the lease, or now, if one was absorbed while no attempt was registered (during a
     * pause, say, or just before the attempt registered). The request counts as honoured only if
     * {@code onCancel} returns true. Replaces any earlier callback.
     */
    void onCancel(ExecutionId id, java.util.function.Predicate<String> onCancel) {
        Held h = held.get(id);
        if (h == null) return;
        String pending;
        synchronized (h) {
            h.onCancel = onCancel;
            pending = h.cancelFired ? null : h.lease.cancelRequest();
        }
        if (pending != null) fireCancel(h, pending);
    }

    /** Runs the registered cancel outside the lock, and marks the request honoured only if it took effect. */
    private void fireCancel(Held h, String reason) {
        java.util.function.Predicate<String> callback;
        synchronized (h) {
            if (h.cancelFired || h.onCancel == null) return;
            callback = h.onCancel;
        }
        if (callback.test(reason)) {
            synchronized (h) {
                h.cancelFired = true;
            }
        }
    }

    /**
     * The cancel request on the lease this process holds for {@code id}: one carried over from the row
     * it took over, or one absorbed at renewal. Only a peek. Nothing is marked, because a caller that
     * reads the request and then decides not to act on it must not cause it to be dropped. Once the
     * cancel is recorded the execution is terminal, and that is what lets a release drop the request.
     */
    Optional<String> pendingCancel(ExecutionId id) {
        Held h = held.get(id);
        if (h == null) return Optional.empty();
        synchronized (h) {
            return h.lease.cancelRequested();
        }
    }

    boolean holds(ExecutionId id) {
        return held.containsKey(id);
    }

    /**
     * Asks the node holding a live lease on {@code id} to cancel it, by adding the request to its
     * lease row. Returns {@code true} if the request is on the row, now or already. Returns
     * {@code false} if no other node holds a live lease, in which case the caller may claim the
     * execution and cancel it directly.
     */
    boolean requestCancel(ExecutionId id, String reason) {
        for (int i = 0; i < CLAIM_ATTEMPTS; i++) {
            Optional<Lease> current = log.readLease(id);
            if (current.isEmpty() || current.get().holder().equals(nodeId) || !current.get().liveAt(clock.instant())) {
                return false;
            }
            if (current.get().cancelRequested().isPresent()) return true;
            if (log.compareAndSetLease(id, current.get(), current.get().withCancelRequest(reason))) return true;
        }
        return true; // the row kept changing under live holders; the latest of them saw a request or will
    }

    /**
     * Gives up the lease on {@code id}, if this process holds it, so another node can take the
     * execution at once instead of waiting for expiry. Conditional on the row being the one this
     * process last wrote, so a lease that already passed to another node is left alone. Best-effort:
     * a failed release costs the next holder a wait until expiry, never correctness.
     */
    void release(ExecutionId id) {
        Held h = held.remove(id);
        if (h == null) return;
        synchronized (h) {
            h.released = true;
            try {
                // From the row this process last wrote, or, if it changed since, from the row as it is
                // now, provided it is still ours (one that passed to another node is left alone).
                if (releaseFrom(id, h, h.lease)) return;
                Optional<Lease> row = log.readLease(id);
                if (row.isPresent() && row.get().holder().equals(nodeId)) releaseFrom(id, h, row.get());
            } catch (RuntimeException e) {
                LOG.debug("lease release for {} failed; it will lapse at expiry: {}", id, e.toString());
            }
        }
    }

    /**
     * Releases from {@code row}: deletes it, unless it carries a cancel request this process never
     * honoured on an execution that is not finished. Whether the request came over with a takeover or
     * was added since, it is kept on a lapsed row so the next claimer honours it. Returns whether the
     * swap happened.
     */
    private boolean releaseFrom(ExecutionId id, Held h, Lease row) {
        boolean keepRequest = row.cancelRequested().isPresent() && !h.cancelFired && !terminal.test(id);
        Lease replacement = keepRequest ? new Lease(nodeId, clock.instant().toEpochMilli(), row.cancelRequest()) : null;
        return log.compareAndSetLease(id, row, replacement);
    }

    /** One heartbeat: extends every held lease, and hands back any that another node has taken. */
    void renewAll() {
        for (Map.Entry<ExecutionId, Held> entry : held.entrySet()) {
            ExecutionId id = entry.getKey();
            Held h = entry.getValue();
            Runnable lost = null;
            String cancel = null;
            synchronized (h) {
                if (h.released) continue;
                Lease next = h.lease.cancelRequested().map(fresh()::withCancelRequest).orElseGet(this::fresh);
                try {
                    if (log.compareAndSetLease(id, h.lease, next)) {
                        h.lease = next;
                    } else {
                        // Changed under us. Our own row plus a cancel request means another node asked
                        // to cancel: absorb it, keep the lease, and cancel the attempt here.
                        Optional<Lease> row = log.readLease(id);
                        String reason = row.flatMap(Lease::cancelRequested).orElse(null);
                        if (reason != null && row.get().equals(h.lease.withCancelRequest(reason))) {
                            Lease renewed = fresh().withCancelRequest(reason);
                            h.lease = log.compareAndSetLease(id, row.get(), renewed) ? renewed : row.get();
                            if (!h.cancelFired) cancel = reason;
                        } else {
                            h.released = true;
                            held.remove(id, h);
                            lost = h.onLost;
                        }
                    }
                } catch (RuntimeException e) {
                    // A transient failure: keep the lease and try again next beat. If renewals keep
                    // failing until it lapses, another node may take over; the fence still holds.
                    LOG.warn("lease renewal for {} failed: {}", id, e.toString());
                }
            }
            if (cancel != null) {
                LOG.info("cancel of {} requested by another node; cancelling it here", id);
                fireCancel(h, cancel); // with no attempt to take it, the request stays for a later holder
            }
            if (lost != null) {
                LOG.warn("lost the lease on {} to another node; stopping its attempt here", id);
                lost.run();
            }
        }
    }

    private Lease fresh() {
        return new Lease(nodeId, clock.instant().plus(duration).toEpochMilli());
    }

    /** Stops the heartbeat and releases every lease still held, so other nodes need not wait them out. */
    @Override
    public void close() {
        heartbeat.shutdownNow();
        for (ExecutionId id : held.keySet()) release(id);
    }
}
