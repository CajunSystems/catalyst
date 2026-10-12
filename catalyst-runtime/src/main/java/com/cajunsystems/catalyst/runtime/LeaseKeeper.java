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

    LeaseKeeper(EventLog log, String nodeId, Clock clock, Duration duration, Duration heartbeatInterval) {
        this.log = log;
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
            Lease mine = fresh();
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

    boolean holds(ExecutionId id) {
        return held.containsKey(id);
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
                log.compareAndSetLease(id, h.lease, null);
            } catch (RuntimeException e) {
                LOG.debug("lease release for {} failed; it will lapse at expiry: {}", id, e.toString());
            }
        }
    }

    /** One heartbeat: extends every held lease, and hands back any that another node has taken. */
    void renewAll() {
        for (Map.Entry<ExecutionId, Held> entry : held.entrySet()) {
            ExecutionId id = entry.getKey();
            Held h = entry.getValue();
            Runnable lost = null;
            synchronized (h) {
                if (h.released) continue;
                Lease next = fresh();
                try {
                    if (log.compareAndSetLease(id, h.lease, next)) {
                        h.lease = next;
                    } else {
                        h.released = true;
                        held.remove(id, h);
                        lost = h.onLost;
                    }
                } catch (RuntimeException e) {
                    // A transient failure: keep the lease and try again next beat. If renewals keep
                    // failing until it lapses, another node may take over; the fence still holds.
                    LOG.warn("lease renewal for {} failed: {}", id, e.toString());
                }
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
