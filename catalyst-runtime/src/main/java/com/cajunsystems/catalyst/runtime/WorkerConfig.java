package com.cajunsystems.catalyst.runtime;

import java.time.Duration;

/**
 * How a {@link Worker} paces itself. Every value is a trade-off between wasted work and recovery
 * latency rather than a tuning knob with a right answer, so each is documented in those terms.
 *
 * @param nodeId            identifies this worker in the leases it takes. Defaults to a per-process
 *                          random id. Give it a stable value (a pod name, a host) and a restarted
 *                          worker reclaims its own in-flight executions immediately instead of
 *                          waiting out their leases, because a claim by the same node id is
 *                          re-entrant.
 * @param leaseTtl          how long a claim survives without renewal. This is the price of a node
 *                          dying: its executions are unavailable for up to this long. Shortening it
 *                          speeds recovery and raises the chance a merely-slow node is declared dead
 *                          and its work duplicated -- which is safe (the fence rejects the stale
 *                          writer) but wastes provider spend, so it is a cost, not a non-issue.
 * @param heartbeat         how often the holder renews. Must be comfortably shorter than
 *                          {@code leaseTtl}; the constructor enforces it.
 * @param pollInterval      how long to wait after an idle poll before asking again. Only paid when
 *                          the queue is empty -- a poll that finds work loops straight round.
 * @param maxConcurrent     how many claimed executions this worker runs at once.
 * @param batchSize         how many queue entries one poll reads.
 */
public record WorkerConfig(
        String nodeId,
        Duration leaseTtl,
        Duration heartbeat,
        Duration pollInterval,
        int maxConcurrent,
        int batchSize) {

    public WorkerConfig {
        if (leaseTtl.isNegative() || leaseTtl.isZero()) {
            throw new IllegalArgumentException("leaseTtl must be positive");
        }
        // A heartbeat that is not comfortably inside the TTL means the holder loses its own lease
        // while still running -- the work continues, another node claims it, and both burn provider
        // spend until the fence rejects one of them. Cheap to check, expensive to discover.
        if (heartbeat.compareTo(leaseTtl.dividedBy(2)) > 0) {
            throw new IllegalArgumentException(
                    "heartbeat (" + heartbeat + ") must be at most half of leaseTtl (" + leaseTtl
                    + "), or a worker will routinely lose leases on work it is still running");
        }
        if (maxConcurrent < 1) throw new IllegalArgumentException("maxConcurrent must be >= 1");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
    }

    /** A 30s lease renewed every 10s, polling every 200ms, 8 concurrent executions. */
    public static WorkerConfig defaults() {
        return new WorkerConfig(
                "node-" + java.util.UUID.randomUUID(),
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofMillis(200),
                8,
                32);
    }

    public WorkerConfig withNodeId(String nodeId) {
        return new WorkerConfig(nodeId, leaseTtl, heartbeat, pollInterval, maxConcurrent, batchSize);
    }

    /**
     * Sets the lease TTL and its heartbeat together.
     *
     * <p>Prefer this to calling {@link #withLeaseTtl} and {@link #withHeartbeat} in sequence. The two
     * are validated against each other, so changing one at a time has to pass through a config where
     * the old value of the other is still in place -- and shortening a TTL below the existing
     * heartbeat is rejected for a combination the caller never asked for and is about to fix on the
     * very next call.
     */
    public WorkerConfig withLease(Duration ttl, Duration heartbeat) {
        return new WorkerConfig(nodeId, ttl, heartbeat, pollInterval, maxConcurrent, batchSize);
    }

    public WorkerConfig withLeaseTtl(Duration ttl) {
        return new WorkerConfig(nodeId, ttl, heartbeat, pollInterval, maxConcurrent, batchSize);
    }

    public WorkerConfig withHeartbeat(Duration hb) {
        return new WorkerConfig(nodeId, leaseTtl, hb, pollInterval, maxConcurrent, batchSize);
    }

    public WorkerConfig withPollInterval(Duration interval) {
        return new WorkerConfig(nodeId, leaseTtl, heartbeat, interval, maxConcurrent, batchSize);
    }

    public WorkerConfig withMaxConcurrent(int n) {
        return new WorkerConfig(nodeId, leaseTtl, heartbeat, pollInterval, n, batchSize);
    }

    public WorkerConfig withBatchSize(int n) {
        return new WorkerConfig(nodeId, leaseTtl, heartbeat, pollInterval, maxConcurrent, n);
    }
}
