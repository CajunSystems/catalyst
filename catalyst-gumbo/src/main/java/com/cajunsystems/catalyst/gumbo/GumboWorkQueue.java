package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.Lease;
import com.cajunsystems.catalyst.log.QueuedExecution;
import com.cajunsystems.catalyst.log.WorkQueue;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.service.SharedLogService;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link WorkQueue} over one Gumbo tag: {@code catalyst-tasks/<name>}.
 *
 * <p>Two Gumbo properties carry the whole implementation, and neither needed a new primitive:
 *
 * <ul>
 *   <li><strong>One entry, several tags.</strong> Submitting appends the execution's first event to
 *       {@code catalyst-exec/<id>} and to the queue tag in a single atomic append, so an execution
 *       is never recorded-but-unclaimable.
 *   <li><strong>Every tag counts its own entries</strong> (Gumbo 0.6.0). The queue tag is dense from
 *       zero in its own right, so a worker's cursor over it is one {@code long} and a version-keyed
 *       tail read is exact. Before 0.6.0 a queue entry inherited its <em>execution's</em> version --
 *       a number from an unrelated stream that could sit below a worker's cursor, and work numbered
 *       below the cursor is work silently never claimed.
 * </ul>
 *
 * <p>Leases live in the same durable tag key-value store the idempotency index and snapshots already
 * use, under the <em>execution's</em> tag rather than the queue's. Keying by execution is what makes
 * a claim a compare-and-set on a single key: contention over one execution touches one key, and two
 * queues can never disagree about who holds it.
 */
final class GumboWorkQueue implements WorkQueue {

    /** The KV key each execution's lease is stored under, within that execution's own tag. */
    private static final String LEASE_KEY = "catalyst.lease";

    /**
     * Field separator inside an encoded lease. Node ids are validated to exclude it on claim, so it
     * cannot appear inside a field and be mistaken for a boundary.
     */
    private static final String SEP = "|";

    private final String name;
    private final LogTag queueTag;
    private final SharedLogService service;
    private final GumboEventLog log;
    private final LogView queueView;

    GumboWorkQueue(String name, LogTag queueTag, SharedLogService service, GumboEventLog log) {
        this.name = name;
        this.queueTag = queueTag;
        this.service = service;
        this.log = log;
        this.queueView = service.getView(queueTag);
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * One append carrying two tags, fenced on the execution's tag at version {@code 0}.
     *
     * <p>The fence is not defensive book-keeping: it is what makes submission idempotent under a
     * racing duplicate. A second submit of the same id -- two nodes recovering the same request, a
     * retried client call -- is refused by storage rather than appending a second
     * {@code ExecutionCreated} into a stream that then folds to nonsense.
     *
     * <p>The fenced tag is named explicitly. A multi-tag {@code AppendRequest} keeps its tags in an
     * immutable {@code Set} whose iteration order Java salts per JVM run, so "the first tag" is a
     * different tag on different runs -- which for an unconditional multi-tag append would make the
     * returned version belong to whichever tag won the toss. Naming it removes the coin.
     */
    @Override
    public long submit(ExecutionId id, CatalystEvent created) {
        LogTag execTag = GumboEventLog.tagFor(id);
        AppendRequest request = AppendRequest.to(Set.of(execTag, queueTag), log.serialize(created));
        return log.appendFenced(id, request, execTag, 0L);
    }

    @Override
    public List<QueuedExecution> poll(long afterCursor, int limit) {
        if (limit <= 0) return List.of();
        List<LogEntry> entries = queueView.readAfterVersion(afterCursor).join();
        List<QueuedExecution> out = new ArrayList<>(Math.min(limit, entries.size()));
        for (LogEntry entry : entries) {
            if (out.size() == limit) break;
            ExecutionId id = executionOf(entry);
            if (id == null) continue; // not an execution entry; skip rather than stall the cursor
            out.add(new QueuedExecution(entry.streamVersion(queueTag), id));
        }
        return List.copyOf(out);
    }

    /**
     * Recovers the execution a queue entry refers to from the entry's <em>other</em> tag, rather
     * than from its payload.
     *
     * <p>That is deliberate. The alternative -- deserialising the event and reading an id field --
     * would make the queue depend on the event schema, and would need the id written into the event
     * purely so the queue could read it back. The tag set already carries it exactly once,
     * atomically, as a consequence of how the entry was published.
     */
    private static ExecutionId executionOf(LogEntry entry) {
        for (LogTag tag : entry.tags()) {
            if (GumboEventLog.EXEC_NAMESPACE.equals(tag.namespace())) {
                return ExecutionId.of(tag.key());
            }
        }
        return null;
    }

    @Override
    public Optional<Lease> claim(ExecutionId id, String nodeId, Duration ttl, Instant now) {
        if (nodeId.contains(SEP)) {
            throw new IllegalArgumentException("node id must not contain '" + SEP + "': " + nodeId);
        }
        LogView execView = service.getView(GumboEventLog.tagFor(id));
        byte[] current = execView.getValue(LEASE_KEY).join();
        Lease existing = decode(id, current);

        if (existing != null && !existing.isExpired(now) && !existing.nodeId().equals(nodeId)) {
            return Optional.empty();
        }

        long token = existing == null ? 1L : existing.token() + 1;
        Lease next = new Lease(id, nodeId, token, now.plus(ttl));

        // Compare against the exact bytes read, so a claimant that decided "expired, therefore mine"
        // from a stale read loses to whoever wrote in between. Two nodes reaching that conclusion
        // from the same expired lease is the one race a lease has to get right; without the CAS both
        // would proceed and the lease would be decorative.
        boolean won = current == null
                ? execView.setValueIfAbsent(LEASE_KEY, encode(next)).join()
                : execView.compareAndSetValue(LEASE_KEY, current, encode(next)).join();
        return won ? Optional.of(next) : Optional.empty();
    }

    @Override
    public Optional<Lease> renew(Lease held, Duration ttl, Instant now) {
        LogView execView = service.getView(GumboEventLog.tagFor(held.execution()));
        Lease renewed = held.renewedUntil(now.plus(ttl));
        boolean won = execView.compareAndSetValue(LEASE_KEY, encode(held), encode(renewed)).join();
        return won ? Optional.of(renewed) : Optional.empty();
    }

    @Override
    public boolean release(Lease held) {
        LogView execView = service.getView(GumboEventLog.tagFor(held.execution()));
        return execView.deleteValueIf(LEASE_KEY, encode(held)).join();
    }

    @Override
    public Optional<Lease> leaseOf(ExecutionId id) {
        LogView execView = service.getView(GumboEventLog.tagFor(id));
        return Optional.ofNullable(decode(id, execView.getValue(LEASE_KEY).join()));
    }

    /**
     * Encodes a lease to bytes. The encoding must be a <em>function</em> of the lease -- same fields,
     * same bytes, every time and in every process -- because compare-and-set compares bytes. A format
     * that could render one lease two ways (map ordering, a timestamp with optional precision) would
     * make renewals fail intermittently for no visible reason.
     */
    private static byte[] encode(Lease lease) {
        return (lease.nodeId() + SEP + lease.token() + SEP + lease.expiresAt().toEpochMilli())
                .getBytes(StandardCharsets.UTF_8);
    }

    private static Lease decode(ExecutionId id, byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        String[] parts = new String(bytes, StandardCharsets.UTF_8).split("\\" + SEP, -1);
        if (parts.length != 3) return null;
        try {
            return new Lease(id, parts[0], Long.parseLong(parts[1]),
                    Instant.ofEpochMilli(Long.parseLong(parts[2])));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
