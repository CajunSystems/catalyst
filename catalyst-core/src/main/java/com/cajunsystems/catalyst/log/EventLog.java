package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;

import java.util.List;
import java.util.Optional;

/**
 * The storage SPI. An append-only, per-execution log of {@link CatalystEvent}s plus a small durable
 * index mapping idempotency keys to executions. Two v0 implementations exist behind this seam: an
 * in-memory log (catalyst-runtime) and the Gumbo-backed durable log (catalyst-gumbo). The runtime
 * knows nothing about the backend.
 */
public interface EventLog extends AutoCloseable {

    /**
     * Appends {@code event} to {@code executionId}'s stream and returns the dense, per-execution
     * sequence number assigned to it (0, 1, 2, …).
     */
    long append(ExecutionId executionId, CatalystEvent event);

    /** Reads all events for {@code executionId} in sequence order. Empty if none. */
    List<SequencedEvent> read(ExecutionId executionId);

    /**
     * Reads the events for {@code executionId} whose seq is <strong>strictly greater than</strong>
     * {@code afterSeqExclusive} (pass {@code -1} for the whole stream). Enables snapshot-based folds
     * to read only the log's tail; the default slices {@link #read} and durable backends override
     * with a real range read.
     */
    default List<SequencedEvent> readFrom(ExecutionId executionId, long afterSeqExclusive) {
        List<SequencedEvent> all = read(executionId);
        if (afterSeqExclusive < 0) return all;
        int from = 0;
        while (from < all.size() && all.get(from).seq() <= afterSeqExclusive) from++;
        return List.copyOf(all.subList(from, all.size()));
    }

    /** The highest sequence number recorded for {@code executionId}, or {@code -1} if empty. */
    long latestSeq(ExecutionId executionId);

    /**
     * The latest fold checkpoint for {@code executionId}, if one has been written (spec §8). The
     * default returns empty — a log that does not persist snapshots simply degrades to a full
     * re-fold, never incorrect.
     */
    default Optional<Snapshot> readSnapshot(ExecutionId executionId) {
        return Optional.empty();
    }

    /**
     * Persists {@code snapshot} as the latest checkpoint for {@code executionId}, replacing any
     * earlier one. The default is a no-op (snapshotting is a pure optimization; dropping a snapshot
     * only costs a re-fold). Durable backends override this to store the checkpoint alongside the log.
     */
    default void writeSnapshot(ExecutionId executionId, Snapshot snapshot) {
        // no-op: see readSnapshot
    }

    /** Looks up the execution previously registered under an idempotency key, if any. */
    Optional<ExecutionId> findByKey(String idempotencyKey);

    /**
     * Durably registers {@code executionId} under {@code idempotencyKey}. Implementations should
     * make this visible to a later {@link #findByKey} even across a process restart.
     */
    void putKey(String idempotencyKey, ExecutionId executionId);

    @Override
    void close();
}
