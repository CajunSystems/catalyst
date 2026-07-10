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

    /** The highest sequence number recorded for {@code executionId}, or {@code -1} if empty. */
    long latestSeq(ExecutionId executionId);

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
