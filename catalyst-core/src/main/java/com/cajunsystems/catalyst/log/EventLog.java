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

    /**
     * Appends {@code event} only if {@code executionId}'s stream is still at {@code expectedSeq} —
     * that is, only if the event would be assigned that sequence number — and rejects the append
     * with {@link StaleWriterException} otherwise.
     *
     * <p>This is the fence that makes single-writer-per-execution enforceable rather than merely
     * arranged. Catalyst holds the invariant in-process today with {@code KeyedLock} plus an
     * in-flight set, which is sufficient within one JVM and worth nothing across two. A lease, a
     * lock service or a placement map can each tell a node it owns an execution, but none of them
     * can tell it that it <em>still</em> owns it at the moment its write lands — a GC pause between
     * the two is enough for a zombie to append into a stream someone else has taken over. With the
     * check in storage, the zombie's write is refused and the coordination layer is free to be
     * wrong.
     *
     * <p><strong>The default throws</strong>, and deliberately does not fall back to an
     * unconditional append. A log that quietly ignored {@code expectedSeq} would look like it was
     * participating in the protocol while providing none of it, which surfaces as a corrupted
     * history rather than as an error — the worst available failure shape for a correctness
     * primitive. Callers check {@link #supportsConditionalAppend()} first, or handle the exception.
     *
     * @param expectedSeq the seq this append must be assigned; equivalently, one past the stream's
     *                    current tip. Use {@code 0} for the first event of a stream
     * @return {@code expectedSeq}, for symmetry with {@link #append(ExecutionId, CatalystEvent)}
     * @throws StaleWriterException          if the stream is not at {@code expectedSeq}
     * @throws UnsupportedOperationException if this log cannot fence an append
     */
    default long append(ExecutionId executionId, CatalystEvent event, long expectedSeq) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support conditional append;"
                + " it cannot compare the stream position and append atomically");
    }

    /**
     * Whether {@link #append(ExecutionId, CatalystEvent, long)} can reject a stale writer.
     *
     * <p>Ask before relying on it: distributed execution requires it, and a runtime should refuse
     * to distribute over a log that reports {@code false} rather than discover the difference from
     * a stream two nodes have both written to.
     *
     * <p>Necessary, not sufficient, for running executions across processes. An implementation may
     * fence perfectly within one JVM and not at all between two — that is exactly what a
     * single-writer file-backed log does — so a distributed runtime also needs to know the log
     * itself is multi-writer. That property belongs to the storage layer and is reported by it;
     * this seam reports only whether the fence exists.
     */
    default boolean supportsConditionalAppend() {
        return false;
    }

    /**
     * Whether this log can be written by several processes at once with their sequence numbers
     * assigned consistently.
     *
     * <p>The other half of the distribution prerequisite, and the reason
     * {@link #supportsConditionalAppend()} is necessary without being sufficient. A log can fence
     * perfectly within one JVM and not at all between two — a file-backed log does exactly that,
     * comparing and appending under its own monitor, which is sound only because it refuses a
     * second process outright. Ask for both before running executions across nodes, and refuse to
     * start when either is missing, rather than discovering the difference from a stream two nodes
     * have both written to.
     *
     * <p>Defaults to {@code false}, in the direction that fails safe: a runtime that believes a log
     * is single-writer declines to distribute over it, where one that believes the opposite
     * corrupts history quietly.
     */
    default boolean supportsMultiWriter() {
        return false;
    }

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

    /**
     * The shared work queue named {@code name}, if this log can host one — the seam distributed
     * execution claims work through (see {@link WorkQueue} and {@code docs/distribution.md}).
     *
     * <p>The default returns empty, which is the honest answer for a log with no way to publish an
     * execution to a second reader. Read empty as "this log runs work in-process only", not as a
     * failure.
     *
     * <p>A queue is only <em>safe</em> on a log that can also fence
     * ({@link #supportsConditionalAppend()}). The two are reported separately rather than folded
     * into one answer because they fail differently: a log with no queue cannot distribute at all
     * and says so on the first call, while a log with a queue and no fence would appear to
     * distribute right up until two writers met on one execution and the stream stopped folding.
     * {@code Worker} refuses to start on the second.
     */
    default Optional<WorkQueue> workQueue(String name) {
        return Optional.empty();
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
