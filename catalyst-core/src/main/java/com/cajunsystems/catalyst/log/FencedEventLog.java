package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;

import java.util.List;
import java.util.Optional;

/**
 * An {@link EventLog} view that fences every append one attempt makes to one execution.
 *
 * <p>A worker that has claimed an execution wraps the log in this for the duration of the attempt.
 * Unconditional appends against the wrapped execution become conditional ones at the exact seq the
 * attempt believes it is writing, so a worker whose lease was stolen -- or that stalled long enough
 * for another node to reclaim its work and move the stream on -- is refused by storage the moment it
 * tries to write, rather than interleaving events into a stream that no longer folds.
 *
 * <p>This is where the design's central claim is actually cashed. {@link Lease} is placement and can
 * be wrong; the fence is correctness and cannot be, because the comparison happens inside the same
 * store operation as the assignment. A zombie holding a lease it lost writes into a stream that has
 * moved past it, and the write is rejected -- so the coordination layer is free to be wrong without
 * corrupting anything.
 *
 * <h2>Why a decorator</h2>
 * <p>Because it makes the property total rather than per-call-site. The attempt appends from a dozen
 * places -- lifecycle events in the runtime, every recorded boundary in {@code ReplayingContext}, the
 * terminal event on each exit path -- and threading an {@code expectedSeq} through each one would put
 * the guarantee at the mercy of whoever adds the thirteenth. Wrapping the log instead means a new
 * append site is fenced by construction, having done nothing to opt in.
 *
 * <h2>Why it counts rather than asks</h2>
 * <p>The next seq is tracked locally, seeded once from the stream's tip and incremented per append,
 * instead of being re-read before each write. Re-reading would defeat the purpose: a stale writer
 * would read the position its usurper just wrote, expect that, and be let through -- the fence would
 * agree with whatever the stream currently says and refuse nobody. The local counter is what encodes
 * "the history I believe I am extending", which is the belief that needs checking.
 *
 * <p>Not thread-safe by design, and it does not need to be: one attempt runs on one thread. It is
 * synchronized only so the counter cannot tear if a future change appends from a second thread,
 * which would silently corrupt every fence thereafter rather than fail loudly.
 */
public final class FencedEventLog implements EventLog {

    private final EventLog delegate;
    private final ExecutionId fenced;
    private long nextSeq;

    private FencedEventLog(EventLog delegate, ExecutionId fenced, long nextSeq) {
        this.delegate = delegate;
        this.fenced = fenced;
        this.nextSeq = nextSeq;
    }

    /**
     * Wraps {@code delegate} so appends to {@code id} are fenced, starting at {@code tipSeq + 1} --
     * the seq the attempt's next append must be assigned.
     *
     * @throws UnsupportedOperationException if {@code delegate} cannot fence. Deliberately loud: a
     *         silent fallback to unconditional appends would leave a worker believing it was
     *         protected while providing no protection at all, which shows up as a corrupted stream
     *         rather than as an error. Callers check {@link EventLog#supportsConditionalAppend()}
     *         before choosing to run distributed.
     */
    public static FencedEventLog forAttempt(EventLog delegate, ExecutionId id, long tipSeq) {
        if (!delegate.supportsConditionalAppend()) {
            throw new UnsupportedOperationException(
                    delegate.getClass().getSimpleName() + " cannot fence appends, so an attempt on it"
                    + " cannot be protected against a stale writer");
        }
        return new FencedEventLog(delegate, id, tipSeq + 1);
    }

    /** The seq the next fenced append will be assigned. Visible for tests and diagnostics. */
    public synchronized long nextSeq() {
        return nextSeq;
    }

    @Override
    public synchronized long append(ExecutionId executionId, CatalystEvent event) {
        if (!fenced.equals(executionId)) {
            // Another execution's stream: this writer holds no belief about where it is, so
            // inventing one would reject correct writes. Pass it through unfenced.
            return delegate.append(executionId, event);
        }
        long seq = delegate.append(executionId, event, nextSeq);
        nextSeq = seq + 1;
        return seq;
    }

    @Override
    public synchronized long append(ExecutionId executionId, CatalystEvent event, long expectedSeq) {
        long seq = delegate.append(executionId, event, expectedSeq);
        if (fenced.equals(executionId)) nextSeq = seq + 1;
        return seq;
    }

    @Override
    public boolean supportsConditionalAppend() {
        return true;
    }

    @Override
    public boolean supportsMultiWriter() {
        return delegate.supportsMultiWriter();
    }

    @Override
    public List<SequencedEvent> read(ExecutionId executionId) {
        return delegate.read(executionId);
    }

    @Override
    public List<SequencedEvent> readFrom(ExecutionId executionId, long afterSeqExclusive) {
        return delegate.readFrom(executionId, afterSeqExclusive);
    }

    @Override
    public long latestSeq(ExecutionId executionId) {
        return delegate.latestSeq(executionId);
    }

    @Override
    public Optional<Snapshot> readSnapshot(ExecutionId executionId) {
        return delegate.readSnapshot(executionId);
    }

    @Override
    public void writeSnapshot(ExecutionId executionId, Snapshot snapshot) {
        delegate.writeSnapshot(executionId, snapshot);
    }

    @Override
    public Optional<ExecutionId> findByKey(String idempotencyKey) {
        return delegate.findByKey(idempotencyKey);
    }

    @Override
    public void putKey(String idempotencyKey, ExecutionId executionId) {
        delegate.putKey(idempotencyKey, executionId);
    }

    @Override
    public Optional<WorkQueue> workQueue(String name) {
        return delegate.workQueue(name);
    }

    /**
     * No-op. This wrapper is scoped to one attempt and does not own the underlying log; closing the
     * delegate here would shut the whole runtime's storage down when a single execution finished.
     */
    @Override
    public void close() {
        // see javadoc: the delegate outlives this wrapper
    }
}
