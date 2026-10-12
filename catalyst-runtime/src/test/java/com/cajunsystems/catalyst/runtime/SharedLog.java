package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Lease;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An in-memory log reporting whatever capabilities a test chooses. Shared by several runtimes in one
 * JVM, it stands in for a multi-writer log: each runtime is a node. The runtime reads only the
 * capability answers, which is the point — it must believe the log, never assume on its behalf.
 */
final class SharedLog implements EventLog {
    private final InMemoryEventLog delegate = new InMemoryEventLog();
    private final boolean fenced;
    private final boolean multiWriter;
    private final boolean leases;
    final AtomicBoolean closed = new AtomicBoolean();
    /** Runs at the start of every {@link #readLease}: a test's way to act "on another node" mid-claim. */
    volatile java.util.function.Consumer<ExecutionId> beforeReadLease = id -> { };

    SharedLog(boolean fenced, boolean multiWriter, boolean leases) {
        this.fenced = fenced;
        this.multiWriter = multiWriter;
        this.leases = leases;
    }

    /** Fenced, multi-writer and lease-storing: what a distributed runtime requires. */
    static SharedLog distributable() {
        return new SharedLog(true, true, true);
    }

    @Override public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
    @Override public long append(ExecutionId id, CatalystEvent event, long expectedSeq) { return delegate.append(id, event, expectedSeq); }
    @Override public boolean supportsConditionalAppend() { return fenced; }
    @Override public boolean supportsMultiWriter() { return multiWriter; }
    @Override public boolean supportsLeases() { return leases; }
    @Override public Optional<Lease> readLease(ExecutionId id) {
        beforeReadLease.accept(id);
        return delegate.readLease(id);
    }
    @Override public boolean compareAndSetLease(ExecutionId id, Lease expected, Lease replacement) {
        return delegate.compareAndSetLease(id, expected, replacement);
    }
    @Override public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
    @Override public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
    @Override public List<ExecutionId> openExecutions() { return delegate.openExecutions(); }
    @Override public boolean supportsExecutionIndex() { return delegate.supportsExecutionIndex(); }
    @Override public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
    @Override public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
    @Override public void close() { closed.set(true); }
}
