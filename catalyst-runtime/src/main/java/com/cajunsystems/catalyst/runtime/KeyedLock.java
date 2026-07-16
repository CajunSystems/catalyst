package com.cajunsystems.catalyst.runtime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Mutual exclusion scoped to a key rather than to the whole runtime: two callers holding different
 * keys never block each other. This is the seam the runtime's per-execution coordination sits behind
 * — replacing the coarse instance-wide {@code synchronized} that made unrelated executions serialize
 * on each other's log I/O and folds.
 *
 * <p>Locks are created on demand and reference-counted, so an idle key holds no memory: a runtime
 * that has scheduled a million executions does not retain a million locks. Locks are reentrant, so a
 * section may nest a call that re-acquires the same key.
 *
 * <p>Scope is one JVM. Enforcing single-writer-per-execution across nodes needs cluster-wide
 * coordination (roadmap v1, distributed execution) — this type is deliberately the only place that
 * would have to change.
 */
final class KeyedLock<K> {

    private final ConcurrentHashMap<K, Entry> entries = new ConcurrentHashMap<>();

    /**
     * A lock plus the number of callers currently holding or waiting for it. {@code refs} is only ever
     * touched inside a {@link ConcurrentHashMap#compute} block, so the map's per-bin lock guards it and
     * an entry cannot be evicted between a waiter arriving and acquiring.
     */
    private static final class Entry {
        final ReentrantLock lock = new ReentrantLock();
        int refs;
    }

    /** Runs {@code body} with the lock for {@code key} held, releasing it on every exit path. */
    <R> R withLock(K key, Supplier<R> body) {
        Entry entry = acquire(key);
        try {
            return body.get();
        } finally {
            release(key, entry);
        }
    }

    /** Runs {@code body} with the lock for {@code key} held, releasing it on every exit path. */
    void withLock(K key, Runnable body) {
        withLock(key, () -> {
            body.run();
            return null;
        });
    }

    private Entry acquire(K key) {
        // Register interest before locking so a concurrent release cannot evict the entry underneath us
        // and hand a second caller a different lock for the same key.
        Entry entry = entries.compute(key, (k, existing) -> {
            Entry e = existing == null ? new Entry() : existing;
            e.refs++;
            return e;
        });
        entry.lock.lock();
        return entry;
    }

    private void release(K key, Entry entry) {
        entry.lock.unlock();
        entries.compute(key, (k, existing) -> --existing.refs == 0 ? null : existing);
    }

    /** Live entry count — for tests asserting that locks do not leak. */
    int trackedKeys() {
        return entries.size();
    }
}
