package com.cajunsystems.catalyst;

import java.util.Optional;

/**
 * Working memory: a key-value store scoped to a single execution, backed by {@code MemoryRead} /
 * {@code MemoryWritten} events. Tiny by design — no semantic memory, no embeddings in v0 (spec §4).
 * Reads are recorded with the value-at-read so replay is deterministic even if writes are reordered
 * by later code changes.
 */
public interface Memory {

    /** Stores {@code value} under {@code key}. Recorded as a {@code MemoryWritten} event. */
    void put(String key, Object value);

    /** Reads the value under {@code key} as {@code type}, or empty if absent. Recorded as a read. */
    <T> Optional<T> get(String key, Class<T> type);

    /** Whether a value is present under {@code key}. Derived from writes; not itself recorded. */
    boolean contains(String key);
}
