package com.cajunsystems.catalyst.events;

/**
 * A content-addressed store for large payload bytes (spec §8). Big completions, tool results, or
 * document payloads are lifted out of the event and stored here, with the event carrying only a small
 * reference (see {@link EventCodec}). Content addressing makes {@link #put} idempotent and deduplicating:
 * identical bytes yield the same reference and are stored once.
 *
 * <p>The reference is an opaque, stable string (the built-in stores use {@code "sha256:<hex>"}). A blob
 * is immutable once written; a reference is valid for the lifetime of the log that produced it.
 */
public interface BlobStore {

    /** Stores {@code content} and returns its content-addressed reference. Idempotent. */
    String put(byte[] content);

    /**
     * Returns the bytes previously stored under {@code ref}.
     *
     * @throws java.util.NoSuchElementException if no blob exists for the reference
     */
    byte[] get(String ref);

    /** Whether a blob exists for {@code ref}. */
    boolean has(String ref);

    /** An in-memory, content-addressed store — for tests and the in-memory runtime. */
    static BlobStore inMemory() {
        return new InMemoryBlobStore();
    }
}
