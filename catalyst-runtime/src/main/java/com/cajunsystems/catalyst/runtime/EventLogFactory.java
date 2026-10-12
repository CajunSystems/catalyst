package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.log.EventLog;

/**
 * Constructs the runtime's {@link EventLog} at {@code build()} time, so builder options that have to
 * be bound into the log when it is created — today, the blob store — can be applied. A log the caller
 * already built has its codec fixed; this is the path where the runtime builds the log itself.
 *
 * <p>{@code catalyst-api}'s {@code Catalyst.builder(Path)} supplies one for an embedded Gumbo log.
 */
@FunctionalInterface
public interface EventLogFactory {

    /**
     * Opens the log.
     *
     * @param blobStore      the store to offload large payloads to, or {@code null} for the backend's
     *                       default; the returned log owns it and closes it on close
     * @param thresholdBytes payloads at or over this size are offloaded to {@code blobStore}
     */
    EventLog open(BlobStore blobStore, int thresholdBytes);
}
