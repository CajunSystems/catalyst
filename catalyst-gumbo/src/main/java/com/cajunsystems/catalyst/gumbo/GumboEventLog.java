package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.catalyst.events.EventUpcaster;
import com.cajunsystems.catalyst.events.FileBlobStore;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Snapshot;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.LogAlreadyOpenException;
import com.cajunsystems.gumbo.persistence.PersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A durable {@link EventLog} backed by the Gumbo shared log (spec §8) — the flagship embedded mode.
 * The mapping is direct: each execution is one Gumbo {@link LogTag}
 * ({@code catalyst-exec/<id>}); Gumbo's per-tag {@code streamVersion} is Catalyst's dense
 * per-execution {@code seq}; a {@code TypedLogView<CatalystEvent>} handles serialization; and the idempotency
 * index is a durable tag-scoped key-value store. Swapping the persistence adapter switches between
 * file-backed durability and in-memory — the same SPI a future Gumbo <em>cluster</em> backend uses.
 */
public final class GumboEventLog implements EventLog {

    private static final String EXEC_NAMESPACE = "catalyst-exec";
    private static final LogTag INDEX_TAG = LogTag.of("catalyst-index", "keys");
    private static final LogTag SNAPSHOT_TAG = LogTag.of("catalyst-index", "snapshots");

    private final SharedLogService service;
    private final EventLogSerializer serializer;
    private final Map<ExecutionId, TypedLogView<CatalystEvent>> views = new ConcurrentHashMap<>();
    /** Cache of the last seq (Gumbo streamVersion) per execution, so latestSeq is O(1) after an append. */
    private final Map<ExecutionId, Long> lastSeq = new ConcurrentHashMap<>();
    private final LogView indexView;
    private final LogView snapshotView;

    private GumboEventLog(SharedLogService service, EventCodec codec) {
        this.service = service;
        this.serializer = new EventLogSerializer(codec);
        this.indexView = service.getView(INDEX_TAG);
        this.snapshotView = service.getView(SNAPSHOT_TAG);
    }

    /**
     * Opens (or creates) an embedded, file-backed durable log rooted at {@code path}. Payloads larger
     * than {@link EventCodec#DEFAULT_BLOB_THRESHOLD_BYTES} are offloaded to a content-addressed
     * {@link FileBlobStore} under {@code path/blobs}, so oversized completions/tool results are stored
     * out-of-line and rehydrated transparently on read (spec §8).
     */
    public static GumboEventLog at(Path path) {
        return at(path, defaultCodec(path));
    }

    /** As {@link #at(Path)}, with a caller-supplied blob store and offload threshold. */
    public static GumboEventLog at(Path path, BlobStore blobStore, int blobThresholdBytes) {
        return at(path, EventCodec.builder().blobStore(blobStore, blobThresholdBytes).build());
    }

    /**
     * As {@link #at(Path)}, registering schema-evolution {@code upcasters} that migrate older recorded
     * events to the current schema on read (spec §13.4). Keeps the default {@code path/blobs} store.
     */
    public static GumboEventLog at(Path path, java.util.List<EventUpcaster> upcasters) {
        BlobStore blobs = FileBlobStore.at(path.resolve("blobs"));
        return at(path, EventCodec.builder()
                .blobStore(blobs, EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES)
                .upcasters(upcasters)
                .build());
    }

    /** As {@link #at(Path)}, with a fully caller-configured codec (blob store + upcasters). */
    public static GumboEventLog at(Path path, EventCodec codec) {
        return open(new FileBasedPersistenceAdapter(path.toString()), codec);
    }

    /** A Gumbo-backed log with in-memory persistence and an in-memory blob store — for fast tests. */
    public static GumboEventLog inMemory() {
        return open(new InMemoryPersistenceAdapter(),
                EventCodec.builder().blobStore(BlobStore.inMemory(), EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES).build());
    }

    private static EventCodec defaultCodec(Path path) {
        return EventCodec.builder()
                .blobStore(FileBlobStore.at(path.resolve("blobs")), EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES)
                .build();
    }

    /**
     * Package-private rather than private so a test can supply an adapter with a different set of
     * declared capabilities.
     *
     * <p>Widening this by one level buys something specific: without it, the capability answers
     * below cannot be distinguished from hardcoded constants, because every adapter reachable
     * through the public factories answers the same way — fenced, not multi-writer. A commit whose
     * subject is "ask the log instead of asserting on its behalf" should not ship a test that
     * assumes the asking happens.
     */
    static GumboEventLog open(PersistenceAdapter adapter, EventCodec codec) {
        try {
            SharedLogConfig config = SharedLogConfig.builder()
                    .persistenceAdapter(adapter)
                    .build();
            return new GumboEventLog(SharedLogService.open(config), codec);
        } catch (LogAlreadyOpenException e) {
            // The file adapter is single-writer and says so precisely. Keep its message at the top
            // level rather than burying it as a cause — a directory already held by another process
            // is a configuration mistake, and the whole value of the check is that it is legible.
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open Gumbo log", e);
        }
    }

    private TypedLogView<CatalystEvent> viewFor(ExecutionId id) {
        return views.computeIfAbsent(id, k -> service.getTypedView(tagFor(k), serializer));
    }

    private static LogTag tagFor(ExecutionId id) {
        return LogTag.of(EXEC_NAMESPACE, id.value());
    }

    /**
     * Finds the rejection in a cause chain, or {@code null} if the failure was something else.
     *
     * <p>It arrives buried: Gumbo wraps it in a {@code LogWriteException} and the future wraps that
     * in a {@code CompletionException}, so the top type says nothing about what happened. Matching
     * on the whole chain rather than the top is the same lesson the retry gate learned — a
     * classification that inspects only the outermost throwable classifies the wrapper.
     */
    private static VersionConflictException conflictIn(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof VersionConflictException v) return v;
            if (c.getCause() == c) break;
        }
        return null;
    }

    @Override
    public long append(ExecutionId executionId, CatalystEvent event) {
        long seq = viewFor(executionId).append(event).join().streamVersion();
        lastSeq.merge(executionId, seq, Math::max);
        return seq;
    }

    /**
     * Conditional append, straight onto Gumbo's fence: Catalyst's {@code seq} <em>is</em> the
     * execution tag's {@code streamVersion}, so "append only if this execution is still at seq N"
     * is "append only if this tag is still at version N" with no translation in between.
     *
     * <p>The comparison happens inside the adapter's own append, which is what makes it a fence
     * rather than a check — a compare here, above storage, would race the assignment underneath it,
     * and this seam exists precisely to stop correctness depending on a caller being right about
     * where a stream is.
     *
     * <p>The single-tag form is deliberate. Gumbo's multi-tag fence needs the conditioned tag named
     * explicitly, because the primary tag of a multi-tag request is {@code tags.iterator().next()}
     * over an immutable {@code Set}, whose iteration order Java salts per JVM run. Catalyst writes
     * one tag per execution, so the question does not arise here — but it will the moment an
     * execution is dual-tagged into a shared work queue, and then the fenced tag must be named.
     */
    @Override
    public long append(ExecutionId executionId, CatalystEvent event, long expectedSeq) {
        AppendRequest request = AppendRequest.to(tagFor(executionId), serializer.serialize(event));
        long seq;
        try {
            seq = service.append(request, expectedSeq).join().streamVersion();
        } catch (RuntimeException e) {
            VersionConflictException conflict = conflictIn(e);
            if (conflict == null) throw e;
            throw new StaleWriterException(executionId, expectedSeq, conflict.actualVersion());
        }
        lastSeq.merge(executionId, seq, Math::max);
        return seq;
    }

    /**
     * Asks the log rather than asserting on its behalf — which is what changed when Gumbo 0.5.0
     * shipped declared capabilities.
     *
     * <p>This method used to return a hardcoded {@code true}, justified by the observation that
     * every adapter Gumbo ships has fenced since 0.3.0. That was correct and it was the wrong
     * shape: a client asserting a guarantee on storage's behalf is precisely the mistake that
     * produced this project's worst defect, where a per-execution cursor was passed to a
     * seqnum-keyed read because nothing said otherwise and the resulting fold silently
     * double-counted. A capability that cannot be interrogated will eventually be assumed.
     *
     * <p>It also stops being true for a custom adapter. Gumbo's own answer is per-adapter, so a
     * third-party {@code PersistenceAdapter} that cannot compare and increment atomically reports
     * {@code false} here now, instead of being described by a claim this class made about
     * adapters it has never seen.
     */
    @Override
    public boolean supportsConditionalAppend() {
        return service.capabilities().conditionalAppend();
    }

    /**
     * Whether this log is safe for writers in different processes — the half a fence cannot supply
     * on its own.
     *
     * <p>Gumbo composes this from both of its layers, and the composition is the point: storage
     * that assigns per-tag versions across processes, <em>and</em> a sequencer whose global
     * {@code seqnum} spans them. Its default sequencer is a per-process {@code AtomicLong}, so a
     * FoundationDB adapter behind it reports {@code false} despite arbitrating every version
     * correctly — the seqnum would still collide, and the adapters index by it.
     *
     * <p>In practice today: {@code true} only for a FoundationDB-backed log with a distributed
     * sequencer. {@link #at(Path)} is file-backed and single-writer by construction — it takes an
     * exclusive directory lock and refuses a second process — so it fences (see
     * {@link #supportsConditionalAppend()}) without being multi-writer. That pair, fenced and not
     * multi-writer, is exactly what a runtime needs to read before deciding it may distribute.
     */
    @Override
    public boolean supportsMultiWriter() {
        return service.capabilities().multiWriter();
    }

    @Override
    public List<SequencedEvent> read(ExecutionId executionId) {
        return decode(viewFor(executionId).rawView().readAll().join());
    }

    @Override
    public List<SequencedEvent> readFrom(ExecutionId executionId, long afterSeqExclusive) {
        // Catalyst's seq is the execution tag's own streamVersion, so the tail read must be
        // version-keyed. readAfter is keyed on the log's *global* seqnum, which coincides with the
        // stream's numbering only while the log holds a single execution — with a second execution
        // sharing it, that read silently returns events already folded into the snapshot and the
        // reducer re-applies them. readAfterVersion takes -1 as "the whole stream".
        return decode(viewFor(executionId).rawView().readAfterVersion(afterSeqExclusive).join());
    }

    private List<SequencedEvent> decode(List<LogEntry> entries) {
        List<SequencedEvent> events = new ArrayList<>(entries.size());
        for (LogEntry entry : entries) {
            events.add(new SequencedEvent(entry.streamVersion(), serializer.deserialize(entry.dataUnsafe())));
        }
        return events;
    }

    @Override
    public long latestSeq(ExecutionId executionId) {
        Long cached = lastSeq.get(executionId);
        if (cached != null) return cached;
        // Cold path (e.g. right after reopen, before any append): ask the tag for its own tip, which
        // Gumbo maintains — and which is -1 for an execution that has never been written.
        long seq = viewFor(executionId).rawView().getLatestVersion();
        if (seq < 0) return -1;
        lastSeq.merge(executionId, seq, Math::max);
        return seq;
    }

    @Override
    public Optional<ExecutionId> findByKey(String idempotencyKey) {
        byte[] value = indexView.getValue(idempotencyKey).join();
        if (value == null) return Optional.empty();
        return Optional.of(ExecutionId.of(new String(value, StandardCharsets.UTF_8)));
    }

    @Override
    public void putKey(String idempotencyKey, ExecutionId executionId) {
        indexView.setValue(idempotencyKey, executionId.value().getBytes(StandardCharsets.UTF_8)).join();
    }

    @Override
    public Optional<Snapshot> readSnapshot(ExecutionId executionId) {
        byte[] framed = snapshotView.getValue(executionId.value()).join();
        if (framed == null || framed.length < Long.BYTES) return Optional.empty();
        ByteBuffer buf = ByteBuffer.wrap(framed);
        long throughSeq = buf.getLong();
        byte[] state = new byte[buf.remaining()];
        buf.get(state);
        return Optional.of(new Snapshot(throughSeq, state));
    }

    @Override
    public void writeSnapshot(ExecutionId executionId, Snapshot snapshot) {
        byte[] state = snapshot.state();
        // Frame as [8-byte throughSeq][state]; the snapshot KV holds one entry per execution.
        byte[] framed = ByteBuffer.allocate(Long.BYTES + state.length)
                .putLong(snapshot.throughSeq())
                .put(state)
                .array();
        snapshotView.setValue(executionId.value(), framed).join();
    }

    @Override
    public void close() {
        service.close();
    }
}
