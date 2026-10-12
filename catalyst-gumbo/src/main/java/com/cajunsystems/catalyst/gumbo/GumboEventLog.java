package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.catalyst.events.EventUpcaster;
import com.cajunsystems.catalyst.events.FileBlobStore;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.ExecutionIndex;
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
import com.cajunsystems.gumbo.sequencer.Sequencer;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    /**
     * The execution index (see {@link ExecutionIndex}): lifecycle events that open or close an
     * execution carry this tag as well as their execution's, in the same append. Since gumbo 0.6.0 a
     * fan-out tag numbers its own entries, so the queue is cursored by its own version.
     */
    static final LogTag QUEUE_TAG = LogTag.of("catalyst-tasks", ExecutionIndex.DEFAULT_QUEUE);
    /** Key in the queue tag's KV holding the folded open set and the queue version it covers. */
    static final String OPEN_SET_KEY = "open-set";
    /** Layout version of the {@link #OPEN_SET_KEY} checkpoint. 2 adds a trailing CRC32 of the frame. */
    static final byte OPEN_SET_FORMAT = 2;

    private final SharedLogService service;
    private final EventLogSerializer serializer;
    private final Map<ExecutionId, TypedLogView<CatalystEvent>> views = new ConcurrentHashMap<>();
    /**
     * Cache of the last seq (Gumbo streamVersion) per execution, so latestSeq is O(1) after an append.
     * Advanced only by this process's appends, so it is used only when no other process can write —
     * see {@link #latestSeq}.
     */
    private final Map<ExecutionId, Long> lastSeq = new ConcurrentHashMap<>();
    /** {@link #supportsMultiWriter()}, read once: it is fixed by the adapter and sequencer chosen at open. */
    private final boolean multiWriter;
    /** What the log owns besides the service and blob store (a FoundationDB connection); closed between them. */
    private final AutoCloseable resource;
    private final LogView indexView;
    private final LogView snapshotView;
    private final LogView queueView;
    /** The codec's blob store, owned by this log and closed with it; null if payloads stay inline. */
    private final BlobStore blobStore;
    /**
     * Whether an execution is in the index, for executions this process has opened or branched and
     * not yet closed — so a terminal append knows whether to dual-tag without reading the stream.
     * Entries leave at the terminal event, so this holds open work, not history.
     */
    private final Map<ExecutionId, Boolean> indexed = new ConcurrentHashMap<>();

    private GumboEventLog(SharedLogService service, EventCodec codec, AutoCloseable resource) {
        this.service = service;
        this.resource = resource;
        this.multiWriter = service.capabilities().multiWriter();
        this.blobStore = codec.blobStore().orElse(null);
        this.serializer = new EventLogSerializer(codec);
        this.indexView = service.getView(INDEX_TAG);
        this.snapshotView = service.getView(SNAPSHOT_TAG);
        this.queueView = service.getView(QUEUE_TAG);
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

    /**
     * Opens (or creates) a <strong>multi-writer</strong> log on a FoundationDB cluster: the one
     * configuration in which runtimes in different processes may write one log. Gumbo's FoundationDB
     * storage adapter and its FoundationDB sequencer share one connection under {@code root}, so this
     * log reports both {@link #supportsConditionalAppend()} and {@link #supportsMultiWriter()} — Gumbo
     * measures that pairing with two writer processes in its CI (0.7.0+).
     *
     * <p>{@code sharedBlobs} is required and must be reachable from every process that opens this
     * root: an offloaded payload is read back by whichever node resumes the execution, so a store on
     * one node's local disk would strand it. It is required rather than optional because
     * FoundationDB refuses a value over 100 KB, so a large completion has to go out of line; payloads
     * over {@link EventCodec#DEFAULT_BLOB_THRESHOLD_BYTES} are offloaded. The log owns the store and
     * closes it, as with {@link #at(Path, BlobStore, int)}.
     *
     * <p><strong>Fenced, not yet coordinated.</strong> Every runtime append is conditional (Dist-1),
     * so a second runtime on the same root can never corrupt a stream — a stale writer is refused and
     * writes nothing. What does not exist yet is the coordination that keeps two runtimes from both
     * <em>starting</em> the same work: leases and the claim loop (Dist-3, Dist-4). Until then, run one
     * runtime per root, or give each runtime disjoint executions.
     *
     * <p>Needs {@code org.foundationdb:fdb-java} on the classpath (optional in this module) and the
     * FoundationDB client library ({@code libfdb_c}) installed.
     *
     * @param clusterFile path to {@code fdb.cluster}, or {@code null} for FoundationDB's default
     * @param root        the subspace this log lives under; every process sharing the log uses the same one
     * @param sharedBlobs a blob store every process opening {@code root} can read
     */
    public static GumboEventLog foundationDB(String clusterFile, String root, BlobStore sharedBlobs) {
        Objects.requireNonNull(sharedBlobs, "sharedBlobs: a FoundationDB log needs a blob store every node can read");
        return foundationDB(clusterFile, root,
                EventCodec.builder().blobStore(sharedBlobs, EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES).build());
    }

    /**
     * As {@link #foundationDB(String, String, BlobStore)}, with a fully caller-configured codec (blob
     * store, threshold, upcasters). The codec must have a blob store, for the reason given there; keep
     * its threshold well under FoundationDB's 100 KB value limit.
     */
    public static GumboEventLog foundationDB(String clusterFile, String root, EventCodec codec) {
        if (root == null || root.isBlank()) throw new IllegalArgumentException("root must be a non-blank subspace name");
        if (codec.blobStore().isEmpty()) {
            throw new IllegalArgumentException("A FoundationDB log needs a blob store: FoundationDB refuses values"
                    + " over 100 KB, so large payloads must be offloaded to a store every node can read");
        }
        return FoundationDBLogs.open(clusterFile, root, codec);
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
        return open(adapter, null, codec, null);
    }

    /**
     * As {@link #open(PersistenceAdapter, EventCodec)}, with a {@code sequencer} ({@code null} for
     * Gumbo's per-process default) and a {@code resource} the log closes after its service
     * ({@code null} for none). If opening fails, {@code resource} is the caller's to close.
     */
    static GumboEventLog open(PersistenceAdapter adapter, Sequencer sequencer, EventCodec codec, AutoCloseable resource) {
        try {
            SharedLogConfig.Builder config = SharedLogConfig.builder().persistenceAdapter(adapter);
            if (sequencer != null) config.sequencer(sequencer);
            return new GumboEventLog(SharedLogService.open(config.build()), codec, resource);
        } catch (LogAlreadyOpenException e) {
            // The file adapter is single-writer and says so precisely. Keep its message at the top
            // level rather than burying it as a cause — a directory already held by another process
            // is a configuration mistake, and the whole value of the check is that it is legible.
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open Gumbo log", e);
        }
    }

    /**
     * The underlying Gumbo service, package-private for tests.
     *
     * <p>Tests reach the queue tag and its checkpoint through it, and drive multi-tag appends
     * directly to pin the property the index rests on: a fan-out tag counts its own entries.
     */
    SharedLogService service() {
        return service;
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
        long seq = writesIndex(executionId, event)
                ? appendFenced(executionId, event, true, PersistenceAdapter.ANY_VERSION)
                : viewFor(executionId).append(event).join().streamVersion();
        afterAppend(executionId, event, seq);
        return seq;
    }

    /**
     * Whether {@code event} must also go to the queue tag. An opening event always does; a closing
     * event only if its execution was opened — a branch child begins with {@code ExecutionBranched},
     * was never listed, and its terminal event would otherwise be a queue entry (and checkpoint
     * churn) that removes nothing.
     */
    private boolean writesIndex(ExecutionId executionId, CatalystEvent event) {
        return switch (ExecutionIndex.changeFor(event)) {
            case OPENS -> true;
            case CLOSES -> wasOpened(executionId);
            case NONE -> false;
        };
    }

    /**
     * Known in-process for an execution this log created or branched; otherwise (a resume after a
     * restart) the stream's first event says. The cold read is the whole stream — Gumbo has no
     * cheaper first-entry read — but it happens once per execution, at its terminal event, right
     * after a resume that has just read the same stream.
     */
    private boolean wasOpened(ExecutionId executionId) {
        Boolean known = indexed.get(executionId);
        if (known != null) return known;
        List<LogEntry> stream = viewFor(executionId).rawView().readAfterVersion(-1).join(); // version order: see read
        return !stream.isEmpty()
                && serializer.deserialize(stream.get(0).dataUnsafe()) instanceof CatalystEvent.ExecutionCreated;
    }

    private void afterAppend(ExecutionId executionId, CatalystEvent event, long seq) {
        lastSeq.merge(executionId, seq, Math::max);
        switch (ExecutionIndex.changeFor(event)) {
            case OPENS -> indexed.put(executionId, true);
            case CLOSES -> indexed.remove(executionId);
            case NONE -> {
                // Only a stream that *starts* with the marker is a branch child; a divergence in
                // BRANCH replay mode records the same event mid-stream of an indexed execution.
                if (seq == 0 && event instanceof CatalystEvent.ExecutionBranched) indexed.put(executionId, false);
            }
        }
    }

    /**
     * Appends {@code event} to its execution's tag — and, if it changes the open set, to the queue
     * tag in the same entry — fenced on the execution tag at {@code expectedSeq}
     * ({@link PersistenceAdapter#ANY_VERSION} for none).
     *
     * <p>The execution tag is always <em>named</em> as the one fenced and reported. For a multi-tag
     * request Gumbo otherwise takes {@code tags.iterator().next()}, and a {@code Set}'s iteration
     * order is salted per JVM run — so the returned version could be the queue's position rather
     * than the execution's seq. The queue tag itself is never fenced: it is a fan-out index that
     * every execution writes to, and conditioning on it would make unrelated executions conflict.
     */
    private long appendFenced(ExecutionId executionId, CatalystEvent event, boolean toQueue, long expectedSeq) {
        LogTag execTag = tagFor(executionId);
        byte[] data = serializer.serialize(event);
        AppendRequest request = toQueue
                ? AppendRequest.to(new LinkedHashSet<>(List.of(execTag, QUEUE_TAG)), data)
                : AppendRequest.to(execTag, data);
        return service.append(request, execTag, expectedSeq).join().streamVersion();
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
     * <p>An event that changes the open set is dual-tagged into the queue here too, and the fence
     * still conditions on the execution tag only — see {@link #appendFenced}.
     */
    @Override
    public long append(ExecutionId executionId, CatalystEvent event, long expectedSeq) {
        long seq;
        try {
            seq = appendFenced(executionId, event, writesIndex(executionId, event), expectedSeq);
        } catch (RuntimeException e) {
            VersionConflictException conflict = conflictIn(e);
            if (conflict == null) throw e;
            throw new StaleWriterException(executionId, expectedSeq, conflict.actualVersion());
        }
        afterAppend(executionId, event, seq);
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
     * <p>In practice: {@code true} only for {@link #foundationDB}, which pairs Gumbo's FoundationDB
     * storage with its FoundationDB sequencer. {@link #at(Path)} is file-backed and single-writer by construction — it takes an
     * exclusive directory lock and refuses a second process — so it fences (see
     * {@link #supportsConditionalAppend()}) without being multi-writer. That pair, fenced and not
     * multi-writer, is exactly what a runtime needs to read before deciding it may distribute.
     */
    @Override
    public boolean supportsMultiWriter() {
        return service.capabilities().multiWriter();
    }

    /**
     * The whole stream, read <em>by version</em> — never with the tag's {@code readAll()}, which is
     * keyed and ordered on the global seqnum. With one writer the two orders agree. With two they do
     * not: a writer claims its seqnum before its transaction and is handed its version inside it, so
     * the entry at seq 6 can carry a lower seqnum than the one at seq 5, and a seqnum-ordered read
     * hands the fold events out of order (measured: {@code FoundationDBEventLogTest}). Version-keyed
     * reads come back in version order on every adapter (Gumbo 0.7.0 contract).
     */
    @Override
    public List<SequencedEvent> read(ExecutionId executionId) {
        return readFrom(executionId, -1);
    }

    @Override
    public List<SequencedEvent> readFrom(ExecutionId executionId, long afterSeqExclusive) {
        // Catalyst's seq is the execution tag's own streamVersion, so the tail read must be
        // version-keyed. readAfter is keyed on the log's *global* seqnum, which coincides with the
        // stream's numbering only while the log holds a single execution — with a second execution
        // sharing it, that read silently returns events already folded into the snapshot and the
        // reducer re-applies them. readAfterVersion takes -1 as "the whole stream".
        return decode(executionId, viewFor(executionId).rawView().readAfterVersion(afterSeqExclusive).join());
    }

    /**
     * Seq is read as the entry's version <em>in this execution's tag</em>. The no-arg
     * {@code streamVersion()} reports the entry's primary tag, which for a dual-tagged lifecycle
     * event is only the execution tag because {@link #appendFenced} names it — reading by tag keeps
     * the seq right without depending on how the entry was written.
     */
    private List<SequencedEvent> decode(ExecutionId executionId, List<LogEntry> entries) {
        LogTag tag = tagFor(executionId);
        List<SequencedEvent> events = new ArrayList<>(entries.size());
        for (LogEntry entry : entries) {
            events.add(new SequencedEvent(entry.streamVersion(tag), serializer.deserialize(entry.dataUnsafe())));
        }
        return events;
    }

    /**
     * Folds the queue tag into the open set, starting from the checkpoint in the queue's KV and
     * reading only the entries after it, then advances the checkpoint. A scan therefore costs the
     * open set plus the churn since the last scan, not the log's history.
     *
     * <p>The checkpoint is an optimization, never a source of truth: the fold is a replay of
     * opens and closes in queue order, so resuming it from <em>any</em> earlier checkpoint yields
     * the same set. A lost or stale checkpoint costs a longer read, not a wrong answer — which is
     * why it is a plain overwrite rather than a compare-and-set. Synchronized so two scans in this
     * process do not race each other's checkpoint backwards.
     */
    @Override
    public synchronized List<ExecutionId> openExecutions() {
        LinkedHashSet<ExecutionId> open = new LinkedHashSet<>();
        long through = readOpenSetCheckpoint(open);
        List<LogEntry> tail = queueView.readAfterVersion(through).join();
        if (tail.isEmpty()) return List.copyOf(open);

        for (LogEntry entry : tail) {
            through = Math.max(through, entry.streamVersion(QUEUE_TAG));
            ExecutionId id = executionOf(entry);
            if (id == null) continue; // not a Catalyst lifecycle entry; nothing to fold
            switch (ExecutionIndex.changeFor(serializer.deserialize(entry.dataUnsafe()))) {
                case OPENS -> open.add(id);
                case CLOSES -> open.remove(id);
                case NONE -> { }
            }
        }
        writeOpenSetCheckpoint(through, open);
        return List.copyOf(open);
    }

    @Override
    public boolean supportsExecutionIndex() {
        return true;
    }

    /** The execution a queue entry belongs to: the one {@code catalyst-exec} tag it also carries. */
    private static ExecutionId executionOf(LogEntry entry) {
        for (LogTag tag : entry.tags()) {
            if (EXEC_NAMESPACE.equals(tag.namespace()) && tag.key() != null) return ExecutionId.of(tag.key());
        }
        return null;
    }

    /**
     * Fills {@code into} from the checkpoint and returns the queue version it covers, or -1 to refold.
     *
     * <p>Any checkpoint that does not verify is treated as missing: an unknown format, a CRC
     * mismatch, a truncated frame, or content that parses into something invalid (a blank id throws
     * {@code IllegalArgumentException}, not {@code IOException}). Refolding is always correct; a
     * checkpoint that made this method throw would fail every scan — and every startup with
     * {@code recoverOnStart} — until someone deleted it by hand. The CRC is what catches corruption
     * that happens to parse, which would otherwise yield a silently wrong set.
     */
    private long readOpenSetCheckpoint(LinkedHashSet<ExecutionId> into) {
        byte[] framed = queueView.getValue(OPEN_SET_KEY).join();
        if (framed == null) return -1;
        try {
            if (framed.length < 1 + Long.BYTES || framed[0] != OPEN_SET_FORMAT) return -1;
            int body = framed.length - Long.BYTES;
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(framed, 0, body);
            if (ByteBuffer.wrap(framed, body, Long.BYTES).getLong() != crc.getValue()) return -1;
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(framed, 1, body - 1))) {
                long through = in.readLong();
                int n = in.readInt();
                for (int i = 0; i < n; i++) into.add(ExecutionId.of(in.readUTF()));
                return through;
            }
        } catch (IOException | RuntimeException e) {
            into.clear();
            return -1;
        }
    }

    /**
     * Frames as {@code [1-byte format][8-byte through][4-byte count][count × modified-UTF-8 id][8-byte CRC32]},
     * creation order, the CRC covering everything before it. The format byte lets the layout change
     * later without a migration: a reader that does not recognise it refolds, which is always correct.
     */
    private void writeOpenSetCheckpoint(long through, LinkedHashSet<ExecutionId> open) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(OPEN_SET_FORMAT);
            out.writeLong(through);
            out.writeInt(open.size());
            for (ExecutionId id : open) out.writeUTF(id.value());
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(bytes.toByteArray());
            out.writeLong(crc.getValue());
        } catch (IOException e) {
            throw new UncheckedIOException(e); // in-memory stream; cannot happen
        }
        queueView.setValue(OPEN_SET_KEY, bytes.toByteArray()).join();
    }

    /**
     * The tag's tip. On a single-writer log this is cached: only this process appends, so the cache is
     * exact. On a multi-writer log it is read through every time — another process's append moves the
     * tip without passing through this one, and a cached answer would report a stream position that
     * history has already passed (the same per-process-cache bug Gumbo 0.7.0 removed underneath).
     */
    @Override
    public long latestSeq(ExecutionId executionId) {
        if (multiWriter) {
            long seq = viewFor(executionId).rawView().getLatestVersion();
            return seq < 0 ? -1 : seq;
        }
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

    /**
     * Closes the log, then anything else it owns (a FoundationDB connection), then the blob store
     * bound into its codec — each even if an earlier close throws.
     */
    @Override
    public void close() {
        try {
            service.close();
        } finally {
            try {
                if (resource != null) resource.close();
            } catch (Exception e) {
                throw e instanceof RuntimeException r ? r : new IllegalStateException("Failed to close the log's connection", e);
            } finally {
                if (blobStore != null) blobStore.close();
            }
        }
    }
}
