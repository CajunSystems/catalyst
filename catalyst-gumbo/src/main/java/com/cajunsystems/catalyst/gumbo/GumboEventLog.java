package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.PersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;

import java.io.IOException;
import java.io.UncheckedIOException;
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
 * ({@code catalyst-exec/<id>}); Gumbo's per-tag {@code localId} is Catalyst's dense per-execution
 * {@code seq}; a {@code TypedLogView<CatalystEvent>} handles serialization; and the idempotency
 * index is a durable tag-scoped key-value store. Swapping the persistence adapter switches between
 * file-backed durability and in-memory — the same SPI a future Gumbo <em>cluster</em> backend uses.
 */
public final class GumboEventLog implements EventLog {

    private static final String EXEC_NAMESPACE = "catalyst-exec";
    private static final LogTag INDEX_TAG = LogTag.of("catalyst-index", "keys");

    private final SharedLogService service;
    private final EventLogSerializer serializer;
    private final Map<ExecutionId, TypedLogView<CatalystEvent>> views = new ConcurrentHashMap<>();
    /** Cache of the last seq (Gumbo localId) per execution, so latestSeq is O(1) after an append. */
    private final Map<ExecutionId, Long> lastSeq = new ConcurrentHashMap<>();
    private final LogView indexView;

    private GumboEventLog(SharedLogService service) {
        this.service = service;
        this.serializer = new EventLogSerializer(new EventCodec());
        this.indexView = service.getView(INDEX_TAG);
    }

    /** Opens (or creates) an embedded, file-backed durable log rooted at {@code path}. */
    public static GumboEventLog at(Path path) {
        return open(new FileBasedPersistenceAdapter(path.toString()));
    }

    /** A Gumbo-backed log with in-memory persistence — for fast tests that still exercise Gumbo. */
    public static GumboEventLog inMemory() {
        return open(new InMemoryPersistenceAdapter());
    }

    private static GumboEventLog open(PersistenceAdapter adapter) {
        try {
            SharedLogConfig config = SharedLogConfig.builder()
                    .persistenceAdapter(adapter)
                    .build();
            return new GumboEventLog(SharedLogService.open(config));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open Gumbo log", e);
        }
    }

    private TypedLogView<CatalystEvent> viewFor(ExecutionId id) {
        return views.computeIfAbsent(id,
                k -> service.getTypedView(LogTag.of(EXEC_NAMESPACE, k.value()), serializer));
    }

    @Override
    public long append(ExecutionId executionId, CatalystEvent event) {
        long seq = viewFor(executionId).append(event).join().localId();
        lastSeq.merge(executionId, seq, Math::max);
        return seq;
    }

    @Override
    public List<SequencedEvent> read(ExecutionId executionId) {
        List<LogEntry> entries = viewFor(executionId).rawView().readAll().join();
        List<SequencedEvent> events = new ArrayList<>(entries.size());
        for (LogEntry entry : entries) {
            events.add(new SequencedEvent(entry.localId(), serializer.deserialize(entry.dataUnsafe())));
        }
        return events;
    }

    @Override
    public long latestSeq(ExecutionId executionId) {
        Long cached = lastSeq.get(executionId);
        if (cached != null) return cached;
        // Cold path (e.g. right after reopen, before any append): scan once and cache.
        List<LogEntry> entries = viewFor(executionId).rawView().readAll().join();
        if (entries.isEmpty()) return -1;
        long seq = entries.get(entries.size() - 1).localId();
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
    public void close() {
        service.close();
    }
}
