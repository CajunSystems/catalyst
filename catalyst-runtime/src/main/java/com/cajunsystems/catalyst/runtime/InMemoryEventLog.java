package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory {@link EventLog} for tests, demos and ephemeral use (spec §8). Durable only for the
 * lifetime of the process; the Gumbo-backed log provides real durability behind the same SPI.
 */
public final class InMemoryEventLog implements EventLog {

    private final Map<ExecutionId, List<SequencedEvent>> streams = new ConcurrentHashMap<>();
    private final Map<String, ExecutionId> keyIndex = new ConcurrentHashMap<>();
    private final Map<ExecutionId, Snapshot> snapshots = new ConcurrentHashMap<>();

    @Override
    public long append(ExecutionId executionId, CatalystEvent event) {
        List<SequencedEvent> stream = streams.computeIfAbsent(executionId, k -> new ArrayList<>());
        synchronized (stream) {
            long seq = stream.size();
            stream.add(new SequencedEvent(seq, event));
            return seq;
        }
    }

    @Override
    public List<SequencedEvent> read(ExecutionId executionId) {
        List<SequencedEvent> stream = streams.get(executionId);
        if (stream == null) return List.of();
        synchronized (stream) {
            return List.copyOf(stream);
        }
    }

    @Override
    public List<SequencedEvent> readFrom(ExecutionId executionId, long afterSeqExclusive) {
        List<SequencedEvent> stream = streams.get(executionId);
        if (stream == null) return List.of();
        synchronized (stream) {
            if (afterSeqExclusive < 0) return List.copyOf(stream);
            // seq is dense from 0, so the tail starts at index afterSeqExclusive + 1.
            int from = (int) Math.min(stream.size(), Math.max(0, afterSeqExclusive + 1));
            return List.copyOf(stream.subList(from, stream.size()));
        }
    }

    @Override
    public long latestSeq(ExecutionId executionId) {
        List<SequencedEvent> stream = streams.get(executionId);
        if (stream == null) return -1;
        synchronized (stream) {
            return stream.size() - 1L;
        }
    }

    @Override
    public Optional<ExecutionId> findByKey(String idempotencyKey) {
        return Optional.ofNullable(keyIndex.get(idempotencyKey));
    }

    @Override
    public void putKey(String idempotencyKey, ExecutionId executionId) {
        keyIndex.put(idempotencyKey, executionId);
    }

    @Override
    public Optional<Snapshot> readSnapshot(ExecutionId executionId) {
        return Optional.ofNullable(snapshots.get(executionId));
    }

    @Override
    public void writeSnapshot(ExecutionId executionId, Snapshot snapshot) {
        snapshots.put(executionId, snapshot);
    }

    @Override
    public void close() {
        // nothing to release
    }
}
