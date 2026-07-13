package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;

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
    public void close() {
        // nothing to release
    }
}
