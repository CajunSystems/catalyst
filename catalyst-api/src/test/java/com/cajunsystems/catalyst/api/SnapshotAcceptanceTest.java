package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Snapshot;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * v0.2 durability exit demo (roadmap §"Durability & storage" — Snapshots): a long execution is
 * folded once, checkpointed, and every later {@code inspect} restores the snapshot and folds only the
 * log's tail — never the whole log — while producing exactly the same {@link ExecutionState} as a
 * full re-fold.
 */
class SnapshotAcceptanceTest {

    private static final int STEPS = 250;

    /** A task that records {@value #STEPS} effects, so its log is far longer than one snapshot interval. */
    private static final Task<Integer> COUNTER = ctx -> {
        int sum = 0;
        for (int i = 0; i < STEPS; i++) {
            final int step = i;
            sum += ctx.effect("step-" + step, () -> step);
        }
        return sum;
    };

    @Test
    void inspectFoldsFromSnapshotNotTheWholeLog(@TempDir Path dir) {
        CountingLog log = new CountingLog(GumboEventLog.at(dir));
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(log)
                .snapshotInterval(100)
                .build()) {

            // Record a long execution to completion with a single blocking run. A fresh execute folds
            // via Reducer.fold directly and never calls foldState, so it deterministically writes no
            // snapshot — the first inspect below is the genuine cold path.
            var handle = runtime.execute(COUNTER, ExecutionOptions.withKey("count:1"));
            int result = handle.result();
            ExecutionId id = handle.id();
            assertThat(result).isEqualTo(STEPS * (STEPS - 1) / 2);
            assertThat(log.readSnapshot(id)).as("a fresh recording run writes no snapshot").isEmpty();

            long totalEvents = log.read(id).size();
            assertThat(totalEvents).isGreaterThan(STEPS); // effects + lifecycle

            // First inspect: no snapshot yet, so it folds the whole log and writes a checkpoint.
            log.resetReadFromCount();
            ExecutionState cold = runtime.inspect(id);
            long coldFolded = log.eventsFoldedViaReadFrom();
            assertThat(coldFolded).as("cold fold reads the whole log").isEqualTo(totalEvents);

            Optional<Snapshot> snap = log.readSnapshot(id);
            assertThat(snap).as("a checkpoint was written").isPresent();
            assertThat(snap.get().throughSeq()).isEqualTo(cold.lastSeq());

            // Second inspect: restores the snapshot and folds only the tail after it.
            log.resetReadFromCount();
            ExecutionState warm = runtime.inspect(id);
            long warmFolded = log.eventsFoldedViaReadFrom();
            assertThat(warmFolded).as("warm fold reads only the tail past the snapshot")
                    .isLessThan(coldFolded)
                    .isLessThan(100);

            // Correctness: the snapshot-based fold equals a full re-fold from the raw log.
            ExecutionState fullRefold = Reducer.fold(id, log.read(id));
            assertThat(warm.status()).isEqualTo(Status.COMPLETED);
            assertThat(warm.status()).isEqualTo(fullRefold.status());
            assertThat(warm.lastSeq()).isEqualTo(fullRefold.lastSeq());
            assertThat(warm.cost()).isEqualTo(fullRefold.cost());
            assertThat(warm.result()).isEqualTo(fullRefold.result());
            assertThat(warm.trajectory()).isEqualTo(fullRefold.trajectory());
        }
    }

    @Test
    void disablingSnapshotsAlwaysFoldsTheFullLog(@TempDir Path dir) {
        CountingLog log = new CountingLog(GumboEventLog.at(dir));
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(log)
                .snapshotInterval(0) // disabled
                .build()) {

            var handle = runtime.execute(COUNTER, ExecutionOptions.withKey("count:2"));
            handle.result();
            ExecutionId id = handle.id();

            runtime.inspect(id);
            assertThat(log.readSnapshot(id)).as("no snapshot when disabled").isEmpty();

            log.resetReadFromCount();
            runtime.inspect(id);
            assertThat(log.eventsFoldedViaReadFrom()).isEqualTo(log.read(id).size());
        }
    }

    /** An {@link EventLog} decorator that counts how many events each snapshot-aware fold reads. */
    private static final class CountingLog implements EventLog {
        private final EventLog delegate;
        private final AtomicLong readFromEvents = new AtomicLong();

        CountingLog(EventLog delegate) {
            this.delegate = delegate;
        }

        void resetReadFromCount() {
            readFromEvents.set(0);
        }

        long eventsFoldedViaReadFrom() {
            return readFromEvents.get();
        }

        @Override
        public List<SequencedEvent> readFrom(ExecutionId id, long afterSeqExclusive) {
            List<SequencedEvent> tail = delegate.readFrom(id, afterSeqExclusive);
            readFromEvents.addAndGet(tail.size());
            return tail;
        }

        @Override public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
        @Override public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        @Override public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        @Override public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        @Override public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        @Override public Optional<Snapshot> readSnapshot(ExecutionId id) { return delegate.readSnapshot(id); }
        @Override public void writeSnapshot(ExecutionId id, Snapshot s) { delegate.writeSnapshot(id, s); }
        @Override public void close() { delegate.close(); }
    }
}
