package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.Snapshot;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GumboEventLogTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void appendAssignsDenseSeqAndReadsInOrder() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId id = ExecutionId.random();
            long s0 = log.append(id, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", ""));
            long s1 = log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
            long s2 = log.append(id, new CatalystEvent.ExecutionCompleted(T, new TextNode("done")));

            assertThat(s0).isEqualTo(0);
            assertThat(s1).isEqualTo(1);
            assertThat(s2).isEqualTo(2);
            assertThat(log.latestSeq(id)).isEqualTo(2);

            List<SequencedEvent> events = log.read(id);
            assertThat(events).extracting(SequencedEvent::seq).containsExactly(0L, 1L, 2L);
            assertThat(events.get(2).event()).isInstanceOf(CatalystEvent.ExecutionCompleted.class);
        }
    }

    @Test
    void isolatesStreamsByExecution() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId a = ExecutionId.random();
            ExecutionId b = ExecutionId.random();
            log.append(a, new CatalystEvent.ExecutionStarted(T, 1, "n"));
            log.append(b, new CatalystEvent.ExecutionStarted(T, 1, "n"));
            log.append(a, new CatalystEvent.ExecutionCompleted(T, new TextNode("a-done")));

            assertThat(log.read(a)).hasSize(2);
            assertThat(log.read(b)).hasSize(1);
            // Each execution's seq is dense from 0, independent of the other's interleaving.
            assertThat(log.read(a)).extracting(SequencedEvent::seq).containsExactly(0L, 1L);
            assertThat(log.read(b)).extracting(SequencedEvent::seq).containsExactly(0L);
        }
    }

    @Test
    void readFromReturnsOnlyEventsStrictlyAfterTheGivenSeq() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId id = ExecutionId.random();
            for (int i = 0; i < 5; i++) {
                log.append(id, new CatalystEvent.EffectRecorded(T, "e" + i, new TextNode("v" + i)));
            }
            // Exclusive lower bound: readFrom(id, 1) must skip seq 0 and 1, returning 2,3,4.
            assertThat(log.readFrom(id, 1)).extracting(SequencedEvent::seq).containsExactly(2L, 3L, 4L);
            assertThat(log.readFrom(id, -1)).extracting(SequencedEvent::seq).containsExactly(0L, 1L, 2L, 3L, 4L);
            assertThat(log.readFrom(id, 4)).isEmpty();
        }
    }

    @Test
    void readFromOnAFileBackedLogReadsOnlyTheTailAcrossReopen(@TempDir Path dir) {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            for (int i = 0; i < 5; i++) {
                log.append(id, new CatalystEvent.EffectRecorded(T, "e" + i, new TextNode("v" + i)));
            }
            assertThat(log.readFrom(id, 1)).extracting(SequencedEvent::seq).containsExactly(2L, 3L, 4L);
        }
        // Reopen with a cold cache so readFrom must go through Gumbo's native file-backed readAfter.
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.readFrom(id, 2)).extracting(SequencedEvent::seq).containsExactly(3L, 4L);
            assertThat(reopened.readFrom(id, 4)).isEmpty();
            assertThat(reopened.readFrom(id, -1)).hasSize(5);
        }
    }

    /**
     * The regression for Gumbo D4 (docs/gumbo-requirements.md): a tail read must be keyed on the
     * execution's own stream version, not on the log's global seqnum. The two coincide only while the
     * log holds a single execution — true of every other test here, false of every real deployment.
     */
    @Test
    void readFromReturnsOnlyThisExecutionsTailWhenAnotherExecutionSharesTheLog() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId other = ExecutionId.random();
            ExecutionId id = ExecutionId.random();
            // Interleave, so `id`'s stream versions (0..4) sit at global seqnums 1,3,5,7,9.
            for (int i = 0; i < 5; i++) {
                log.append(other, new CatalystEvent.EffectRecorded(T, "other-" + i, new TextNode("o" + i)));
                log.append(id, new CatalystEvent.EffectRecorded(T, "e" + i, new TextNode("v" + i)));
            }

            assertThat(log.readFrom(id, 1)).extracting(SequencedEvent::seq).containsExactly(2L, 3L, 4L);
            assertThat(log.readFrom(id, 4)).isEmpty();
            assertThat(log.readFrom(id, -1)).extracting(SequencedEvent::seq)
                    .containsExactly(0L, 1L, 2L, 3L, 4L);
            // And the events themselves are this execution's, not the neighbour's.
            assertThat(log.readFrom(id, 1))
                    .extracting(e -> ((CatalystEvent.EffectRecorded) e.event()).label())
                    .containsExactly("e2", "e3", "e4");
        }
    }

    /** As above, on the file-backed adapter and across a reopen, where the read goes through Gumbo natively. */
    @Test
    void readFromOnAFileBackedSharedLogReadsOnlyThisExecutionsTail(@TempDir Path dir) {
        ExecutionId other = ExecutionId.random();
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            for (int i = 0; i < 5; i++) {
                log.append(other, new CatalystEvent.EffectRecorded(T, "other-" + i, new TextNode("o" + i)));
                log.append(id, new CatalystEvent.EffectRecorded(T, "e" + i, new TextNode("v" + i)));
            }
        }
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.readFrom(id, 2)).extracting(SequencedEvent::seq).containsExactly(3L, 4L);
            assertThat(reopened.readFrom(id, 4)).isEmpty();
            assertThat(reopened.readFrom(id, -1)).hasSize(5);
            assertThat(reopened.latestSeq(id)).isEqualTo(4);
        }
    }

    @Test
    void snapshotRoundTripsAndSurvivesReopen(@TempDir Path dir) {
        ExecutionId id = ExecutionId.random();
        byte[] state = "folded-state-bytes".getBytes(StandardCharsets.UTF_8);
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            assertThat(log.readSnapshot(id)).isEmpty();
            log.writeSnapshot(id, new Snapshot(41, state));
            log.writeSnapshot(id, new Snapshot(42, state)); // latest wins
        }
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.readSnapshot(id)).hasValueSatisfying(s -> {
                assertThat(s.throughSeq()).isEqualTo(42);
                assertThat(s.state()).isEqualTo(state);
            });
        }
    }

    /**
     * Gumbo D1/D2/D3: a second writer on one directory used to be accepted silently, then assign the
     * same seqs and clobber the first's index. It is now refused, and the refusal has to stay legible
     * — a log held by another process is a configuration mistake, not something to debug from a cause
     * chain. The lock is released on close, so a sequential reopen (crash → resume) still works.
     */
    @Test
    void aSecondWriterOnTheSameDirectoryIsRefusedWithAClearMessage(@TempDir Path dir) {
        try (GumboEventLog first = GumboEventLog.at(dir)) {
            first.append(ExecutionId.random(), new CatalystEvent.ExecutionStarted(T, 1, "n"));
            assertThatThrownBy(() -> GumboEventLog.at(dir))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining("already open")
                    .hasRootCauseInstanceOf(OverlappingFileLockException.class);
        }
        // Released on close: the resume-after-crash path must still be able to reopen the directory.
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened).isNotNull();
        }
    }

    @Test
    void fileBackedLogAndKeyIndexSurviveReopen(@TempDir Path dir) {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            log.putKey("doc:1", id);
            log.append(id, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", "doc:1"));
            log.append(id, new CatalystEvent.ExecutionCompleted(T, new TextNode("done")));
        }

        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.findByKey("doc:1")).contains(id);
            List<SequencedEvent> events = reopened.read(id);
            assertThat(events).extracting(SequencedEvent::seq).containsExactly(0L, 1L);
            assertThat(events.get(1).event()).isInstanceOf(CatalystEvent.ExecutionCompleted.class);
        }
    }
}
