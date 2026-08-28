package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.Snapshot;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogCapabilities;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.service.SharedLogService;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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

    /**
     * The fence: an append conditioned on a seq the stream has moved past is rejected, and rejected
     * by storage rather than by anything Catalyst decided beforehand.
     *
     * <p>Two executions share the log, as every test at this layer should. The tag a fence applies
     * to and the number it compares are both per-execution, and a single-execution fixture is the
     * one configuration where a per-stream position and the log's global sequence are the same
     * number — which is how the last defect at this seam shipped.
     */
    @Test
    void conditionalAppendRejectsAWriterTheStreamHasMovedPast() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId other = ExecutionId.random();
            ExecutionId id = ExecutionId.random();
            for (int i = 0; i < 4; i++) log.append(other, new CatalystEvent.ExecutionStarted(T, i, "node-0"));

            assertThat(log.supportsConditionalAppend()).isTrue();
            assertThat(log.append(id, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", ""), 0))
                    .isEqualTo(0);
            assertThat(log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-0"), 1))
                    .isEqualTo(1);

            // A node that read the stream at seq 1 and has been overtaken since.
            assertThatThrownBy(() -> log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-1"), 1))
                    .isInstanceOf(StaleWriterException.class)
                    .hasMessageContaining(id.value());

            assertThat(log.read(id)).extracting(SequencedEvent::seq)
                    .as("a rejected append writes nothing")
                    .containsExactly(0L, 1L);
            assertThat(log.latestSeq(id)).isEqualTo(1);
            assertThat(log.read(other)).as("and touches no other execution").hasSize(4);
        }
    }

    /**
     * The rejection carries where the stream actually is, which is what a caller needs to recover:
     * re-read from there, rebuild, decide again. Blind retry of the same append is the one response
     * that is always wrong.
     */
    @Test
    void aRejectedAppendReportsWhereTheStreamActuallyIs() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId id = ExecutionId.random();
            for (int i = 0; i < 3; i++) log.append(id, new CatalystEvent.ExecutionStarted(T, i, "node-0"));

            assertThatThrownBy(() -> log.append(id, new CatalystEvent.ExecutionStarted(T, 9, "zombie"), 1))
                    .isInstanceOfSatisfying(StaleWriterException.class, e -> {
                        assertThat(e.expectedSeq()).isEqualTo(1);
                        assertThat(e.actualSeq()).isEqualTo(3);
                    });
        }
    }

    @Test
    void conditionalAppendIsFencedOnAFileBackedLogToo(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            ExecutionId other = ExecutionId.random();
            ExecutionId id = ExecutionId.random();
            log.append(other, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", ""));
            log.append(id, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", ""), 0);

            assertThatThrownBy(() -> log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-1"), 0))
                    .isInstanceOf(StaleWriterException.class);
            assertThat(log.read(id)).hasSize(1);
        }
    }

    /**
     * What a runtime must read before deciding it may distribute: the log fences, and it is not
     * multi-writer. Both halves matter and they are separable — a file-backed log compares and
     * appends under its own monitor, which is sound only because it refuses a second process
     * outright. Fenced within one JVM is not fenced across two.
     */
    @Test
    void aFileBackedLogIsFencedButNotMultiWriter(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            assertThat(log.supportsConditionalAppend()).isTrue();
            assertThat(log.supportsMultiWriter())
                    .as("single-writer by construction — it holds an exclusive directory lock")
                    .isFalse();
        }
    }

    @Test
    void anInMemoryLogIsFencedButNotMultiWriter() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            assertThat(log.supportsConditionalAppend()).isTrue();
            assertThat(log.supportsMultiWriter()).isFalse();
        }
    }

    /**
     * The answers are asked for, not asserted. An adapter that disclaims the fence is reported as
     * disclaiming it — which no hardcoded {@code true} could do, and which is the whole distinction
     * this delegation exists to make. Gumbo's capabilities are per-adapter, so a third-party
     * adapter that cannot compare and increment atomically is described by its own answer rather
     * than by a claim this class makes about adapters it has never seen.
     */
    @Test
    void theFenceAnswerComesFromTheAdapterRatherThanFromThisClass() {
        try (GumboEventLog log = GumboEventLog.open(new UnfencedAdapter(), EventCodec.builder().build())) {
            assertThat(log.supportsConditionalAppend()).isFalse();
        }
    }

    /**
     * And the multi-writer answer is composed, not forwarded. This adapter claims multi-writer,
     * and the log still reports {@code false} — because Gumbo requires a sequencer whose global
     * seqnum spans processes too, and the default is a per-process counter.
     *
     * <p>So no log Catalyst builds through these factories is multi-writer today, however capable
     * its storage. That is worth pinning rather than assuming: it is the difference between a
     * distributed runtime refusing to start and one discovering the problem from a stream two
     * nodes have both written to.
     *
     * <p>Being straight about what this one proves: unlike the fence test above, it would also
     * pass against a hardcoded {@code false}, because every configuration reachable from here
     * reports {@code false} anyway. It pins the contract, not the delegation. It starts
     * distinguishing them the moment a distributed sequencer is configurable through these
     * factories — which is the change that would make the answer flip.
     */
    @Test
    void claimingMultiWriterStorageIsNotEnoughOnItsOwn() {
        try (GumboEventLog log = GumboEventLog.open(new MultiWriterClaimingAdapter(),
                EventCodec.builder().build())) {
            assertThat(log.supportsMultiWriter())
                    .as("storage says yes; the default sequencer is a per-process AtomicLong")
                    .isFalse();
        }
    }

    /** Declares no fence, to prove the answer tracks the adapter. */
    private static final class UnfencedAdapter extends InMemoryPersistenceAdapter {
        @Override public LogCapabilities capabilities() {
            return LogCapabilities.builder(super.capabilities()).conditionalAppend(false).build();
        }
    }

    /** Claims cross-process storage, which the sequencer half still has to agree with. */
    private static final class MultiWriterClaimingAdapter extends InMemoryPersistenceAdapter {
        @Override public LogCapabilities capabilities() {
            return LogCapabilities.builder(super.capabilities()).multiWriter(true).build();
        }
    }

    /**
     * The property Catalyst's claimable-work design rests on, pinned rather than assumed.
     *
     * <p>[docs/distribution.md] resolves "how does a node find work to run" by dual-tagging: one
     * atomic append writes an execution's first event into both its own {@code catalyst-exec/<id>}
     * stream and a shared {@code catalyst-tasks/<queue>}, so there is no window where an execution
     * is recorded but not yet claimable. A worker then holds one number — its position in the queue
     * — and asks for what came after it.
     *
     * <p>That read only works if a fan-out tag counts its <em>own</em> entries. It did not until
     * Gumbo 0.6.0: an entry carried one version from its primary tag and told every tag that number,
     * so an item enqueued by a workflow whose history sat at 4 was numbered 4, and a worker already
     * advanced past 4 behind a busier workflow silently never saw it. The design was written against
     * a guarantee the layer below did not provide, and this repository's own document asserted it
     * for months before anyone measured it.
     *
     * <p>So it is measured here, through the log Catalyst actually builds. Catalyst does not
     * dual-tag yet — the claim loop is what would — which is exactly why the dependency is worth a
     * test now: a regression underneath would otherwise surface as work that is never claimed,
     * long after the change that caused it.
     */
    @Test
    void aFanOutTagIsCursoredByItsOwnVersion() {
        LogTag queue = LogTag.of("catalyst-tasks", "default");
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            SharedLogService service = log.service();

            // Three executions, each with a different amount of history, each enqueueing one item
            // in the same atomic append that records its creation. Histories descend deliberately:
            // under the old numbering the queue would have inherited 8, then 4, then 0, and a
            // cursor that reached 8 would never be handed the rest.
            int[] historyLengths = {8, 4, 0};
            for (int i = 0; i < historyLengths.length; i++) {
                ExecutionId id = ExecutionId.random();
                LogTag execTag = LogTag.of("catalyst-exec", id.value());
                for (int h = 0; h < historyLengths[i]; h++) {
                    log.append(id, new CatalystEvent.ExecutionStarted(T, h, "node-0"));
                }
                service.append(
                        AppendRequest.to(new LinkedHashSet<>(List.of(execTag, queue)),
                                ("work-" + i).getBytes(StandardCharsets.UTF_8)),
                        execTag, historyLengths[i]).join();
            }

            // A worker cursoring the queue: one position, advanced by what it was handed.
            List<String> claimed = new ArrayList<>();
            long cursor = -1;
            for (int round = 0; round < 4; round++) {
                for (LogEntry e : service.getView(queue).readAfterVersion(cursor).join()) {
                    claimed.add(new String(e.dataUnsafe(), StandardCharsets.UTF_8));
                    cursor = Math.max(cursor, e.streamVersion(queue));
                }
            }

            assertThat(claimed)
                    .as("every enqueued item claimed exactly once, whatever its execution's history")
                    .containsExactly("work-0", "work-1", "work-2");
            assertThat(cursor)
                    .as("the queue counts its own entries: three items, positions 0..2")
                    .isEqualTo(2L);
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
