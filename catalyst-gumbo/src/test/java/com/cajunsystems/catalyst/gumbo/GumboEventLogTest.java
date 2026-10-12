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
     * <p>So it is measured here, through the logs Catalyst actually builds. Catalyst does not
     * dual-tag yet — the claim loop is what would — which is exactly why the dependency is worth a
     * test now: a regression underneath would otherwise surface as work that is never claimed,
     * long after the change that caused it.
     */
    @Test
    void anInMemoryLogCursorsAFanOutTagByItsOwnVersion() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            assertQueueIsCursoredByItsOwnVersion(log);
        }
    }

    /**
     * The same property on the log a deployment actually runs. Worth its own case rather than
     * trusting the in-memory one: a file-backed log carries per-tag versions in the record itself,
     * so this exercises the encode/decode path that the in-memory adapter never touches.
     */
    @Test
    void aFileBackedLogCursorsAFanOutTagByItsOwnVersion(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            assertQueueIsCursoredByItsOwnVersion(log);
        }
    }

    /**
     * And the positions survive a reopen, which is the case a persisted cursor depends on and the
     * one most likely to break quietly. A restart rebuilds the per-tag indices from the log, so a
     * regression there would renumber the queue and either replay or skip work for every worker
     * holding a stored position — with nothing failing at the seam.
     */
    @Test
    void aQueuesPositionsSurviveAReopen(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            assertQueueIsCursoredByItsOwnVersion(log);
        }

        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            List<LogEntry> queued = reopened.service().getView(QUEUE).readAfterVersion(-1).join();
            assertThat(queued.stream().map(e -> e.streamVersion(QUEUE)))
                    .as("rebuilt from the log, the queue still numbers its own entries 0..2")
                    .containsExactly(0L, 1L, 2L);

            // A worker that stopped after the first item resumes from where it left off, and is
            // handed the rest exactly once.
            List<String> rest = new ArrayList<>();
            for (LogEntry e : reopened.service().getView(QUEUE).readAfterVersion(0).join()) {
                rest.add(new String(e.dataUnsafe(), StandardCharsets.UTF_8));
            }
            assertThat(rest)
                    .as("a cursor stored before the restart still means the same position")
                    .containsExactly("work-1", "work-2");
        }
    }

    private static final LogTag QUEUE = LogTag.of("catalyst-tasks", "default");

    /**
     * Three executions, each with a different amount of history, each enqueueing one item in the
     * same atomic append that records its creation. Histories <strong>descend</strong>: under the
     * old numbering the queue would have inherited 8, then 4, then 0, so a cursor that reached 8
     * would never be handed the rest. The worker drains between enqueues, because draining
     * everything first and advancing once hides the bug entirely.
     */
    private void assertQueueIsCursoredByItsOwnVersion(GumboEventLog log) {
        SharedLogService service = log.service();
        int[] historyLengths = {8, 4, 0};
        List<String> claimed = new ArrayList<>();
        long cursor = -1;

        for (int i = 0; i < historyLengths.length; i++) {
            ExecutionId id = ExecutionId.random();
            LogTag execTag = LogTag.of("catalyst-exec", id.value());
            for (int h = 0; h < historyLengths[i]; h++) {
                log.append(id, new CatalystEvent.ExecutionStarted(T, h, "node-0"));
            }
            // Fence on the execution tag explicitly: left implicit, which tag the entry is numbered
            // by is Set iteration order, salted per JVM run.
            service.append(
                    AppendRequest.to(new LinkedHashSet<>(List.of(execTag, QUEUE)),
                            ("work-" + i).getBytes(StandardCharsets.UTF_8)),
                    execTag, historyLengths[i]).join();

            for (LogEntry e : service.getView(QUEUE).readAfterVersion(cursor).join()) {
                claimed.add(new String(e.dataUnsafe(), StandardCharsets.UTF_8));
                cursor = Math.max(cursor, e.streamVersion(QUEUE));
            }
        }

        assertThat(claimed)
                .as("every enqueued item claimed exactly once, whatever its execution's history")
                .containsExactly("work-0", "work-1", "work-2");
        assertThat(cursor)
                .as("the queue counts its own entries: three items, positions 0..2")
                .isEqualTo(2L);
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

    // ── Execution index (Deploy-1) ───────────────────────────────────────────────────────────

    /**
     * Lifecycle events are dual-tagged into the queue, and the execution's own seq must not notice:
     * still dense from 0, still the execution tag's version — with a second execution interleaving
     * appends, so a seq read off the queue's numbering or the global seqnum would show.
     */
    @Test
    void dualTaggedLifecycleEventsKeepTheExecutionsSeqDense(@TempDir Path dir) {
        ExecutionId a = ExecutionId.random();
        ExecutionId b = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            log.append(b, created());
            log.append(b, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
            assertThat(log.append(a, created())).isEqualTo(0);
            log.append(b, new CatalystEvent.ExecutionCompleted(T, new TextNode("b")));
            assertThat(log.append(a, new CatalystEvent.ExecutionStarted(T, 1, "node-0"))).isEqualTo(1);
            assertThat(log.append(a, new CatalystEvent.ExecutionCompleted(T, new TextNode("a")))).isEqualTo(2);
        }
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.read(a)).extracting(SequencedEvent::seq).containsExactly(0L, 1L, 2L);
            assertThat(reopened.readFrom(a, 0)).extracting(SequencedEvent::seq).containsExactly(1L, 2L);
            assertThat(reopened.latestSeq(a)).isEqualTo(2);
            assertThat(reopened.read(b)).extracting(SequencedEvent::seq).containsExactly(0L, 1L, 2L);

            // The queue holds the four open/close events and numbers them itself.
            assertThat(reopened.service().getView(QUEUE).readAfterVersion(-1).join())
                    .extracting(e -> e.streamVersion(QUEUE))
                    .containsExactly(0L, 1L, 2L, 3L);
        }
    }

    @Test
    void theOpenSetListsUnfinishedExecutionsInCreationOrderAcrossAReopen(@TempDir Path dir) {
        ExecutionId crashed = ExecutionId.random();
        ExecutionId completed = ExecutionId.random();
        ExecutionId paused = ExecutionId.random();
        ExecutionId failed = ExecutionId.random();
        ExecutionId cancelled = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            assertThat(log.supportsExecutionIndex()).isTrue();
            assertThat(log.openExecutions()).isEmpty();

            for (ExecutionId id : List.of(crashed, completed, paused, failed, cancelled)) log.append(id, created());
            log.append(crashed, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
            log.append(completed, new CatalystEvent.ExecutionCompleted(T, new TextNode("ok")));
            log.append(paused, new CatalystEvent.ExecutionPaused(T, "operator"));
            log.append(failed, new CatalystEvent.ExecutionFailed(T, "boom", 0));
            log.append(cancelled, new CatalystEvent.ExecutionCancelled(T, "stop", 0));

            assertThat(log.openExecutions()).containsExactly(crashed, paused);
        }
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.openExecutions())
                    .as("rebuilt from the log alone — this is what a restarted runtime sees")
                    .containsExactly(crashed, paused);
        }
    }

    /**
     * A scan resumes from the checkpoint and reads only the queue after it. Shown by planting a
     * checkpoint the queue does not support: if the scan refolded from the start, the planted id
     * would vanish; it survives, so the prefix was not re-read — and the tail still folds onto it.
     */
    @Test
    void aScanFoldsOnlyTheQueueTailAfterItsCheckpoint() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId first = ExecutionId.random();
            log.append(first, created());
            assertThat(log.openExecutions()).containsExactly(first);
            assertThat(checkpointVersion(log)).isEqualTo(0L);

            ExecutionId planted = ExecutionId.random();
            log.service().getView(QUEUE).setValue(GumboEventLog.OPEN_SET_KEY,
                    checkpoint(0L, planted)).join();

            ExecutionId second = ExecutionId.random();
            log.append(second, created());
            log.append(second, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));

            assertThat(log.openExecutions()).containsExactly(planted, second);
            assertThat(checkpointVersion(log)).as("advanced to the queue's tip").isEqualTo(1L);
        }
    }

    /**
     * The checkpoint is an optimization, not a source of truth: lost or rolled back, the scan reads
     * further back and reaches the same set.
     */
    @Test
    void aLostOrStaleCheckpointCostsAReadNotACorrectAnswer() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId a = ExecutionId.random();
            ExecutionId b = ExecutionId.random();
            log.append(a, created());
            log.append(b, created());
            assertThat(log.openExecutions()).containsExactly(a, b);
            log.append(a, new CatalystEvent.ExecutionCompleted(T, new TextNode("a")));
            assertThat(log.openExecutions()).containsExactly(b);

            // Rolled back to before a closed: replaying the close from there gives the same answer.
            log.service().getView(QUEUE).setValue(GumboEventLog.OPEN_SET_KEY, checkpoint(1L, a, b)).join();
            assertThat(log.openExecutions()).containsExactly(b);

            log.service().getView(QUEUE).deleteValue(GumboEventLog.OPEN_SET_KEY).join();
            assertThat(log.openExecutions()).containsExactly(b);

            log.service().getView(QUEUE).setValue(GumboEventLog.OPEN_SET_KEY, new byte[] {1, 2}).join();
            assertThat(log.openExecutions()).as("an unreadable checkpoint is refolded").containsExactly(b);

            // A layout this reader does not know — a later format, or the unversioned one — is
            // refolded rather than misread, even when its bytes happen to parse.
            byte[] future = checkpoint(1L, a, b);
            future[0] = (byte) (GumboEventLog.OPEN_SET_FORMAT + 1);
            log.service().getView(QUEUE).setValue(GumboEventLog.OPEN_SET_KEY, future).join();
            assertThat(log.openExecutions()).as("an unknown format is refolded").containsExactly(b);
        }
    }

    /**
     * A fenced lifecycle append is dual-tagged and conditions on the execution tag only. Other
     * executions advancing the shared queue must not make it look stale, and a genuinely stale
     * writer is still refused — with nothing reaching the index.
     */
    @Test
    void aFencedLifecycleAppendConditionsOnTheExecutionNotTheQueue() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId other = ExecutionId.random();
            log.append(other, created());
            log.append(other, new CatalystEvent.ExecutionCompleted(T, new TextNode("x")));

            ExecutionId id = ExecutionId.random();
            assertThat(log.append(id, created(), 0)).isEqualTo(0);
            assertThat(log.openExecutions()).containsExactly(id);

            assertThatThrownBy(() -> log.append(id, new CatalystEvent.ExecutionCancelled(T, "zombie", 0), 0))
                    .isInstanceOfSatisfying(StaleWriterException.class, e -> assertThat(e.actualSeq()).isEqualTo(1));
            assertThat(log.openExecutions()).as("the refused close never reached the index").containsExactly(id);

            assertThat(log.append(id, new CatalystEvent.ExecutionCancelled(T, "stop", 0), 1)).isEqualTo(1);
            assertThat(log.openExecutions()).isEmpty();
        }
    }

    private static CatalystEvent created() {
        return new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", "");
    }

    private static long checkpointVersion(GumboEventLog log) {
        byte[] framed = log.service().getView(QUEUE).getValue(GumboEventLog.OPEN_SET_KEY).join();
        assertThat(framed[0]).as("format byte").isEqualTo(GumboEventLog.OPEN_SET_FORMAT);
        return java.nio.ByteBuffer.wrap(framed, 1, Long.BYTES).getLong();
    }

    private static byte[] checkpoint(long through, ExecutionId... open) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream out = new java.io.DataOutputStream(bytes)) {
            out.writeByte(GumboEventLog.OPEN_SET_FORMAT);
            out.writeLong(through);
            out.writeInt(open.length);
            for (ExecutionId id : open) out.writeUTF(id.value());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /**
     * A branch child was never listed, so its terminal event has nothing to remove — and is kept out
     * of the queue rather than written there as churn. A divergence marker mid-stream (BRANCH replay
     * mode) does not make an indexed execution look like a branch child.
     */
    @Test
    void aBranchChildsTerminalEventStaysOutOfTheQueue() {
        try (GumboEventLog log = GumboEventLog.inMemory()) {
            ExecutionId parent = ExecutionId.random();
            log.append(parent, created());

            ExecutionId child = ExecutionId.random();
            log.append(child, new CatalystEvent.ExecutionBranched(T, parent.value(), 0, "branch@0"));
            log.append(child, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
            log.append(child, new CatalystEvent.ExecutionCompleted(T, new TextNode("fork")));

            log.append(parent, new CatalystEvent.ExecutionBranched(T, null, 0, "divergence"));
            log.append(parent, new CatalystEvent.ExecutionCompleted(T, new TextNode("parent")));

            assertThat(queuedExecutions(log)).containsExactly(parent, parent);
            assertThat(log.openExecutions()).isEmpty();
        }
    }

    /**
     * The same rule from a process that did not write the execution's first event — the restart
     * case, where whether it was opened has to come from the stream itself.
     */
    @Test
    void afterAReopenATerminalEventIsQueuedOnlyIfItsExecutionWasOpened(@TempDir Path dir) {
        ExecutionId opened = ExecutionId.random();
        ExecutionId child = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            log.append(opened, created());
            log.append(child, new CatalystEvent.ExecutionBranched(T, opened.value(), 0, "branch@0"));
        }
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            reopened.append(child, new CatalystEvent.ExecutionFailed(T, "boom", 0));
            reopened.append(opened, new CatalystEvent.ExecutionCompleted(T, new TextNode("ok")));

            assertThat(queuedExecutions(reopened)).containsExactly(opened, opened);
            assertThat(reopened.openExecutions()).isEmpty();
        }
    }

    @Test
    void closingTheLogClosesItsBlobStore(@TempDir Path dir) {
        boolean[] closed = {false};
        com.cajunsystems.catalyst.events.BlobStore store = new com.cajunsystems.catalyst.events.BlobStore() {
            final com.cajunsystems.catalyst.events.BlobStore bytes = com.cajunsystems.catalyst.events.BlobStore.inMemory();
            public String put(byte[] content) { return bytes.put(content); }
            public byte[] get(String ref) { return bytes.get(ref); }
            public boolean has(String ref) { return bytes.has(ref); }
            @Override public void close() { closed[0] = true; }
        };
        GumboEventLog log = GumboEventLog.at(dir, store, 1024);
        log.append(ExecutionId.random(), created());
        assertThat(closed[0]).isFalse();
        log.close();
        assertThat(closed[0]).isTrue();
    }

    /** The executions the queue's entries belong to, in queue order. */
    private static List<ExecutionId> queuedExecutions(GumboEventLog log) {
        List<ExecutionId> ids = new ArrayList<>();
        for (LogEntry e : log.service().getView(QUEUE).readAfterVersion(-1).join()) {
            for (LogTag tag : e.tags()) {
                if (tag.namespace().equals("catalyst-exec")) ids.add(ExecutionId.of(tag.key()));
            }
        }
        return ids;
    }
}
