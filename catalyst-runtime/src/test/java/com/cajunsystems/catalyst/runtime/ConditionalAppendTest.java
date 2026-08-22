package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The conditional-append seam: an append that names the seq it must be assigned, and is rejected
 * rather than renumbered if the stream has moved on.
 *
 * <p>Catalyst enforces single-writer-per-execution in-process today, with {@code KeyedLock} and an
 * in-flight set. That is sufficient inside one JVM and worth nothing across two, which is why the
 * seam exists: the fence has to be where the write lands, not where the decision to write was made.
 */
class ConditionalAppendTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void anAppendAtTheStreamsCurrentSeqIsAccepted() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();

        assertThat(log.supportsConditionalAppend()).isTrue();
        assertThat(log.append(id, created(), 0)).isEqualTo(0);
        assertThat(log.append(id, started("node-0"), 1)).isEqualTo(1);
        assertThat(log.read(id)).extracting(SequencedEvent::seq).containsExactly(0L, 1L);
    }

    @Test
    void anAppendAtAStaleSeqIsRejectedAndWritesNothing() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        log.append(id, created());
        log.append(id, started("node-0"));

        assertThatThrownBy(() -> log.append(id, started("zombie"), 1))
                .isInstanceOfSatisfying(StaleWriterException.class, e -> {
                    assertThat(e.executionId()).isEqualTo(id);
                    assertThat(e.expectedSeq()).isEqualTo(1);
                    assertThat(e.actualSeq()).isEqualTo(2);
                });

        assertThat(log.read(id)).hasSize(2);
        assertThat(log.read(id)).extracting(SequencedEvent::event)
                .as("the rejected event is not in the stream under any seq")
                .noneMatch(e -> e instanceof CatalystEvent.ExecutionStarted s && s.nodeId().equals("zombie"));
    }

    /**
     * An append that runs <em>ahead</em> of the stream is rejected too. The fence is an equality,
     * not a floor: a writer expecting seq 5 on a stream at 2 has read something this log never
     * wrote, and letting it through would leave a hole in a sequence the whole engine treats as
     * dense.
     */
    @Test
    void anAppendAheadOfTheStreamIsRejectedAsWell() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        log.append(id, created());

        assertThatThrownBy(() -> log.append(id, started("node-0"), 5))
                .isInstanceOf(StaleWriterException.class);
        assertThat(log.latestSeq(id)).isEqualTo(0);
    }

    @Test
    void executionsAreFencedIndependently() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId a = ExecutionId.random();
        ExecutionId b = ExecutionId.random();
        log.append(a, created());
        log.append(a, started("node-0"));

        assertThat(log.append(b, created(), 0))
                .as("b's fence is b's own position, not the log's")
                .isEqualTo(0);
    }

    /**
     * A log that cannot fence must say so and must throw, never quietly append unconditionally.
     * A log that ignored {@code expectedSeq} would look like it was participating in the protocol
     * while providing none of it — which shows up as two nodes' events interleaved in one stream,
     * not as an error.
     */
    @Test
    void aLogThatCannotFenceRefusesRatherThanAppendingUnconditionally() {
        EventLog unfenced = new UnfencedLog();

        assertThat(unfenced.supportsConditionalAppend()).isFalse();
        assertThatThrownBy(() -> unfenced.append(ExecutionId.random(), created(), 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("conditional append");
        assertThat(unfenced.read(ExecutionId.random()))
                .as("and the rejected append really did not write")
                .isEmpty();
    }

    private static CatalystEvent created() {
        return new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", "");
    }

    private static CatalystEvent started(String nodeId) {
        return new CatalystEvent.ExecutionStarted(T, 1, nodeId);
    }

    /** Implements only what the SPI requires, to pin the inherited default. */
    private static final class UnfencedLog implements EventLog {
        @Override public long append(ExecutionId executionId, CatalystEvent event) { return 0; }
        @Override public List<SequencedEvent> read(ExecutionId executionId) { return List.of(); }
        @Override public long latestSeq(ExecutionId executionId) { return -1; }
        @Override public Optional<ExecutionId> findByKey(String idempotencyKey) { return Optional.empty(); }
        @Override public void putKey(String idempotencyKey, ExecutionId executionId) { }
        @Override public void close() { }
    }
}
