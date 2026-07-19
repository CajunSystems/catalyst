package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Model;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Seeding behaviour for retry-as-attempt: a {@code RetryRequested} declines to substitute the failed
 * boundary it names, so the retry re-runs that boundary live rather than replaying the recorded failure.
 */
class ReplayingContextRetrySeedTest {

    private final ExecutionId id = ExecutionId.of("exec-1");
    private static final Instant T = Instant.EPOCH;

    /** A tool that counts its live invocations and echoes its input. */
    private static final class CountingTool implements Tool<String, String> {
        final AtomicInteger invocations = new AtomicInteger();
        public String name() { return "flaky"; }
        public Class<String> inputType() { return String.class; }
        public String apply(String input) { invocations.incrementAndGet(); return "ok:" + input; }
    }

    @Test
    void retriedToolFailureRunsLiveInsteadOfReplayingTheRecordedFailure() {
        // seq 0 ToolRequested, seq 1 ToolCompleted(error), seq 2 RetryRequested(failedSeq=1)
        List<SequencedEvent> recorded = seq(
                new ToolRequested(T, "flaky", new TextNode("in")),
                new ToolCompleted(T, null, "boom", 1),
                new RetryRequested(T, "boom", 10, 1));

        CountingTool tool = new CountingTool();
        String out = newContext(recorded).call(tool, "in");

        assertThat(out).as("the tool ran live and produced a fresh result").isEqualTo("ok:in");
        assertThat(tool.invocations).hasValue(1);
    }

    @Test
    void retriedToolFailureDoesNotBecomeInDoubtUnderFailPolicy() {
        // Same log: clearing the pending request in seed() is what stops the dropped boundary from
        // dangling into the in-doubt path (which under FAIL would throw InDoubtException).
        List<SequencedEvent> recorded = seq(
                new ToolRequested(T, "flaky", new TextNode("in")),
                new ToolCompleted(T, null, "boom", 1),
                new RetryRequested(T, "boom", 10, 1));

        CountingTool tool = new CountingTool();
        // No throw — resolves to a live run, not an InDoubtException.
        assertThat(newContext(recorded).call(tool, "in")).isEqualTo("ok:in");
    }

    @Test
    void aFinalisedRecordedToolFailureStillSubstitutesAndThrows() {
        // A genuine terminal recorded failure — the error is followed by ExecutionFailed, so it is NOT
        // the last event — remains a substitutable boundary: replay reproduces the recorded failure
        // without re-running the tool (determinism preserved for ordinary, non-retry replays).
        List<SequencedEvent> recorded = seq(
                new ToolRequested(T, "flaky", new TextNode("in")),
                new ToolCompleted(T, null, "boom", 1),
                new ExecutionFailed(T, "boom", 1));

        CountingTool tool = new CountingTool();
        assertThatThrownBy(() -> newContext(recorded).call(tool, "in"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Recorded tool 'flaky' failed");
        assertThat(tool.invocations).as("substituted — tool not invoked").hasValue(0);
    }

    @Test
    void aTrailingErroredToolWithNoRetryIsInDoubtNotSubstituted() {
        // A crash between recording the tool failure and deciding whether to retry leaves the errored
        // ToolCompleted as the last event, with no RetryRequested naming it. Its side effect is in-doubt:
        // it must route through InDoubtPolicy (FAIL here) rather than replay the recorded failure — which
        // would otherwise burn the retry budget on a boundary that can never succeed by substitution.
        List<SequencedEvent> recorded = seq(
                new ToolRequested(T, "flaky", new TextNode("in")),
                new ToolCompleted(T, null, "boom", 1));

        CountingTool tool = new CountingTool();
        assertThatThrownBy(() -> newContext(recorded).call(tool, "in"))
                .isInstanceOf(InDoubtException.class);
        assertThat(tool.invocations).as("in-doubt under FAIL — tool not re-run").hasValue(0);
    }

    private ReplayingContext newContext(List<SequencedEvent> recorded) {
        return new ReplayingContext(id, new InMemoryLog(recorded), (Model) null,
                new ExecutionInfo(id, 2, "TaskA", Map.of()), Map.of(),
                EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                ReplayMode.STRICT, null, Clock.systemUTC(), org.slf4j.LoggerFactory.getLogger("test"),
                recorded, /* appendEnabled */ true);
    }

    private static List<SequencedEvent> seq(CatalystEvent... events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.length; i++) out.add(new SequencedEvent(i, events[i]));
        return out;
    }

    /** A minimal append-only log: seeds from a recorded prefix and appends past its tail. */
    private static final class InMemoryLog implements EventLog {
        private final List<SequencedEvent> events;
        InMemoryLog(List<SequencedEvent> seed) { this.events = new ArrayList<>(seed); }
        public long append(ExecutionId executionId, CatalystEvent event) {
            long seq = events.size();
            events.add(new SequencedEvent(seq, event));
            return seq;
        }
        public List<SequencedEvent> read(ExecutionId executionId) { return List.copyOf(events); }
        public long latestSeq(ExecutionId executionId) { return events.size() - 1; }
        public Optional<ExecutionId> findByKey(String idempotencyKey) { return Optional.empty(); }
        public void putKey(String idempotencyKey, ExecutionId executionId) { }
        public void close() { }
    }
}
