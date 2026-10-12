package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The execution index: the set of executions created and not yet terminal, which is what a
 * restarted runtime needs to find its in-flight work without already holding the ids.
 */
class ExecutionIndexTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    private static final Task<String> ONE_SHOT = ctx ->
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();

    @Test
    void anExecutionIsOpenFromCreationUntilItsTerminalEvent() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();

        assertThat(log.supportsExecutionIndex()).isTrue();
        assertThat(log.openExecutions()).isEmpty();

        log.append(id, created());
        assertThat(log.openExecutions()).containsExactly(id);

        // Lifecycle moments short of terminal leave it open — including a pause, which is work an
        // operator is holding, not work that has finished.
        log.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
        log.append(id, new CatalystEvent.RetryRequested(T, "transient", 0, 1));
        log.append(id, new CatalystEvent.ExecutionResumed(T, 2));
        log.append(id, new CatalystEvent.ExecutionPaused(T, "operator"));
        assertThat(log.openExecutions()).containsExactly(id);

        log.append(id, new CatalystEvent.ExecutionCompleted(T, new TextNode("done")));
        assertThat(log.openExecutions()).isEmpty();
    }

    @Test
    void everyTerminalEventClosesAndTheRestStayInCreationOrder() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId completed = ExecutionId.random();
        ExecutionId running = ExecutionId.random();
        ExecutionId failed = ExecutionId.random();
        ExecutionId paused = ExecutionId.random();
        ExecutionId cancelled = ExecutionId.random();
        for (ExecutionId id : List.of(completed, running, failed, paused, cancelled)) log.append(id, created());

        log.append(completed, new CatalystEvent.ExecutionCompleted(T, new TextNode("ok")));
        log.append(running, new CatalystEvent.ExecutionStarted(T, 1, "node-0"));
        log.append(failed, new CatalystEvent.ExecutionFailed(T, "boom", 0));
        log.append(paused, new CatalystEvent.ExecutionPaused(T, "operator"));
        log.append(cancelled, new CatalystEvent.ExecutionCancelled(T, "stop", 0));

        assertThat(log.openExecutions()).containsExactly(running, paused);
    }

    /** The fenced path writes the index too — Dist-1 moves the runtime onto it. */
    @Test
    void aConditionalAppendMaintainsTheIndex() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();

        log.append(id, created(), 0);
        assertThat(log.openExecutions()).containsExactly(id);
        log.append(id, new CatalystEvent.ExecutionCancelled(T, "stop", 0), 1);
        assertThat(log.openExecutions()).isEmpty();
    }

    /**
     * A log that cannot enumerate must say so. An empty list would tell a recovery scan there is
     * nothing to recover — the same answer as success.
     */
    @Test
    void aLogWithoutAnIndexRefusesRatherThanReportingNothingOpen() {
        EventLog bare = new BareLog();

        assertThat(bare.supportsExecutionIndex()).isFalse();
        assertThatThrownBy(bare::openExecutions)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("BareLog");
    }

    /**
     * Through the runtime: finished executions — completed or failed — leave nothing open, and a
     * branch child, which cannot be resumed after a restart, is never listed.
     */
    @Test
    void theRuntimesFinishedExecutionsAndBranchesLeaveNothingOpen() {
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(MockModel.alwaysReturn("pong")).build()) {

            ExecutionHandle<String> first = runtime.execute(ONE_SHOT);
            first.result();
            ExecutionId done = first.id();
            runtime.executeAndWait(ONE_SHOT);
            Task<String> failing = ctx -> { throw new IllegalStateException("boom"); };
            assertThatThrownBy(() -> runtime.executeAndWait(failing)).isNotNull();
            runtime.branch(done, Long.MAX_VALUE).run(ONE_SHOT);

            assertThat(log.openExecutions()).isEmpty();
        }
    }

    private static CatalystEvent created() {
        return new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", "");
    }

    /** An EventLog implementing only the required methods — what a third-party backend starts as. */
    private static final class BareLog implements EventLog {
        public long append(ExecutionId executionId, CatalystEvent event) { return 0; }
        public List<SequencedEvent> read(ExecutionId executionId) { return List.of(); }
        public long latestSeq(ExecutionId executionId) { return -1; }
        public Optional<ExecutionId> findByKey(String idempotencyKey) { return Optional.empty(); }
        public void putKey(String idempotencyKey, ExecutionId executionId) { }
        public void close() { }
    }
}
