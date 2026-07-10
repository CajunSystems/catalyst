package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
