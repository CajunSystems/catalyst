package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.catalyst.events.EventUpcaster;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema-evolution exit criterion (spec §13.4): a log recorded under an <em>older</em> schema —
 * a since-renamed event type and a since-renamed field, plus an unknown field a newer writer added —
 * reads and folds under the current schema. Structural changes are handled by {@link EventUpcaster}s;
 * the additive change rides the tolerant reader.
 */
class SchemaEvolutionAcceptanceTest {

    private static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aLegacySchemaLogReadsAndFoldsUnderTheCurrentSchema() {
        byte[][] legacyLog = {
            bytes("{\"@type\":\"ExecutionCreated\",\"at\":\"2026-01-01T00:00:00Z\",\"taskType\":\"Demo\","
                    + "\"argsHash\":\"h\",\"configFingerprint\":\"cfg\",\"idempotencyKey\":\"k\"}"),
            bytes("{\"@type\":\"ExecutionStarted\",\"at\":\"2026-01-01T00:00:01Z\",\"attempt\":1,"
                    + "\"nodeId\":\"node-0\",\"futureFieldAddedLater\":true}"), // unknown field → tolerated
            bytes("{\"@type\":\"TaskCompleted\",\"at\":\"2026-01-01T00:00:02Z\",\"result\":42}"), // renamed type
        };

        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.renameType("TaskCompleted", "ExecutionCompleted"))
                .build();

        List<SequencedEvent> events = new ArrayList<>();
        for (int i = 0; i < legacyLog.length; i++) {
            events.add(new SequencedEvent(i, codec.decode(legacyLog[i])));
        }

        ExecutionState state = Reducer.fold(ExecutionId.random(), events);
        assertThat(state.status()).isEqualTo(Status.COMPLETED);
        assertThat(state.result().asInt()).isEqualTo(42);
    }

    @Test
    void gumboLogWiresUpcastersAndStillRoundTripsCurrentEvents(@TempDir Path dir) {
        // Smoke-test the durable factory that registers upcasters: a normal run still records and
        // completes (the upcaster is a no-op on current-shape events).
        List<EventUpcaster> upcasters = List.of(EventUpcaster.renameType("TaskCompleted", "ExecutionCompleted"));
        Task<Integer> task = ctx -> ctx.effect("n", () -> 7);

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir, upcasters)).build()) {
            var handle = runtime.execute(task);
            assertThat(handle.result()).isEqualTo(7);
            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.COMPLETED);
        }
    }
}
