package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ShardRing;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A sharded runtime enforces the routing contract on its side: it takes only keyed work routed to it,
 * stamps its shard into what it creates, and refuses ids that belong to another shard — all before
 * anything is written.
 */
class ShardingTest {

    private static final ShardRing RING = ShardRing.of("shard-a", "shard-b");

    private static final Task<String> ONE_SHOT = ctx ->
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("hi").build())).message();

    private static CatalystRuntime shard(String self, InMemoryEventLog log, MockModel model) {
        return CatalystRuntime.builder().log(log).model(model).shard(self, RING).build();
    }

    static String keyRoutedTo(String shard) {
        for (int i = 0; ; i++) if (RING.shardFor("key-" + i).equals(shard)) return "key-" + i;
    }

    @Test
    void routedWorkIsStampedAndAReSubmissionAttaches() {
        InMemoryEventLog log = new InMemoryEventLog();
        MockModel model = MockModel.alwaysReturn("pong");
        try (CatalystRuntime a = shard("shard-a", log, model)) {
            String key = keyRoutedTo("shard-a");
            ExecutionHandle<String> first = a.execute(ONE_SHOT, ExecutionOptions.withKey(key));
            assertThat(first.result()).isEqualTo("pong");
            assertThat(first.id().shard()).contains("shard-a");
            assertThat(RING.shardFor(key)).isEqualTo(ShardRing.shardOf(first.id()).orElseThrow());

            ExecutionHandle<String> again = a.execute(ONE_SHOT, ExecutionOptions.withKey(key));
            assertThat(again.id()).isEqualTo(first.id());
            assertThat(model.callCount()).as("attached, not re-run").isEqualTo(1);
        }
    }

    /** No key, nothing to route on; a key routed elsewhere would duplicate on re-submission. Both refused before writing. */
    @Test
    void unroutableOrMisroutedSubmissionsAreRefusedBeforeAnythingIsWritten() {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime a = shard("shard-a", log, MockModel.alwaysReturn("x"))) {
            assertThatThrownBy(() -> a.execute(ONE_SHOT))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires an idempotency key");
            String elsewhere = keyRoutedTo("shard-b");
            assertThatThrownBy(() -> a.execute(ONE_SHOT, ExecutionOptions.withKey(elsewhere)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("routes to shard 'shard-b'");

            assertThat(log.openExecutions()).as("nothing created").isEmpty();
            assertThat(log.findByKey(elsewhere)).as("key not claimed here").isEmpty();
        }
    }

    @Test
    void idAddressedCallsForAnotherShardsIdAreRefused() {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime a = shard("shard-a", log, MockModel.alwaysReturn("x"))) {
            ExecutionId foreign = ExecutionId.random("shard-b");
            List<Consumer<ExecutionId>> calls = List.of(
                    a::inspect, a::resume, a::cancel, a::pause, a::replay,
                    id -> a.replay(id, ONE_SHOT), id -> a.branch(id, 0));
            for (Consumer<ExecutionId> call : calls) {
                assertThatThrownBy(() -> call.accept(foreign))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("belongs to shard 'shard-b'");
            }
            assertThat(log.read(foreign)).as("nothing written to the foreign id").isEmpty();
        }
    }

    /** Executions from before the deployment was sharded carry no shard; they stay addressable. */
    @Test
    void unstampedIdsStayAddressable() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId legacy;
        try (CatalystRuntime unsharded = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("x")).build()) {
            ExecutionHandle<String> h = unsharded.execute(ONE_SHOT);
            h.result();
            legacy = h.id();
        }
        assertThat(legacy.shard()).isEmpty();
        try (CatalystRuntime a = shard("shard-a", log, MockModel.alwaysReturn("x"))) {
            assertThat(a.inspect(legacy).isTerminal()).isTrue();
        }
    }

    @Test
    void aBranchChildIsStampedWithTheOwningShard() {
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime a = shard("shard-a", log, MockModel.alwaysReturn("x"))) {
            ExecutionHandle<String> h = a.execute(ONE_SHOT, ExecutionOptions.withKey(keyRoutedTo("shard-a")));
            h.result();
            ExecutionId child = a.branch(h.id(), Long.MAX_VALUE).run(ONE_SHOT).id();
            assertThat(child.shard()).contains("shard-a");
        }
    }

    @Test
    void aShardMustBeInItsRing() {
        assertThatThrownBy(() -> CatalystRuntime.builder().log(new InMemoryEventLog()).shard("shard-z", RING))
                .hasMessageContaining("not in");
    }
}
