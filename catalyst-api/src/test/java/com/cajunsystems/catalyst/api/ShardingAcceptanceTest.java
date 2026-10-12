package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ShardRing;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deploy-5 sharding end to end: independent nodes, each on its own durable log, behind a client-side
 * router that sends every submission by idempotency key and every id-addressed call by the shard in
 * the id. Re-submitting every key — the case that duplicates model calls if routing is wrong — makes
 * no new calls anywhere.
 */
class ShardingAcceptanceTest {

    private static final ShardRing RING = ShardRing.of("shard-a", "shard-b", "shard-c");

    private static final Task<String> ONE_SHOT = ctx ->
            ctx.model().complete(CompletionRequest.of(Prompt.builder().user("summarize").build())).message();

    /** What an application's front door does: route by key before submit, by id afterwards. */
    private record Router(Map<String, CatalystRuntime> nodes) {
        ExecutionHandle<String> submit(String key) {
            return nodes.get(RING.shardFor(key)).execute(ONE_SHOT, ExecutionOptions.withKey(key));
        }
        CatalystRuntime owner(ExecutionId id) {
            return nodes.get(ShardRing.shardOf(id).orElseThrow());
        }
    }

    @Test
    void routedByKeyThenByIdWithNoDuplicateCallsOnReSubmission(@TempDir Path dir) {
        Map<String, MockModel> models = new HashMap<>();
        Map<String, CatalystRuntime> nodes = new HashMap<>();
        for (String shard : RING.shards()) {
            models.put(shard, MockModel.alwaysReturn("SUMMARY"));
            nodes.put(shard, Catalyst.builder(dir.resolve(shard)).model(models.get(shard)).shard(shard, RING).build());
        }
        try {
            Router router = new Router(nodes);
            Map<String, ExecutionId> ids = new HashMap<>();
            for (int i = 0; i < 30; i++) {
                String key = "doc:" + i;
                ExecutionHandle<String> h = router.submit(key);
                assertThat(h.result()).isEqualTo("SUMMARY");
                ids.put(key, h.id());
            }
            int callsAfterFirstPass = models.values().stream().mapToInt(MockModel::callCount).sum();
            assertThat(callsAfterFirstPass).isEqualTo(30);
            assertThat(models.values()).as("work spread over every shard").allMatch(m -> m.callCount() > 0);

            // Re-submit everything: each key reaches the node whose index holds it, and attaches.
            for (int i = 0; i < 30; i++) {
                String key = "doc:" + i;
                assertThat(router.submit(key).id()).isEqualTo(ids.get(key));
            }
            assertThat(models.values().stream().mapToInt(MockModel::callCount).sum())
                    .as("zero duplicate model calls").isEqualTo(callsAfterFirstPass);

            // Id-addressed calls route on the id alone — no lookup table.
            for (ExecutionId id : ids.values()) {
                assertThat(router.owner(id).inspect(id).status()).isEqualTo(Status.COMPLETED);
            }
        } finally {
            nodes.values().forEach(CatalystRuntime::close);
        }
    }
}
