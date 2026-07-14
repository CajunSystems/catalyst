package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The generic-collection payloads increment (roadmap ④): a task may return, or capture through
 * {@code ctx.effect(...)}, a {@link List}/{@link Map} of records — and it round-trips through the log
 * with element-type fidelity. On replay the recorded collection is substituted from the log; if the
 * element types were lost (decoded as maps rather than records) the typed stream below would throw,
 * so a clean replay is the proof that fidelity is preserved.
 */
class CollectionPayloadAcceptanceTest {

    record Item(String sku, int qty) {}

    @Test
    void collectionEffectRoundTripsAndIsSubstitutedOnReplay(@TempDir Path dir) {
        AtomicInteger supplierCalls = new AtomicInteger();

        Task<Integer> task = ctx -> {
            List<Item> cart = ctx.effect("load-cart", () -> {
                supplierCalls.incrementAndGet();
                return List.of(new Item("A", 2), new Item("B", 3));
            });
            // Fails with ClassCastException if elements came back as maps instead of Items.
            return cart.stream().mapToInt(Item::qty).sum();
        };

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            var handle = runtime.execute(task);
            assertThat(handle.result()).isEqualTo(5);
            ExecutionId id = handle.id();
            assertThat(supplierCalls.get()).isEqualTo(1);

            // Replay: the effect is substituted from the log (supplier not re-run), and the typed list
            // is reconstructed with element types intact.
            ExecutionState replayed = runtime.replay(id, task);
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(supplierCalls.get()).isEqualTo(1); // substituted, not re-executed
        }
    }

    @Test
    void mapResultRoundTripsThroughTheLog(@TempDir Path dir) {
        Task<Map<String, Item>> task = ctx -> Map.of(
                "first", new Item("A", 2),
                "second", new Item("B", 3));

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            Map<String, Item> result = runtime.execute(task).result();
            assertThat(result).containsEntry("first", new Item("A", 2))
                    .containsEntry("second", new Item("B", 3));
            assertThat(result.get("first")).isInstanceOf(Item.class);
        }
    }
}
