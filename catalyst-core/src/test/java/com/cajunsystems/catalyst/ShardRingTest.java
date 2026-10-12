package com.cajunsystems.catalyst;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardRingTest {

    /**
     * The property everything rests on: the same key reaches the same shard from any router — another
     * instance, another JVM, another ordering of the same set. A router that disagreed would send a
     * re-submission to a node whose idempotency index has never seen the key.
     */
    @Test
    void routingIsAFunctionOfTheKeyAndTheSetAlone() {
        ShardRing a = ShardRing.of("shard-a", "shard-b", "shard-c");
        ShardRing reordered = ShardRing.of("shard-c", "shard-a", "shard-b");
        for (int i = 0; i < 500; i++) {
            String key = "doc:" + i;
            assertThat(reordered.shardFor(key)).isEqualTo(a.shardFor(key));
            assertThat(ShardRing.of("shard-a", "shard-b", "shard-c").shardFor(key)).isEqualTo(a.shardFor(key));
        }
        // Pinned: routing is part of the data's contract. If these ever change, every existing key is
        // re-routed and its re-submission duplicates on the new shard — so a change here is a break.
        assertThat(a.shardFor("doc:1")).isEqualTo("shard-a");
        assertThat(a.shardFor("doc:42")).isEqualTo("shard-c");
        assertThat(a.shardFor("order-7")).isEqualTo("shard-a");
        assertThat(a.shardFor("tenant/acme")).isEqualTo("shard-b");
    }

    @Test
    void keysSpreadAcrossShards() {
        ShardRing ring = ShardRing.of("a", "b", "c");
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 3000; i++) counts.merge(ring.shardFor("key-" + i), 1, Integer::sum);
        assertThat(counts).containsOnlyKeys("a", "b", "c");
        counts.values().forEach(n -> assertThat(n).isBetween(800, 1200));
    }

    /**
     * Why rendezvous hashing: adding a shard moves only the keys the new shard wins — none move
     * between the old shards. Re-routed keys are still a duplication risk on re-submission (see the
     * class javadoc), but the blast radius is the new shard's share, not a reshuffle of everything.
     */
    @Test
    void addingAShardOnlyMovesKeysToTheNewShard() {
        ShardRing before = ShardRing.of("a", "b", "c");
        ShardRing after = ShardRing.of("a", "b", "c", "d");
        int moved = 0;
        for (int i = 0; i < 2000; i++) {
            String key = "key-" + i;
            String was = before.shardFor(key);
            String now = after.shardFor(key);
            if (!was.equals(now)) {
                assertThat(now).as("a moved key only ever moves to the new shard").isEqualTo("d");
                moved++;
            }
        }
        assertThat(moved).isBetween(350, 650); // ~1/4 of keys
    }

    @Test
    void shardIdsAndRingsAreValidated() {
        assertThatThrownBy(() -> ShardRing.of()).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShardRing.of("a", "a")).hasMessageContaining("duplicate");
        for (String bad : List.of("", "A", "-a", "a b", "a@b", "a/b", "x".repeat(33))) {
            assertThatThrownBy(() -> ShardRing.of(bad)).as(bad).hasMessageContaining("invalid shard id");
        }
        assertThatThrownBy(() -> ShardRing.of("a").shardFor("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anIdCarriesItsShardOnlyWhenTheRuntimeStampedIt() {
        ExecutionId stamped = ExecutionId.random("shard-b");
        assertThat(stamped.shard()).contains("shard-b");
        assertThat(ShardRing.shardOf(stamped)).contains("shard-b");
        assertThat(ExecutionId.of(stamped.value())).isEqualTo(stamped); // round-trips through its string

        assertThat(ExecutionId.random().shard()).isEmpty();
        // Look-alikes are not misread as stamped: only the exact minted shape counts.
        assertThat(ExecutionId.of("exec-whatever@shard-b").shard()).isEmpty();
        assertThat(ExecutionId.of("order@shard-b").shard()).isEmpty();
        assertThatThrownBy(() -> ExecutionId.random("Bad Shard")).hasMessageContaining("invalid shard id");
    }
}
