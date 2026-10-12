package com.cajunsystems.catalyst;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

/**
 * The fixed set of shards in a sharded deployment, and the routing rule that assigns work to them.
 *
 * <p>Catalyst scales past one node by running independent nodes, each with its own log, and routing
 * every submission <strong>by idempotency key</strong>. The key is the only thing a client holds
 * before submitting — the {@link ExecutionId} is minted inside {@code execute()} — and the idempotency
 * index lives in each node's own log. So the same key must always reach the same node: routed
 * anywhere else, a re-submission would miss the index, create a second execution, and duplicate every
 * model call. {@link #shardFor(String)} is that rule, and a sharded runtime enforces it on its side
 * too, refusing a key that routes elsewhere.
 *
 * <p>Once an execution exists, its id carries its shard ({@link ExecutionId#shard()}), so id-addressed
 * calls route with {@link #shardOf(ExecutionId)} and no lookup.
 *
 * <p>Routing is rendezvous (highest-random-weight) hashing over SHA-256 — stable across JVMs and
 * languages, with no ring of virtual nodes to keep in sync. <strong>The shard set is fixed for the
 * life of the data.</strong> Adding or removing a shard re-routes some keys, and a re-routed key that
 * is re-submitted creates a duplicate execution on its new shard. Change the set only with no
 * re-submissions of existing keys in flight (see {@code docs/deployment.md}).
 */
public final class ShardRing {

    /** Shard ids: lowercase letters, digits and hyphens, starting with a letter or digit, at most 32. */
    static final String SHARD_ID = "[a-z0-9][a-z0-9-]{0,31}";

    private final List<String> shards;

    private ShardRing(List<String> shards) {
        this.shards = shards;
    }

    /** A ring over {@code shards}. Order does not affect routing; duplicates are rejected. */
    public static ShardRing of(String... shards) {
        return of(List.of(shards));
    }

    public static ShardRing of(List<String> shards) {
        if (shards.isEmpty()) throw new IllegalArgumentException("a shard ring needs at least one shard");
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String s : shards) {
            requireValidShardId(s);
            if (!unique.add(s)) throw new IllegalArgumentException("duplicate shard id: " + s);
        }
        return new ShardRing(List.copyOf(unique));
    }

    static void requireValidShardId(String shard) {
        if (shard == null || !shard.matches(SHARD_ID)) {
            throw new IllegalArgumentException("invalid shard id '" + shard + "': use 1-32 lowercase"
                    + " letters, digits or hyphens, starting with a letter or digit");
        }
    }

    public List<String> shards() {
        return shards;
    }

    public boolean contains(String shard) {
        return shards.contains(shard);
    }

    /** The shard that owns {@code idempotencyKey}. Deterministic for a given shard set. */
    public String shardFor(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isEmpty()) {
            throw new IllegalArgumentException("routing needs a non-empty idempotency key");
        }
        String best = null;
        long bestWeight = 0;
        for (String shard : shards) {
            long w = weight(shard, idempotencyKey);
            // Unsigned compare; ties (astronomically unlikely) broken by shard id so the result is total.
            if (best == null || Long.compareUnsigned(w, bestWeight) > 0
                    || (w == bestWeight && shard.compareTo(best) < 0)) {
                best = shard;
                bestWeight = w;
            }
        }
        return best;
    }

    /** The shard an existing execution lives on, read from its id; empty for an unstamped id. */
    public static Optional<String> shardOf(ExecutionId id) {
        return id.shard();
    }

    private static long weight(String shard, String key) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(shard.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0); // separator: ("ab","c") and ("a","bc") must not collide
            sha.update(key.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(sha.digest()).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }

    @Override
    public String toString() {
        return "ShardRing" + shards;
    }
}
