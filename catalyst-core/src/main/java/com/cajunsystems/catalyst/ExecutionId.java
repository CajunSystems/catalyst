package com.cajunsystems.catalyst;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The stable identity of one durable execution. Also the key that scopes an execution's event
 * stream in the log.
 *
 * <p>A runtime that is one shard of a sharded deployment stamps its shard into the ids it mints
 * ({@code exec-<uuid>@<shard>}), so an id-addressed call — {@code inspect}, {@code resume},
 * {@code cancel} — can be routed to the owning node from the id alone, with no lookup. See
 * {@link ShardRing}.
 */
public record ExecutionId(String value) {

    /** Strict on purpose: only an id the runtime minted is ever read as carrying a shard. */
    private static final Pattern STAMPED =
            Pattern.compile("exec-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}@(" + ShardRing.SHARD_ID + ")");

    public ExecutionId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ExecutionId value must not be blank");
        }
    }

    /** Generates a fresh random id. */
    public static ExecutionId random() {
        return new ExecutionId("exec-" + UUID.randomUUID());
    }

    /** Generates a fresh random id stamped with the shard that owns it. */
    public static ExecutionId random(String shard) {
        ShardRing.requireValidShardId(shard);
        return new ExecutionId("exec-" + UUID.randomUUID() + "@" + shard);
    }

    public static ExecutionId of(String value) {
        return new ExecutionId(value);
    }

    /** The shard stamped into this id, or empty for an id minted by an unsharded runtime. */
    public Optional<String> shard() {
        Matcher m = STAMPED.matcher(value);
        return m.matches() ? Optional.of(m.group(1)) : Optional.empty();
    }

    @Override
    public String toString() {
        return value;
    }
}
