package com.cajunsystems.catalyst;

import com.cajunsystems.catalyst.engine.RetryPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Options for starting an execution. The idempotency {@code key} is the important one: re-submitting
 * a task with the same key resumes/attaches to the existing execution instead of starting a new one
 * (spec §4).
 *
 * <p>Immutable and copy-on-write: every mutator returns a new instance. A per-execution
 * {@link #retryPolicy(RetryPolicy)} overrides the runtime-wide default; it is a dedicated field rather
 * than a {@code var} because vars are hashed into the idempotency fingerprint and a policy lambda is
 * not stably hashable.
 */
public final class ExecutionOptions {

    private final String key;
    private final Map<String, Object> vars;
    private final Map<String, String> metadata;
    private final RetryPolicy retryPolicy; // nullable: null ⇒ use the runtime-wide default

    private ExecutionOptions(String key, Map<String, Object> vars, Map<String, String> metadata,
                             RetryPolicy retryPolicy) {
        this.key = key;
        this.vars = vars;
        this.metadata = metadata;
        this.retryPolicy = retryPolicy;
    }

    /** No idempotency key, no vars — a fresh execution each time. */
    public static ExecutionOptions none() {
        return new ExecutionOptions(null, new HashMap<>(), new HashMap<>(), null);
    }

    /** Sets the idempotency key. */
    public static ExecutionOptions withKey(String key) {
        return none().key(key);
    }

    public ExecutionOptions key(String key) {
        return new ExecutionOptions(key, new HashMap<>(vars), new HashMap<>(metadata), retryPolicy);
    }

    /** Adds a task input variable, readable via {@link Context#var(String)}. */
    public ExecutionOptions var(String name, Object value) {
        var next = new HashMap<>(vars);
        next.put(name, value);
        return new ExecutionOptions(key, next, new HashMap<>(metadata), retryPolicy);
    }

    /** Adds an execution metadata entry. */
    public ExecutionOptions meta(String name, String value) {
        var next = new HashMap<>(metadata);
        next.put(name, value);
        return new ExecutionOptions(key, new HashMap<>(vars), next, retryPolicy);
    }

    /** Overrides the runtime-wide retry policy for this execution. */
    public ExecutionOptions retryPolicy(RetryPolicy retryPolicy) {
        return new ExecutionOptions(key, new HashMap<>(vars), new HashMap<>(metadata), retryPolicy);
    }

    public Optional<String> idempotencyKey() {
        return Optional.ofNullable(key);
    }

    public Map<String, Object> vars() {
        return Map.copyOf(vars);
    }

    public Map<String, String> metadata() {
        return Map.copyOf(metadata);
    }

    /** The per-execution retry policy override, if one was set. */
    public Optional<RetryPolicy> retryPolicy() {
        return Optional.ofNullable(retryPolicy);
    }
}
