package com.cajunsystems.catalyst;

import java.util.UUID;

/**
 * The stable identity of one durable execution. Also the key that scopes an execution's event
 * stream in the log.
 */
public record ExecutionId(String value) {

    public ExecutionId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ExecutionId value must not be blank");
        }
    }

    /** Generates a fresh random id. */
    public static ExecutionId random() {
        return new ExecutionId("exec-" + UUID.randomUUID());
    }

    public static ExecutionId of(String value) {
        return new ExecutionId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
