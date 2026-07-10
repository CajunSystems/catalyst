package com.cajunsystems.catalyst.model;

/** A single message in a {@link Prompt}. */
public record Message(Role role, String content) {

    public Message {
        if (role == null) throw new IllegalArgumentException("role must not be null");
        if (content == null) throw new IllegalArgumentException("content must not be null");
    }
}
