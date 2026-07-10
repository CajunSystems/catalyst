package com.cajunsystems.catalyst.model;

import java.util.ArrayList;
import java.util.List;

/**
 * An immutable prompt: an ordered list of {@link Message}s. Built via {@link #builder()}. The prompt
 * is part of what gets canonically hashed for replay, so it is a pure value with a stable order.
 */
public record Prompt(List<Message> messages) {

    public Prompt {
        messages = List.copyOf(messages);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<Message> messages = new ArrayList<>();

        public Builder system(String content) {
            messages.add(new Message(Role.SYSTEM, content));
            return this;
        }

        public Builder user(String content) {
            messages.add(new Message(Role.USER, content));
            return this;
        }

        public Builder assistant(String content) {
            messages.add(new Message(Role.ASSISTANT, content));
            return this;
        }

        public Builder tool(String content) {
            messages.add(new Message(Role.TOOL, content));
            return this;
        }

        public Builder message(Message message) {
            messages.add(message);
            return this;
        }

        public Prompt build() {
            return new Prompt(messages);
        }
    }
}
