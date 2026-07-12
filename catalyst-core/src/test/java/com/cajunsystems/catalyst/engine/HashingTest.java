package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HashingTest {

    @Test
    void identicalRequestsHashEqual() {
        CompletionRequest a = CompletionRequest.of(Prompt.builder().system("s").user("u").build());
        CompletionRequest b = CompletionRequest.of(Prompt.builder().system("s").user("u").build());
        assertThat(Hashing.canonicalRequestHash(a)).isEqualTo(Hashing.canonicalRequestHash(b));
    }

    @Test
    void embeddedDelimitersDoNotForgeACollision() {
        // One user message whose content embeds a fake "\nSYSTEM:" delimiter ...
        CompletionRequest single = CompletionRequest.of(
                Prompt.builder().message(new Message(Role.USER, "a\nSYSTEM:b")).build());
        // ... must NOT hash the same as two genuine messages.
        CompletionRequest two = CompletionRequest.of(
                Prompt.builder().message(new Message(Role.USER, "a")).message(new Message(Role.SYSTEM, "b")).build());
        assertThat(Hashing.canonicalRequestHash(single)).isNotEqualTo(Hashing.canonicalRequestHash(two));
    }
}
