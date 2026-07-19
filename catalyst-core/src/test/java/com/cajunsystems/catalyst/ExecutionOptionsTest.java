package com.cajunsystems.catalyst;

import com.cajunsystems.catalyst.engine.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionOptionsTest {

    @Test
    void retryPolicyOverrideSurvivesEveryCopyOnWriteMutator() {
        RetryPolicy policy = RetryPolicy.maxRetries(3, Duration.ofMillis(1));
        // Set the policy first, then apply every other mutator: each returns a fresh instance and must
        // carry the policy forward. Missing any one site would silently drop the override.
        ExecutionOptions opts = ExecutionOptions.none()
                .retryPolicy(policy)
                .key("k")
                .var("a", 1)
                .meta("m", "v");

        assertThat(opts.retryPolicy()).as("survives key/var/meta").containsSame(policy);
        assertThat(opts.idempotencyKey()).contains("k");
        assertThat(opts.vars()).containsEntry("a", 1);
        assertThat(opts.metadata()).containsEntry("m", "v");
    }

    @Test
    void noPolicyByDefault() {
        assertThat(ExecutionOptions.none().retryPolicy()).isEmpty();
        assertThat(ExecutionOptions.withKey("k").retryPolicy()).isEmpty();
    }
}
