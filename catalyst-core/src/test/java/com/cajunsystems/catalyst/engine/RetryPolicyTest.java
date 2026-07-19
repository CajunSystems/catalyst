package com.cajunsystems.catalyst.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    private static final Throwable ANY = new RuntimeException("transient");

    @Test
    void noneNeverRetries() {
        assertThat(RetryPolicy.none().nextBackoff(0, ANY)).isEmpty();
    }

    @Test
    void maxRetriesStopsAfterBudget() {
        RetryPolicy p = RetryPolicy.maxRetries(3, Duration.ofMillis(50));
        assertThat(p.nextBackoff(0, ANY)).contains(Duration.ofMillis(50));
        assertThat(p.nextBackoff(1, ANY)).contains(Duration.ofMillis(50));
        assertThat(p.nextBackoff(2, ANY)).contains(Duration.ofMillis(50));
        assertThat(p.nextBackoff(3, ANY)).as("budget exhausted").isEmpty();
    }

    @Test
    void maxRetriesZeroIsEffectivelyNone() {
        assertThat(RetryPolicy.maxRetries(0, Duration.ofSeconds(1)).nextBackoff(0, ANY)).isEmpty();
    }

    @Test
    void exponentialGrowsAndCaps() {
        RetryPolicy p = RetryPolicy.exponential(3, Duration.ofMillis(100), 2.0, Duration.ofMillis(300));
        assertThat(p.nextBackoff(0, ANY)).contains(Duration.ofMillis(100));   // 100 * 2^0
        assertThat(p.nextBackoff(1, ANY)).contains(Duration.ofMillis(200));   // 100 * 2^1
        assertThat(p.nextBackoff(2, ANY)).contains(Duration.ofMillis(300));   // 100 * 2^2 = 400, capped to 300
        assertThat(p.nextBackoff(3, ANY)).as("budget exhausted").isEmpty();
    }

    @Test
    void policyMaySwitchOnFailureType() {
        // A policy is free to classify by exception; the engine only guarantees it sees retryable kinds.
        RetryPolicy retryTimeoutsOnly = (retriesSoFar, failure) ->
                failure.getMessage() != null && failure.getMessage().contains("timeout")
                        ? Optional.of(Duration.ofMillis(1)) : Optional.empty();
        assertThat(retryTimeoutsOnly.nextBackoff(0, new RuntimeException("read timeout"))).isPresent();
        assertThat(retryTimeoutsOnly.nextBackoff(0, new RuntimeException("bad request"))).isEmpty();
    }
}
