package com.cajunsystems.catalyst.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallPolicyUnitTest {

    @Test
    void noneMakesOneAttemptWithNoTimeout() {
        assertThat(CallPolicy.none().maxAttempts()).isEqualTo(1);
        assertThat(CallPolicy.none().retries(1, new RuntimeException())).isFalse();
        assertThat(CallPolicy.none().timeout()).isEmpty();
    }

    @Test
    void retriesCountAttemptsAfterTheFirst() {
        CallPolicy p = CallPolicy.retries(2, Duration.ofMillis(100));
        assertThat(p.maxAttempts()).isEqualTo(3);
        assertThat(p.retries(1, new RuntimeException())).isTrue();
        assertThat(p.retries(2, new RuntimeException())).isTrue();
        assertThat(p.retries(3, new RuntimeException())).isFalse();
    }

    @Test
    void backoffDoublesUpToTheCap() {
        CallPolicy p = CallPolicy.retries(10, Duration.ofMillis(100)).maxBackoff(Duration.ofMillis(500));
        assertThat(p.backoffAfter(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(p.backoffAfter(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(p.backoffAfter(3)).isEqualTo(Duration.ofMillis(400));
        assertThat(p.backoffAfter(4)).isEqualTo(Duration.ofMillis(500));
        assertThat(p.backoffAfter(9)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void thePredicateDecidesWhichFailuresAreRetried() {
        CallPolicy p = CallPolicy.retries(3, Duration.ZERO).retryOn(e -> e instanceof IllegalStateException);
        assertThat(p.retries(1, new IllegalStateException())).isTrue();
        assertThat(p.retries(1, new IllegalArgumentException())).isFalse();
    }

    @Test
    void invalidSettingsAreRefused() {
        assertThatThrownBy(() -> CallPolicy.retries(-1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CallPolicy.retries(1, Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CallPolicy.timeoutAfter(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }
}
