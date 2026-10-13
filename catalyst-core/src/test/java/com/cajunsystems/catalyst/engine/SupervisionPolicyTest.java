package com.cajunsystems.catalyst.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupervisionPolicyTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void escalateNeverRestarts() {
        assertThat(Supervision.escalate().restarts("FAILED", List.of(T))).isFalse();
    }

    @Test
    void restartCountsEveryFailureWithoutAWindow() {
        Supervision s = Supervision.restart(2);
        assertThat(s.restarts("FAILED", List.of(T))).isTrue();
        assertThat(s.restarts("FAILED", List.of(T, T.plusSeconds(1)))).isTrue();
        assertThat(s.restarts("FAILED", List.of(T, T.plusSeconds(1), T.plusSeconds(2)))).isFalse();
    }

    @Test
    void aCancelledChildIsNeverRestarted() {
        assertThat(Supervision.restart(5).restarts("CANCELLED", List.of(T))).isFalse();
    }

    @Test
    void theWindowCountsOnlyRecentFailures() {
        Supervision s = Supervision.restart(1).within(Duration.ofMinutes(10));
        assertThat(s.restarts("FAILED", List.of(T, T.plus(Duration.ofMinutes(11))))).isTrue();
        assertThat(s.restarts("FAILED", List.of(T, T.plus(Duration.ofMinutes(9))))).isFalse();
        // Exactly one window apart: the older failure has left it.
        assertThat(s.restarts("FAILED", List.of(T, T.plus(Duration.ofMinutes(10))))).isTrue();
    }

    @Test
    void invalidSettingsAreRefused() {
        assertThatThrownBy(() -> Supervision.restart(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Supervision.restart(1).within(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Supervision.restart(1).backoff(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }
}
