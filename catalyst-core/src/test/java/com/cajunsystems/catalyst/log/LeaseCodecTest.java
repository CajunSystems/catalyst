package com.cajunsystems.catalyst.log;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lease rows are compared by their bytes, so the encoding must be canonical and must round-trip. */
class LeaseCodecTest {

    @Test
    void roundTripsIncludingAHolderWithTheSeparatorInIt() {
        Lease lease = new Lease("pod|7", 1_767_225_600_000L);
        assertThat(Lease.decode(lease.encode())).isEqualTo(lease);
    }

    @Test
    void equalLeasesEncodeToEqualBytes() {
        assertThat(new Lease("node-a", 42).encode()).isEqualTo(new Lease("node-a", 42).encode());
        assertThat(new String(new Lease("node-a", 42).encode(), StandardCharsets.UTF_8)).isEqualTo("lease1|42|node-a");
    }

    @Test
    void liveUntilItsExpiryExclusive() {
        Lease lease = new Lease("node-a", 1_000);
        assertThat(lease.liveAt(Instant.ofEpochMilli(999))).isTrue();
        assertThat(lease.liveAt(Instant.ofEpochMilli(1_000))).isFalse();
    }

    @Test
    void refusesBytesThatAreNotALease() {
        assertThatThrownBy(() -> Lease.decode("owner@123".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Lease.decode("lease1|soon|node-a".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Lease(" ", 1)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Dist-4's cancel request rides on the row; the reason is base64 so it cannot collide with the separator. */
    @Test
    void aCancelRequestRoundTripsAndOldRowsStillRead() {
        Lease plain = new Lease("pod|7", 42);
        Lease asked = plain.withCancelRequest("stop | now: ünïcode");
        assertThat(Lease.decode(asked.encode())).isEqualTo(asked);
        assertThat(asked.cancelRequested()).contains("stop | now: ünïcode");
        assertThat(asked.holder()).isEqualTo("pod|7");
        assertThat(asked.expiresAtMillis()).isEqualTo(42);
        assertThat(new String(plain.encode(), StandardCharsets.UTF_8)).as("unchanged format without a request")
                .isEqualTo("lease1|42|pod|7");
        assertThat(asked.encode()).isNotEqualTo(plain.encode());
    }
}
