package com.cajunsystems.catalyst.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-call retries and timeouts on the durable log, driving the same code as {@code Demo calls}: a
 * flaky provider and a hung tool are each retried in place, so the task finishes in one attempt, and
 * a strict replay makes no call.
 */
class CallsAcceptanceTest {

    @Test
    void eachCallIsRetriedInPlaceAndReplaysWithNoCalls(@TempDir Path dir) {
        CallsDemo.Outcome o = CallsDemo.run(dir);
        assertThat(o.result()).isEqualTo("order-1138: delivered, damaged -> refund approved");
        assertThat(o.lookups()).as("one hung, timed out, then succeeded").isEqualTo(2);
        assertThat(o.providerCalls()).as("two 503s, then an answer").isEqualTo(3);
        assertThat(o.callRetries()).isEqualTo(3);
        assertThat(o.taskRetries()).as("no whole-task retry").isZero();
        assertThat(o.took()).isLessThan(Duration.ofSeconds(10));
        assertThat(o.replayProviderCalls() + o.replayLookups()).isZero();
    }
}
