package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ShardRing;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-2: a runtime that declares it shares its log ({@code distributed()}) refuses to start unless
 * the log <em>reports</em> both halves of what that needs. It also refuses options that cannot be
 * safe on a shared log yet. Every refusal happens in {@code build()}, before the runtime exists.
 */
class DistributionGuardTest {

    private static CatalystRuntime.Builder distributedOn(EventLog log) {
        return CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("x")).nodeId("node-a").distributed();
    }

    @Test
    void aFencedMultiWriterLogStartsADistributedRuntime() {
        try (CatalystRuntime runtime = distributedOn(SharedLog.distributable()).build()) {
            assertThat(runtime.isDistributed()).isTrue();
        }
    }

    /** The in-memory log fences but is single-process. That is the pair the guard exists to catch. */
    @Test
    void aFencedButSingleWriterLogIsRefused() {
        assertThatThrownBy(() -> distributedOn(EventLogs.inMemory()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supportsConditionalAppend()=true")
                .hasMessageContaining("supportsMultiWriter()=false");
    }

    /** Multi-writer without a fence would let a zombie append after a takeover: refused as well. */
    @Test
    void aMultiWriterLogThatCannotFenceIsRefused() {
        assertThatThrownBy(() -> distributedOn(new SharedLog(false, true, true)).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supportsConditionalAppend()=false")
                .hasMessageContaining("supportsMultiWriter()=true");
    }

    /** Refused a log it opened itself: the builder closes it, as it does for recoverOnStart's refusal. */
    @Test
    void aLogTheBuilderOpenedIsClosedWhenRefused() {
        SharedLog log = new SharedLog(true, false, true);
        assertThatThrownBy(() -> CatalystRuntime.builder().logFactory((store, threshold) -> log)
                .nodeId("node-a").distributed().build())
                .isInstanceOf(IllegalStateException.class);
        assertThat(log.closed).as("builder-opened log released").isTrue();
    }

    /** A log the caller passed in stays the caller's, refused or not. */
    @Test
    void aLogTheCallerSuppliedIsLeftOpenWhenRefused() {
        SharedLog log = new SharedLog(true, false, true);
        assertThatThrownBy(() -> distributedOn(log).build()).isInstanceOf(IllegalStateException.class);
        assertThat(log.closed).isFalse();
    }

    @Test
    void aDistributedRuntimeNeedsAnExplicitNodeId() {
        assertThatThrownBy(() -> CatalystRuntime.builder().log(SharedLog.distributable()).distributed().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nodeId");
    }

    @Test
    void distributedAndShardedAreAlternatives() {
        ShardRing ring = ShardRing.of("shard-a", "shard-b");
        assertThatThrownBy(() -> distributedOn(SharedLog.distributable()).shard("shard-a", ring).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("alternatives");
    }

    /** Without leases, nodes could not tell who is running what: refused like the other two. */
    @Test
    void aLogThatStoresNoLeasesIsRefused() {
        assertThatThrownBy(() -> distributedOn(new SharedLog(true, true, false)).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supportsLeases()=false");
    }

    /** Dist-3 made the recovery scan lease-aware, so a distributed node may recover on start. */
    @Test
    void aDistributedRuntimeMayRecoverOnStart() {
        try (CatalystRuntime runtime = distributedOn(SharedLog.distributable()).recoverOnStart(true).build()) {
            assertThat(runtime.startupRecovery()).isPresent();
        }
    }

    @Test
    void leaseTimingIsRefusedOnARuntimeThatIsNotDistributed() {
        assertThatThrownBy(() -> CatalystRuntime.builder().log(EventLogs.inMemory())
                .lease(java.time.Duration.ofSeconds(30), java.time.Duration.ofSeconds(10)).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("distributed()");
        assertThatThrownBy(() -> CatalystRuntime.builder()
                .lease(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shorter");
    }

    /** Nothing changes for a runtime that did not ask: the in-memory log still builds and recovers. */
    @Test
    void aRuntimeThatIsNotDistributedIsUnaffected() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(MockModel.alwaysReturn("x")).recoverOnStart(true).build()) {
            assertThat(runtime.isDistributed()).isFalse();
            assertThat(runtime.startupRecovery()).isPresent();
        }
    }
}
