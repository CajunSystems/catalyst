package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ShardRing;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-2: a runtime that declares it shares its log ({@code distributed()}) refuses to start unless
 * the log <em>reports</em> both halves of what that needs. It also refuses options that cannot be
 * safe on a shared log yet. Every refusal happens in {@code build()}, before the runtime exists.
 */
class DistributionGuardTest {

    /**
     * An in-memory log reporting whatever capabilities a test chooses. The guard reads only the
     * answers, which is the point: it must believe the log, never assume on its behalf.
     */
    static final class ReportingLog implements EventLog {
        private final InMemoryEventLog delegate = new InMemoryEventLog();
        private final boolean fenced;
        private final boolean multiWriter;
        final AtomicBoolean closed = new AtomicBoolean();

        ReportingLog(boolean fenced, boolean multiWriter) {
            this.fenced = fenced;
            this.multiWriter = multiWriter;
        }

        @Override public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
        @Override public long append(ExecutionId id, CatalystEvent event, long expectedSeq) { return delegate.append(id, event, expectedSeq); }
        @Override public boolean supportsConditionalAppend() { return fenced; }
        @Override public boolean supportsMultiWriter() { return multiWriter; }
        @Override public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        @Override public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        @Override public List<ExecutionId> openExecutions() { return delegate.openExecutions(); }
        @Override public boolean supportsExecutionIndex() { return delegate.supportsExecutionIndex(); }
        @Override public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        @Override public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        @Override public void close() { closed.set(true); }
    }

    private static CatalystRuntime.Builder distributedOn(EventLog log) {
        return CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("x")).nodeId("node-a").distributed();
    }

    @Test
    void aFencedMultiWriterLogStartsADistributedRuntime() {
        try (CatalystRuntime runtime = distributedOn(new ReportingLog(true, true)).build()) {
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
        assertThatThrownBy(() -> distributedOn(new ReportingLog(false, true)).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supportsConditionalAppend()=false")
                .hasMessageContaining("supportsMultiWriter()=true");
    }

    /** Refused a log it opened itself: the builder closes it, as it does for recoverOnStart's refusal. */
    @Test
    void aLogTheBuilderOpenedIsClosedWhenRefused() {
        ReportingLog log = new ReportingLog(true, false);
        assertThatThrownBy(() -> CatalystRuntime.builder().logFactory((store, threshold) -> log)
                .nodeId("node-a").distributed().build())
                .isInstanceOf(IllegalStateException.class);
        assertThat(log.closed).as("builder-opened log released").isTrue();
    }

    /** A log the caller passed in stays the caller's, refused or not. */
    @Test
    void aLogTheCallerSuppliedIsLeftOpenWhenRefused() {
        ReportingLog log = new ReportingLog(true, false);
        assertThatThrownBy(() -> distributedOn(log).build()).isInstanceOf(IllegalStateException.class);
        assertThat(log.closed).isFalse();
    }

    @Test
    void aDistributedRuntimeNeedsAnExplicitNodeId() {
        assertThatThrownBy(() -> CatalystRuntime.builder().log(new ReportingLog(true, true)).distributed().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nodeId");
    }

    @Test
    void distributedAndShardedAreAlternatives() {
        ShardRing ring = ShardRing.of("shard-a", "shard-b");
        assertThatThrownBy(() -> distributedOn(new ReportingLog(true, true)).shard("shard-a", ring).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("alternatives");
    }

    /**
     * On a shared index, "open and not running here" includes work running on another node, so the
     * recovery scan would take it over mid-call. Refused at build and when called directly, until leases.
     */
    @Test
    void theRecoveryScanIsRefusedOnADistributedRuntime() {
        assertThatThrownBy(() -> distributedOn(new ReportingLog(true, true)).recoverOnStart(true).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovery scan");
        try (CatalystRuntime runtime = distributedOn(new ReportingLog(true, true)).build()) {
            assertThatThrownBy(runtime::recoverAll)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("leases");
        }
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
