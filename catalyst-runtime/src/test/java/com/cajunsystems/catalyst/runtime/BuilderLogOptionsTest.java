package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How the builder obtains its log, and which options can only apply when it constructs the log
 * itself. The rule that matters: an option the builder cannot honour is refused, never dropped.
 */
class BuilderLogOptionsTest {

    /** A store passed alongside a pre-built log could not reach that log's codec. Saying so beats ignoring it. */
    @Test
    void aBlobStoreWithAPrebuiltLogIsRejected() {
        assertThatThrownBy(() -> CatalystRuntime.builder()
                .log(EventLogs.inMemory()).blobStore(BlobStore.inMemory()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blobStore")
                .hasMessageContaining("log(...)");
    }

    @Test
    void aLogAndALogFactoryAreExclusive() {
        assertThatThrownBy(() -> CatalystRuntime.builder()
                .log(EventLogs.inMemory()).logFactory((store, threshold) -> EventLogs.inMemory()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not both");
    }

    @Test
    void aLogFactoryIsHandedTheBlobStoreAndThreshold() {
        BlobStore store = BlobStore.inMemory();
        AtomicReference<BlobStore> seenStore = new AtomicReference<>();
        int[] seenThreshold = {0};
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .logFactory((s, threshold) -> { seenStore.set(s); seenThreshold[0] = threshold; return EventLogs.inMemory(); })
                .blobStore(store, 1024)
                .model(MockModel.alwaysReturn("x"))
                .build()) {
            assertThat(runtime.log()).isNotNull();
        }
        assertThat(seenStore.get()).isSameAs(store);
        assertThat(seenThreshold[0]).isEqualTo(1024);
    }

    @Test
    void withoutABlobStoreTheFactoryIsToldToUseItsDefault() {
        AtomicReference<BlobStore> seenStore = new AtomicReference<>(BlobStore.inMemory());
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .logFactory((s, threshold) -> { seenStore.set(s); return EventLogs.inMemory(); })
                .build()) {
            assertThat(runtime.log()).isNotNull();
        }
        assertThat(seenStore.get()).isNull();
    }

    /** The log owns the store only once constructed; if construction fails the builder must release it. */
    @Test
    void aSuppliedStoreIsClosedWhenOpeningTheLogFails() {
        boolean[] closed = {false};
        BlobStore store = new BlobStore() {
            final BlobStore bytes = BlobStore.inMemory();
            public String put(byte[] content) { return bytes.put(content); }
            public byte[] get(String ref) { return bytes.get(ref); }
            public boolean has(String ref) { return bytes.has(ref); }
            @Override public void close() { closed[0] = true; }
        };
        assertThatThrownBy(() -> CatalystRuntime.builder()
                .logFactory((s, threshold) -> { throw new IllegalStateException("directory already open"); })
                .blobStore(store)
                .build())
                .hasMessageContaining("already open");
        assertThat(closed[0]).isTrue();
    }

    @Test
    void aNonPositiveThresholdIsRejected() {
        assertThatThrownBy(() -> CatalystRuntime.builder().blobStore(BlobStore.inMemory(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
