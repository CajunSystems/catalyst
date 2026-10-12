package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The blob store increment (roadmap ⑤): a payload larger than the offload threshold is stored in a
 * content-addressed blob store <em>out of line</em>, with the durable event stream carrying only a
 * small reference. On replay/inspect the payload is rehydrated transparently — the task code and the
 * folded {@link ExecutionState} never see a reference.
 */
class BlobStoreAcceptanceTest {

    @Test
    void largePayloadIsStoredOutOfLineAndRehydratedOnReplay(@TempDir Path dir) throws Exception {
        // A payload comfortably over the 64 KiB default offload threshold.
        String bigDocument = "LOREM-".repeat(30_000); // ~180 KB

        Task<Integer> task = ctx -> {
            String doc = ctx.effect("fetch-document", () -> bigDocument);
            return doc.length();
        };

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            var handle = runtime.execute(task);
            assertThat(handle.result()).isEqualTo(bigDocument.length());
            ExecutionId id = handle.id();

            // The blob store under the log directory holds the offloaded payload.
            Path blobsDir = dir.resolve("blobs");
            assertThat(Files.isDirectory(blobsDir)).isTrue();
            long blobCount;
            try (Stream<Path> walk = Files.walk(blobsDir)) {
                blobCount = walk.filter(Files::isRegularFile).count();
            }
            assertThat(blobCount).isGreaterThanOrEqualTo(1);

            // The big document is NOT inlined anywhere in the raw Gumbo log files.
            assertThat(logContainsText(dir, "LOREM-LOREM-LOREM-")).isFalse();

            // Inspect folds the recorded value back — rehydrated transparently.
            ExecutionState state = runtime.inspect(id);
            assertThat(state.status()).isEqualTo(Status.COMPLETED);

            // A strict replay re-runs the task with the effect substituted from the (rehydrated) log.
            ExecutionState replayed = runtime.replay(id, task);
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
        }
    }

    /**
     * Deploy-3: a caller-supplied store — standing in for S3/GCS — is one builder line on the
     * embedded path. Large payloads go to it instead of {@code dir/blobs}, it is closed with the
     * runtime, and a later runtime handed the same store reads them back.
     */
    @Test
    void aSuppliedBlobStoreIsOneBuilderLineAndClosesWithTheRuntime(@TempDir Path dir) throws Exception {
        String bigDocument = "IPSUM-".repeat(30_000);
        Task<Integer> task = ctx -> ctx.effect("fetch-document", () -> bigDocument).length();
        RemoteStore remote = new RemoteStore();

        ExecutionId id;
        try (CatalystRuntime runtime = Catalyst.builder(dir).blobStore(remote).build()) {
            var handle = runtime.execute(task);
            assertThat(handle.result()).isEqualTo(bigDocument.length());
            id = handle.id();
            assertThat(remote.puts).as("offloaded to the supplied store").isGreaterThanOrEqualTo(1);
            assertThat(Files.exists(dir.resolve("blobs"))).as("not to the default store").isFalse();
            assertThat(logContainsText(dir, "IPSUM-IPSUM-IPSUM-")).isFalse();
            assertThat(remote.closed).isFalse();
        }
        assertThat(remote.closed).as("closed with the runtime").isTrue();

        remote.closed = false;
        try (CatalystRuntime runtime = Catalyst.builder(dir).blobStore(remote).build()) {
            assertThat(runtime.replay(id, task).status()).isEqualTo(Status.COMPLETED);
        }
    }

    /** A remote store as far as Catalyst can tell: content-addressed, and it has a client to close. */
    private static final class RemoteStore implements com.cajunsystems.catalyst.events.BlobStore {
        private final com.cajunsystems.catalyst.events.BlobStore bytes =
                com.cajunsystems.catalyst.events.BlobStore.inMemory();
        int puts;
        boolean closed;

        public String put(byte[] content) { puts++; return bytes.put(content); }
        public byte[] get(String ref) { return bytes.get(ref); }
        public boolean has(String ref) { return bytes.has(ref); }
        @Override public void close() { closed = true; }
    }

    /** True if any file under {@code dir} that is not in the blob store inlines {@code needle}. */
    private static boolean logContainsText(Path dir, String needle) throws Exception {
        Path blobs = dir.resolve("blobs");
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                if (p.startsWith(blobs)) continue; // the payload legitimately lives here
                if (new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8).contains(needle)) {
                    return true;
                }
            }
        }
        return false;
    }
}
