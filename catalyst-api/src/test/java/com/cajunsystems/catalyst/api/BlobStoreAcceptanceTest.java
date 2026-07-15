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
