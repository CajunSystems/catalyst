package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;

/**
 * The thin entry-point facade (spec §4, §9). {@link #embedded(Path)} gives you a durable,
 * Gumbo-backed runtime in one line; {@link #builder(Path)} is the full configuration surface over
 * that same embedded log, and {@link #builder()} the same surface for a log you construct yourself.
 */
public final class Catalyst {

    private Catalyst() {}

    /** A durable runtime backed by an embedded Gumbo file log rooted at {@code logDir}. */
    public static CatalystRuntime embedded(Path logDir) {
        return builder(logDir).build();
    }

    /** A durable, Gumbo-backed runtime with a model configured. */
    public static CatalystRuntime embedded(Path logDir, Model model) {
        return builder(logDir).model(model).build();
    }

    /**
     * The full builder over an embedded Gumbo file log at {@code logDir}, which {@code build()}
     * constructs — so options bound in at construction apply. A remote blob store is one line:
     * {@code Catalyst.builder(dir).blobStore(s3Store).build()}; without one, large payloads go to
     * {@code logDir/blobs}. The store is closed with the runtime.
     */
    public static CatalystRuntime.Builder builder(Path logDir) {
        return CatalystRuntime.builder().logFactory((blobStore, thresholdBytes) -> blobStore == null
                ? GumboEventLog.at(logDir)
                : GumboEventLog.at(logDir, blobStore, thresholdBytes));
    }

    /** The full builder: {@code Catalyst.builder().log(...).model(...).serializer(...).build()}. */
    public static CatalystRuntime.Builder builder() {
        return CatalystRuntime.builder();
    }
}
