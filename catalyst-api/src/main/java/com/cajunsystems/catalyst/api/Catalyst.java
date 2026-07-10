package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;

/**
 * The thin entry-point facade (spec §4, §9). {@link #embedded(Path)} gives you a durable,
 * Gumbo-backed runtime in one line; {@link #builder()} exposes the full configuration surface
 * (log, model, serializer, clock, replay mode).
 */
public final class Catalyst {

    private Catalyst() {}

    /** A durable runtime backed by an embedded Gumbo file log rooted at {@code logDir}. */
    public static CatalystRuntime embedded(Path logDir) {
        return CatalystRuntime.builder()
                .log(GumboEventLog.at(logDir))
                .build();
    }

    /** A durable, Gumbo-backed runtime with a model configured. */
    public static CatalystRuntime embedded(Path logDir, Model model) {
        return CatalystRuntime.builder()
                .log(GumboEventLog.at(logDir))
                .model(model)
                .build();
    }

    /** The full builder: {@code Catalyst.builder().log(...).model(...).serializer(...).build()}. */
    public static CatalystRuntime.Builder builder() {
        return CatalystRuntime.builder();
    }
}
