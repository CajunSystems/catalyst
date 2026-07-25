package com.cajunsystems.catalyst.agent;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * A family of JDK nondeterminism the agent can rewrite into recorded boundaries. Selectable so a task
 * can capture only what it actually treats as data — a task that reads {@code System.nanoTime()} purely
 * to time itself, for instance, gains nothing from recording it and pays an event per call.
 */
public enum CaptureSource {

    /** Wall-clock and monotonic time: {@code Instant.now()}, {@code System.currentTimeMillis()}, {@code System.nanoTime()}, and the no-argument {@code java.time} {@code now()} factories. */
    TIME,

    /** {@code UUID.randomUUID()}. */
    IDENTITY,

    /** {@code Math.random()} and draws from {@link java.util.Random} and its subtypes. */
    RANDOM;

    /** Parses a comma-separated list; {@code "all"} (or blank) selects every source. */
    static java.util.Set<CaptureSource> parse(String value) {
        if (value == null || value.isBlank() || value.trim().equalsIgnoreCase("all")) {
            return java.util.EnumSet.allOf(CaptureSource.class);
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(CaptureSource::of)
                .collect(Collectors.toCollection(() -> java.util.EnumSet.noneOf(CaptureSource.class)));
    }

    private static CaptureSource of(String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown capture source '" + name + "'. Known sources: "
                    + Arrays.stream(values()).map(s -> s.name().toLowerCase(Locale.ROOT))
                            .collect(Collectors.joining(", ")));
        }
    }
}
