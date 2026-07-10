package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Factory for the canonical Catalyst {@link ObjectMapper}. A single, consistently configured mapper
 * is used everywhere Catalyst serializes — events for the log and opaque payloads inside them — so
 * round-trips are stable and deterministic (fields in a fixed order, {@link java.time.Instant} as
 * ISO-8601 rather than numeric timestamps).
 */
public final class EventJson {

    private EventJson() {}

    /** Creates a fresh, fully configured mapper. Callers may share a single instance; it is thread-safe. */
    public static ObjectMapper newMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /** A lazily-created shared mapper for convenience. */
    private static final class Holder {
        static final ObjectMapper SHARED = newMapper();
    }

    /** Returns a shared, thread-safe mapper instance. */
    public static ObjectMapper shared() {
        return Holder.SHARED;
    }
}
