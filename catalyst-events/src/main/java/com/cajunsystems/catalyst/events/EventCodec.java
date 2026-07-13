package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UncheckedIOException;

/**
 * Binary (de)serialization for {@link CatalystEvent}s. This is the bridge between the event schema
 * and any byte-oriented log (e.g. the Gumbo shared log's {@code LogSerializer}). Encoding is UTF-8
 * JSON with a {@code "@type"} discriminator so the sealed hierarchy round-trips exactly.
 */
public final class EventCodec {

    private final ObjectMapper mapper;

    public EventCodec() {
        this(EventJson.shared());
    }

    public EventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public byte[] encode(CatalystEvent event) {
        try {
            return mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode event: " + event, e);
        }
    }

    public CatalystEvent decode(byte[] data) {
        try {
            return mapper.readValue(data, CatalystEvent.class);
        } catch (Exception e) {
            throw new UncheckedIOException("Failed to decode event", asIO(e));
        }
    }

    private static java.io.IOException asIO(Exception e) {
        return (e instanceof java.io.IOException io) ? io : new java.io.IOException(e);
    }

    /** The mapper backing this codec, for callers that need to (de)serialize opaque payloads. */
    public ObjectMapper mapper() {
        return mapper;
    }
}
