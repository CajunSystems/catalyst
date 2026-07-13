package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
import com.cajunsystems.gumbo.serialization.LogSerializer;

/**
 * Bridges Catalyst's {@link EventCodec} to Gumbo's {@link LogSerializer} so a
 * {@code TypedLogView<CatalystEvent>} can (de)serialize events transparently. Thread-safe: the
 * underlying Jackson mapper is thread-safe.
 */
final class EventLogSerializer implements LogSerializer<CatalystEvent> {

    private final EventCodec codec;

    EventLogSerializer(EventCodec codec) {
        this.codec = codec;
    }

    @Override
    public byte[] serialize(CatalystEvent value) {
        return codec.encode(value);
    }

    @Override
    public CatalystEvent deserialize(byte[] data) {
        return codec.decode(data);
    }
}
