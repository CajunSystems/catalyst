package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Factories for the serializers the runtime can be configured with (spec §4). */
public final class Serializers {

    private Serializers() {}

    /** The default Jackson-based event serializer. */
    public static ObjectMapper jackson() {
        return EventJson.newMapper();
    }
}
