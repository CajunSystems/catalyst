package com.cajunsystems.catalyst.engine;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * One typed entry in an execution's timeline, folded from an event. The timeline is a fold over the
 * log — there is no separate instrumentation layer (spec §2, §5).
 */
public record TimelineStep(long seq, Kind kind, String label, Instant at, long latencyMillis, JsonNode detail) {

    public TimelineStep {
        // Canonicalize an explicit JSON null to "no detail" so a step round-tripped through a
        // snapshot (where a null JsonNode field deserializes to NullNode) equals a freshly folded one.
        if (detail != null && detail.isNull()) detail = null;
    }

    public enum Kind {
        CREATED,
        STARTED,
        RESUMED,
        PAUSED,
        PROMPT,
        MODEL,
        TOOL,
        EFFECT,
        MEMORY_READ,
        MEMORY_WRITE,
        RETRY,
        BRANCHED,
        COMPLETED,
        FAILED
    }
}
