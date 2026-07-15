package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Catalyst's schema-evolution policy (spec §13.4): additive changes ride the tolerant reader, while
 * structural changes (renames, type changes) are migrated by {@link EventUpcaster}s on decode.
 */
class SchemaEvolutionTest {

    private final ObjectMapper mapper = EventJson.shared();

    private byte[] raw(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    // ── Tolerant reader: additive changes need no code ───────────────────────

    @Test
    void ignoresUnknownFieldsAWriterAddedLater() {
        // An old reader meeting a newer log that added a field must not fail.
        byte[] withExtra = raw("{\"@type\":\"ExecutionStarted\",\"at\":\"2026-01-01T00:00:00Z\","
                + "\"attempt\":1,\"nodeId\":\"node-0\",\"someFutureField\":\"ignored\"}");
        CatalystEvent event = new EventCodec().decode(withExtra);
        assertThat(event).isInstanceOf(CatalystEvent.ExecutionStarted.class);
        assertThat(((CatalystEvent.ExecutionStarted) event).attempt()).isEqualTo(1);
    }

    @Test
    void defaultsAFieldAnOlderWriterOmitted() {
        // A newer reader meeting an old log that predates a field: the missing field defaults.
        byte[] old = raw("{\"@type\":\"ExecutionResumed\",\"at\":\"2026-01-01T00:00:00Z\"}"); // no attempt
        CatalystEvent event = new EventCodec().decode(old);
        assertThat(event).isInstanceOf(CatalystEvent.ExecutionResumed.class);
        assertThat(((CatalystEvent.ExecutionResumed) event).attempt()).isEqualTo(0); // primitive default
    }

    // ── Upcasters: structural changes ────────────────────────────────────────

    @Test
    void upcasterRenamesAField() {
        // Pretend ExecutionFailed once stored its error under "message"; migrate it to "error".
        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.renameField("ExecutionFailed", "message", "error"))
                .build();
        byte[] legacy = raw("{\"@type\":\"ExecutionFailed\",\"at\":\"2026-01-01T00:00:00Z\","
                + "\"message\":\"boom\",\"failedSeq\":3}");
        CatalystEvent.ExecutionFailed event = (CatalystEvent.ExecutionFailed) codec.decode(legacy);
        assertThat(event.error()).isEqualTo("boom");
        assertThat(event.failedSeq()).isEqualTo(3);
    }

    @Test
    void upcasterRenamesAnEventType() {
        // Pretend "TaskCompleted" was renamed to "ExecutionCompleted".
        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.renameType("TaskCompleted", "ExecutionCompleted"))
                .build();
        byte[] legacy = raw("{\"@type\":\"TaskCompleted\",\"at\":\"2026-01-01T00:00:00Z\",\"result\":42}");
        assertThat(codec.decode(legacy)).isInstanceOf(CatalystEvent.ExecutionCompleted.class);
    }

    @Test
    void upcasterFillsInADefaultForAMissingField() {
        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.defaultField("ExecutionStarted", "attempt", IntNode.valueOf(1)))
                .build();
        byte[] legacy = raw("{\"@type\":\"ExecutionStarted\",\"at\":\"2026-01-01T00:00:00Z\",\"nodeId\":\"n\"}");
        assertThat(((CatalystEvent.ExecutionStarted) codec.decode(legacy)).attempt()).isEqualTo(1);
    }

    @Test
    void upcastersAreIdempotentOnCurrentShape() {
        // A current-shape event handed to a rename upcaster must round-trip unchanged.
        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.renameField("ExecutionFailed", "message", "error"))
                .build();
        CatalystEvent.ExecutionFailed current =
                new CatalystEvent.ExecutionFailed(Instant.parse("2026-01-01T00:00:00Z"), "already-current", 1);
        assertThat(codec.decode(new EventCodec().encode(current))).isEqualTo(current);
    }

    @Test
    void chainedUpcastersComposeInOrder() {
        // v1 → v2 (type rename) then v2 → v3 (field rename): both apply on a single decode.
        EventCodec codec = EventCodec.builder()
                .upcaster(EventUpcaster.renameType("OldFail", "ExecutionFailed"))
                .upcaster(EventUpcaster.renameField("ExecutionFailed", "reason", "error"))
                .build();
        byte[] veryOld = raw("{\"@type\":\"OldFail\",\"at\":\"2026-01-01T00:00:00Z\","
                + "\"reason\":\"legacy\",\"failedSeq\":0}");
        assertThat(((CatalystEvent.ExecutionFailed) codec.decode(veryOld)).error()).isEqualTo("legacy");
    }
}
