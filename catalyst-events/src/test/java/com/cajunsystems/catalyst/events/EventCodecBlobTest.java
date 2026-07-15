package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventCodecBlobTest {

    private final InMemoryBlobStore blobs = new InMemoryBlobStore();
    // A small threshold so the test doesn't need megabyte payloads.
    private final EventCodec codec = new EventCodec(EventJson.shared(), blobs, 1024);
    private final EventCodec plain = new EventCodec();

    private static JsonNode json(String text) {
        return com.fasterxml.jackson.databind.node.TextNode.valueOf(text);
    }

    @Test
    void offloadsLargePayloadFieldsAndRehydratesOnDecode() {
        String big = "A".repeat(5_000);
        CatalystEvent event = new CatalystEvent.ExecutionCompleted(Instant.parse("2026-01-01T00:00:00Z"), json(big));

        byte[] stored = codec.encode(event);

        // The event bytes no longer inline the payload — it lives in the blob store instead. The event
        // records which fields were offloaded in the reserved $catalystBlobs sidecar.
        assertThat(new String(stored, StandardCharsets.UTF_8)).doesNotContain(big).contains("$catalystBlobs");
        assertThat(stored.length).isLessThan(big.length());
        assertThat(blobs.has(resultRef(stored))).isTrue();

        // Decode rehydrates the payload transparently — the event is fully inlined again.
        CatalystEvent back = codec.decode(stored);
        assertThat(back).isInstanceOf(CatalystEvent.ExecutionCompleted.class);
        assertThat(((CatalystEvent.ExecutionCompleted) back).result().textValue()).isEqualTo(big);
        assertThat(back).isEqualTo(event);
    }

    @Test
    void aPayloadThatLooksLikeAReferenceIsNotMistakenForOne() {
        // A small payload containing the reserved marker key nested inside it must round-trip untouched:
        // the offload decision is driven by the event-level $catalystBlobs sidecar, not payload shape.
        ObjectNode lookalike = EventJson.newMapper().createObjectNode();
        lookalike.put("$catalystBlobs", "sha256:deadbeef");
        lookalike.put("$catalystBlob", "sha256:cafe");
        CatalystEvent event = new CatalystEvent.EffectRecorded(
                Instant.parse("2026-01-01T00:00:00Z"), "label", lookalike);

        assertThat(codec.decode(codec.encode(event))).isEqualTo(event);
    }

    @Test
    void rejectsNonPositiveThreshold() {
        assertThatThrownBy(() -> new EventCodec(EventJson.shared(), blobs, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EventCodec(EventJson.shared(), blobs, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void smallEventsAreByteIdenticalToTheNoBlobEncoding() {
        CatalystEvent event = new CatalystEvent.EffectRecorded(
                Instant.parse("2026-01-01T00:00:00Z"), "label", json("small"));
        assertThat(codec.encode(event)).isEqualTo(plain.encode(event));
        assertThat(blobs.has("sha256:whatever")).isFalse(); // nothing offloaded
    }

    @Test
    void aBlobBackedCodecStillReadsLegacyInlinedEvents() {
        // An event written by a codec with NO blob store must still decode via a blob-backed codec.
        CatalystEvent event = new CatalystEvent.ExecutionCompleted(
                Instant.parse("2026-01-01T00:00:00Z"), json("B".repeat(5_000)));
        byte[] inlined = plain.encode(event);
        assertThat(codec.decode(inlined)).isEqualTo(event);
    }

    private String resultRef(byte[] stored) {
        try {
            JsonNode tree = EventJson.shared().readTree(stored);
            assertThat(tree.get("$catalystBlobs").toString()).contains("result"); // offloaded field listed
            return tree.get("result").textValue(); // the field value is now the string ref
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
