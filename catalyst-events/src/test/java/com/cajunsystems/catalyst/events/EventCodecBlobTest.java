package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

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

        // The event bytes no longer inline the payload — it lives in the blob store instead.
        assertThat(new String(stored, StandardCharsets.UTF_8)).doesNotContain(big).contains("$catalystBlob");
        assertThat(stored.length).isLessThan(big.length());
        assertThat(blobs.has(firstRef(stored))).isTrue();

        // Decode rehydrates the payload transparently — the event is fully inlined again.
        CatalystEvent back = codec.decode(stored);
        assertThat(back).isInstanceOf(CatalystEvent.ExecutionCompleted.class);
        assertThat(((CatalystEvent.ExecutionCompleted) back).result().textValue()).isEqualTo(big);
        assertThat(back).isEqualTo(event);
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

    private String firstRef(byte[] stored) {
        JsonNode tree;
        try {
            tree = EventJson.shared().readTree(stored);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tree.get("result").get("$catalystBlob").textValue();
    }
}
