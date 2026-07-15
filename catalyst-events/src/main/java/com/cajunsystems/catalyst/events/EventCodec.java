package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Binary (de)serialization for {@link CatalystEvent}s. This is the bridge between the event schema
 * and any byte-oriented log (e.g. the Gumbo shared log's {@code LogSerializer}). Encoding is UTF-8
 * JSON with a {@code "@type"} discriminator so the sealed hierarchy round-trips exactly.
 *
 * <p><strong>Blob offloading (spec §8).</strong> When constructed with a {@link BlobStore}, a large
 * payload field (a completion, tool result, or document that would otherwise be inlined) is lifted out
 * to the content-addressed store and its field value is replaced by a string reference; {@link #decode}
 * rehydrates it transparently. The threshold is per top-level field, so only genuinely large payloads
 * are externalized and small events stay byte-identical to the no-blob encoding. The rest of the system
 * only ever sees fully-inlined {@link CatalystEvent}s — offloading lives entirely at this seam.
 *
 * <p>Which fields were offloaded is recorded in a reserved <em>event-level</em> field
 * {@code "$catalystBlobs"} (an array of field names), not by sniffing payload shape — so a user payload
 * that happens to look like a reference can never be mistaken for one. The key lives in the event's own
 * namespace (a task payload is always the <em>value</em> of a field, nested a level below), so it never
 * collides with user data; it is reserved and must not be emitted as an event field name.
 */
public final class EventCodec {

    /** Default size above which a top-level payload field is offloaded to the blob store: 64 KiB. */
    public static final int DEFAULT_BLOB_THRESHOLD_BYTES = 64 * 1024;

    /** Reserved event-level field listing the names of offloaded fields. Never a real event field. */
    private static final String BLOB_FIELDS_KEY = "$catalystBlobs";
    /** The discriminator field, never offloaded. */
    private static final String TYPE_KEY = "@type";

    private final ObjectMapper mapper;
    private final BlobStore blobStore;   // null → inline everything (legacy behavior)
    private final int blobThresholdBytes;

    public EventCodec() {
        this(EventJson.shared());
    }

    public EventCodec(ObjectMapper mapper) {
        this(mapper, null, DEFAULT_BLOB_THRESHOLD_BYTES);
    }

    /** A codec that offloads payload fields larger than {@code blobThresholdBytes} to {@code blobStore}. */
    public EventCodec(ObjectMapper mapper, BlobStore blobStore, int blobThresholdBytes) {
        if (blobStore != null && blobThresholdBytes <= 0) {
            throw new IllegalArgumentException("blobThresholdBytes must be positive, got: " + blobThresholdBytes);
        }
        this.mapper = mapper;
        this.blobStore = blobStore;
        this.blobThresholdBytes = blobThresholdBytes;
    }

    public byte[] encode(CatalystEvent event) {
        try {
            byte[] full = mapper.writeValueAsBytes(event);
            // No blob store, or the whole event is small: keep the direct (byte-identical) encoding.
            // This fast path is the common case and avoids the tree round-trip entirely.
            if (blobStore == null || full.length < blobThresholdBytes) {
                return full;
            }
            // Large event: reuse the bytes we just produced (parse, don't re-serialize the event).
            ObjectNode tree = (ObjectNode) mapper.readTree(full);
            List<String> offloaded = offloadLargeFields(tree);
            if (offloaded.isEmpty()) {
                return full; // large event, but no single field crossed the threshold — leave it inline
            }
            ArrayNode names = tree.putArray(BLOB_FIELDS_KEY);
            offloaded.forEach(names::add);
            return mapper.writeValueAsBytes(tree);
        } catch (java.io.IOException e) {
            throw new UncheckedIOException("Failed to encode event: " + event, e);
        }
    }

    public CatalystEvent decode(byte[] data) {
        try {
            if (blobStore == null) {
                return mapper.readValue(data, CatalystEvent.class);
            }
            JsonNode tree = mapper.readTree(data);
            if (tree instanceof ObjectNode obj && obj.has(BLOB_FIELDS_KEY)) {
                inlineBlobRefs(obj);
            }
            return mapper.treeToValue(tree, CatalystEvent.class);
        } catch (Exception e) {
            throw new UncheckedIOException("Failed to decode event", asIO(e));
        }
    }

    /**
     * Replaces each top-level field whose serialized size meets the threshold with its blob reference
     * (a string) and returns the names of the fields that were offloaded.
     */
    private List<String> offloadLargeFields(ObjectNode tree) throws JsonProcessingException {
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        tree.fields().forEachRemaining(fields::add);
        List<String> offloaded = new ArrayList<>();
        for (Map.Entry<String, JsonNode> field : fields) {
            if (field.getKey().equals(TYPE_KEY)) continue;
            byte[] bytes = mapper.writeValueAsBytes(field.getValue());
            if (bytes.length >= blobThresholdBytes) {
                tree.put(field.getKey(), blobStore.put(bytes)); // field value becomes the string ref
                offloaded.add(field.getKey());
            }
        }
        return offloaded;
    }

    /** Resolves the offloaded fields listed in {@code $catalystBlobs} back to their stored payloads. */
    private void inlineBlobRefs(ObjectNode obj) throws java.io.IOException {
        JsonNode names = obj.remove(BLOB_FIELDS_KEY);
        if (names == null || !names.isArray()) return;
        for (JsonNode nameNode : names) {
            String field = nameNode.textValue();
            JsonNode ref = field == null ? null : obj.get(field);
            if (ref != null && ref.isTextual()) {
                obj.set(field, mapper.readTree(blobStore.get(ref.textValue())));
            }
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
