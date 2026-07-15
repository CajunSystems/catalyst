package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * to the content-addressed store and replaced in the event by a small reference; {@link #decode}
 * rehydrates it transparently. The threshold is per top-level field, so only genuinely large payloads
 * are externalized and small events stay byte-identical to the no-blob encoding. The rest of the system
 * only ever sees fully-inlined {@link CatalystEvent}s — offloading lives entirely at this seam.
 */
public final class EventCodec {

    /** Default size above which a top-level payload field is offloaded to the blob store: 64 KiB. */
    public static final int DEFAULT_BLOB_THRESHOLD_BYTES = 64 * 1024;

    /** Marker key of a blob-reference node: {@code {"$catalystBlob":"sha256:..."}}. */
    private static final String BLOB_REF_KEY = "$catalystBlob";
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
        this.mapper = mapper;
        this.blobStore = blobStore;
        this.blobThresholdBytes = blobThresholdBytes;
    }

    public byte[] encode(CatalystEvent event) {
        try {
            byte[] full = mapper.writeValueAsBytes(event);
            // No blob store, or the whole event is small: keep the direct (byte-identical) encoding.
            if (blobStore == null || full.length < blobThresholdBytes) {
                return full;
            }
            ObjectNode tree = (ObjectNode) mapper.valueToTree(event);
            if (offloadLargeFields(tree)) {
                return mapper.writeValueAsBytes(tree);
            }
            return full; // large event, but no single field crossed the threshold — leave it inline
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode event: " + event, e);
        }
    }

    public CatalystEvent decode(byte[] data) {
        try {
            if (blobStore == null) {
                return mapper.readValue(data, CatalystEvent.class);
            }
            JsonNode tree = mapper.readTree(data);
            inlineBlobRefs(tree);
            return mapper.treeToValue(tree, CatalystEvent.class);
        } catch (Exception e) {
            throw new UncheckedIOException("Failed to decode event", asIO(e));
        }
    }

    /** Replaces each top-level field whose serialized size meets the threshold with a blob reference. */
    private boolean offloadLargeFields(ObjectNode tree) throws JsonProcessingException {
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        tree.fields().forEachRemaining(fields::add);
        boolean offloaded = false;
        for (Map.Entry<String, JsonNode> field : fields) {
            if (field.getKey().equals(TYPE_KEY)) continue;
            JsonNode value = field.getValue();
            byte[] bytes = mapper.writeValueAsBytes(value);
            if (bytes.length >= blobThresholdBytes) {
                String ref = blobStore.put(bytes);
                ObjectNode refNode = mapper.createObjectNode();
                refNode.put(BLOB_REF_KEY, ref);
                tree.set(field.getKey(), refNode);
                offloaded = true;
            }
        }
        return offloaded;
    }

    /** Resolves each top-level blob reference back to its stored payload. */
    private void inlineBlobRefs(JsonNode tree) throws java.io.IOException {
        if (!(tree instanceof ObjectNode obj)) return;
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        obj.fields().forEachRemaining(fields::add);
        for (Map.Entry<String, JsonNode> field : fields) {
            JsonNode value = field.getValue();
            if (isBlobRef(value)) {
                String ref = value.get(BLOB_REF_KEY).textValue();
                obj.set(field.getKey(), mapper.readTree(blobStore.get(ref)));
            }
        }
    }

    private static boolean isBlobRef(JsonNode node) {
        return node != null && node.isObject() && node.size() == 1
                && node.has(BLOB_REF_KEY) && node.get(BLOB_REF_KEY).isTextual();
    }

    private static java.io.IOException asIO(Exception e) {
        return (e instanceof java.io.IOException io) ? io : new java.io.IOException(e);
    }

    /** The mapper backing this codec, for callers that need to (de)serialize opaque payloads. */
    public ObjectMapper mapper() {
        return mapper;
    }
}
