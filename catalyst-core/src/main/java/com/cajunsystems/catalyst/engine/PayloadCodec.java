package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * (De)serializes opaque payload values embedded in events — effect results and tool outputs — where
 * the concrete type is not statically known at the substitution site. The concrete class name is
 * recorded alongside the value in a small typed envelope, so {@link #fromTree} reconstructs the exact
 * type on replay. This keeps payloads human-readable in the log and avoids the pitfalls of Jackson
 * default typing when round-tripping through {@link JsonNode}.
 *
 * <p>M0 scope: values are records, primitives, strings, or simple POJOs. Preserving element types of
 * generic collections is a later refinement.
 */
public final class PayloadCodec {

    /** A value paired with its concrete class name so it can be reconstructed without a static type. */
    record Typed(String type, JsonNode value) {}

    private final ObjectMapper mapper;

    public PayloadCodec() {
        this.mapper = EventJson.newMapper();
    }

    public JsonNode toTree(Object value) {
        if (value == null) return NullNode.getInstance();
        return mapper.valueToTree(new Typed(value.getClass().getName(), mapper.valueToTree(value)));
    }

    /** Reconstructs a value whose concrete type was recorded alongside it. */
    public Object fromTree(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            Typed typed = mapper.treeToValue(node, Typed.class);
            Class<?> type = Class.forName(typed.type());
            return mapper.treeToValue(typed.value(), type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize payload value", e);
        }
    }
}
