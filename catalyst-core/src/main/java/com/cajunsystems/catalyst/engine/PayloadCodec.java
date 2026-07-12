package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

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

    /** Non-record/enum value types that are safe to reconstruct from an untrusted log. */
    private static final Set<Class<?>> SAFE_TYPES = Set.of(
            String.class, Boolean.class, Character.class,
            Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
            BigInteger.class, BigDecimal.class, Instant.class, Duration.class);

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
            Class<?> type = resolveAllowed(typed.type());
            return mapper.treeToValue(typed.value(), type);
        } catch (IllegalStateException e) {
            throw e; // allowlist rejection — propagate unwrapped
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown payload type in log: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize payload value", e);
        }
    }

    /**
     * Resolves a recorded type name, refusing anything outside the allowlist. Because the durable log
     * is a file on disk, a tampered {@code type} field must not be able to drive the runtime to load
     * and deserialize arbitrary classpath classes (a classic Jackson gadget vector). The class is
     * loaded without initialization, using this module's loader, and accepted only if it is a record,
     * an enum, or a known value type — the value shapes M0 actually stores.
     */
    private static Class<?> resolveAllowed(String typeName) throws ClassNotFoundException {
        Class<?> type = Class.forName(typeName, false, PayloadCodec.class.getClassLoader());
        if (type.isRecord() || type.isEnum() || SAFE_TYPES.contains(type)) {
            return type;
        }
        throw new IllegalStateException("Refusing to deserialize non-allowlisted payload type: " + typeName
                + ". M0 payloads (tool outputs, effect values, memory values) must be a record, an enum,"
                + " or a value type; collections/generics (e.g. List<T>, Map<K,V>) are not yet supported"
                + " — wrap them in a record.");
    }
}
