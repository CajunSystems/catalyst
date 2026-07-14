package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.events.EventJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * (De)serializes opaque payload values embedded in events — effect results and tool outputs — where
 * the concrete type is not statically known at the substitution site. The concrete class name is
 * recorded alongside the value in a small typed envelope, so {@link #fromTree} reconstructs the exact
 * type on replay. This keeps payloads human-readable in the log and avoids the pitfalls of Jackson
 * default typing when round-tripping through {@link JsonNode}.
 *
 * <p>Leaf values are records, enums, primitives, strings, or a small set of value types. Generic
 * collections ({@link List}, {@link Set}, {@link Map}) and arrays are encoded <em>structurally</em>:
 * the envelope carries each element in its own typed envelope, recursively, so element types survive
 * the round-trip (a {@code List<Point>} comes back as a list of {@code Point}, not of maps). The
 * allowlist that guards against loading arbitrary classes from a tampered log is enforced at every
 * leaf, however deeply nested. Collections are reconstructed as {@link ArrayList}/{@link LinkedHashSet}/
 * {@link LinkedHashMap} (equal by content to the originals; concrete collection classes are not
 * preserved).
 */
public final class PayloadCodec {

    /** A value paired with its concrete class name so it can be reconstructed without a static type. */
    record Typed(String type, JsonNode value) {}

    // Sentinel "type" tags for structural (non-leaf) envelopes. The leading '[' cannot begin a Java
    // binary class name for a plain type, so these never collide with a real leaf type name.
    private static final String LIST = "[]list";
    private static final String SET = "[]set";
    private static final String MAP = "[]map";
    private static final String NULL = "[]null";
    private static final String ARRAY_PREFIX = "[]array:"; // ARRAY_PREFIX + componentType

    /** Non-record/enum value types that are safe to reconstruct from an untrusted log. */
    private static final Set<Class<?>> SAFE_TYPES = Set.of(
            String.class, Boolean.class, Character.class,
            Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
            BigInteger.class, BigDecimal.class, Instant.class, Duration.class);

    private static final Map<String, Class<?>> PRIMITIVES = Map.of(
            "boolean", boolean.class, "byte", byte.class, "char", char.class, "short", short.class,
            "int", int.class, "long", long.class, "float", float.class, "double", double.class);

    private final ObjectMapper mapper;

    public PayloadCodec() {
        this.mapper = EventJson.newMapper();
    }

    public JsonNode toTree(Object value) {
        if (value == null) return NullNode.getInstance();
        return encode(value);
    }

    /** Reconstructs a value whose concrete type was recorded alongside it. */
    public Object fromTree(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            return decode(node);
        } catch (IllegalStateException e) {
            throw e; // allowlist rejection — propagate unwrapped
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown payload type in log: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize payload value", e);
        }
    }

    // ── Encoding ─────────────────────────────────────────────────────────────

    private JsonNode encode(Object value) {
        if (value == null) return mapper.valueToTree(new Typed(NULL, NullNode.getInstance()));
        if (value instanceof List<?> list) return structural(LIST, encodeEach(list));
        if (value instanceof Set<?> set) return structural(SET, encodeEach(set));
        if (value instanceof Map<?, ?> map) return structural(MAP, encodeEntries(map));
        if (value.getClass().isArray()) {
            String component = value.getClass().getComponentType().getName();
            return structural(ARRAY_PREFIX + component, encodeArray(value));
        }
        // Leaf: record / enum / primitive-wrapper / value type. Byte-identical to the pre-collections
        // envelope so existing logs and new logs interoperate.
        return mapper.valueToTree(new Typed(value.getClass().getName(), mapper.valueToTree(value)));
    }

    private JsonNode structural(String tag, ArrayNode elements) {
        return mapper.valueToTree(new Typed(tag, elements));
    }

    private ArrayNode encodeEach(Iterable<?> values) {
        ArrayNode out = mapper.createArrayNode();
        for (Object element : values) out.add(encode(element));
        return out;
    }

    private ArrayNode encodeArray(Object array) {
        ArrayNode out = mapper.createArrayNode();
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) out.add(encode(Array.get(array, i)));
        return out;
    }

    private ArrayNode encodeEntries(Map<?, ?> map) {
        ArrayNode out = mapper.createArrayNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            ArrayNode pair = mapper.createArrayNode();
            pair.add(encode(entry.getKey()));
            pair.add(encode(entry.getValue()));
            out.add(pair);
        }
        return out;
    }

    // ── Decoding ─────────────────────────────────────────────────────────────

    private Object decode(JsonNode node) throws Exception {
        Typed typed = mapper.treeToValue(node, Typed.class);
        String type = typed.type();
        return switch (type) {
            case NULL -> null;
            case LIST -> decodeInto(typed.value(), new ArrayList<>());
            case SET -> decodeInto(typed.value(), new LinkedHashSet<>());
            case MAP -> decodeMap(typed.value());
            default -> type.startsWith(ARRAY_PREFIX)
                    ? decodeArray(type.substring(ARRAY_PREFIX.length()), typed.value())
                    : decodeLeaf(type, typed.value());
        };
    }

    private <C extends java.util.Collection<Object>> C decodeInto(JsonNode elements, C into) throws Exception {
        for (JsonNode element : elements) into.add(decode(element));
        return into;
    }

    private Map<Object, Object> decodeMap(JsonNode entries) throws Exception {
        Map<Object, Object> out = new LinkedHashMap<>();
        for (JsonNode pair : entries) out.put(decode(pair.get(0)), decode(pair.get(1)));
        return out;
    }

    private Object decodeArray(String componentType, JsonNode elements) throws Exception {
        Class<?> component = resolveArrayComponent(componentType);
        Object array = Array.newInstance(component, elements.size());
        for (int i = 0; i < elements.size(); i++) {
            Array.set(array, i, decode(elements.get(i))); // Array.set unboxes for primitive components
        }
        return array;
    }

    private Object decodeLeaf(String type, JsonNode value) throws Exception {
        return mapper.treeToValue(value, resolveAllowed(type));
    }

    // ── Type resolution (allowlist) ──────────────────────────────────────────

    /**
     * Resolves a recorded type name, refusing anything outside the allowlist. Because the durable log
     * is a file on disk, a tampered {@code type} field must not be able to drive the runtime to load
     * and deserialize arbitrary classpath classes (a classic Jackson gadget vector). The class is
     * loaded without initialization, using this module's loader, and accepted only if it is a record,
     * an enum, or a known value type — the leaf shapes payloads actually store.
     */
    private static Class<?> resolveAllowed(String typeName) throws ClassNotFoundException {
        Class<?> type = Class.forName(typeName, false, PayloadCodec.class.getClassLoader());
        if (type.isRecord() || type.isEnum() || SAFE_TYPES.contains(type)) {
            return type;
        }
        throw new IllegalStateException("Refusing to deserialize non-allowlisted payload type: " + typeName
                + ". Payloads (tool outputs, effect values, memory values) must be a record, an enum, a"
                + " value type, or a collection/array of those.");
    }

    /**
     * Resolves and validates an array's component type. The component type is only used to
     * <em>allocate</em> the array — each element is decoded through its own envelope and allowlisted
     * individually — so besides primitives and allowlisted leaf types we also accept {@code Object},
     * interfaces, and abstract types (the declared component of a heterogeneous array such as
     * {@code Object[]} or {@code Number[]}). Concrete non-allowlisted classes are still refused, so a
     * tampered log cannot name an arbitrary gadget class to load.
     */
    private static Class<?> resolveArrayComponent(String componentType) throws ClassNotFoundException {
        Class<?> primitive = PRIMITIVES.get(componentType);
        if (primitive != null) return primitive;
        Class<?> type = Class.forName(componentType, false, PayloadCodec.class.getClassLoader());
        if (isAllowedArrayComponent(type)) {
            return type;
        }
        throw new IllegalStateException("Refusing to deserialize non-allowlisted array component type: "
                + componentType);
    }

    private static boolean isAllowedArrayComponent(Class<?> type) {
        if (type.isArray()) return isAllowedArrayComponent(type.getComponentType());
        if (type.isPrimitive() || type == Object.class) return true;
        // A heterogeneous array's declared component is Object/an interface/an abstract type; the array
        // is only allocated with it, while its concrete elements are allowlisted on decode.
        if (type.isInterface() || java.lang.reflect.Modifier.isAbstract(type.getModifiers())) return true;
        return type.isRecord() || type.isEnum() || SAFE_TYPES.contains(type);
    }
}
