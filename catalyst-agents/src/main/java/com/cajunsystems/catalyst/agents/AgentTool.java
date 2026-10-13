package com.cajunsystems.catalyst.agents;

import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.model.ToolSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

/**
 * A {@link Tool} as an {@link Agent} offers it to the model: the tool, a description the model reads,
 * and a JSON input schema derived from the tool's input type.
 *
 * <p>The input type is a record whose components are strings, numbers, booleans or enums (a flat
 * object, what tool-calling models and the LangChain4j adapter handle reliably), or a single scalar
 * of those kinds, which is offered as {@code {"value": ...}}. Anything richer is refused when the
 * agent is built, rather than half-working mid-conversation.
 */
public final class AgentTool<I, O> {

    static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private final Tool<I, O> tool;
    private final ToolSpec spec;
    private final boolean wrapped;
    /** The input is the model's arguments object as-is (a tool with its own schema, e.g. from MCP). */
    private final boolean raw;

    private AgentTool(Tool<I, O> tool, String description) {
        this.tool = Objects.requireNonNull(tool, "tool");
        Class<I> type = Objects.requireNonNull(tool.inputType(), "tool " + tool.name() + " has no input type");
        this.wrapped = !type.isRecord();
        this.raw = false;
        this.spec = new ToolSpec(tool.name(), description, schemaFor(tool.name(), type).toString());
    }

    private AgentTool(Tool<I, O> tool, String description, String inputSchemaJson) {
        this.tool = Objects.requireNonNull(tool, "tool");
        if (tool.inputType() != JsonNode.class) {
            throw new IllegalArgumentException("Tool '" + tool.name() + "': an explicit schema is for tools that take the"
                    + " arguments as JSON (inputType JsonNode); this one takes " + tool.inputType().getName());
        }
        this.wrapped = false;
        this.raw = true;
        this.spec = new ToolSpec(tool.name(), description, canonicalSchema(tool.name(), inputSchemaJson));
    }

    /** {@code tool}, described to the model as {@code description}. */
    public static <I, O> AgentTool<I, O> of(Tool<I, O> tool, String description) {
        return new AgentTool<>(tool, description);
    }

    /**
     * A tool that takes the model's arguments object as JSON and brings its own JSON schema, such as
     * one from an MCP server. The arguments are passed through as given; the tool (or the server
     * behind it) validates them, and a failure goes back to the model like any tool failure. The
     * schema is part of every request's hash, so it is written in canonical form (keys sorted), and
     * the same schema always hashes the same.
     */
    public static <O> AgentTool<JsonNode, O> withSchema(Tool<JsonNode, O> tool, String description, String inputSchemaJson) {
        return new AgentTool<>(tool, description, inputSchemaJson);
    }

    private static String canonicalSchema(String toolName, String schemaJson) {
        try {
            JsonNode schema = JSON.readTree(Objects.requireNonNull(schemaJson, "inputSchemaJson"));
            if (!schema.isObject()) throw new IllegalArgumentException("not a JSON object");
            Object plain = JSON.treeToValue(schema, Object.class); // maps, so ORDER_MAP_ENTRIES_BY_KEYS applies
            return JSON.writeValueAsString(plain);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Tool '" + toolName + "': input schema is not a JSON object schema: "
                    + getOriginalMessageOf(e), e);
        }
    }

    public Tool<I, O> tool() {
        return tool;
    }

    /** What the model is told about this tool; part of every request, so part of its hash. */
    public ToolSpec spec() {
        return spec;
    }

    /** The model's arguments as the tool's input. Throws {@link IllegalArgumentException} if they do not fit. */
    I decode(String argumentsJson) {
        try {
            JsonNode args = JSON.readTree(argumentsJson == null || argumentsJson.isBlank() ? "{}" : argumentsJson);
            if (raw) {
                if (!args.isObject()) throw new IllegalArgumentException("arguments must be a JSON object");
                @SuppressWarnings("unchecked") I asIs = (I) args;
                return asIs;
            }
            if (wrapped) {
                JsonNode value = args.get("value");
                if (value == null || value.isNull()) throw new IllegalArgumentException("missing \"value\"");
                return JSON.treeToValue(value, tool.inputType());
            }
            // The schema marks every component required. Jackson would fill a missing one with null or 0
            // and the tool would run on it; the model is told instead, as it is for a missing "value".
            for (RecordComponent c : tool.inputType().getRecordComponents()) {
                JsonNode v = args.get(c.getName());
                if (v == null || v.isNull()) throw new IllegalArgumentException("missing \"" + c.getName() + "\"");
            }
            return JSON.treeToValue(args, tool.inputType());
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new IllegalArgumentException("arguments do not fit " + tool.name() + ": " + getOriginalMessageOf(e), e);
        }
    }

    /** The tool's output as text for the model. Deterministic: maps are written in key order. */
    String encode(O output) {
        if (output == null) return "null";
        if (output instanceof CharSequence s) return s.toString();
        try {
            return JSON.writeValueAsString(output);
        } catch (JsonProcessingException e) {
            return String.valueOf(output);
        }
    }

    // ── Schema ───────────────────────────────────────────────────────────────

    static ObjectNode schemaFor(String toolName, Class<?> type) {
        ObjectNode schema = JSON.createObjectNode().put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        ArrayNode required = schema.putArray("required");
        if (type.isRecord()) {
            for (RecordComponent c : type.getRecordComponents()) {
                properties.set(c.getName(), scalar(toolName, c.getName(), c.getType()));
                required.add(c.getName());
            }
        } else {
            properties.set("value", scalar(toolName, "input", type));
            required.add("value");
        }
        return schema;
    }

    private static ObjectNode scalar(String toolName, String field, Class<?> t) {
        ObjectNode p = JSON.createObjectNode();
        if (t == String.class || t == char.class || t == Character.class) {
            p.put("type", "string");
        } else if (t == int.class || t == Integer.class || t == long.class || t == Long.class
                || t == short.class || t == Short.class || t == byte.class || t == Byte.class || t == BigInteger.class) {
            p.put("type", "integer");
        } else if (t == double.class || t == Double.class || t == float.class || t == Float.class || t == BigDecimal.class) {
            p.put("type", "number");
        } else if (t == boolean.class || t == Boolean.class) {
            p.put("type", "boolean");
        } else if (t.isEnum()) {
            p.put("type", "string");
            ArrayNode values = p.putArray("enum");
            for (Object constant : t.getEnumConstants()) values.add(((Enum<?>) constant).name());
        } else {
            throw new IllegalArgumentException("Tool '" + toolName + "': " + field + " is a " + t.getName()
                    + "; agent tools take a record of strings, numbers, booleans and enums, or one such value");
        }
        return p;
    }

    private static String getOriginalMessageOf(Throwable e) {
        return e instanceof JsonProcessingException j ? j.getOriginalMessage() : e.getMessage();
    }
}
