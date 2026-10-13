package com.cajunsystems.catalyst.mcp;

import com.cajunsystems.catalyst.Tool;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * One tool on an MCP server, as a Catalyst {@link Tool}: the input is the arguments object, the output
 * the result's text. Called through {@code ctx.call}, so every call is a recorded boundary, and
 * resume, replay, branch and eval substitute the recorded result instead of calling the server
 * again. It is deliberately not {@code @Deterministic}: a server's answer is outside data, recorded
 * once.
 *
 * <p>Text content is joined with newlines. Non-text content (an image, audio, an embedded resource)
 * is described as a placeholder such as {@code [image: image/png]}, since only text goes back to the
 * model here. A result with {@code isError: true} throws {@link McpToolError}, recorded as the call's
 * failure.
 */
public final class McpTool implements Tool<JsonNode, String> {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> ARGS = new TypeReference<>() { };

    private final McpSyncClient client;
    private final McpSchema.Tool definition;
    private final String inputSchemaJson;

    McpTool(McpSyncClient client, McpSchema.Tool definition) {
        this.client = Objects.requireNonNull(client, "client");
        this.definition = Objects.requireNonNull(definition, "definition");
        try {
            Map<String, Object> schema = definition.inputSchema();
            this.inputSchemaJson = JSON.writeValueAsString(schema == null ? Map.of("type", "object") : schema);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("MCP tool '" + definition.name() + "' has an unreadable input schema", e);
        }
    }

    @Override
    public String name() {
        return definition.name();
    }

    @Override
    public Class<JsonNode> inputType() {
        return JsonNode.class;
    }

    /** What the server says the tool does: what the model reads. */
    public String description() {
        return definition.description() == null ? "" : definition.description();
    }

    /** The server's JSON schema for the arguments. */
    public String inputSchemaJson() {
        return inputSchemaJson;
    }

    @Override
    public String apply(JsonNode arguments) {
        Map<String, Object> args = arguments == null || arguments.isNull() ? Map.of() : JSON.convertValue(arguments, ARGS);
        McpSchema.CallToolResult result = client.callTool(new McpSchema.CallToolRequest(name(), args));
        String text = textOf(result);
        if (Boolean.TRUE.equals(result.isError())) throw new McpToolError(name(), text);
        return text;
    }

    static String textOf(McpSchema.CallToolResult result) {
        StringJoiner out = new StringJoiner("\n");
        if (result.content() != null) {
            for (McpSchema.Content c : result.content()) {
                if (c instanceof McpSchema.TextContent t) out.add(t.text());
                else if (c instanceof McpSchema.ImageContent i) out.add("[image: " + i.mimeType() + "]");
                else if (c instanceof McpSchema.AudioContent a) out.add("[audio: " + a.mimeType() + "]");
                else out.add("[" + c.type() + "]");
            }
        }
        return out.toString();
    }
}
