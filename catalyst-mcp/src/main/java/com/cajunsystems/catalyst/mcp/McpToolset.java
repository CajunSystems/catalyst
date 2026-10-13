package com.cajunsystems.catalyst.mcp;

import com.cajunsystems.catalyst.agents.Agent;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The tools an MCP server offers, as Catalyst tools, ready to hand to an {@link Agent}:
 *
 * <pre>{@code
 * McpSyncClient client = McpClient.sync(transport).build();
 * client.initialize();
 * Agent agent = McpToolset.from(client).addTo(Agent.builder().instructions("...")).build();
 * }</pre>
 *
 * <p><strong>The tool list is part of the recording.</strong> Every model request carries the tools'
 * names and schemas, so they are part of the request hash. A replay or resume must offer the same
 * tools with the same schemas, or it diverges at the first model call. A server that changed its
 * tools since a run was recorded is a different agent as far as that run is concerned. Pin the
 * server version, or use {@link #only} to offer a fixed subset.
 */
public final class McpToolset {

    private final List<McpTool> tools;

    private McpToolset(List<McpTool> tools) {
        this.tools = List.copyOf(tools);
    }

    /** Every tool {@code client}'s server lists, across all pages. The client must be initialized. */
    public static McpToolset from(McpSyncClient client) {
        Objects.requireNonNull(client, "client");
        List<McpTool> out = new ArrayList<>();
        String cursor = null;
        do {
            McpSchema.ListToolsResult page = cursor == null ? client.listTools() : client.listTools(cursor);
            for (McpSchema.Tool t : page.tools()) out.add(new McpTool(client, t));
            cursor = page.nextCursor();
        } while (cursor != null && !cursor.isBlank());
        out.sort((a, b) -> a.name().compareTo(b.name())); // a stable order, whatever order the server lists in
        return new McpToolset(out);
    }

    /** Only the named tools; a name the server does not offer is refused rather than silently dropped. */
    public McpToolset only(String... names) {
        Set<String> wanted = Set.of(names);
        List<McpTool> kept = tools.stream().filter(t -> wanted.contains(t.name())).toList();
        if (kept.size() != wanted.size()) {
            List<String> offered = tools.stream().map(McpTool::name).toList();
            List<String> missing = Arrays.stream(names).filter(n -> !offered.contains(n)).toList();
            throw new IllegalArgumentException("The MCP server offers no tool named " + missing + "; it offers " + offered);
        }
        return new McpToolset(kept);
    }

    public List<McpTool> tools() {
        return tools;
    }

    /** Offers every tool to {@code builder}, each with the server's description and input schema. */
    public Agent.Builder addTo(Agent.Builder builder) {
        for (McpTool tool : tools) builder.tool(tool, tool.description(), tool.inputSchemaJson());
        return builder;
    }
}
